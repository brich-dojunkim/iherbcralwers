"""
공통 유틸리티 모듈
- 컬럼 관리
- 실시간 저장
- 진행 상황 추적
"""

import pandas as pd
import os
from typing import Set, Optional
from datetime import datetime


class ColumnManager:
    """CSV 컬럼 구조 관리"""
    
    # 전체 컬럼 정의
    ALL_COLUMNS = [
        # 원본 정보
        'Part Number',
        '원본제품명',
        
        # 매칭 결과
        '매칭제품명',
        '매칭URL',
        '가격',
        '판매유형',
        
        # 매칭 분석 - 여부
        '정수개수일치',
        '브랜드일치',
        '썸네일_일치여부',
        
        # 매칭 분석 - 상세
        '정수개수일치_상세',
        '브랜드일치_상세',
        '썸네일_신뢰도',
        '아이허브_썸네일URL',
        '쿠팡_썸네일URL',
        
        # 최종 판정
        '최종_신뢰도',
        
        # 참고용 (숨김 가능)
        '썸네일_판단근거',
        '최종_사유',
        '사유',
    ]
    
    @staticmethod
    def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        필요한 모든 컬럼이 존재하는지 확인하고 없으면 추가
        
        Args:
            df: 원본 DataFrame
        
        Returns:
            컬럼이 추가된 DataFrame
        """
        for col in ColumnManager.ALL_COLUMNS:
            if col not in df.columns:
                df[col] = None
        
        return df
    
    @staticmethod
    def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        컬럼 순서를 ALL_COLUMNS 순서대로 재정렬
        
        Args:
            df: DataFrame
        
        Returns:
            컬럼 순서가 정렬된 DataFrame
        """
        # ALL_COLUMNS에 있는 컬럼만 선택 (순서대로)
        existing_cols = [col for col in ColumnManager.ALL_COLUMNS if col in df.columns]
        
        # ALL_COLUMNS에 없는 추가 컬럼 (있다면 뒤에 추가)
        extra_cols = [col for col in df.columns if col not in ColumnManager.ALL_COLUMNS]
        
        # 최종 컬럼 순서
        ordered_cols = existing_cols + extra_cols
        
        return df[ordered_cols]
    
    @staticmethod
    def parse_reason_column(df: pd.DataFrame) -> pd.DataFrame:
        """
        '사유' 컬럼을 파싱하여 4개 컬럼으로 분리:
        - 정수개수일치: True/False
        - 브랜드일치: True/False  
        - 정수개수일치_상세: "240정 x 1개"
        - 브랜드일치_상세: "닥터스베스트"
        
        Args:
            df: DataFrame
        
        Returns:
            파싱된 DataFrame
        """
        # 사유 컬럼이 없으면 스킵
        if '사유' not in df.columns:
            return df
        
        import re
        
        for idx, row in df.iterrows():
            # 이미 파싱된 경우 스킵
            if pd.notna(row.get('정수개수일치')) and row.get('정수개수일치') != '':
                continue
            
            reason = str(row.get('사유', ''))
            
            # 정수·개수 일치 파싱
            count_pattern = r'정수[^(]*\(([^)]+)\)'
            count_match = re.search(count_pattern, reason)
            if count_match:
                df.at[idx, '정수개수일치'] = True
                df.at[idx, '정수개수일치_상세'] = count_match.group(1)
            else:
                df.at[idx, '정수개수일치'] = False
                df.at[idx, '정수개수일치_상세'] = ''
            
            # 브랜드 일치 파싱
            brand_pattern = r'브랜드[^(]*\(([^)]+)\)'
            brand_match = re.search(brand_pattern, reason)
            if brand_match:
                df.at[idx, '브랜드일치'] = True
                df.at[idx, '브랜드일치_상세'] = brand_match.group(1)
            else:
                df.at[idx, '브랜드일치'] = False
                df.at[idx, '브랜드일치_상세'] = ''
        
        # 컬럼 순서 정렬
        df = ColumnManager.reorder_columns(df)
        
        return df
    
    @staticmethod
    def calculate_final_confidence(df: pd.DataFrame) -> pd.DataFrame:
        """
        최종 신뢰도 계산
        
        기준:
        - 확신: 정수+개수+브랜드+썸네일 모두 일치
        - 검토필요: 일부만 일치
        - 불일치: 매칭 실패 또는 검증 실패
        
        Args:
            df: DataFrame
        
        Returns:
            최종 판정이 추가된 DataFrame
        """
        for idx, row in df.iterrows():
            # 이미 계산된 경우 스킵
            if pd.notna(row.get('최종_신뢰도')):
                continue
            
            # 매칭이 안 된 경우
            if pd.isna(row.get('매칭제품명')) or row.get('매칭제품명') == '':
                df.at[idx, '최종_신뢰도'] = '불일치'
                df.at[idx, '최종_사유'] = '매칭 실패'
                continue
            
            # 각 검증 항목 확인
            count_match = row.get('정수개수일치', False)
            brand_match = row.get('브랜드일치', False)
            thumbnail_match = row.get('썸네일_일치여부', False)
            
            # 썸네일 검증이 완료되지 않은 경우
            if pd.isna(thumbnail_match):
                df.at[idx, '최종_신뢰도'] = '검토필요'
                df.at[idx, '최종_사유'] = '썸네일 검증 대기 중'
                continue
            
            # 모든 항목 일치
            if count_match and brand_match and thumbnail_match:
                df.at[idx, '최종_신뢰도'] = '확신'
                df.at[idx, '최종_사유'] = '모든 검증 항목 일치'
            # 썸네일 불일치
            elif not thumbnail_match:
                df.at[idx, '최종_신뢰도'] = '불일치'
                df.at[idx, '최종_사유'] = '썸네일 불일치'
            # 브랜드 불일치
            elif not brand_match:
                df.at[idx, '최종_신뢰도'] = '검토필요'
                df.at[idx, '최종_사유'] = '브랜드 불일치'
            # 정수/개수 불일치
            elif not (count_match):
                df.at[idx, '최종_신뢰도'] = '불일치'
                df.at[idx, '최종_사유'] = '정수/개수 불일치'
            else:
                df.at[idx, '최종_신뢰도'] = '검토필요'
                df.at[idx, '최종_사유'] = '일부 항목 불일치'
        
        # 컬럼 순서 정렬
        df = ColumnManager.reorder_columns(df)
        
        return df


class ResultSaver:
    """실시간 저장 관리"""
    
    def __init__(self, output_path: str):
        """
        Args:
            output_path: 저장할 CSV 파일 경로
        """
        self.output_path = output_path
        self.save_count = 0
    
    def save(self, df: pd.DataFrame, message: str = "저장 완료"):
        """
        DataFrame을 CSV로 저장
        
        Args:
            df: 저장할 DataFrame
            message: 저장 메시지
        """
        try:
            df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
            self.save_count += 1
            print(f"  💾 {message} (저장 횟수: {self.save_count})")
        except Exception as e:
            print(f"  ✗ 저장 실패: {e}")
    
    def get_processed_ids(
        self, 
        df: pd.DataFrame, 
        stage: str
    ) -> Set[str]:
        """
        각 단계별로 처리 완료된 Part Number 추출
        
        Args:
            df: DataFrame
            stage: 'matching', 'thumbnail', 'final'
        
        Returns:
            처리 완료된 Part Number 집합
        """
        if stage == 'matching':
            # 매칭제품명이 있으면 매칭 완료
            return set(
                df[df['매칭제품명'].notna()]['Part Number'].astype(str)
            )
        elif stage == 'thumbnail':
            # 썸네일_일치여부가 있으면 썸네일 검증 완료
            return set(
                df[df['썸네일_일치여부'].notna()]['Part Number'].astype(str)
            )
        elif stage == 'final':
            # 최종_신뢰도가 있으면 최종 판정 완료
            return set(
                df[df['최종_신뢰도'].notna()]['Part Number'].astype(str)
            )
        else:
            return set()


class ProgressTracker:
    """진행 상황 추적 및 출력"""
    
    def __init__(self, total: int, stage_name: str):
        """
        Args:
            total: 전체 작업 수
            stage_name: 단계 이름
        """
        self.total = total
        self.stage_name = stage_name
        self.current = 0
        self.success = 0
        self.fail = 0
        self.start_time = datetime.now()
    
    def update(self, success: bool = True):
        """
        진행 상황 업데이트
        
        Args:
            success: 성공 여부
        """
        self.current += 1
        if success:
            self.success += 1
        else:
            self.fail += 1
    
    def print_progress(self, part_number: str):
        """
        현재 진행 상황 출력
        
        Args:
            part_number: 현재 처리 중인 Part Number
        """
        percentage = (self.current / self.total * 100) if self.total > 0 else 0
        print(f"\n[{self.current}/{self.total}] ({percentage:.1f}%) {part_number}")
    
    def print_summary(self):
        """
        최종 요약 출력
        """
        elapsed = datetime.now() - self.start_time
        
        print(f"\n{'='*60}")
        print(f"{self.stage_name} 완료!")
        print(f"{'='*60}")
        print(f"전체: {self.total}개")
        print(f"성공: {self.success}개 ({self.success/self.total*100:.1f}%)")
        print(f"실패: {self.fail}개 ({self.fail/self.total*100:.1f}%)")
        print(f"소요 시간: {elapsed}")
        print(f"{'='*60}\n")


class StageManager:
    """파이프라인 단계 관리"""
    
    STAGES = ['matching', 'thumbnail', 'final']
    
    @staticmethod
    def get_next_stage(current_stage: Optional[str] = None) -> Optional[str]:
        """
        다음 단계 반환
        
        Args:
            current_stage: 현재 단계
        
        Returns:
            다음 단계 이름 또는 None
        """
        if current_stage is None:
            return StageManager.STAGES[0]
        
        try:
            idx = StageManager.STAGES.index(current_stage)
            if idx + 1 < len(StageManager.STAGES):
                return StageManager.STAGES[idx + 1]
        except ValueError:
            pass
        
        return None
    
    @staticmethod
    def print_stage_header(stage: str):
        """
        단계 시작 헤더 출력
        
        Args:
            stage: 단계 이름
        """
        stage_names = {
            'matching': '제품 매칭',
            'thumbnail': '썸네일 검증',
            'final': '최종 판정'
        }
        
        name = stage_names.get(stage, stage)
        
        print(f"\n{'='*60}")
        print(f"{name} 단계 시작")
        print(f"{'='*60}\n")