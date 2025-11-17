"""
쿠팡 제품 매칭 통합 파이프라인
- 1단계: 초기화 & 컬럼 준비
- 2단계: 제품 매칭
- 3단계: 썸네일 검증
- 4단계: 최종 판정
"""

import sys
import os
import argparse

# 현재 디렉토리를 sys.path에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# config.py 로드
try:
    import importlib.util
    config_path = os.path.join(current_dir, 'config.py')
    spec = importlib.util.spec_from_file_location("matcher_config", config_path)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
except Exception as e:
    print(f"✗ config.py 로드 실패: {e}")
    sys.exit(1)

# 모듈 import
from product_matcher import ProductMatchingSystem
from gemini_validator import ThumbnailValidationSystem
from utils import ColumnManager, ResultSaver, StageManager

# BrowserManager import
try:
    project_root = os.path.dirname(os.path.dirname(current_dir))  # iherb_price 디렉토리
    sys.path.insert(1, project_root)
    from coupang.coupang_manager import BrowserManager
    print("✓ BrowserManager 로드 성공")
except ImportError as e:
    print(f"✗ BrowserManager 로드 실패: {e}")
    print(f"프로젝트 루트: {project_root}")
    print("coupang/coupang_manager.py 경로를 확인하세요")
    sys.exit(1)


class CompletePipeline:
    """전체 파이프라인 통합 관리"""
    
    def __init__(
        self,
        csv_path: str,
        output_path: str,
        gemini_api_key: str,
        browser_manager=None
    ):
        """
        Args:
            csv_path: 입력 CSV 경로
            output_path: 출력 CSV 경로
            gemini_api_key: Gemini API 키
            browser_manager: BrowserManager 인스턴스
        """
        self.csv_path = csv_path
        self.output_path = output_path
        self.gemini_api_key = gemini_api_key
        self.browser = browser_manager
        
        # 시스템 초기화
        self.matcher = ProductMatchingSystem(csv_path, browser_manager, output_path)
        self.thumbnail_validator = ThumbnailValidationSystem(gemini_api_key, browser_manager)
    
    def run_all(self, start_idx: int = 0, end_idx: int = None):
        """
        전체 파이프라인 실행
        
        Args:
            start_idx: 시작 인덱스
            end_idx: 종료 인덱스 (None이면 끝까지)
        """
        print("\n" + "="*60)
        print("쿠팡 제품 매칭 통합 파이프라인")
        print("="*60)
        print(f"입력: {self.csv_path}")
        print(f"출력: {self.output_path}")
        print("="*60 + "\n")
        
        try:
            # 1단계: 초기화
            if not self.matcher.initialize():
                return
            
            # 2단계: 제품 매칭
            self.matcher.process_matching(start_idx, end_idx)
            
            # 3단계: 썸네일 검증
            self.thumbnail_validator.validate_csv(
                input_csv=self.output_path,
                output_csv=self.output_path,
                delay_seconds=3.0,
                skip_existing=True
            )
            
            # 4단계: 최종 판정
            self.calculate_final_judgement()
            
            print("\n" + "="*60)
            print("✓ 전체 파이프라인 완료!")
            print(f"결과 파일: {self.output_path}")
            print("="*60 + "\n")
            
        except KeyboardInterrupt:
            print("\n\n⚠ 사용자에 의해 중단되었습니다")
            print(f"현재까지 결과가 {self.output_path}에 저장되었습니다")
            print("다시 실행하면 이어서 진행됩니다")
        except Exception as e:
            print(f"\n✗ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
    
    def run_stage(self, stage: str, start_idx: int = 0, end_idx: int = None):
        """
        특정 단계만 실행
        
        Args:
            stage: 'matching', 'thumbnail', 'final'
            start_idx: 시작 인덱스
            end_idx: 종료 인덱스
        """
        print(f"\n단계별 실행 모드: {stage}")
        
        if stage == 'matching':
            if not self.matcher.initialize():
                return
            self.matcher.process_matching(start_idx, end_idx)
        
        elif stage == 'thumbnail':
            self.thumbnail_validator.validate_csv(
                input_csv=self.output_path,
                output_csv=self.output_path,
                delay_seconds=3.0,
                skip_existing=True
            )
        
        elif stage == 'final':
            self.calculate_final_judgement()
        
        else:
            print(f"✗ 알 수 없는 단계: {stage}")
            print("사용 가능한 단계: matching, thumbnail, final")
    
    def calculate_final_judgement(self):
        """
        4단계: 최종 판정 계산
        """
        StageManager.print_stage_header('final')
        
        import pandas as pd
        
        # CSV 로드
        df = pd.read_csv(self.output_path, encoding='utf-8-sig')
        print(f"✓ 데이터 로드: {len(df)}개 행\n")
        
        # 최종 판정 계산
        df = ColumnManager.calculate_final_confidence(df)
        
        # 저장
        df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        print(f"✓ 최종 판정 완료: {self.output_path}\n")
        
        # 통계 출력
        self.print_final_statistics(df)
    
    def print_final_statistics(self, df):
        """최종 통계 출력"""
        print("="*60)
        print("최종 통계")
        print("="*60)
        
        # 전체 통계
        total = len(df)
        matched = len(df[df['매칭제품명'].notna()])
        print(f"전체 제품: {total}개")
        print(f"매칭 완료: {matched}개 ({matched/total*100:.1f}%)")
        
        # 최종 신뢰도 분포
        if '최종_신뢰도' in df.columns:
            final_counts = df['최종_신뢰도'].value_counts()
            print(f"\n[최종 신뢰도 분포]")
            for level, count in final_counts.items():
                print(f"  {level}: {count}개 ({count/total*100:.1f}%)")
        
        # 매칭 분석
        if '정수일치' in df.columns:
            count_match = len(df[df['정수일치'] == True])
            print(f"\n[매칭 분석]")
            print(f"  정수 일치: {count_match}개")
        
        if '브랜드일치' in df.columns:
            brand_match = len(df[df['브랜드일치'] == True])
            print(f"  브랜드 일치: {brand_match}개")
        
        # 썸네일 검증
        if '썸네일_일치여부' in df.columns:
            thumbnail_checked = len(df[df['썸네일_일치여부'].notna()])
            thumbnail_match = len(df[df['썸네일_일치여부'] == True])
            print(f"\n[썸네일 검증]")
            print(f"  검증 완료: {thumbnail_checked}개")
            if thumbnail_checked > 0:
                print(f"  일치: {thumbnail_match}개 ({thumbnail_match/thumbnail_checked*100:.1f}%)")
        
        print("="*60 + "\n")


def main():
    """메인 실행 함수"""
    
    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(description='쿠팡 제품 매칭 통합 파이프라인')
    parser.add_argument('--csv', type=str, 
                       default="/Users/brich/Desktop/iherb_price/coupang2/251111/inputs/iHerb_쿠팡_추가_가격조사_대조결과_20251112.csv",
                       help='입력 CSV 파일 경로')
    parser.add_argument('--output', type=str,
                       default=None,
                       help='출력 CSV 파일 경로 (기본: outputs/matching_results.csv)')
    parser.add_argument('--stage', type=str, choices=['matching', 'thumbnail', 'final', 'all'],
                       default='all',
                       help='실행할 단계 (기본: all)')
    parser.add_argument('--start', type=int, default=0,
                       help='시작 인덱스 (기본: 0)')
    parser.add_argument('--end', type=int, default=None,
                       help='종료 인덱스 (기본: 끝까지)')
    parser.add_argument('--headless', action='store_true',
                       help='헤드리스 모드로 실행')
    
    args = parser.parse_args()
    
    # 설정
    CSV_PATH = args.csv
    
    if args.output:
        OUTPUT_PATH = args.output
    else:
        output_dir = os.path.join(current_dir, "outputs")
        os.makedirs(output_dir, exist_ok=True)
        OUTPUT_PATH = os.path.join(output_dir, "matching_results.csv")
    
    GEMINI_API_KEY = "AIzaSyC9m-6vYIRXBQLSctElXTCQfPdTzfV2Ck8"
    
    # CSV 파일 존재 확인
    if not os.path.exists(CSV_PATH):
        print(f"✗ CSV 파일을 찾을 수 없습니다: {CSV_PATH}")
        return
    
    # 브라우저 시작 (썸네일 검증 또는 매칭 단계인 경우에만)
    browser = None
    if args.stage in ['matching', 'thumbnail', 'all']:
        print("\n브라우저 시작 중...")
        browser = BrowserManager(headless=args.headless)
        
        if not browser.start_driver():
            print("✗ 브라우저 시작 실패")
            return
    
    try:
        # 파이프라인 초기화
        pipeline = CompletePipeline(
            csv_path=CSV_PATH,
            output_path=OUTPUT_PATH,
            gemini_api_key=GEMINI_API_KEY,
            browser_manager=browser
        )
        
        # 실행
        if args.stage == 'all':
            pipeline.run_all(args.start, args.end)
        else:
            pipeline.run_stage(args.stage, args.start, args.end)
        
    finally:
        # 브라우저 종료
        if browser:
            print("\n브라우저 종료 중...")
            browser.close()


if __name__ == "__main__":
    main()