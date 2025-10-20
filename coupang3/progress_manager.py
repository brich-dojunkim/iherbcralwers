"""
진행 상황 관리 모듈
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional


class ProgressManager:
    """스크린샷 캡처 진행 상황 관리"""
    
    def __init__(self, progress_file: str):
        """
        Args:
            progress_file: 진행 상황 파일 경로
        """
        self.progress_file = progress_file
        self.progress_data = self._load_or_initialize()
    
    def _load_or_initialize(self) -> Dict:
        """진행 상황 로드 또는 초기화"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 진행 상황 파일 로드 실패: {e}")
                return self._create_initial_progress()
        else:
            return self._create_initial_progress()
    
    def _create_initial_progress(self) -> Dict:
        """초기 진행 상황 생성"""
        return {
            'last_processed_row': 0,
            'completed_files': [],
            'failed_rows': [],
            'total_success': 0,
            'total_failed': 0,
            'total_skipped': 0,
            'started_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat()
        }
    
    def save(self):
        """진행 상황 저장 (원자적 쓰기)"""
        try:
            # 임시 파일에 먼저 저장
            temp_file = self.progress_file + '.tmp'
            
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, indent=2, ensure_ascii=False)
            
            # 성공하면 원본으로 rename
            os.replace(temp_file, self.progress_file)
            
        except Exception as e:
            print(f"⚠️ 진행 상황 저장 실패: {e}")
    
    def update_success(self, row_number: int, filename: str):
        """성공 업데이트"""
        self.progress_data['last_processed_row'] = row_number
        self.progress_data['completed_files'].append(filename)
        self.progress_data['total_success'] += 1
        self.progress_data['last_updated'] = datetime.now().isoformat()
        self.save()
    
    def update_failure(self, row_number: int, reason: str = ''):
        """실패 업데이트"""
        self.progress_data['last_processed_row'] = row_number
        
        failure_info = {
            'row': row_number,
            'reason': reason,
            'timestamp': datetime.now().isoformat()
        }
        
        # failed_rows가 리스트면 딕셔너리로 변환
        if isinstance(self.progress_data.get('failed_rows'), list):
            if not self.progress_data['failed_rows'] or isinstance(self.progress_data['failed_rows'][0], int):
                self.progress_data['failed_rows'] = []
        
        self.progress_data['failed_rows'].append(failure_info)
        self.progress_data['total_failed'] += 1
        self.progress_data['last_updated'] = datetime.now().isoformat()
        self.save()
    
    def update_skip(self):
        """스킵 업데이트"""
        self.progress_data['total_skipped'] += 1
        self.progress_data['last_updated'] = datetime.now().isoformat()
        self.save()
    
    def is_completed(self, filename: str) -> bool:
        """파일이 이미 완료되었는지 확인"""
        return filename in self.progress_data['completed_files']
    
    def get_start_row(self) -> int:
        """시작 행 번호 반환"""
        return self.progress_data['last_processed_row'] + 1
    
    def has_previous_progress(self) -> bool:
        """이전 진행 상황이 있는지 확인"""
        return self.progress_data['last_processed_row'] > 0
    
    def print_summary(self):
        """진행 상황 요약 출력"""
        print(f"\n📊 기존 진행 상황 발견")
        print(f"{'='*60}")
        print(f"마지막 처리: {self.progress_data['last_processed_row']}행")
        print(f"✅ 성공: {self.progress_data['total_success']}개")
        print(f"❌ 실패: {self.progress_data['total_failed']}개")
        print(f"⏭️ 스킵: {self.progress_data['total_skipped']}개")
        print(f"{'='*60}")
    
    def backup(self, backup_file: str):
        """백업 생성"""
        try:
            import shutil
            shutil.copy2(self.progress_file, backup_file)
            print(f"💾 진행 상황 백업: {backup_file}")
        except Exception as e:
            print(f"⚠️ 백업 실패: {e}")
    
    def reset(self):
        """진행 상황 초기화"""
        self.progress_data = self._create_initial_progress()
        self.save()
        print("🔄 진행 상황이 초기화되었습니다")
    
    def get_stats(self) -> Dict:
        """통계 반환"""
        return {
            'total_processed': self.progress_data['last_processed_row'],
            'success': self.progress_data['total_success'],
            'failed': self.progress_data['total_failed'],
            'skipped': self.progress_data['total_skipped']
        }