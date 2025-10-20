"""
쿠팡 상품 상세 페이지 스크린샷 캡처 도구
CSV에서 URL을 읽어 각 상품 페이지를 캡처합니다.
"""

import sys
import os
import time
import random
import re
from datetime import datetime
import pandas as pd
from PIL import Image

# 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
coupang_dir = os.path.join(project_root, 'coupang')

# path에 추가
sys.path.insert(0, project_root)
sys.path.insert(0, coupang_dir)

from coupang_manager import BrowserManager
from progress_manager import ProgressManager

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


class ScreenshotCapturer:
    """쿠팡 상품 페이지 스크린샷 캡처"""
    
    def __init__(self, csv_path: str, screenshots_dir: str = 'screenshots'):
        """
        Args:
            csv_path: CSV 파일 경로
            screenshots_dir: 스크린샷 저장 디렉토리
        """
        self.csv_path = csv_path
        self.screenshots_dir = screenshots_dir
        self.progress_file = os.path.join(screenshots_dir, 'progress.json')
        self.error_log_file = os.path.join(screenshots_dir, 'error_log.txt')
        
        # 디렉토리 생성
        os.makedirs(screenshots_dir, exist_ok=True)
        
        # 진행 상황 관리자
        self.progress = ProgressManager(self.progress_file)
        
        # 브라우저 관리자
        self.browser = BrowserManager(headless=False)
        
        # 통계
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0
        }
    
    def sanitize_filename(self, filename: str) -> str:
        """
        파일명을 파일시스템에 안전하게 변환
        
        Args:
            filename: 원본 파일명
            
        Returns:
            안전한 파일명
        """
        # 확장자 분리
        if '.' in filename:
            name, ext = filename.rsplit('.', 1)
        else:
            name, ext = filename, 'png'
        
        # 특수문자 제거/치환
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', name)
        safe_name = re.sub(r'\s+', '_', safe_name)
        
        # 최대 길이 제한 (200자)
        if len(safe_name) > 200:
            safe_name = safe_name[:200]
        
        return f"{safe_name}.{ext}"
    
    def wait_for_page_load(self, timeout: int = 20):
        """
        페이지 로딩 대기 및 검증
        
        Args:
            timeout: 타임아웃 (초)
            
        Returns:
            bool: 정상 페이지 여부 (False면 스킵해야 함)
        """
        try:
            # 먼저 페이지가 로드될 때까지 기본 대기
            time.sleep(3)
            
            # document.readyState 확인
            ready_state = self.browser.driver.execute_script("return document.readyState")
            print(f"    📄 페이지 상태: {ready_state}")
            
            # loading 상태면 스킵
            if ready_state == 'loading':
                print(f"    ⚠️ 페이지가 로딩 중입니다. 스킵합니다.")
                return False
            
            # 페이지 제목 확인
            title = self.browser.driver.title
            print(f"    📋 페이지 제목: {title[:50]}...")
            
            # 로그인 페이지 확인
            if '로그인' in title or 'login' in title.lower():
                print(f"    ⚠️ 로그인 페이지입니다. 스킵합니다.")
                return False
            
            # 차단 페이지 확인
            if '차단' in title.lower() or 'blocked' in title.lower():
                print(f"    ⚠️ 차단 페이지입니다. 스킵합니다.")
                return False
            
            # 추가 대기 (이미지 로딩)
            time.sleep(2)
            
            return True
            
        except Exception as e:
            print(f"    ⚠️ 페이지 로딩 확인 중 오류: {e}")
            return False
    
    def check_blocked(self) -> bool:
        """차단 페이지 확인"""
        title = self.browser.driver.title.lower()
        return '차단' in title or 'blocked' in title
    
    def scroll_page(self):
        """페이지 스크롤 (상세 정보 로딩)"""
        try:
            # 페이지 높이
            total_height = self.browser.driver.execute_script(
                "return document.body.scrollHeight"
            )
            
            # 중간까지 스크롤
            scroll_position = total_height // 2
            self.browser.driver.execute_script(
                f"window.scrollTo(0, {scroll_position});"
            )
            time.sleep(0.5)
            
            # 다시 상단으로
            self.browser.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.5)
            
        except Exception as e:
            print(f"    ⚠️ 스크롤 중 오류: {e}")
    
    def capture_screenshot(self, filepath: str) -> bool:
        """
        스크린샷 캡처 및 저장 (macOS 전체 화면 - 상단 시계 포함)
        
        Args:
            filepath: 저장 경로
            
        Returns:
            성공 여부
        """
        try:
            import subprocess
            import platform
            
            # macOS screencapture 사용
            if platform.system() == 'Darwin':  # macOS
                # 전체 화면 캡처 (-x: 소리 없이, -T 0: 딜레이 없이)
                result = subprocess.run(
                    ['screencapture', '-x', '-T', '0', filepath],
                    capture_output=True,
                    text=True
                )
                
                if result.returncode != 0:
                    print(f"    ❌ screencapture 실행 실패: {result.stderr}")
                    return False
            else:
                # macOS 아닌 경우 selenium 스크린샷 사용 (백업)
                print(f"    ⚠️ macOS가 아닙니다. 브라우저 창만 캡처됩니다.")
                self.browser.driver.save_screenshot(filepath)
            
            # 파일 검증
            if os.path.exists(filepath) and os.path.getsize(filepath) > 10240:  # 10KB 이상
                # 이미지 유효성 검증
                try:
                    with Image.open(filepath) as img:
                        width, height = img.size
                        print(f"    📐 이미지 크기: {width}x{height}")
                        img.verify()
                    return True
                except Exception as img_error:
                    print(f"    ❌ 이미지 검증 실패: {img_error}")
                    if os.path.exists(filepath):
                        os.remove(filepath)
                    return False
            else:
                print(f"    ❌ 파일 크기 부족")
                return False
                
        except Exception as e:
            print(f"    ❌ 스크린샷 저장 실패: {e}")
            return False
    
    def log_error(self, row_number: int, url: str, error: str):
        """에러 로그 기록"""
        try:
            with open(self.error_log_file, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"[{timestamp}] Row {row_number}\n")
                f.write(f"URL: {url}\n")
                f.write(f"Error: {error}\n")
                f.write("-" * 80 + "\n")
        except Exception as e:
            print(f"    ⚠️ 에러 로그 기록 실패: {e}")
    
    def process_row(self, row_number: int, url: str, filename: str) -> bool:
        """
        단일 행 처리
        
        Args:
            row_number: CSV 행 번호
            url: 쿠팡 URL
            filename: 저장할 파일명
            
        Returns:
            성공 여부
        """
        print(f"\n{'='*80}")
        print(f"📸 Row {row_number}: {filename}")
        print(f"{'='*80}")
        
        # URL 유효성 검사
        if pd.isna(url) or not url or 'coupang.com' not in str(url):
            print(f"    ⚠️ 유효하지 않은 URL, 스킵")
            self.progress.update_skip()
            self.stats['skipped'] += 1
            return False
        
        # 파일명 처리
        safe_filename = self.sanitize_filename(filename) if not filename.endswith('.png') else filename
        filepath = os.path.join(self.screenshots_dir, safe_filename)
        
        # 이미 완료된 파일 확인
        if self.progress.is_completed(safe_filename):
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1024:
                print(f"    ⏭️ 이미 처리됨, 스킵")
                self.stats['skipped'] += 1
                return True
        
        try:
            # URL 접속
            print(f"    🌐 접속 중: {url[:80]}...")
            self.browser.driver.get(url)
            
            # 페이지 로딩 대기 및 검증
            page_ok = self.wait_for_page_load()
            
            # loading/로그인/차단 페이지는 스킵
            if not page_ok:
                print(f"    ⏭️ 페이지를 스킵합니다.")
                self.progress.update_skip()
                self.stats['skipped'] += 1
                return False
            
            # 페이지 스크롤
            self.scroll_page()
            
            # 스크린샷 캡처
            print(f"    📷 스크린샷 캡처 중...")
            if self.capture_screenshot(filepath):
                print(f"    ✅ 저장 완료: {safe_filename}")
                self.progress.update_success(row_number, safe_filename)
                self.stats['success'] += 1
                return True
            else:
                raise Exception("스크린샷 캡처 실패")
        
        except Exception as e:
            error_msg = str(e)
            print(f"    ❌ 실패: {error_msg}")
            
            self.log_error(row_number, url, error_msg)
            self.progress.update_failure(row_number, error_msg)
            self.stats['failed'] += 1
            
            return False
    
    def run(self, start_row: int = None, max_rows: int = None):
        """
        메인 실행
        
        Args:
            start_row: 시작 행 (None이면 진행 상황에서 결정)
            max_rows: 최대 처리 행 수 (None이면 전체)
        """
        print(f"\n{'='*80}")
        print(f"📸 쿠팡 상품 스크린샷 캡처 시작")
        print(f"{'='*80}")
        print(f"CSV: {self.csv_path}")
        print(f"저장 위치: {self.screenshots_dir}")
        print(f"{'='*80}\n")
        
        # CSV 읽기
        try:
            df = pd.read_csv(self.csv_path)
            print(f"✅ CSV 로드 완료: {len(df)}개 행")
        except Exception as e:
            print(f"❌ CSV 로드 실패: {e}")
            return
        
        # 컬럼 확인
        if '쿠팡 최저가 링크' not in df.columns or '파일명' not in df.columns:
            print(f"❌ 필수 컬럼 없음: '쿠팡 최저가 링크', '파일명'")
            return
        
        # 진행 상황 확인 및 Resume 여부 결정
        if self.progress.has_previous_progress() and start_row is None:
            self.progress.print_summary()
            
            response = input("\n이어서 진행하시겠습니까? (y/n): ").strip().lower()
            
            if response == 'y':
                start_row = self.progress.get_start_row()
                print(f"\n▶️ {start_row}행부터 재개합니다...")
            else:
                # 백업 후 초기화
                backup_file = self.progress_file.replace('.json', '_backup.json')
                self.progress.backup(backup_file)
                self.progress.reset()
                start_row = 1
                print(f"\n▶️ 1행부터 새로 시작합니다...")
        
        if start_row is None:
            start_row = 1
        
        # 브라우저 시작
        if not self.browser.start_driver():
            print("❌ 브라우저 시작 실패")
            return
        
        try:
            # 처리 범위 결정
            end_row = min(start_row + max_rows - 1, len(df)) if max_rows else len(df)
            total_to_process = end_row - start_row + 1
            
            print(f"\n처리 범위: {start_row}~{end_row}행 (총 {total_to_process}개)")
            print(f"{'='*80}\n")
            
            # 메인 루프
            for idx in range(start_row - 1, end_row):
                row = df.iloc[idx]
                row_number = idx + 1  # 1-based
                
                url = row['쿠팡 최저가 링크']
                filename = row['파일명']
                
                self.stats['total'] += 1
                
                # 행 처리
                self.process_row(row_number, url, filename)
                
                # 딜레이 (차단 방지)
                if row_number < end_row:
                    delay = random.uniform(2, 4)
                    print(f"    ⏱️ {delay:.1f}초 대기...")
                    time.sleep(delay)
            
            # 최종 요약
            self.print_final_summary()
        
        except KeyboardInterrupt:
            print(f"\n\n⚠️ 사용자 중단 (Ctrl+C)")
            self.print_final_summary()
            print(f"\n💾 진행 상황 저장됨: {self.progress_file}")
            print(f"🔄 다시 실행하면 {self.progress.get_start_row()}행부터 이어서 진행됩니다")
        
        except Exception as e:
            print(f"\n\n❌ 예상치 못한 오류: {e}")
            import traceback
            traceback.print_exc()
            self.print_final_summary()
        
        finally:
            self.browser.close()
    
    def print_final_summary(self):
        """최종 요약 출력"""
        print(f"\n\n{'='*80}")
        print(f"📊 스크린샷 캡처 완료")
        print(f"{'='*80}")
        print(f"총 처리: {self.stats['total']}개")
        print(f"✅ 성공: {self.stats['success']}개")
        print(f"❌ 실패: {self.stats['failed']}개")
        print(f"⏭️ 스킵: {self.stats['skipped']}개")
        print(f"\n📁 저장 위치: {os.path.abspath(self.screenshots_dir)}")
        
        # 실패한 행 출력
        if self.stats['failed'] > 0:
            print(f"\n❌ 실패한 행:")
            for fail_info in self.progress.progress_data['failed_rows'][-10:]:  # 최근 10개만
                if isinstance(fail_info, dict):
                    print(f"  - Row {fail_info['row']}: {fail_info.get('reason', 'Unknown')}")
            
            print(f"\n자세한 내용: {self.error_log_file}")
        
        print(f"{'='*80}")


def main():
    """메인 함수"""
    # 경로 설정
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    csv_path = os.path.join(script_dir, '쿠팡로켓배송 자료조사 - 251016.csv')
    screenshots_dir = os.path.join(script_dir, 'screenshots')
    
    # CSV 파일 존재 확인
    if not os.path.exists(csv_path):
        print(f"❌ CSV 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    # 캡처 실행
    capturer = ScreenshotCapturer(csv_path, screenshots_dir)
    
    # 옵션: 테스트용으로 처음 5개만 처리
    # capturer.run(max_rows=5)
    
    # 전체 처리
    capturer.run()


if __name__ == "__main__":
    main()