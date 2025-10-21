"""
로켓직구(Rocket Global) 상품 확인 모듈
CSV의 상품 링크들을 방문하여 로켓직구 여부를 체크합니다.
"""

import sys
import os
import time
import random
from datetime import datetime
import pandas as pd
from typing import Dict, Optional, Tuple

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
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class RocketGlobalChecker:
    """로켓직구 상품 확인 클래스"""
    
    # 로켓직구 판별 셀렉터
    ROCKET_GLOBAL_SELECTORS = [
        'img[src*="global_b.png"]',
        'img[src*="global_b/global_b.png"]',
        'img[alt*="로켓직구"]',
        'img[alt*="rocket"]',
        'img[alt*="Rocket"]',
        '.price-badge img[src*="global"]',
        'div.delivery-badge img[src*="global"]',
        'div.price-badge img[src*="global"]',
        'div[class*="badge"] img[src*="global"]',
        # 추가 셀렉터 (Tailwind 클래스 기반)
        'div.price-container img[src*="global"]',
        'div[class*="price"] img[src*="global"]'
    ]
    
    def __init__(self, csv_path: str, output_dir: str = 'results'):
        """
        Args:
            csv_path: 입력 CSV 파일 경로
            output_dir: 결과 저장 디렉토리
        """
        self.csv_path = csv_path
        self.output_dir = output_dir
        
        # 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)
        
        # 파일 경로들
        self.progress_file = os.path.join(output_dir, 'progress.json')
        self.error_log_file = os.path.join(output_dir, 'error_log.txt')
        self.output_csv = os.path.join(output_dir, 'wing_rocket_global_results.csv')
        
        # 진행 상황 관리
        self.progress = ProgressManager(self.progress_file)
        
        # 브라우저 관리
        self.browser = BrowserManager(headless=False)
        
        # 통계
        self.stats = {
            'total': 0,
            'checked': 0,
            'rocket_global': 0,
            'not_rocket_global': 0,
            'failed': 0,
            'skipped': 0
        }
        
        # 결과 데이터
        self.results = []
    
    def load_csv(self) -> pd.DataFrame:
        """CSV 파일 로드"""
        try:
            df = pd.read_csv(self.csv_path)
            print(f"✅ CSV 로드 완료: {len(df)}개 행")
            
            # 필수 컬럼 확인
            if 'product_link' not in df.columns:
                raise ValueError("'product_link' 컬럼이 없습니다")
            
            return df
        except Exception as e:
            print(f"❌ CSV 로드 실패: {e}")
            raise
    
    def wait_for_page_load(self, timeout: int = 20) -> Tuple[bool, str]:
        """
        페이지 로딩 대기 및 상태 확인
        
        Returns:
            (성공 여부, 페이지 상태 메시지)
        """
        try:
            # 기본 대기
            time.sleep(3)
            
            # document ready 상태 확인
            ready_state = self.browser.driver.execute_script("return document.readyState")
            
            if ready_state == 'loading':
                # 추가 대기
                WebDriverWait(self.browser.driver, 10).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
            
            # 페이지 제목 확인
            title = self.browser.driver.title
            
            # 차단/로그인 페이지 확인
            if '차단' in title or 'blocked' in title.lower():
                return False, "차단 페이지"
            
            if '로그인' in title or 'login' in title.lower():
                return False, "로그인 페이지"
            
            if '찾을 수 없' in title or 'not found' in title.lower():
                return False, "상품 없음"
            
            # 상품 페이지 요소 확인 (가격 정보가 있는지)
            try:
                WebDriverWait(self.browser.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 
                        'div.price-container, div[class*="price"], span[class*="price"]'))
                )
            except TimeoutException:
                # 가격 정보가 없어도 계속 진행 (품절 등)
                pass
            
            return True, "정상"
            
        except Exception as e:
            return False, f"로딩 오류: {str(e)}"
    
    def check_rocket_global(self) -> Tuple[bool, Optional[str]]:
        """
        로켓직구 여부 확인
        
        Returns:
            (로켓직구 여부, 발견된 요소 정보)
        """
        try:
            # 모든 셀렉터로 시도
            for selector in self.ROCKET_GLOBAL_SELECTORS:
                try:
                    elements = self.browser.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements:
                        # 요소가 표시되는지 확인
                        if element.is_displayed():
                            # src 속성 확인
                            src = element.get_attribute('src') or ''
                            alt = element.get_attribute('alt') or ''
                            
                            # 로켓직구 키워드 확인
                            if ('global' in src.lower() or 
                                'rocket' in alt.lower() or 
                                '로켓직구' in alt or
                                'global_b' in src):
                                
                                return True, f"Found: {selector} (src={src[:50]}, alt={alt})"
                
                except Exception:
                    continue
            
            # XPath로 추가 확인 (텍스트 기반)
            try:
                xpath_checks = [
                    "//*[contains(text(), '로켓직구')]",
                    "//*[contains(@class, 'rocket')]",
                    "//*[contains(@class, 'global')]"
                ]
                
                for xpath in xpath_checks:
                    elements = self.browser.driver.find_elements(By.XPATH, xpath)
                    if elements:
                        for element in elements:
                            if element.is_displayed():
                                text = element.text or element.get_attribute('class') or ''
                                if '로켓직구' in text or 'rocket' in text.lower():
                                    return True, f"Found by XPath: {text[:50]}"
            except:
                pass
            
            return False, None
            
        except Exception as e:
            print(f"    ⚠️ 체크 중 오류: {e}")
            return False, None
    
    def extract_product_info(self) -> Dict:
        """상품 페이지 정보 추출"""
        info = {
            'page_title': '',
            'product_name': '',
            'price': ''
        }
        
        try:
            # 페이지 제목
            info['page_title'] = self.browser.driver.title[:100]
            
            # 상품명 추출 시도
            name_selectors = [
                'h1.prod-buy-header__title',
                'h2.prod-buy-header__title',
                'div.prod-buy-header__title',
                'h1[class*="title"]',
                'h2[class*="title"]'
            ]
            
            for selector in name_selectors:
                try:
                    element = self.browser.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text:
                        info['product_name'] = element.text[:100]
                        break
                except:
                    continue
            
            # 가격 정보 추출
            price_selectors = [
                'span.total-price strong',
                'span.price',
                'div.price-amount',
                'strong.total-price'
            ]
            
            for selector in price_selectors:
                try:
                    element = self.browser.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text:
                        info['price'] = element.text[:50]
                        break
                except:
                    continue
            
        except Exception as e:
            print(f"    ⚠️ 정보 추출 중 오류: {e}")
        
        return info
    
    def log_error(self, row_data: Dict, error: str):
        """에러 로그 기록"""
        try:
            with open(self.error_log_file, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"[{timestamp}]\n")
                f.write(f"Option ID: {row_data.get('option_id', 'N/A')}\n")
                f.write(f"Vendor Item ID: {row_data.get('vendor_item_id', 'N/A')}\n")
                f.write(f"URL: {row_data.get('product_link', 'N/A')}\n")
                f.write(f"Error: {error}\n")
                f.write("-" * 80 + "\n")
        except Exception as e:
            print(f"    ⚠️ 에러 로그 기록 실패: {e}")
    
    def process_row(self, row_index: int, row_data: pd.Series) -> Dict:
        """
        단일 행 처리
        
        Returns:
            처리 결과 딕셔너리
        """
        result = {
            'option_id': row_data.get('option_id', ''),
            'vendor_item_id': row_data.get('vendor_item_id', ''),
            'product_link': row_data.get('product_link', ''),
            'seller_name': row_data.get('seller_name', ''),
            'collected_at': row_data.get('collected_at', ''),
            'is_rocket_global': None,
            'checked_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'page_title': '',
            'product_name': '',
            'price': '',
            'error_message': ''
        }
        
        url = row_data.get('product_link', '')
        
        print(f"\n{'='*80}")
        print(f"🔍 Row {row_index + 1}: Option ID {result['option_id']}, Vendor ID {result['vendor_item_id']}")
        print(f"{'='*80}")
        
        # URL 유효성 검사
        if pd.isna(url) or not url or 'coupang.com' not in str(url):
            print(f"    ⚠️ 유효하지 않은 URL, 스킵")
            result['error_message'] = "Invalid URL"
            self.stats['skipped'] += 1
            return result
        
        try:
            # URL 접속
            print(f"    🌐 접속 중: {url[:80]}...")
            self.browser.driver.get(url)
            
            # 페이지 로딩 대기
            page_loaded, status_msg = self.wait_for_page_load()
            
            if not page_loaded:
                print(f"    ⚠️ 페이지 로딩 실패: {status_msg}")
                result['error_message'] = status_msg
                self.stats['failed'] += 1
                self.log_error(row_data.to_dict(), status_msg)
                return result
            
            # 상품 정보 추출
            product_info = self.extract_product_info()
            result.update(product_info)
            
            # 로켓직구 체크
            print(f"    🔍 로켓직구 확인 중...")
            is_rocket, found_info = self.check_rocket_global()
            
            result['is_rocket_global'] = is_rocket
            
            if is_rocket:
                print(f"    ✅ 로켓직구 상품입니다! ({found_info})")
                self.stats['rocket_global'] += 1
            else:
                print(f"    ❌ 로켓직구 상품이 아닙니다")
                self.stats['not_rocket_global'] += 1
            
            self.stats['checked'] += 1
            
            # 진행 상황 업데이트
            self.progress.update_success(row_index + 1, f"row_{row_index + 1}")
            
        except Exception as e:
            error_msg = str(e)
            print(f"    ❌ 처리 중 오류: {error_msg}")
            result['error_message'] = error_msg
            self.stats['failed'] += 1
            self.log_error(row_data.to_dict(), error_msg)
            self.progress.update_failure(row_index + 1, error_msg)
        
        return result
    
    def save_results(self):
        """결과를 CSV로 저장"""
        try:
            if self.results:
                df_results = pd.DataFrame(self.results)
                df_results.to_csv(self.output_csv, index=False, encoding='utf-8-sig')
                print(f"\n💾 결과 저장 완료: {self.output_csv}")
                return True
        except Exception as e:
            print(f"\n❌ 결과 저장 실패: {e}")
            return False
    
    def print_summary(self):
        """최종 요약 출력"""
        print(f"\n{'='*80}")
        print(f"📊 로켓직구 확인 완료")
        print(f"{'='*80}")
        print(f"총 처리: {self.stats['total']}개")
        print(f"✅ 확인 완료: {self.stats['checked']}개")
        print(f"  - 🚀 로켓직구: {self.stats['rocket_global']}개")
        print(f"  - 📦 일반 상품: {self.stats['not_rocket_global']}개")
        print(f"❌ 실패: {self.stats['failed']}개")
        print(f"⏭️ 스킵: {self.stats['skipped']}개")
        
        if self.stats['rocket_global'] > 0:
            percentage = (self.stats['rocket_global'] / max(self.stats['checked'], 1)) * 100
            print(f"\n🚀 로켓직구 비율: {percentage:.1f}%")
        
        print(f"\n📁 결과 파일: {os.path.abspath(self.output_csv)}")
        
        if self.stats['failed'] > 0:
            print(f"📝 에러 로그: {os.path.abspath(self.error_log_file)}")
        
        print(f"{'='*80}")
    
    def run(self, start_row: int = None, max_rows: int = None):
        """
        메인 실행 함수
        
        Args:
            start_row: 시작 행 번호 (1-based)
            max_rows: 최대 처리 행 수
        """
        print(f"\n{'='*80}")
        print(f"🚀 로켓직구 상품 확인 시작")
        print(f"{'='*80}")
        print(f"입력 CSV: {self.csv_path}")
        print(f"출력 디렉토리: {self.output_dir}")
        print(f"{'='*80}\n")
        
        # CSV 로드
        try:
            df = self.load_csv()
        except Exception as e:
            print(f"❌ 프로그램 종료: {e}")
            return
        
        # 진행 상황 확인
        if self.progress.has_previous_progress() and start_row is None:
            self.progress.print_summary()
            
            response = input("\n이어서 진행하시겠습니까? (y/n): ").strip().lower()
            
            if response == 'y':
                start_row = self.progress.get_start_row()
                print(f"\n▶️ {start_row}행부터 재개합니다...")
                
                # 이전 결과 로드
                if os.path.exists(self.output_csv):
                    try:
                        prev_results = pd.read_csv(self.output_csv)
                        self.results = prev_results.to_dict('records')
                        print(f"📂 기존 결과 {len(self.results)}개 로드")
                    except:
                        pass
            else:
                # 백업 후 초기화
                if os.path.exists(self.output_csv):
                    backup_file = self.output_csv.replace('.csv', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
                    import shutil
                    shutil.copy2(self.output_csv, backup_file)
                    print(f"💾 기존 결과 백업: {backup_file}")
                
                self.progress.reset()
                start_row = 1
                self.results = []
                print(f"\n▶️ 처음부터 새로 시작합니다...")
        
        if start_row is None:
            start_row = 1
        
        # 브라우저 시작
        if not self.browser.start_driver():
            print("❌ 브라우저 시작 실패")
            return
        
        try:
            # 처리 범위 결정
            start_idx = start_row - 1  # 0-based index
            end_idx = min(start_idx + max_rows, len(df)) if max_rows else len(df)
            
            print(f"\n처리 범위: {start_row}~{end_idx}행 (총 {end_idx - start_idx}개)")
            print(f"{'='*80}\n")
            
            # 메인 루프
            for idx in range(start_idx, end_idx):
                self.stats['total'] += 1
                
                # 행 처리
                result = self.process_row(idx, df.iloc[idx])
                
                # 새로운 결과만 추가 (재개 시 중복 방지)
                if idx >= len(self.results):
                    self.results.append(result)
                
                # 중간 저장 (10개마다)
                if (idx + 1) % 10 == 0:
                    self.save_results()
                    print(f"    💾 중간 저장 완료 ({len(self.results)}개)")
                
                # 딜레이 (차단 방지)
                if idx < end_idx - 1:
                    delay = random.uniform(2, 4)
                    print(f"    ⏱️ {delay:.1f}초 대기...")
                    time.sleep(delay)
            
            # 최종 저장
            self.save_results()
            
            # 요약 출력
            self.print_summary()
            
        except KeyboardInterrupt:
            print(f"\n\n⚠️ 사용자 중단 (Ctrl+C)")
            self.save_results()
            self.print_summary()
            print(f"\n💾 진행 상황 저장됨: {self.progress_file}")
            print(f"🔄 다시 실행하면 {self.progress.get_start_row()}행부터 이어서 진행됩니다")
        
        except Exception as e:
            print(f"\n\n❌ 예상치 못한 오류: {e}")
            import traceback
            traceback.print_exc()
            self.save_results()
            self.print_summary()
        
        finally:
            self.browser.close()


def main():
    """메인 함수"""
    # 경로 설정
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # CSV 파일 경로 (coupang3 폴더 기준)
    csv_path = os.path.join(script_dir, 'wing_collected_links_current.csv')
    output_dir = os.path.join(script_dir, 'results')
    
    # CSV 파일 존재 확인
    if not os.path.exists(csv_path):
        print(f"❌ CSV 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    # 체커 실행
    checker = RocketGlobalChecker(csv_path, output_dir)
    
    # 옵션 1: 테스트용으로 처음 10개만 처리
    # checker.run(max_rows=10)
    
    # 옵션 2: 전체 처리
    checker.run()


if __name__ == "__main__":
    main()