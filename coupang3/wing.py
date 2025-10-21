"""
쿠팡 윙 링크 수집기
판매자가 IHERB LLC가 아닌 상품 링크 수집
- 실시간 저장 기능
- 중단 후 재개 기능
- 처리 개수 조절 가능
"""

import sys
import os
import time
import random
import pandas as pd
from datetime import datetime
import json

# 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
coupang_dir = os.path.join(parent_dir, 'coupang')
config_dir = os.path.join(parent_dir, 'config')

# coupang 디렉토리와 config 디렉토리를 path에 추가
sys.path.insert(0, parent_dir)
sys.path.insert(0, coupang_dir)
sys.path.insert(0, config_dir)

# 이제 import 가능
from coupang_manager import BrowserManager
from coupang_config import CoupangConfig
from global_config import PathConfig

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup


class WingLinkCollector:
    """쿠팡 윙 링크 수집기"""
    
    def __init__(self, csv_path: str, headless: bool = False, resume: bool = True, max_items: int = 200):
        """
        Args:
            csv_path: 251020.csv 파일 경로
            headless: 헤드리스 모드 (False 권장 - 수동 로그인 필요)
            resume: 중단된 작업 재개 여부
            max_items: 최대 처리 개수 (기본값: 200, None이면 전체)
        """
        self.csv_path = csv_path
        self.browser = BrowserManager(headless=headless)
        self.driver = None
        self.resume = resume
        self.max_items = max_items
        
        # 진행 상태 파일
        self.progress_file = os.path.join(current_dir, 'wing_progress.json')
        self.results_file = os.path.join(current_dir, 'outputs', 'wing_collected_links_current.csv')
        
        # outputs 디렉토리 생성
        os.makedirs(os.path.join(current_dir, 'outputs'), exist_ok=True)
        
        # 수집 결과
        self.collected_links = []
        self.processed_option_ids = set()
        self.processed_count = 0
        self.found_count = 0
        
        # 통계
        self.stats = {
            'total_ids': 0,
            'processed': 0,
            'found_seller_delivery': 0,
            'found_non_iherb': 0,
            'iherb_seller': 0,
            'errors': 0,
            'started_at': datetime.now().isoformat(),
            'last_saved_at': None
        }
        
        # 진행 상태 로드
        if resume:
            self.load_progress()
    
    def load_progress(self):
        """이전 진행 상태 로드"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress_data = json.load(f)
                
                self.processed_option_ids = set(progress_data.get('processed_option_ids', []))
                self.stats = progress_data.get('stats', self.stats)
                
                print(f"\n{'='*80}")
                print(f"📂 이전 진행 상태 로드")
                print(f"{'='*80}")
                print(f"처리 완료: {len(self.processed_option_ids)}개 옵션ID")
                print(f"수집된 링크: {self.stats.get('found_non_iherb', 0)}개")
                print(f"마지막 저장: {self.stats.get('last_saved_at', 'N/A')}")
                print(f"{'='*80}\n")
            
            # 기존 결과 파일 로드
            if os.path.exists(self.results_file):
                df = pd.read_csv(self.results_file, encoding='utf-8-sig')
                self.collected_links = df.to_dict('records')
                self.found_count = len(self.collected_links)
                print(f"✓ 기존 결과 파일 로드: {self.found_count}개 링크\n")
        
        except Exception as e:
            print(f"⚠️ 진행 상태 로드 실패: {e}")
            print("처음부터 시작합니다.\n")
    
    def save_progress(self):
        """현재 진행 상태 저장"""
        try:
            progress_data = {
                'processed_option_ids': list(self.processed_option_ids),
                'stats': self.stats,
                'last_updated': datetime.now().isoformat()
            }
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)
        
        except Exception as e:
            print(f"  ⚠️ 진행 상태 저장 실패: {e}")
    
    def save_results_realtime(self, new_link: dict = None):
        """실시간 결과 저장"""
        try:
            if new_link:
                self.collected_links.append(new_link)
            
            if self.collected_links:
                df = pd.DataFrame(self.collected_links)
                df.to_csv(self.results_file, index=False, encoding='utf-8-sig')
                
                self.stats['last_saved_at'] = datetime.now().isoformat()
                self.stats['found_non_iherb'] = len(self.collected_links)
        
        except Exception as e:
            print(f"  ⚠️ 실시간 저장 실패: {e}")
    
    def start(self):
        """브라우저 시작"""
        if not self.browser.start_driver():
            raise Exception("브라우저 시작 실패")
        
        self.driver = self.browser.driver
        print("✓ 브라우저 시작 완료")
    
    def wait_for_manual_setup(self):
        """수동 설정 대기"""
        print(f"\n{'='*80}")
        print("🔧 수동 설정이 필요합니다")
        print(f"{'='*80}")
        print("1. 쿠팡 윙에 로그인해주세요")
        print("2. '가격관리' 탭으로 이동해주세요")
        print("3. '아이템위너 상태'를 '전체'로 설정해주세요")
        print("4. 준비가 되면 터미널에서 Enter를 눌러주세요")
        print(f"{'='*80}\n")
        
        input("준비 완료 후 Enter를 누르세요...")
        print("\n✓ 자동 수집을 시작합니다\n")
    
    def load_option_ids(self) -> list:
        """CSV에서 쿠팡 옵션ID 로드"""
        try:
            df = pd.read_csv(self.csv_path, encoding='utf-8')
            
            # 옵션ID 컬럼 찾기
            if '쿠팡 옵션ID' in df.columns:
                option_ids = df['쿠팡 옵션ID'].dropna().astype(int).tolist()
            elif '쿠팡\n 옵션ID' in df.columns:
                option_ids = df['쿠팡\n 옵션ID'].dropna().astype(int).tolist()
            else:
                # 모든 컬럼명 출력
                print("사용 가능한 컬럼:", df.columns.tolist())
                raise ValueError("'쿠팡 옵션ID' 컬럼을 찾을 수 없습니다")
            
            original_count = len(option_ids)
            
            # 최대 개수만큼만 선택
            if self.max_items is not None:
                option_ids = option_ids[:self.max_items]
                print(f"✓ CSV 전체: {original_count}개 옵션ID")
                print(f"✓ 작업 대상: 상위 {len(option_ids)}개 옵션ID")
            else:
                print(f"✓ CSV 전체: {original_count}개 옵션ID (전체 처리)")
            
            # 이미 처리된 ID 제외
            if self.resume and self.processed_option_ids:
                before_count = len(option_ids)
                option_ids = [oid for oid in option_ids if oid not in self.processed_option_ids]
                skipped_count = before_count - len(option_ids)
                
                if skipped_count > 0:
                    print(f"✓ 이미 처리된 {skipped_count}개 옵션ID 건너뛰기")
            
            self.stats['total_ids'] = len(option_ids)
            print(f"✓ 최종 처리 대상: {len(option_ids)}개 옵션ID\n")
            return option_ids
            
        except Exception as e:
            print(f"✗ CSV 로드 실패: {e}")
            raise
    
    def search_option_id(self, option_id: int):
        """옵션ID로 검색"""
        try:
            # 검색어 입력창 찾기
            search_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR, 
                    'input[placeholder*="상품 ID"]'
                ))
            )
            
            # 기존 검색어 지우기
            search_input.clear()
            time.sleep(0.3)
            
            # 새 검색어 입력
            search_input.send_keys(str(option_id))
            time.sleep(0.5)
            
            # 검색 버튼 클릭 또는 Enter
            search_input.send_keys(Keys.RETURN)
            
            # 검색 결과 로딩 대기
            time.sleep(random.uniform(2, 3))
            
            return True
            
        except Exception as e:
            print(f"  ✗ 검색 실패: {e}")
            return False
    
    def find_seller_delivery_products(self) -> list:
        """판매자배송 상품 찾기"""
        try:
            # 현재 페이지 HTML 파싱
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # 상품 목록 찾기
            product_rows = soup.select('tbody.data-body tr.table-data-row-container')
            
            if not product_rows:
                return []
            
            seller_delivery_products = []
            
            for row in product_rows:
                # 판매자배송 텍스트 확인
                delivery_info = row.select_one('div.delivery-info div.delivery-type-text')
                
                if delivery_info and '판매자배송' in delivery_info.get_text():
                    # 상품 링크 추출
                    link_elem = row.select_one('a.view-in-coupang')
                    
                    if link_elem:
                        product_link = link_elem.get('href', '')
                        if product_link:
                            # 전체 URL로 변환
                            if product_link.startswith('//'):
                                product_link = 'https:' + product_link
                            elif not product_link.startswith('http'):
                                product_link = 'https://www.coupang.com' + product_link
                            
                            # 상품 정보
                            product_info = {
                                'option_id': None,
                                'product_link': product_link,
                                'row_element': row,
                                'vendor_item_id': row.get('data-vendor-item-id', '')
                            }
                            
                            seller_delivery_products.append(product_info)
            
            return seller_delivery_products
            
        except Exception as e:
            print(f"  ✗ 판매자배송 상품 찾기 실패: {e}")
            return []
    
    def check_seller_on_product_page(self, product_link: str) -> dict:
        """상품 페이지에서 판매자 확인"""
        original_window = self.driver.current_window_handle
        
        try:
            # 새 탭에서 상품 페이지 열기
            self.driver.execute_script(f"window.open('{product_link}', '_blank');")
            time.sleep(1)
            
            # 새 탭으로 전환
            windows = self.driver.window_handles
            self.driver.switch_to.window(windows[-1])
            
            # 페이지 로딩 대기
            time.sleep(random.uniform(2, 3))
            
            # 판매자 정보 찾기
            try:
                # HTML 파싱
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # 판매자 정보 영역 찾기
                seller_info = soup.select_one('div.seller-info')
                
                if not seller_info:
                    return {
                        'is_iherb': None,
                        'seller_name': None,
                        'reason': 'seller_info_not_found'
                    }
                
                # 판매자 링크에서 텍스트 추출
                seller_link = seller_info.select_one('a')
                
                if seller_link:
                    seller_name = seller_link.get_text().strip()
                    
                    # IHERB LLC 확인 (대소문자 무시)
                    is_iherb = 'IHERB LLC' in seller_name.upper()
                    
                    return {
                        'is_iherb': is_iherb,
                        'seller_name': seller_name,
                        'reason': 'success'
                    }
                else:
                    return {
                        'is_iherb': None,
                        'seller_name': None,
                        'reason': 'seller_link_not_found'
                    }
            
            except Exception as parse_error:
                return {
                    'is_iherb': None,
                    'seller_name': None,
                    'reason': f'parse_error: {parse_error}'
                }
        
        except Exception as e:
            return {
                'is_iherb': None,
                'seller_name': None,
                'reason': f'page_error: {e}'
            }
        
        finally:
            # 원래 탭으로 돌아가기
            try:
                # 새 탭 닫기
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                
                # 원래 탭으로 전환
                self.driver.switch_to.window(original_window)
                time.sleep(0.5)
            except:
                pass
    
    def collect_links_for_option_id(self, option_id: int) -> list:
        """특정 옵션ID에 대해 조건 만족하는 링크 수집"""
        print(f"\n[{self.processed_count + 1}/{self.stats['total_ids']}] 옵션ID: {option_id}")
        
        # 검색
        if not self.search_option_id(option_id):
            self.stats['errors'] += 1
            return []
        
        # 판매자배송 상품 찾기
        seller_products = self.find_seller_delivery_products()
        
        if not seller_products:
            print(f"  ℹ️  판매자배송 상품 없음")
            return []
        
        print(f"  ✓ 판매자배송 상품 {len(seller_products)}개 발견")
        self.stats['found_seller_delivery'] += len(seller_products)
        
        # 각 상품에 대해 판매자 확인
        matched_links = []
        
        for idx, product in enumerate(seller_products, 1):
            product_link = product['product_link']
            vendor_item_id = product['vendor_item_id']
            
            print(f"    [{idx}/{len(seller_products)}] 판매자 확인 중...", end=' ')
            
            seller_result = self.check_seller_on_product_page(product_link)
            
            if seller_result['is_iherb'] is False:
                # IHERB LLC가 아닌 경우
                print(f"✓ 수집 대상! (판매자: {seller_result['seller_name']})")
                
                new_link = {
                    'option_id': option_id,
                    'vendor_item_id': vendor_item_id,
                    'product_link': product_link,
                    'seller_name': seller_result['seller_name'],
                    'collected_at': datetime.now().isoformat()
                }
                
                matched_links.append(new_link)
                
                # 실시간 저장
                self.save_results_realtime(new_link)
                self.found_count += 1
                
                self.stats['found_non_iherb'] += 1
            
            elif seller_result['is_iherb'] is True:
                # IHERB LLC인 경우 (제외)
                print(f"✗ 제외 (IHERB LLC)")
                self.stats['iherb_seller'] += 1
            
            else:
                # 판매자 정보를 찾지 못한 경우
                reason = seller_result.get('reason', 'unknown')
                print(f"✗ 판매자 확인 실패 ({reason})")
            
            # 다음 상품 확인 전 대기
            time.sleep(random.uniform(1, 2))
        
        return matched_links
    
    def collect_all(self):
        """전체 수집 프로세스"""
        print(f"\n{'='*80}")
        print("🔍 쿠팡 윙 링크 수집 시작")
        print(f"{'='*80}\n")
        
        # CSV 로드
        option_ids = self.load_option_ids()
        
        if not option_ids:
            print("\n✓ 모든 옵션ID가 이미 처리되었습니다!")
            self.print_summary()
            return
        
        # 브라우저 시작
        self.start()
        
        # 수동 설정 대기
        self.wait_for_manual_setup()
        
        print(f"\n{'='*80}")
        print(f"자동 수집 시작: {len(option_ids)}개 옵션ID")
        print(f"목표: 판매자가 'IHERB LLC'가 아닌 상품 수집")
        print(f"실시간 저장: {self.results_file}")
        print(f"{'='*80}")
        
        # 각 옵션ID 처리
        for option_id in option_ids:
            try:
                matched_links = self.collect_links_for_option_id(option_id)
                
                # 처리 완료 표시
                self.processed_option_ids.add(option_id)
                self.processed_count += 1
                self.stats['processed'] = len(self.processed_option_ids)
                
                # 진행 상태 저장 (10개마다)
                if self.processed_count % 10 == 0:
                    self.save_progress()
                    print(f"  💾 진행 상태 저장됨")
                
                # 진행률 표시
                progress = (self.processed_count / len(option_ids)) * 100
                print(f"  진행률: {progress:.1f}% | 수집: {self.found_count}개 | IHERB 제외: {self.stats['iherb_seller']}개")
                
                # 요청 간 대기
                time.sleep(random.uniform(2, 3))
                
            except KeyboardInterrupt:
                print("\n\n⚠️ 사용자 중단")
                print("진행 상태를 저장합니다...")
                self.save_progress()
                break
            
            except Exception as e:
                print(f"  ✗ 오류: {e}")
                self.stats['errors'] += 1
                # 오류가 발생해도 처리 완료로 표시
                self.processed_option_ids.add(option_id)
                continue
        
        # 최종 진행 상태 저장
        self.save_progress()
        
        # 최종 결과 파일 생성
        self.save_final_results()
        
        # 최종 통계
        self.print_summary()
    
    def save_final_results(self):
        """최종 결과 파일 생성"""
        if not self.collected_links:
            return
        
        try:
            # 타임스탬프가 포함된 최종 파일명
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"wing_non_iherb_links_final_{timestamp}.csv"
            filepath = os.path.join(current_dir, 'outputs', filename)
            
            df = pd.DataFrame(self.collected_links)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            print(f"\n✓ 최종 결과 저장: {filepath}")
            print(f"  총 {len(self.collected_links)}개 링크")
            
        except Exception as e:
            print(f"\n✗ 최종 결과 저장 실패: {e}")
    
    def print_summary(self):
        """최종 통계 출력"""
        print(f"\n{'='*80}")
        print("📊 수집 완료 요약")
        print(f"{'='*80}")
        print(f"총 처리: {len(self.processed_option_ids)}개 옵션ID")
        print(f"판매자배송 발견: {self.stats['found_seller_delivery']}개")
        print(f"IHERB LLC 아님 (수집): {self.stats['found_non_iherb']}개")
        print(f"IHERB LLC (제외): {self.stats['iherb_seller']}개")
        print(f"오류: {self.stats['errors']}개")
        print(f"{'='*80}")
        
        if self.collected_links:
            print(f"\n수집된 링크 샘플:")
            for i, link in enumerate(self.collected_links[:5], 1):
                print(f"  {i}. 옵션ID: {link['option_id']}")
                print(f"     판매자: {link['seller_name']}")
                print(f"     링크: {link['product_link'][:80]}...")
        
        print(f"\n💾 결과 파일: {self.results_file}")
        print(f"💾 진행 상태: {self.progress_file}")
    
    def close(self):
        """브라우저 종료"""
        self.browser.close()


def main():
    """실행"""
    # CSV 파일 경로 (현재 디렉토리 기준)
    csv_path = os.path.join(current_dir, "251020.csv")
    
    if not os.path.exists(csv_path):
        print(f"❌ CSV 파일을 찾을 수 없습니다: {csv_path}")
        print("파일 경로를 확인해주세요")
        return
    
    # 재개 옵션 확인
    resume = True
    if os.path.exists(os.path.join(current_dir, 'wing_progress.json')):
        user_input = input("이전 진행 상태를 발견했습니다. 이어서 진행하시겠습니까? (Y/n): ").strip().lower()
        if user_input == 'n':
            resume = False
            print("처음부터 시작합니다.")
    
    # 처리 개수 설정
    max_items_input = input("처리할 옵션ID 개수를 입력하세요 (기본값: 200, 전체는 'all' 입력): ").strip()
    
    if max_items_input.lower() == 'all':
        max_items = None
        print("전체 옵션ID를 처리합니다.")
    elif max_items_input.isdigit():
        max_items = int(max_items_input)
        print(f"상위 {max_items}개 옵션ID를 처리합니다.")
    else:
        max_items = 200
        print(f"기본값 {max_items}개 옵션ID를 처리합니다.")
    
    # 수집기 생성
    collector = WingLinkCollector(
        csv_path=csv_path,
        headless=False,  # 수동 로그인 필요
        resume=resume,
        max_items=max_items
    )
    
    try:
        collector.collect_all()
    
    except KeyboardInterrupt:
        print("\n⚠️ 프로그램 중단")
        print("다음 실행 시 이어서 진행할 수 있습니다.")
    
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        collector.close()


if __name__ == "__main__":
    main()