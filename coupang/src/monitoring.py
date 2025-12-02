#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
로켓직구 모니터링 시스템 (통합 버전)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
크롤링 + 엑셀 업로드 통합

🔄 리팩토링: coupang_manager 모듈 사용 (undetected-chromedriver)
"""

import sys
import os
import time
import re
import random
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# 절대 경로 기반 import
COUPANG2_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IHERB_PRICE_ROOT = os.path.dirname(COUPANG2_ROOT)

# 경로 추가
sys.path.insert(0, IHERB_PRICE_ROOT)
sys.path.insert(0, COUPANG2_ROOT)

# 쿠팡 매니저 (undetected-chromedriver)
from coupang_manager import CoupangBrowser


class ScrollExtractor:
    """무한 스크롤 상품 추출기"""
    
    def __init__(self, browser):
        """
        Args:
            browser: CoupangBrowser 인스턴스
        """
        self.browser = browser
        self.filter_applied = False
    
    @property
    def driver(self):
        return self.browser.driver if self.browser else None
    
    def extract_all_products_with_scroll(self, page_url: str, max_retry_filter: int = 3) -> tuple:
        """무한 스크롤로 모든 상품 추출"""
        if not self.driver:
            return [], False, "브라우저 드라이버 초기화 실패"
        
        # 필터 적용 재시도 로직
        for attempt in range(max_retry_filter):
            try:
                print(f"📜 무한 스크롤 크롤링 시작 (시도 {attempt + 1}/{max_retry_filter}): {page_url}")
                self.driver.get(page_url)
                time.sleep(random.uniform(3, 5))
                
                # 판매량순 필터 적용 시도
                filter_success = self._click_sales_filter()
                
                if filter_success:
                    self.filter_applied = True
                    break
                else:
                    if attempt < max_retry_filter - 1:
                        print(f"  ⚠️  필터 적용 실패, 페이지 새로고침 후 재시도...")
                        time.sleep(3)
                    else:
                        print(f"\n{'='*70}")
                        print(f"⚠️  {max_retry_filter}회 시도 후에도 판매량순 필터 적용 실패")
                        print(f"{'='*70}")
                        print(f"선택지:")
                        print(f"  1. 'skip'  - 이 URL을 건너뛰고 다음 작업으로 진행")
                        print(f"  2. 'abort' - 전체 크롤링 중단")
                        print(f"  3. 'force' - 필터 없이 강제 크롤링")
                        print(f"{'='*70}")
                        
                        while True:
                            user_input = input("선택 (skip/abort/force): ").strip().lower()
                            
                            if user_input == 'skip':
                                print("  ⏭️  건너뜀")
                                return [], False, "판매량순 필터 적용 실패 - skip"
                            elif user_input == 'abort':
                                print("  🛑 중단")
                                return [], False, "판매량순 필터 적용 실패 - abort"
                            elif user_input == 'force':
                                print("  ⚠️  필터 없이 강제 크롤링")
                                self.filter_applied = False
                                break
                            else:
                                print(f"  ❌ 잘못된 입력")
                        
                        break
            
            except Exception as e:
                error_msg = f"페이지 로드 오류 (시도 {attempt + 1}): {e}"
                print(f"  ❌ {error_msg}")
                if attempt < max_retry_filter - 1:
                    time.sleep(5)
                else:
                    return [], False, error_msg
        
        # 스크롤 크롤링 시작
        all_products = []
        seen_product_ids = set()
        scroll_count = 0
        max_scrolls = 200
        no_new_products_count = 0
        max_no_new_attempts = 15
        consecutive_no_height_change = 0
        
        print("🔄 상품 수집 중...")
        
        try:
            while scroll_count < max_scrolls:
                scroll_count += 1
                
                new_products = self._extract_products_from_current_page(seen_product_ids)
                
                if new_products:
                    all_products.extend(new_products)
                    no_new_products_count = 0
                    consecutive_no_height_change = 0
                    print(f"  [스크롤 {scroll_count}] 신규: {len(new_products)}개, 총: {len(all_products)}개")
                else:
                    no_new_products_count += 1
                
                height_changed = self._scroll_to_bottom()
                
                if not height_changed:
                    consecutive_no_height_change += 1
                    if consecutive_no_height_change >= 5 and no_new_products_count >= 5:
                        print(f"🏁 더 이상 신규 상품이 없습니다")
                        break
                else:
                    consecutive_no_height_change = 0
                
                if no_new_products_count >= max_no_new_attempts:
                    print(f"🏁 {max_no_new_attempts}회 연속 신규 없음")
                    break
                
                time.sleep(random.uniform(1, 2))
            
            # 순위 할당
            ranked_products = []
            for rank, product in enumerate(all_products, 1):
                product['rank'] = rank
                ranked_products.append(product)
            
            print(f"✅ 무한 스크롤 완료: 총 {len(ranked_products)}개 상품 수집")
            
            if ranked_products:
                self._verify_ranks(ranked_products)
            
            return ranked_products, self.filter_applied, None
            
        except Exception as e:
            error_msg = f"스크롤 크롤링 중 오류: {e}"
            print(f"❌ {error_msg}")
            return [], False, error_msg
    
    def _verify_ranks(self, products: list):
        """순위 무결성 검증"""
        if not products:
            return
        
        ranks = [p['rank'] for p in products]
        
        if min(ranks) != 1:
            raise ValueError(f"순위가 1부터 시작하지 않습니다: min={min(ranks)}")
        
        expected_ranks = set(range(1, len(products) + 1))
        actual_ranks = set(ranks)
        
        if expected_ranks != actual_ranks:
            missing = expected_ranks - actual_ranks
            raise ValueError(f"순위가 연속적이지 않습니다. 누락: {missing}")
        
        print(f"  ✅ 순위 검증 완료: 1~{len(products)}위")
    
    def _click_sales_filter(self) -> bool:
        """판매량순 필터 클릭"""
        try:
            print("🔍 판매량순 필터 적용 중...")
            time.sleep(2)
            
            filter_buttons = self.driver.find_elements("css selector", 'li.sortkey')
            
            for button in filter_buttons:
                try:
                    if '판매량순' in button.text:
                        button.click()
                        print("✅ 판매량순 필터 적용 완료")
                        time.sleep(3)
                        return True
                except:
                    continue
            
            print("⚠️ 판매량순 필터 없음")
            return False
            
        except Exception as e:
            print(f"⚠️ 필터 적용 오류: {e}")
            return False
    
    def _extract_products_from_current_page(self, seen_product_ids: set) -> list:
        """현재 페이지에서 신규 상품만 추출"""
        try:
            new_products = []
            product_elements = self.driver.find_elements("css selector", 'li.product-wrap')
            
            for element in product_elements:
                try:
                    link_elem = element.find_element("css selector", 'a.product-wrapper')
                    product_url = link_elem.get_attribute('href')
                    
                    if not product_url:
                        continue
                    
                    match = re.search(r'itemId=(\d+)', product_url)
                    if not match:
                        continue
                    
                    product_id = match.group(1)
                    
                    if product_id in seen_product_ids:
                        continue
                    
                    seen_product_ids.add(product_id)
                    
                    product_data = self._parse_product_data(element, product_id, product_url)
                    if product_data and product_data.get('product_name'):
                        new_products.append(product_data)
                except:
                    continue
            
            return new_products
        except:
            return []
    
    def _parse_product_data(self, element, product_id: str, product_url: str) -> dict:
        """상품 데이터 파싱"""
        try:
            html = element.get_attribute('outerHTML')
            soup = BeautifulSoup(html, 'html.parser')

            name_elem = soup.select_one('div.name')
            if not name_elem:
                return None

            product_name = name_elem.get_text(strip=True)
            if not product_name:
                return None

            # URL에서 vendorItemId 추출
            vendor_item_id = None
            try:
                m_vid = re.search(r'vendorItemId=(\d+)', product_url)
                if m_vid:
                    vendor_item_id = m_vid.group(1)
            except:
                vendor_item_id = None

            # 가격/리뷰 등 파싱
            current_price = 0
            price_elem = soup.select_one('strong.price-value')
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price_text = re.sub(r'[^\d]', '', price_text)
                current_price = int(price_text) if price_text else 0

            original_price = 0
            original_elem = soup.select_one('del.base-price')
            if original_elem:
                price_text = original_elem.get_text(strip=True)
                price_text = re.sub(r'[^\d]', '', price_text)
                original_price = int(price_text) if price_text else 0

            if original_price == 0:
                original_price = current_price

            discount_rate = 0
            discount_elem = soup.select_one('span.discount-percentage')
            if discount_elem:
                discount_text = discount_elem.get_text(strip=True)
                discount_text = re.sub(r'[^\d]', '', discount_text)
                discount_rate = int(discount_text) if discount_text else 0

            review_count = 0
            review_elem = soup.select_one('span.rating-total-count')
            if review_elem:
                review_text = review_elem.get_text(strip=True)
                review_text = re.sub(r'[^\d]', '', review_text)
                review_count = int(review_text) if review_text else 0

            rating_score = 0.0
            rating_elem = soup.select_one('div.rating-light')
            if rating_elem and rating_elem.has_attr('data-rating'):
                try:
                    rating_score = float(rating_elem['data-rating'])
                except:
                    rating_score = 0.0

            return {
                'product_id': product_id,
                'product_name': product_name,
                'product_url': product_url,
                'current_price': current_price,
                'original_price': original_price,
                'discount_rate': discount_rate,
                'review_count': review_count,
                'rating_score': rating_score,
                'vendor_item_id': vendor_item_id
            }

        except Exception as e:
            return None
    
    def _scroll_to_bottom(self) -> bool:
        """스크롤 & 새 콘텐츠 확인"""
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            scroll_steps = random.randint(3, 5)
            for i in range(scroll_steps):
                scroll_amount = (last_height / scroll_steps) * (i + 1)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_amount});")
                time.sleep(0.3)
            
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            for _ in range(5):
                time.sleep(1)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height > last_height:
                    return True
            
            return False
        except:
            return False


class RocketDirectMonitorIntegrated:
    """로켓직구 모니터 (통합 DB 버전)"""
    
    def __init__(self, integrated_db, category_config: dict, headless: bool = False):
        """
        Args:
            integrated_db: IntegratedDatabase 인스턴스
            category_config: Config.ROCKET_CATEGORIES의 항목
            headless: 헤드리스 모드
        """
        self.category_config = category_config
        self.integrated_db = integrated_db
        self.browser = CoupangBrowser(headless=headless)
        self.extractor = ScrollExtractor(self.browser)
        
        print(f"✅ {category_config['name']} 모니터 초기화 완료")
    
    def run_monitoring_cycle(self, snapshot_id: int, base_url: str) -> dict:
        """모니터링 사이클 실행
        
        Args:
            snapshot_id: 통합 DB의 snapshot ID
            base_url: 로켓직구 기본 URL
        
        Returns:
            결과 딕셔너리
        """
        category_name = self.category_config['name']
        page_url = base_url + self.category_config['url_path']
        
        print(f"\n{'='*70}")
        print(f"📊 [{category_name}] 로켓직구 모니터링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")
        
        start_time = time.time()
        
        try:
            print(f"\n[1/2] 📜 페이지 크롤링 중...")
            current_products, filter_applied, error_message = self.extractor.extract_all_products_with_scroll(page_url)
            
            if not current_products:
                print(f"❌ 상품 수집 실패")
                
                if error_message and 'abort' in error_message.lower():
                    return {
                        'success': False,
                        'error_message': error_message,
                        'action': 'abort'
                    }
                else:
                    return {
                        'success': False,
                        'error_message': error_message or '상품 수집 실패',
                        'action': 'continue'
                    }
            
            print(f"✅ {len(current_products)}개 상품 수집 완료")
            
            if not filter_applied:
                print(f"\n⚠️⚠️⚠️  주의: 판매량순 필터가 적용되지 않았습니다!")
            
            print(f"\n[2/2] 💾 통합 DB에 저장 중...")
            crawl_duration = time.time() - start_time
            
            try:
                self._save_to_integrated_db(snapshot_id, current_products)
                print(f"✅ 통합 DB 저장 완료")
                
            except ValueError as e:
                return {
                    'success': False,
                    'error_message': f'DB 저장 실패: {e}',
                    'action': 'continue'
                }
            
            print(f"\n{'='*70}")
            print(f"✅ [{category_name}] 모니터링 완료 ({crawl_duration:.1f}초)")
            print(f"{'='*70}\n")
            
            return {
                'success': True,
                'snapshot_id': snapshot_id,
                'product_count': len(current_products),
                'crawl_duration': crawl_duration,
                'filter_applied': filter_applied,
                'error_message': None,
                'action': 'continue'
            }
            
        except Exception as e:
            print(f"❌ [{category_name}] 모니터링 오류: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error_message': str(e),
                'action': 'continue'
            }
    
    def _save_to_integrated_db(self, snapshot_id: int, products: list):
        """통합 DB에 저장"""
        
        # URL에서 product_id, item_id 추출
        products_data = []
        prices_data = []
        features_data = []
        
        for p in products:
            vendor_id = p.get('vendor_item_id')
            if not vendor_id:
                continue
            
            url = p['product_url']
            product_id = None
            item_id = None
            
            if url:
                m_product = re.search(r'/products/(\d+)', url)
                if m_product:
                    product_id = m_product.group(1)
                
                m_item = re.search(r'itemId=(\d+)', url)
                if m_item:
                    item_id = m_item.group(1)
            
            # products
            products_data.append({
                'vendor_item_id': vendor_id,
                'product_id': product_id,
                'item_id': item_id,
                'part_number': None,
                'upc': None,
                'name': p['product_name']
            })
            
            # product_price
            prices_data.append({
                'vendor_item_id': vendor_id,
                'rocket_price': p['current_price'],
                'rocket_original_price': p['original_price'],
                'iherb_price': None,
                'iherb_original_price': None,
                'iherb_recommended_price': None
            })
            
            # product_features
            features_data.append({
                'vendor_item_id': vendor_id,
                'rocket_rank': p['rank'],
                'rocket_rating': p['rating_score'],
                'rocket_reviews': p['review_count'],
                'rocket_category': self.category_config['name'],
                'iherb_stock': None,
                'iherb_stock_status': None,
                'iherb_revenue': None,
                'iherb_sales_quantity': None,
                'iherb_item_winner_ratio': None,
                'iherb_category': None
            })
        
        # 일괄 저장
        if products_data:
            self.integrated_db.batch_upsert_products(products_data)
        
        if prices_data:
            self.integrated_db.batch_save_product_prices(snapshot_id, prices_data)
        
        if features_data:
            self.integrated_db.batch_save_product_features(snapshot_id, features_data)
    
    def close(self):
        """리소스 정리"""
        if self.browser:
            self.browser.close()
 
def check_excel_date(excel_dir: str, today: datetime) -> bool:
    """엑셀 파일 날짜 검증"""
    excel_path = Path(excel_dir)
    if not excel_path.exists():
        print(f"\n⚠️ 엑셀 디렉토리가 존재하지 않습니다: {excel_path}")
        return False

    excel_files = sorted(excel_path.glob("*.xlsx"))
    if not excel_files:
        print(f"\n⚠️ 엑셀 파일(.xlsx)이 없습니다: {excel_path}")
        return False

    today_ymd = today.strftime("%Y%m%d")
    found_dates = set()

    for f in excel_files:
        m = re.search(r"(20\d{6})", f.stem)
        if m:
            found_dates.add(m.group(1))

    if not found_dates:
        print("\n⚠️ 엑셀 파일 이름에서 날짜(YYYYMMDD)를 찾지 못했습니다.")
        print("   예: iherb_20251119.xlsx 처럼 날짜를 포함시키면 자동 검증이 가능합니다.")
        ans = input("날짜 검증 없이 계속 진행할까요? (y/n): ").strip().lower()
        if ans != "y":
            print("🚫 작업을 중단합니다.")
            return False
        return True

    print("\n📂 발견된 엑셀 파일 날짜들:")
    for d in sorted(found_dates):
        print(f"  - {d}")

    if found_dates == {today_ymd}:
        print(f"\n✅ 엑셀 파일 날짜가 오늘({today_ymd})과 일치합니다.")
        return True
    else:
        print(f"\n⚠️ 엑셀 파일 날짜가 오늘({today_ymd})과 다릅니다.")
        ans = input("그래도 계속 진행할까요? (y/n): ").strip().lower()
        if ans != "y":
            print("🚫 작업을 중단합니다.")
            return False
        print("➡️ 사용자 선택에 따라 계속 진행합니다.")
        return True


def main():
    """메인"""
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))
    
    # Config import
    config_path = project_root / "config" / "settings.py"
    import importlib.util
    spec = importlib.util.spec_from_file_location("settings", config_path)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)
    Config = config_module.Config
    
    from database import IntegratedDatabase
    from excel_loader import ExcelLoader
    
    Config.ensure_directories()

    today_dt = datetime.now()
    today = today_dt.strftime('%Y-%m-%d')

    # 엑셀 날짜 검증
    print("\n🕵️ 엑셀 파일 날짜 사전 검증 중...")
    if not check_excel_date(Config.IHERB_EXCEL_DIR, today_dt):
        return

    integrated_db = IntegratedDatabase(Config.INTEGRATED_DB_PATH)
    integrated_db.init_database()
    
    # 크롤링
    def run_crawling_phase() -> int:
        rocket_urls = {}
        for category_config in Config.ROCKET_CATEGORIES:
            url_column = category_config['url_column']
            url_key = url_column.replace('rocket_category_', '')
            full_url = Config.ROCKET_BASE_URL + category_config['url_path']
            rocket_urls[url_key] = full_url
        
        snapshot_id = integrated_db.create_snapshot(
            snapshot_date=today,
            rocket_urls=rocket_urls
        )

        print(f"\n✅ Snapshot 생성: ID={snapshot_id}, 날짜={today}")
        
        all_success = True
        
        for idx, category_config in enumerate(Config.ROCKET_CATEGORIES, 1):
            print(f"\n{'='*80}")
            print(f"📦 [{idx}/{len(Config.ROCKET_CATEGORIES)}] {category_config['name']}")
            print(f"{'='*80}")
            
            monitor = RocketDirectMonitorIntegrated(
                integrated_db=integrated_db,
                category_config=category_config,
                headless=False
            )
            
            try:
                result = monitor.run_monitoring_cycle(snapshot_id, Config.ROCKET_BASE_URL)
                
                if result['success']:
                    print(f"\n✅ 크롤링 완료: {category_config['name']}")
                else:
                    print(f"❌ 크롤링 실패: {category_config['name']}")
                    all_success = False
            
            except KeyboardInterrupt:
                print(f"\n⚠️ 사용자 중단: {category_config['name']}")
                monitor.close()
                raise
            except Exception as e:
                print(f"❌ 오류 발생: {category_config['name']} - {e}")
                import traceback
                traceback.print_exc()
                all_success = False
            finally:
                monitor.close()
        
        if all_success:
            print(f"\n{'='*80}")
            print(f"✅ 모든 카테고리 크롤링 완료")
            print(f"{'='*80}")
        else:
            print(f"\n{'='*80}")
            print(f"⚠️ 일부 카테고리 크롤링 실패")
            print(f"{'='*80}")
        
        return snapshot_id

    # 엑셀 업로드
    def run_excel_phase(snapshot_id: int):
        try:
            print(f"\n{'='*80}")
            print(f"📥 엑셀 파일 업로드 시작 (snapshot_id={snapshot_id})")
            print(f"{'='*80}")
            
            loader = ExcelLoader(integrated_db)
            loader.load_all_excel_files(
                snapshot_id=snapshot_id,
                excel_dir=Config.IHERB_EXCEL_DIR
            )
            
            print(f"\n🎉 엑셀 업로드 포함 전체 작업 완료!")
        
        except Exception as e:
            print(f"\n❌ 엑셀 업로드 실패: {e}")
            import traceback
            traceback.print_exc()

    snapshot_id = run_crawling_phase()
    run_excel_phase(snapshot_id)


if __name__ == "__main__":
    main()