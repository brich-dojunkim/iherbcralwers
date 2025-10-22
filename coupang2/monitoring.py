#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
간소화된 모니터링 시스템
- product_id 기반 단순 매칭
- 순위 할당 로직 유지
- 복잡한 하이브리드 매칭 제거
"""

import sys
import os
import time
import re
import random
from datetime import datetime
from bs4 import BeautifulSoup

# 기존 모듈 임포트
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, os.path.join(parent_dir, 'coupang'))

from coupang.coupang_manager import BrowserManager
from selenium.webdriver.common.by import By

# 간소화된 DB 임포트
from database import MonitoringDatabase


class ScrollExtractor:
    """무한 스크롤 상품 추출기"""
    
    def __init__(self, browser_manager):
        self.browser = browser_manager
    
    @property
    def driver(self):
        return self.browser.driver if self.browser else None
    
    def extract_all_products_with_scroll(self, page_url: str) -> list:
        """무한 스크롤로 모든 상품 추출"""
        if not self.driver:
            print("❌ 브라우저 드라이버가 초기화되지 않았습니다")
            return []
        
        try:
            print(f"📜 무한 스크롤 크롤링 시작: {page_url}")
            self.driver.get(page_url)
            time.sleep(random.uniform(3, 5))
            
            # 판매량순 필터 적용
            self._click_sales_filter()
            
            all_products = []
            seen_product_ids = set()
            scroll_count = 0
            max_scrolls = 200
            no_new_products_count = 0
            max_no_new_attempts = 15
            consecutive_no_height_change = 0
            
            print("🔄 상품 수집 중...")
            
            while scroll_count < max_scrolls:
                scroll_count += 1
                
                # 현재 페이지에서 상품 추출
                new_products = self._extract_products_from_current_page(seen_product_ids)
                
                if new_products:
                    all_products.extend(new_products)
                    no_new_products_count = 0
                    consecutive_no_height_change = 0
                    print(f"  [스크롤 {scroll_count}] 신규: {len(new_products)}개, 총: {len(all_products)}개")
                else:
                    no_new_products_count += 1
                
                # 스크롤
                height_changed = self._scroll_to_bottom()
                
                if not height_changed:
                    consecutive_no_height_change += 1
                    if consecutive_no_height_change >= 5 and no_new_products_count >= 5:
                        print(f"🏁 더 이상 신규 상품이 없습니다")
                        break
                else:
                    consecutive_no_height_change = 0
                
                if no_new_products_count >= max_no_new_attempts:
                    print(f"🏁 {max_no_new_attempts}회 연속 신규 없음, 크롤링 종료")
                    break
                
                time.sleep(random.uniform(1, 2))
            
            # 순위 할당 (DOM 순서 = 순위)
            ranked_products = []
            for rank, product in enumerate(all_products, 1):
                product['rank'] = rank
                ranked_products.append(product)
            
            print(f"✅ 무한 스크롤 완료: 총 {len(ranked_products)}개 상품 수집")
            
            # 순위 검증
            self._verify_ranks(ranked_products)
            
            return ranked_products
            
        except Exception as e:
            print(f"❌ 크롤링 오류: {e}")
            import traceback
            traceback.print_exc()
            return []
    
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
    
    def _click_sales_filter(self):
        """판매량순 필터 클릭"""
        try:
            print("🔍 판매량순 필터 적용 중...")
            time.sleep(2)
            
            filter_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'li.sortkey')
            
            for button in filter_buttons:
                try:
                    if '판매량순' in button.text:
                        button.click()
                        print("✅ 판매량순 필터 적용 완료")
                        time.sleep(3)
                        return
                except:
                    continue
            
            print("⚠️ 판매량순 필터 없음 (기본 정렬 사용)")
        except Exception as e:
            print(f"⚠️ 필터 적용 오류: {e}")
    
    def _extract_products_from_current_page(self, seen_product_ids: set) -> list:
        """현재 페이지에서 신규 상품만 추출"""
        try:
            new_products = []
            product_elements = self.driver.find_elements(By.CSS_SELECTOR, 'li.product-wrap')
            
            for element in product_elements:
                try:
                    link_elem = element.find_element(By.CSS_SELECTOR, 'a.product-wrapper')
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
                    
                    # 상품 데이터 파싱
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
            
            # 상품명 (필수)
            name_elem = soup.select_one('div.name, div.product-name, div[class*="name"]')
            if not name_elem:
                return None
            
            product_name = name_elem.get_text(strip=True)
            if not product_name:
                return None
            
            # 가격
            current_price = 0
            price_selectors = ['strong.price-value', 'span.price-value', 'em.sale', 'span[class*="price"]']
            for selector in price_selectors:
                price_elem = soup.select_one(selector)
                if price_elem:
                    price_text = price_elem.get_text(strip=True)
                    price_text = re.sub(r'[^\d]', '', price_text)
                    if price_text:
                        current_price = int(price_text)
                        break
            
            # 할인율
            discount_rate = 0
            discount_selectors = ['span.discount-percentage', 'em.discount-rate', 'span[class*="discount"]']
            for selector in discount_selectors:
                discount_elem = soup.select_one(selector)
                if discount_elem:
                    discount_text = discount_elem.get_text(strip=True)
                    discount_text = re.sub(r'[^\d]', '', discount_text)
                    if discount_text:
                        discount_rate = int(discount_text)
                        break
            
            # 리뷰 수
            review_count = 0
            review_selectors = ['span.rating-total-count', 'span[class*="review"]', 'em.rating-count']
            for selector in review_selectors:
                review_elem = soup.select_one(selector)
                if review_elem:
                    review_text = review_elem.get_text(strip=True)
                    review_text = re.sub(r'[^\d]', '', review_text)
                    if review_text:
                        review_count = int(review_text)
                        break
            
            # 평점
            rating_score = 0.0
            rating_elem = soup.select_one('em.rating, span[class*="rating"]')
            if rating_elem:
                try:
                    rating_text = rating_elem.get_text(strip=True)
                    rating_score = float(re.sub(r'[^\d.]', '', rating_text))
                except:
                    pass
            
            return {
                'product_id': product_id,
                'product_name': product_name,
                'product_url': product_url,
                'current_price': current_price,
                'discount_rate': discount_rate,
                'review_count': review_count,
                'rating_score': rating_score
            }
        except:
            return None
    
    def _scroll_to_bottom(self) -> bool:
        """스크롤 & 새 콘텐츠 확인"""
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            # 천천히 단계적으로 스크롤
            scroll_steps = random.randint(3, 5)
            for i in range(scroll_steps):
                scroll_amount = (last_height / scroll_steps) * (i + 1)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_amount});")
                time.sleep(0.3)
            
            # 완전히 끝까지
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # 새 콘텐츠 로딩 대기
            for _ in range(5):
                time.sleep(1)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height > last_height:
                    return True
            
            return False
        except:
            return False


class ChangeDetector:
    """간소화된 변화 감지기 - product_id만 사용"""
    
    def detect_changes(self, previous_products: list, current_products: list) -> dict:
        """이전 데이터와 현재 데이터 비교 - product_id 기반"""
        changes = {
            'new_products': [],
            'removed_products': [],
            'rank_changes': [],
            'price_changes': [],
            'review_surges': []
        }
        
        # product_id로 딕셔너리 생성
        prev_dict = {p['product_id']: p for p in previous_products}
        curr_dict = {p['product_id']: p for p in current_products}
        
        # 매칭된 상품 (ID가 같은 것)
        common_ids = set(prev_dict.keys()) & set(curr_dict.keys())
        
        for product_id in common_ids:
            old_product = prev_dict[product_id]
            new_product = curr_dict[product_id]
            
            # 순위 변화
            if old_product['rank'] != new_product['rank']:
                rank_improvement = old_product['rank'] - new_product['rank']
                changes['rank_changes'].append({
                    'product_id': product_id,
                    'product_name': new_product['product_name'],
                    'old_rank': old_product['rank'],
                    'new_rank': new_product['rank'],
                    'change_magnitude': rank_improvement
                })
            
            # 가격 변화
            if old_product['current_price'] != new_product['current_price']:
                price_change = new_product['current_price'] - old_product['current_price']
                changes['price_changes'].append({
                    'product_id': product_id,
                    'product_name': new_product['product_name'],
                    'old_price': old_product['current_price'],
                    'new_price': new_product['current_price'],
                    'change_magnitude': price_change
                })
            
            # 리뷰 급증 (50개 이상)
            review_increase = new_product['review_count'] - old_product['review_count']
            if review_increase >= 50:
                changes['review_surges'].append({
                    'product_id': product_id,
                    'product_name': new_product['product_name'],
                    'old_count': old_product['review_count'],
                    'new_count': new_product['review_count'],
                    'change_magnitude': review_increase
                })
        
        # 신규 상품 (현재에만 있음)
        new_ids = set(curr_dict.keys()) - set(prev_dict.keys())
        for product_id in new_ids:
            changes['new_products'].append(curr_dict[product_id])
        
        # 제거된 상품 (이전에만 있음)
        removed_ids = set(prev_dict.keys()) - set(curr_dict.keys())
        for product_id in removed_ids:
            changes['removed_products'].append(prev_dict[product_id])
        
        return changes


class CategoryMonitor:
    """간소화된 카테고리 모니터"""
    
    def __init__(self, category_config: dict, csv_baseline_path: str = None,
                 db_path: str = "improved_monitoring.db", headless: bool = True):
        self.category_config = category_config
        self.db = MonitoringDatabase(db_path)
        self.browser = BrowserManager(headless=headless)
        self.extractor = None
        self.change_detector = ChangeDetector()
        
        # 카테고리 등록 (URL 기반!)
        self.category_id = self.db.register_category(
            category_config['name'], 
            category_config['url']
        )
        
        # CSV 베이스라인 로드
        if csv_baseline_path and os.path.exists(csv_baseline_path):
            self.db.load_csv_baseline(csv_baseline_path)
        
        print(f"✅ 카테고리 '{category_config['name']}' 모니터 초기화 완료 (ID: {self.category_id})")
    
    def start_driver(self) -> bool:
        """브라우저 드라이버 시작"""
        print("🚀 Chrome 드라이버 시작 중...")
        if self.browser.start_driver():
            print("✅ Chrome 드라이버 시작 완료")
            self.extractor = ScrollExtractor(self.browser)
            return True
        print("❌ Chrome 드라이버 시작 실패")
        return False
    
    def run_monitoring_cycle(self) -> dict:
        """모니터링 사이클 실행"""
        category_name = self.category_config['name']
        page_url = self.category_config['url']
        
        print(f"\n{'='*70}")
        print(f"📊 [{category_name}] 모니터링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")
        
        start_time = time.time()
        
        try:
            if not self.extractor:
                print(f"❌ Extractor 초기화 안됨")
                return None
            
            # 1. 페이지 크롤링
            print(f"\n[1/5] 📜 페이지 크롤링 중...")
            current_products = self.extractor.extract_all_products_with_scroll(page_url)
            
            if not current_products:
                print(f"❌ 상품 수집 실패")
                return None
            
            print(f"✅ {len(current_products)}개 상품 수집 완료")
            
            # 2. 이전 데이터 조회
            print(f"\n[2/5] 🔍 이전 데이터 조회 중...")
            previous_products = self.db.get_latest_snapshot_data(self.category_id)
            print(f"✅ 이전 데이터: {len(previous_products)}개")
            
            # 3. 변화 감지 (product_id만 사용)
            print(f"\n[3/5] 🔄 변화 감지 중 (product_id 기반)...")
            changes = self.change_detector.detect_changes(previous_products, current_products)
            print(f"✅ 변화 감지 완료")
            
            # 4. 스냅샷 저장
            print(f"\n[4/5] 💾 스냅샷 저장 중...")
            crawl_duration = time.time() - start_time
            
            try:
                snapshot_id = self.db.save_snapshot(
                    self.category_id, page_url, current_products, crawl_duration
                )
                print(f"✅ 스냅샷 저장 완료: ID {snapshot_id}")
            except ValueError as e:
                print(f"❌ 스냅샷 저장 실패: {e}")
                return None
            
            # 5. 변화 이벤트 로깅
            print(f"\n[5/5] 📝 변화 이벤트 로깅 중...")
            self._log_changes(snapshot_id, changes)
            print(f"✅ 이벤트 로깅 완료")
            
            # 6. 변화 보고
            self._report_changes(category_name, changes)
            
            print(f"\n{'='*70}")
            print(f"✅ [{category_name}] 모니터링 완료 (소요시간: {crawl_duration:.1f}초)")
            print(f"{'='*70}\n")
            
            return changes
            
        except Exception as e:
            print(f"❌ [{category_name}] 모니터링 오류: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _log_changes(self, snapshot_id: int, changes: dict):
        """변화 이벤트 로깅"""
        # 신규 상품
        for product in changes['new_products']:
            self.db.log_change_event(
                snapshot_id, product['product_id'], self.category_id,
                'new_product', None, product['product_name'], 0,
                f"신규 상품 (순위: {product['rank']}위)"
            )
        
        # 제거된 상품
        for product in changes['removed_products']:
            self.db.log_change_event(
                snapshot_id, product['product_id'], self.category_id,
                'removed_product', product['product_name'], None, 0,
                f"제거된 상품 (이전 순위: {product['rank']}위)"
            )
        
        # 순위 변화
        for change in changes['rank_changes']:
            self.db.log_change_event(
                snapshot_id, change['product_id'], self.category_id,
                'rank_change', change['old_rank'], change['new_rank'],
                change['change_magnitude'],
                f"순위 {abs(change['change_magnitude'])}단계 {'상승' if change['change_magnitude'] > 0 else '하락'}"
            )
        
        # 가격 변화
        for change in changes['price_changes']:
            self.db.log_change_event(
                snapshot_id, change['product_id'], self.category_id,
                'price_change', change['old_price'], change['new_price'],
                change['change_magnitude'],
                f"가격 {abs(change['change_magnitude']):,}원 {'인상' if change['change_magnitude'] > 0 else '인하'}"
            )
        
        # 리뷰 급증
        for change in changes['review_surges']:
            self.db.log_change_event(
                snapshot_id, change['product_id'], self.category_id,
                'review_surge', change['old_count'], change['new_count'],
                change['change_magnitude'],
                f"리뷰 {change['change_magnitude']}개 급증"
            )
    
    def _report_changes(self, category_name: str, changes: dict):
        """변화 보고"""
        print(f"\n📈 [{category_name}] 변화 요약:")
        print(f"  • 신규: {len(changes['new_products'])}개")
        print(f"  • 제거: {len(changes['removed_products'])}개")
        print(f"  • 순위 변화: {len(changes['rank_changes'])}개")
        print(f"  • 가격 변화: {len(changes['price_changes'])}개")
        print(f"  • 리뷰 급증: {len(changes['review_surges'])}개")
        
        # 주요 순위 변화
        if changes['rank_changes']:
            print(f"\n🔥 주요 순위 변화 (TOP 3):")
            sorted_changes = sorted(changes['rank_changes'], 
                                   key=lambda x: abs(x['change_magnitude']), 
                                   reverse=True)
            for i, change in enumerate(sorted_changes[:3], 1):
                direction = "📈 상승" if change['change_magnitude'] > 0 else "📉 하락"
                print(f"  {i}. {change['product_name'][:40]}...")
                print(f"     {change['old_rank']}위 → {change['new_rank']}위 "
                      f"({direction} {abs(change['change_magnitude'])}단계)")
    
    def close(self):
        """리소스 정리"""
        if self.browser:
            self.browser.close()


class MultiCategoryMonitoringSystem:
    """다중 카테고리 모니터링 시스템"""
    
    def __init__(self, categories_config: list, csv_baseline_path: str = None,
                 db_path: str = "improved_monitoring.db", headless: bool = True):
        self.categories_config = categories_config
        self.csv_baseline_path = csv_baseline_path
        self.db_path = db_path
        self.headless = headless
        
        print(f"\n{'='*70}")
        print(f"🎯 간소화된 다중 카테고리 모니터링 시스템")
        print(f"{'='*70}")
        print(f"대상 카테고리: {len(categories_config)}개")
        for cat in categories_config:
            print(f"  • {cat['name']}")
        print(f"{'='*70}\n")
    
    def run_full_monitoring_cycle(self, cycles: int = 1):
        """전체 카테고리 모니터링 사이클 실행"""
        print(f"🚀 모니터링 시작 ({cycles}회 반복)\n")
        
        for cycle in range(cycles):
            if cycles > 1:
                print(f"\n{'='*70}")
                print(f"🔄 사이클 [{cycle + 1}/{cycles}]")
                print(f"{'='*70}\n")
            
            for i, category_config in enumerate(self.categories_config, 1):
                print(f"\n{'='*70}")
                print(f"📂 [{i}/{len(self.categories_config)}] {category_config['name']} 모니터링")
                print(f"{'='*70}")
                
                monitor = CategoryMonitor(
                    category_config=category_config,
                    csv_baseline_path=self.csv_baseline_path,
                    db_path=self.db_path,
                    headless=self.headless
                )
                
                try:
                    if not monitor.start_driver():
                        print(f"❌ {category_config['name']} 브라우저 시작 실패\n")
                        continue
                    
                    changes = monitor.run_monitoring_cycle()
                    
                    if changes:
                        print(f"✅ {category_config['name']} 모니터링 성공!")
                    else:
                        print(f"❌ {category_config['name']} 모니터링 실패")
                
                except KeyboardInterrupt:
                    print(f"\n⚠️ {category_config['name']} 모니터링 중단")
                    monitor.close()
                    return
                except Exception as e:
                    print(f"❌ {category_config['name']} 오류: {e}")
                finally:
                    monitor.close()
                
                # 카테고리 간 대기
                if i < len(self.categories_config):
                    wait_time = 30
                    print(f"\n⏰ 다음 카테고리까지 {wait_time}초 대기...\n")
                    time.sleep(wait_time)
            
            # 사이클 간 대기
            if cycle < cycles - 1:
                print(f"\n⏰ 다음 사이클까지 10분 대기...\n")
                time.sleep(600)
        
        print(f"\n{'='*70}")
        print(f"🎉 모든 모니터링 사이클 완료!")
        print(f"{'='*70}\n")


def main():
    """메인 함수"""
    
    # 카테고리 설정
    categories = [
        {
            'name': '헬스/건강식품',
            'url': 'https://shop.coupang.com/coupangus/74511?category=305433&platform=p&brandId=0'
        },
        {
            'name': '출산유아동',
            'url': 'https://shop.coupang.com/coupangus/74511?category=219079&platform=p&brandId=0'
        },
        {
            'name': '스포츠레저',
            'url': 'https://shop.coupang.com/coupangus/74511?category=317675&platform=p&brandId=0'
        }
    ]
    
    # CSV 베이스라인 경로
    csv_baseline = "coupang_iherb_products_updated_upc.csv"
    
    # 모니터링 시스템 생성
    monitoring_system = MultiCategoryMonitoringSystem(
        categories_config=categories,
        csv_baseline_path=csv_baseline if os.path.exists(csv_baseline) else None,
        db_path="improved_monitoring.db",
        headless=False
    )
    
    try:
        monitoring_system.run_full_monitoring_cycle(cycles=1)
        
    except KeyboardInterrupt:
        print("\n⚠️ 모니터링 중단됨")
    except Exception as e:
        print(f"❌ 시스템 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()