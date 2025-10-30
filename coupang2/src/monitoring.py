#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
로켓직구 모니터링 시스템 (수정됨)
- Config 기반 경로 관리
- 카테고리 중복 방지
"""

import sys
import os
import time
import re
import random
from datetime import datetime
from bs4 import BeautifulSoup

# 프로젝트 루트 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 쿠팡 크롤러
from coupang.coupang_manager import BrowserManager
from selenium.webdriver.common.by import By

# 프로젝트 모듈
from config.settings import Config
from src.database import MonitoringDatabase


class ScrollExtractor:
    """무한 스크롤 상품 추출기"""
    
    def __init__(self, browser_manager):
        self.browser = browser_manager
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
            
            filter_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'li.sortkey')
            
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
            
            # 가격 등 파싱
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


class RocketDirectMonitor:
    """로켓직구 카테고리 모니터"""
    
    def __init__(self, category_config: dict, headless: bool = True):
        """
        Args:
            category_config: Config.ROCKET_CATEGORIES의 항목
            headless: 헤드리스 모드
        """
        self.category_config = category_config
        self.db = MonitoringDatabase(Config.DB_PATH)
        self.browser = BrowserManager(headless=headless)
        self.extractor = None
        
        # 로켓직구 소스 등록
        self.source_id = self.db.register_source(
            'rocket_direct',
            '로켓직구',
            Config.ROCKET_BASE_URL
        )
        
        # 카테고리 등록 (중복 방지: category_id만 사용)
        self.category_id = self.db.register_category(
            category_config['name'],
            category_config['category_id']  # URL 파라미터 제외한 숫자만
        )
        
        print(f"✅ {category_config['name']} 모니터 초기화 완료")
    
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
        page_url = Config.ROCKET_BASE_URL + self.category_config['url_path']
        
        print(f"\n{'='*70}")
        print(f"📊 [{category_name}] 로켓직구 모니터링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")
        
        start_time = time.time()
        
        try:
            if not self.extractor:
                return {
                    'success': False,
                    'error_message': 'Extractor 초기화 실패',
                    'action': 'continue'
                }
            
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
            
            print(f"\n[2/2] 💾 스냅샷 저장 중...")
            crawl_duration = time.time() - start_time
            
            try:
                snapshot_id = self.db.save_snapshot(
                    self.source_id, self.category_id, page_url, 
                    current_products, crawl_duration, 
                    snapshot_time=datetime.now(),
                    error_message=None if filter_applied else "판매량순 필터 미적용"
                )
                print(f"✅ 스냅샷 저장 완료: ID {snapshot_id}")
                
            except ValueError as e:
                return {
                    'success': False,
                    'error_message': f'스냅샷 저장 실패: {e}',
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
    
    def close(self):
        """리소스 정리"""
        if self.browser:
            self.browser.close()


class RocketDirectMonitoringSystem:
    """로켓직구 다중 카테고리 모니터링 시스템"""
    
    def __init__(self, headless: bool = True):
        """
        Args:
            headless: 헤드리스 모드
        """
        self.headless = headless
    
    def interactive_selection(self) -> list:
        """터미널에서 카테고리 선택"""
        print(f"\n{'='*70}")
        print(f"🎯 크롤링 대상 선택")
        print(f"{'='*70}\n")
        
        print(f"📂 카테고리:")
        print("  0. 전체")
        for i, category in enumerate(Config.ROCKET_CATEGORIES, 1):
            print(f"  {i}. {category['name']}")
        
        while True:
            category_input = input("\n번호 선택 (쉼표 구분, 예: 1,2 또는 0): ").strip()
            
            if category_input == '0':
                selected = None
                print("  ✅ 전체 선택")
                break
            
            try:
                indices = [int(x.strip()) for x in category_input.split(',')]
                selected = [Config.ROCKET_CATEGORIES[i-1]['name'] for i in indices if 1 <= i <= len(Config.ROCKET_CATEGORIES)]
                
                if selected:
                    print(f"  ✅ 선택: {', '.join(selected)}")
                    break
                else:
                    print(f"  ❌ 유효하지 않은 번호")
            except (ValueError, IndexError):
                print(f"  ❌ 잘못된 입력")
        
        print(f"{'='*70}\n")
        return selected
    
    def run_full_monitoring_cycle(self, cycles: int = 1, 
                                  selected_categories: list = None,
                                  interactive: bool = False):
        """전체 카테고리 모니터링"""
        
        if interactive:
            selected_categories = self.interactive_selection()
        
        categories_to_run = Config.ROCKET_CATEGORIES
        if selected_categories:
            categories_to_run = [c for c in Config.ROCKET_CATEGORIES if c['name'] in selected_categories]
        
        total_jobs = len(categories_to_run)
        
        print(f"\n{'='*70}")
        print(f"🎯 로켓직구 모니터링 시작")
        print(f"{'='*70}")
        print(f"카테고리: {', '.join([c['name'] for c in categories_to_run])}")
        print(f"총 작업: {total_jobs}개")
        print(f"사이클: {cycles}회")
        print(f"{'='*70}\n")
        
        stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'filter_not_applied': 0
        }
        
        for cycle in range(cycles):
            if cycles > 1:
                print(f"\n{'='*70}")
                print(f"🔄 사이클 [{cycle + 1}/{cycles}]")
                print(f"{'='*70}\n")
            
            job_num = 1
            
            for category_config in categories_to_run:
                print(f"\n{'='*70}")
                print(f"📂 [{job_num}/{total_jobs}] {category_config['name']}")
                print(f"{'='*70}")
                
                stats['total'] += 1
                
                monitor = RocketDirectMonitor(
                    category_config=category_config,
                    headless=self.headless
                )
                
                try:
                    if not monitor.start_driver():
                        print(f"❌ 브라우저 시작 실패\n")
                        stats['failed'] += 1
                        job_num += 1
                        continue
                    
                    result = monitor.run_monitoring_cycle()
                    
                    if result['success']:
                        stats['success'] += 1
                        if not result.get('filter_applied', True):
                            stats['filter_not_applied'] += 1
                        print(f"✅ 성공: {result['product_count']}개")
                    else:
                        if result.get('action') == 'abort':
                            print(f"\n🛑 전체 중단")
                            monitor.close()
                            self._print_final_stats(stats)
                            return
                        
                        stats['skipped'] += 1
                        print(f"⏭️  건너뜀")
                
                except KeyboardInterrupt:
                    print(f"\n⚠️ 사용자 중단")
                    monitor.close()
                    self._print_final_stats(stats)
                    return
                except Exception as e:
                    print(f"❌ 오류: {e}")
                    stats['failed'] += 1
                finally:
                    monitor.close()
                
                job_num += 1
                
                if job_num <= total_jobs:
                    wait_time = 30
                    print(f"\n⏰ 다음 작업까지 {wait_time}초 대기...\n")
                    time.sleep(wait_time)
            
            if cycle < cycles - 1:
                print(f"\n⏰ 다음 사이클까지 10분 대기...\n")
                time.sleep(600)
        
        self._print_final_stats(stats)
    
    def _print_final_stats(self, stats: dict):
        """최종 통계"""
        print(f"\n{'='*70}")
        print(f"🎉 완료!")
        print(f"{'='*70}")
        print(f"총 작업: {stats['total']}개")
        print(f"  ✅ 성공: {stats['success']}개")
        print(f"  ❌ 실패: {stats['failed']}개")
        print(f"  ⏭️  건너뜀: {stats['skipped']}개")
        if stats['filter_not_applied'] > 0:
            print(f"  ⚠️  필터 미적용: {stats['filter_not_applied']}개")
        print(f"{'='*70}\n")


def main():
    """메인"""
    
    # 디렉토리 생성
    Config.ensure_directories()
    
    # DB 초기화
    db = MonitoringDatabase(Config.DB_PATH)
    db.init_database()
    db.cleanup_duplicate_categories()
    
    # 모니터링 시스템
    system = RocketDirectMonitoringSystem(headless=False)
    
    try:
        system.run_full_monitoring_cycle(cycles=1, interactive=True)
    except KeyboardInterrupt:
        print("\n⚠️ 중단됨")
    except Exception as e:
        print(f"❌ 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()