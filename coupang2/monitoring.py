#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
재설계된 모니터링 시스템 v2
- 순수 크롤링만 수행 (매칭 로직 제거)
- 로켓직구 / iHerb 공식 통합 지원
- matching_reference는 참조만
"""

import sys
import os
import time
import re
import random
import argparse
from datetime import datetime
from bs4 import BeautifulSoup

# 기존 모듈 임포트
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, os.path.join(parent_dir, 'coupang'))

from coupang.coupang_manager import BrowserManager
from selenium.webdriver.common.by import By

# 재설계된 DB 임포트
from database import MonitoringDatabase


class ScrollExtractor:
    """무한 스크롤 상품 추출기 (기존 코드 재사용)"""
    
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
            
            last_dom_count = 0
            same_count_runs = 0  # DOM 상품 수가 동일하게 유지된 연속 횟수

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
                    if consecutive_no_height_change >= 8 and no_new_products_count >= 8:
                        print(f"🏁 더 이상 신규 상품이 없습니다")
                        break
                else:
                    consecutive_no_height_change = 0
                
                # 현재 DOM에 보이는 상품 li 개수 (셀렉터 기준)
                try:
                    dom_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'li.product-wrap'))
                except:
                    dom_count = 0

                if dom_count == last_dom_count:
                    same_count_runs += 1
                else:
                    same_count_runs = 0
                last_dom_count = dom_count

                # --- 강화된 종료 조건 ---
                # 1) 페이지 높이 변화 없음이 누적되고
                # 2) DOM 상품 수가 여러 번 연속 그대로이고
                # 3) 로더가 보이지 않으며
                # 4) 실제 바닥에 닿아있다면 → 종료
                END_SAME_COUNT_THRESH = 5
                NO_HEIGHT_THRESH = 5

                if (not height_changed and
                    consecutive_no_height_change >= NO_HEIGHT_THRESH and
                    same_count_runs >= END_SAME_COUNT_THRESH and
                    not self._loader_visible() and
                    self._at_bottom()):
                    print("🏁 바닥 도달 + 로더 없음 + 상품 수 정지 → 스크롤 종료")
                    break

                if no_new_products_count >= max_no_new_attempts:
                    print(f"🏁 {max_no_new_attempts}회 연속 신규 없음, 크롤링 종료")
                    break
                
                # 스크롤 간 대기 시간 증가 (봇 감지 회피)
                time.sleep(random.uniform(2, 3))
            
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
            
            # 1. 상품명 (필수)
            name_elem = soup.select_one('div.name')
            if not name_elem:
                return None
            
            product_name = name_elem.get_text(strip=True)
            if not product_name:
                return None
            
            # 2. 현재가 (할인가)
            current_price = 0
            price_elem = soup.select_one('strong.price-value')
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price_text = re.sub(r'[^\d]', '', price_text)
                current_price = int(price_text) if price_text else 0
            
            # 3. 정가 (원가)
            original_price = 0
            original_elem = soup.select_one('del.base-price')
            if original_elem:
                price_text = original_elem.get_text(strip=True)
                price_text = re.sub(r'[^\d]', '', price_text)
                original_price = int(price_text) if price_text else 0
            
            if original_price == 0:
                original_price = current_price
            
            # 4. 할인율
            discount_rate = 0
            discount_elem = soup.select_one('span.discount-percentage')
            if discount_elem:
                discount_text = discount_elem.get_text(strip=True)
                discount_text = re.sub(r'[^\d]', '', discount_text)
                discount_rate = int(discount_text) if discount_text else 0
            
            # 5. 리뷰 수
            review_count = 0
            review_elem = soup.select_one('span.rating-total-count')
            if review_elem:
                review_text = review_elem.get_text(strip=True)
                review_text = re.sub(r'[^\d]', '', review_text)
                review_count = int(review_text) if review_text else 0
            
            # 6. 평점
            rating = 0.0
            rating_elem = soup.select_one('span.rating')
            if rating_elem:
                rating_text = rating_elem.get_text(strip=True)
                rating_text = rating_text.replace('별점', '').strip()
                try:
                    rating = float(rating_text)
                except:
                    rating = 0.0
            
            # 7. 배지
            badge = ""
            badge_elem = soup.select_one('span.badge-text')
            if badge_elem:
                badge = badge_elem.get_text(strip=True)
            
            # 8. 썸네일
            thumbnail_url = ""
            img_elem = soup.select_one('img.product-image')
            if img_elem:
                thumbnail_url = img_elem.get('src', '')
            
            return {
                'product_id': product_id,
                'product_name': product_name,
                'current_price': current_price,
                'original_price': original_price,
                'discount_rate': discount_rate,
                'review_count': review_count,
                'rating': rating,
                'badge': badge,
                'product_url': product_url,
                'thumbnail_url': thumbnail_url
            }
        except:
            return None
    
    def _scroll_to_bottom(self) -> bool:
        """페이지 하단으로 스크롤 (로딩바 확실히 트리거: 크게 올렸다가 다시 끝까지 내림)"""
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            viewport_h = self.driver.execute_script("return window.innerHeight || document.documentElement.clientHeight || 800")

            # 현재 스크롤 위치
            current_y = self.driver.execute_script("return window.pageYOffset || document.documentElement.scrollTop || 0")

            # 1) 먼저 자연스러운 하향 스텝 스크롤로 끝까지 진입
            steps = 5
            target_y = last_height
            step = max(1, int((target_y - current_y) / steps))
            for i in range(steps):
                self.driver.execute_script(f"window.scrollTo(0, {int(current_y + step*(i+1))});")
                time.sleep(random.uniform(0.25, 0.5))

            # 2) 로딩바를 유발하기 위해 '크게' 위로 올렸다가(뷰포트 0.8~1.2배) 다시 내림
            bump_up = int(viewport_h * random.uniform(0.9, 1.3))
            self.driver.execute_script(f"window.scrollBy(0, {-bump_up});")  # 크게 위로
            time.sleep(random.uniform(0.7, 1.2))

            # 3) 다시 끝까지 + 여유분으로 내리기 (트리거 강제)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # 하단 여유로 추가 스크롤 (일부 페이지는 끝자락 추가 이동이 트리거 됨)
            self.driver.execute_script("window.scrollBy(0, 300);")
            time.sleep(random.uniform(0.6, 1.0))

            # 4) 로딩바/높이 변화 대기 (최대 12초)
            #    - 높이 증가 또는 로딩 요소가 보였다가 사라지는 경우를 감지
            def has_loader():
                return self.driver.execute_script("""
                    const sels = [
                    '.loading', '.loading-bar', '.progress', '.spinner',
                    '.infinite-loader', '.search-loading-indicator'
                    ];
                    for (const s of sels) {
                    const el = document.querySelector(s);
                    if (el && getComputedStyle(el).display !== 'none' && el.offsetParent !== null) return true;
                    }
                    return false;
                """)

            saw_loader = False
            for _ in range(12):
                time.sleep(1)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if has_loader():
                    saw_loader = True
                if new_height > last_height:
                    print(f"    ↓ 페이지 확장됨: {last_height} -> {new_height}")
                    return True

            # 5) 첫 시도 실패 시 한 번 더 공격적으로 '큰 범프' 재시도
            #    (현재 높이의 10% 지점으로 점프 후 다시 끝까지)
            self.driver.execute_script("window.scrollTo(0, Math.floor(document.body.scrollHeight * 0.9));")
            time.sleep(random.uniform(0.4, 0.8))
            self.driver.execute_script(f"window.scrollBy(0, {-int(viewport_h * 1.2)});")  # 더 크게 올림
            time.sleep(random.uniform(0.6, 1.0))
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self.driver.execute_script("window.scrollBy(0, 500);")  # 여유분 더 크게
            time.sleep(random.uniform(0.6, 1.0))

            for _ in range(10):
                time.sleep(1)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height > last_height:
                    print(f"    ↓ 페이지 확장됨(2차): {last_height} -> {new_height}")
                    return True

            # 높이 변화가 없더라도 로더를 봤다면 다음 루프에서 변화를 기대
            if saw_loader:
                print("    ↺ 로딩바 감지됨(높이 변화 대기 중이었음)")
            return False

        except Exception as e:
            print(f"    ⚠️ 스크롤 오류: {e}")
            return False

    def _at_bottom(self) -> bool:
        """뷰포트가 실질적으로 페이지 바닥에 닿았는지"""
        try:
            return self.driver.execute_script("""
                const scrollY = window.scrollY || window.pageYOffset || 0;
                const innerH = window.innerHeight || document.documentElement.clientHeight || 0;
                const docH = document.body.scrollHeight || 0;
                return (scrollY + innerH) >= (docH - 4);  // 4px 여유
            """) is True
        except:
            return False

    def _loader_visible(self) -> bool:
        """무한스크롤 로더가 보이는지(일반적인 클래스들 대상)"""
        try:
            return self.driver.execute_script("""
                const sels = [
                    '.loading', '.loading-bar', '.progress', '.spinner',
                    '.infinite-loader', '.search-loading-indicator'
                ];
                for (const s of sels) {
                    const el = document.querySelector(s);
                    if (el && getComputedStyle(el).display !== 'none' && el.offsetParent !== null) {
                        return true;
                    }
                }
                return false;
            """) is True
        except:
            return False


class UnifiedMonitor:
    """통합 모니터 (로켓직구 + iHerb 공식)"""
    
    def __init__(self, source_type: str, config: dict, 
                 db_path: str = "monitoring.db", headless: bool = True):
        """
        통합 모니터 초기화
        
        Args:
            source_type: 'rocket_direct' or 'iherb_official'
            config: 설정 딕셔너리 (name, url, category_id 등)
            db_path: DB 경로
            headless: 헤드리스 모드
        """
        self.source_type = source_type
        self.config = config
        self.db = MonitoringDatabase(db_path)
        self.browser = BrowserManager(headless=headless)
        self.extractor = None
        
        # 카테고리 등록
        if config.get('category_id'):
            self.category_id = self.db.register_category(
                config['name'], 
                config['category_id']
            )
        else:
            self.category_id = None
        
        print(f"✅ {config['name']} 모니터 초기화 완료 ({source_type})")
    
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
        name = self.config['name']
        page_url = self.config['url']
        
        print(f"\n{'='*70}")
        print(f"📊 [{name}] 모니터링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📍 소스: {self.source_type}")
        print(f"{'='*70}")
        
        start_time = time.time()
        
        try:
            if not self.extractor:
                print(f"❌ Extractor 초기화 안됨")
                return None
            
            # 1. 페이지 크롤링
            print(f"\n[1/2] 📜 페이지 크롤링 중...")
            current_products = self.extractor.extract_all_products_with_scroll(page_url)
            
            if not current_products:
                print(f"❌ 상품 수집 실패")
                return None
            
            print(f"✅ {len(current_products)}개 상품 수집 완료")
            
            # 2. 스냅샷 저장 (순수 크롤링 데이터만)
            print(f"\n[2/2] 💾 스냅샷 저장 중...")
            crawl_duration = time.time() - start_time
            
            try:
                snapshot_id = self.db.save_snapshot(
                    self.source_type,
                    self.category_id,
                    page_url,
                    current_products,
                    crawl_duration
                )
                print(f"✅ 스냅샷 저장 완료: ID {snapshot_id}")
            except ValueError as e:
                print(f"❌ 스냅샷 저장 실패: {e}")
                return None
            
            print(f"\n{'='*70}")
            print(f"✅ [{name}] 모니터링 완료 (소요시간: {crawl_duration:.1f}초)")
            print(f"{'='*70}\n")
            
            return {
                'snapshot_id': snapshot_id,
                'product_count': len(current_products),
                'crawl_duration': crawl_duration
            }
            
        except Exception as e:
            print(f"❌ [{name}] 모니터링 오류: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def close(self):
        """리소스 정리"""
        if self.browser:
            self.browser.close()


def main():
    """메인 함수"""
    
    parser = argparse.ArgumentParser(description='통합 모니터링 시스템 v2')
    parser.add_argument('--source', choices=['rocket', 'official'],
                       help='모니터링 소스: rocket(로켓직구) or official(아이허브 공식) - 생략 시 모두 실행')
    parser.add_argument('--category', help='특정 카테고리만 (생략 시 전체)')
    parser.add_argument('--headless', action='store_true', help='헤드리스 모드')
    parser.add_argument('--db', default='monitoring.db', help='DB 파일 경로')
    
    args = parser.parse_args()
    
    # 소스 리스트 결정
    if args.source:
        # 특정 소스만 실행
        sources = [args.source]
    else:
        # 모든 소스 실행
        sources = ['rocket', 'official']
    
    # 모든 소스별 설정 생성
    all_source_configs = []
    
    for source in sources:
        if source == 'rocket':
            source_type = 'rocket_direct'
            
            # 로켓직구 카테고리 설정
            categories = [
                {
                    'name': '헬스/건강식품',
                    'category_id': '305433',
                    'url': 'https://shop.coupang.com/coupangus/74511?category=305433&platform=p&brandId=0'
                },
                {
                    'name': '출산유아동',
                    'category_id': '219079',
                    'url': 'https://shop.coupang.com/coupangus/74511?category=219079&platform=p&brandId=0'
                },
                {
                    'name': '스포츠레저',
                    'category_id': '317675',
                    'url': 'https://shop.coupang.com/coupangus/74511?category=317675&platform=p&brandId=0'
                }
            ]
        
        else:  # official
            source_type = 'iherb_official'
            
            # iHerb 공식 스토어 설정 (로켓직구와 동일한 카테고리)
            # storeId: 135493 (125596 아님!)
            categories = [
                {
                    'name': '헬스/건강식품',
                    'category_id': '305433',
                    'url': 'https://shop.coupang.com/iherb/135493?category=305433&platform=p&brandId=0&source=brandstore_direct'
                },
                {
                    'name': '출산유아동',
                    'category_id': '219079',
                    'url': 'https://shop.coupang.com/iherb/135493?category=219079&platform=p&brandId=0&source=brandstore_direct'
                },
                {
                    'name': '스포츠레저',
                    'category_id': '317675',
                    'url': 'https://shop.coupang.com/iherb/135493?category=317675&platform=p&brandId=0&source=brandstore_direct'
                }
            ]
        
        # 특정 카테고리 필터
        if args.category:
            categories = [c for c in categories if args.category.lower() in c['name'].lower()]
            if not categories:
                continue
        
        # 소스별 설정 추가
        for cat in categories:
            all_source_configs.append({
                'source_type': source_type,
                'config': cat
            })
    
    # 설정이 없으면 종료
    if not all_source_configs:
        print(f"❌ 실행할 소스/카테고리가 없습니다")
        return
    
    print(f"\n{'='*70}")
    print(f"🎯 모니터링 시작")
    print(f"{'='*70}")
    print(f"소스: {', '.join(sources)}")
    print(f"총 작업: {len(all_source_configs)}개")
    for idx, sc in enumerate(all_source_configs, 1):
        print(f"  {idx}. [{sc['source_type']}] {sc['config']['name']}")
    print(f"{'='*70}\n")
    
    # 순차 실행
    for i, source_config in enumerate(all_source_configs, 1):
        print(f"\n{'='*70}")
        print(f"📂 [{i}/{len(all_source_configs)}] [{source_config['source_type']}] {source_config['config']['name']}")
        print(f"{'='*70}")
        
        monitor = UnifiedMonitor(
            source_type=source_config['source_type'],
            config=source_config['config'],
            db_path=args.db,
            headless=args.headless
        )
        
        try:
            if not monitor.start_driver():
                print(f"❌ 브라우저 시작 실패\n")
                continue
            
            result = monitor.run_monitoring_cycle()
            
            if result:
                print(f"✅ 성공: {result['product_count']}개 제품")
            else:
                print(f"❌ 실패")
        
        except KeyboardInterrupt:
            print(f"\n⚠️ 사용자 중단")
            monitor.close()
            return
        except Exception as e:
            print(f"❌ 오류: {e}")
        finally:
            monitor.close()
        
        # 작업 간 대기
        if i < len(all_source_configs):
            wait_time = 30
            print(f"\n⏰ 다음 작업까지 {wait_time}초 대기...\n")
            time.sleep(wait_time)
    
    print(f"\n{'='*70}")
    print(f"🎉 모든 모니터링 완료!")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()