#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
순수 모니터링 시스템 - 실시간 데이터 수집 및 변화 감지
쿠팡 페이지의 모든 상품을 카테고리별로 수집하여 변화 추적
"""

import sys
import os
import time
import re
import sqlite3
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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class CategoryMonitoringDatabase:
    """카테고리별 모니터링 전용 데이터베이스"""
    
    def __init__(self, db_path="page_monitoring.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """데이터베이스 테이블 초기화 - 순수 모니터링용"""
        conn = sqlite3.connect(self.db_path)
        
        # 1. 카테고리 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                url TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 2. 매칭 참조 테이블 (CSV에서 로드)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS matching_reference (
                coupang_product_id TEXT PRIMARY KEY,
                coupang_product_name TEXT,
                coupang_product_url TEXT,
                original_category TEXT,
                iherb_upc TEXT,
                iherb_part_number TEXT,
                created_from_csv BOOLEAN DEFAULT TRUE,
                created_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 3. 페이지 스냅샷 테이블 (카테고리별)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS page_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id INTEGER NOT NULL,
                category_name TEXT NOT NULL,
                page_url TEXT NOT NULL,
                snapshot_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_products INTEGER,
                crawl_duration_seconds REAL,
                status TEXT DEFAULT 'completed',
                FOREIGN KEY (category_id) REFERENCES categories (id)
            )
        """)
        
        # 4. 상품 상태 테이블 (카테고리별 순위)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS product_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL,
                coupang_product_id TEXT NOT NULL,
                category_rank INTEGER NOT NULL,
                
                -- 쿠팡 실시간 정보
                product_name TEXT,
                product_url TEXT,
                current_price INTEGER,
                original_price INTEGER,
                discount_rate INTEGER,
                review_count INTEGER,
                rating_score REAL,
                is_rocket_delivery BOOLEAN,
                is_free_shipping BOOLEAN,
                cashback_amount INTEGER,
                delivery_info TEXT,
                
                -- 아이허브 매칭 정보
                iherb_upc TEXT,
                iherb_part_number TEXT,
                matching_status TEXT DEFAULT 'unknown',
                
                FOREIGN KEY (snapshot_id) REFERENCES page_snapshots (id)
            )
        """)
        
        # 5. 변화 이벤트 로그 테이블 (카테고리 정보 포함)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS change_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coupang_product_id TEXT NOT NULL,
                category_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                description TEXT,
                event_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 인덱스 생성
        conn.execute("CREATE INDEX IF NOT EXISTS idx_product_states_snapshot ON product_states(snapshot_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_product_states_product ON product_states(coupang_product_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_product_states_category_rank ON product_states(snapshot_id, category_rank)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_change_events_product ON change_events(coupang_product_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_change_events_category ON change_events(category_name)")
        
        conn.commit()
        conn.close()
    
    def register_category(self, name, url):
        """카테고리 등록"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO categories (name, url)
                VALUES (?, ?)
            """, (name, url))
            conn.commit()
            
            category_id = conn.execute("""
                SELECT id FROM categories WHERE name = ?
            """, (name,)).fetchone()[0]
            
            return category_id
        finally:
            conn.close()
    
    def load_csv_baseline(self, csv_path):
        """CSV 베이스라인 로드 (매칭 참조용)"""
        import pandas as pd
        
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            
            conn = sqlite3.connect(self.db_path)
            
            for _, row in df.iterrows():
                product_id = str(row.get('coupang_product_id', ''))
                if not product_id:
                    continue
                
                conn.execute("""
                    INSERT OR REPLACE INTO matching_reference 
                    (coupang_product_id, coupang_product_name, coupang_product_url,
                     original_category, iherb_upc, iherb_part_number)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    product_id,
                    str(row.get('coupang_product_name', '')),
                    str(row.get('coupang_product_url', '')),
                    str(row.get('category', '')),
                    str(row.get('iherb_upc', '')),
                    str(row.get('iherb_part_number', ''))
                ))
            
            conn.commit()
            conn.close()
            
            print(f"CSV 베이스라인 로드 완료: {len(df)}개 상품")
            
        except Exception as e:
            print(f"CSV 로드 실패: {e}")
    
    def save_snapshot(self, category_name, page_url, products, crawl_duration):
        """스냅샷 저장"""
        conn = sqlite3.connect(self.db_path)
        
        # 카테고리 ID 조회
        category_id = conn.execute("""
            SELECT id FROM categories WHERE name = ?
        """, (category_name,)).fetchone()[0]
        
        # 스냅샷 생성
        cursor = conn.execute("""
            INSERT INTO page_snapshots 
            (category_id, category_name, page_url, total_products, crawl_duration_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, (category_id, category_name, page_url, len(products), crawl_duration))
        
        snapshot_id = cursor.lastrowid
        
        # 상품 상태 저장
        for product in products:
            # 매칭 정보 조회
            matching_info = conn.execute("""
                SELECT iherb_upc, iherb_part_number 
                FROM matching_reference 
                WHERE coupang_product_id = ?
            """, (product['product_id'],)).fetchone()
            
            iherb_upc = matching_info[0] if matching_info else None
            iherb_part_number = matching_info[1] if matching_info else None
            matching_status = 'matched' if iherb_upc else 'unmatched'
            
            conn.execute("""
                INSERT INTO product_states 
                (snapshot_id, coupang_product_id, category_rank,
                 product_name, product_url, current_price, original_price,
                 discount_rate, review_count, rating_score,
                 is_rocket_delivery, is_free_shipping, cashback_amount, delivery_info,
                 iherb_upc, iherb_part_number, matching_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot_id, product['product_id'], product['rank'],
                product['product_name'], product['product_url'],
                product.get('current_price', 0), product.get('original_price', 0),
                product.get('discount_rate', 0), product.get('review_count', 0),
                product.get('rating_score', 0.0),
                product.get('is_rocket_delivery', False),
                product.get('is_free_shipping', False),
                product.get('cashback_amount', 0),
                product.get('delivery_info', ''),
                iherb_upc, iherb_part_number, matching_status
            ))
        
        conn.commit()
        conn.close()
        
        return snapshot_id
    
    def get_latest_snapshot_data(self, category_name):
        """최신 스냅샷 데이터 조회"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            ps.coupang_product_id as product_id,
            ps.category_rank as rank,
            ps.product_name,
            ps.product_url,
            ps.current_price,
            ps.original_price,
            ps.discount_rate,
            ps.review_count
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        WHERE snap.category_name = ?
        AND snap.id = (
            SELECT MAX(id) FROM page_snapshots WHERE category_name = ?
        )
        ORDER BY ps.category_rank
        """
        
        cursor = conn.execute(query, (category_name, category_name))
        products = []
        
        for row in cursor:
            products.append({
                'product_id': row[0],
                'rank': row[1],
                'product_name': row[2],
                'product_url': row[3],
                'current_price': row[4],
                'original_price': row[5],
                'discount_rate': row[6],
                'review_count': row[7]
            })
        
        conn.close()
        return products
    
    def log_change_event(self, product_id, category_name, event_type, old_value, new_value, description):
        """변화 이벤트 로깅"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO change_events 
            (coupang_product_id, category_name, event_type, old_value, new_value, description)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (product_id, category_name, event_type, str(old_value), str(new_value), description))
        conn.commit()
        conn.close()


class InfiniteScrollExtractor:
    """무한 스크롤 상품 추출기"""
    
    def __init__(self, browser_manager):
        self.browser = browser_manager
    
    @property
    def driver(self):
        """브라우저 드라이버 접근"""
        return self.browser.driver if self.browser else None
    
    def extract_all_products_with_scroll(self, page_url):
        """무한 스크롤로 모든 상품 추출"""
        if not self.driver:
            print("❌ 브라우저 드라이버가 초기화되지 않았습니다")
            return []
        
        try:
            print(f"무한 스크롤 크롤링 시작: {page_url}")
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
            
            print("📜 무한 스크롤로 모든 상품 수집 중...")
            
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
                    
                    # 5회 연속 높이 변화 없으면서 신규도 없으면 종료
                    if consecutive_no_height_change >= 5 and no_new_products_count >= 5:
                        print(f"🏁 더 이상 신규 상품이 없습니다")
                        break
                else:
                    consecutive_no_height_change = 0
                
                # 오래 신규 없으면 종료
                if no_new_products_count >= max_no_new_attempts:
                    print(f"🏁 {max_no_new_attempts}회 연속 신규 없음, 크롤링 종료")
                    break
                
                time.sleep(random.uniform(1, 2))
            
            print(f"✅ 무한 스크롤 완료: 총 {len(all_products)}개 상품 수집")
            return all_products
            
        except Exception as e:
            print(f"❌ 크롤링 오류: {e}")
            return []
    
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
    
    def _extract_products_from_current_page(self, seen_product_ids):
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
    
    def _parse_product_data(self, element, product_id, product_url):
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
                'rating_score': rating_score,
                'rank': 0
            }
            
        except:
            return None
    
    def _scroll_to_bottom(self):
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
            
            # 새 콘텐츠 로딩 대기 (최대 5초)
            for _ in range(5):
                time.sleep(1)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height > last_height:
                    return True
            
            return False
            
        except:
            return False


class CategoryChangeDetector:
    """카테고리 변화 감지기"""
    
    def detect_changes(self, previous_products, current_products):
        """이전 데이터와 현재 데이터 비교"""
        changes = {
            'new_products': [],
            'removed_products': [],
            'rank_changes': [],
            'price_changes': []
        }
        
        # 순위 할당
        for idx, product in enumerate(current_products, 1):
            product['rank'] = idx
        
        # 이전 상품 ID 세트
        prev_ids = {p['product_id']: p for p in previous_products}
        curr_ids = {p['product_id']: p for p in current_products}
        
        # 신규 상품
        for pid in curr_ids:
            if pid not in prev_ids:
                changes['new_products'].append(curr_ids[pid])
        
        # 제거된 상품
        for pid in prev_ids:
            if pid not in curr_ids:
                changes['removed_products'].append(prev_ids[pid])
        
        # 순위 및 가격 변화
        for pid in set(prev_ids.keys()) & set(curr_ids.keys()):
            old_product = prev_ids[pid]
            new_product = curr_ids[pid]
            
            # 순위 변화
            if old_product['rank'] != new_product['rank']:
                changes['rank_changes'].append({
                    'product_id': pid,
                    'product_name': new_product['product_name'],
                    'old_rank': old_product['rank'],
                    'new_rank': new_product['rank'],
                    'rank_change': old_product['rank'] - new_product['rank']
                })
            
            # 가격 변화
            if old_product['current_price'] != new_product['current_price']:
                changes['price_changes'].append({
                    'product_id': pid,
                    'product_name': new_product['product_name'],
                    'old_price': old_product['current_price'],
                    'new_price': new_product['current_price'],
                    'price_change': new_product['current_price'] - old_product['current_price']
                })
        
        return changes


class CategoryPageMonitor:
    """카테고리별 페이지 모니터링 메인 클래스"""
    
    def __init__(self, category_config, csv_baseline_path=None, db_path="page_monitoring.db", headless=True):
        self.category_config = category_config
        self.db = CategoryMonitoringDatabase(db_path)
        self.browser = BrowserManager(headless=headless)
        self.extractor = None
        self.change_detector = CategoryChangeDetector()
        
        # 카테고리 등록
        self.category_id = self.db.register_category(
            category_config['name'], 
            category_config['url']
        )
        
        # CSV 베이스라인 로드 (초기 1회만)
        if csv_baseline_path:
            self.db.load_csv_baseline(csv_baseline_path)
        
        print(f"카테고리 '{category_config['name']}' 모니터링 시스템 초기화 완료")
    
    def start_driver(self):
        """브라우저 드라이버 시작"""
        print("Chrome 드라이버 시작 중...")
        if self.browser.start_driver():
            print("Chrome 드라이버 시작 완료")
            self.extractor = InfiniteScrollExtractor(self.browser)
            return True
        print("❌ Chrome 드라이버 시작 실패")
        return False
    
    def run_monitoring_cycle(self):
        """카테고리별 모니터링 사이클 실행"""
        category_name = self.category_config['name']
        page_url = self.category_config['url']
        
        print(f"\n{'='*60}")
        print(f"[{category_name}] 모니터링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        start_time = time.time()
        
        try:
            if not self.extractor:
                print(f"❌ Extractor 초기화 안됨")
                return None
            
            # 1. 페이지 크롤링
            current_products = self.extractor.extract_all_products_with_scroll(page_url)
            
            if not current_products:
                print(f"❌ 상품 수집 실패: 상품이 없습니다")
                return None
            
            print(f"✅ {len(current_products)}개 상품 수집 완료")
            
            # 2. 이전 데이터와 비교
            previous_products = self.db.get_latest_snapshot_data(category_name)
            changes = self.change_detector.detect_changes(previous_products, current_products)
            
            # 3. 변화 보고
            self.report_changes(category_name, changes)
            
            # 4. 변화 이벤트 로깅
            self.log_changes(category_name, changes)
            
            # 5. 스냅샷 저장
            crawl_duration = time.time() - start_time
            snapshot_id = self.db.save_snapshot(category_name, page_url, current_products, crawl_duration)
            
            print(f"스냅샷 저장 완료: ID {snapshot_id}, {len(current_products)}개 상품")
            return changes
            
        except Exception as e:
            print(f"❌ [{category_name}] 모니터링 오류: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def report_changes(self, category_name, changes):
        """카테고리별 변화 보고"""
        print(f"\n[{category_name}] 변화 감지:")
        print(f"  신규: {len(changes['new_products'])}개")
        print(f"  제거: {len(changes['removed_products'])}개")
        print(f"  순위 변화: {len(changes['rank_changes'])}개")
        print(f"  가격 변화: {len(changes['price_changes'])}개")
        
        # 주요 순위 변화
        if changes['rank_changes']:
            print(f"\n주요 순위 변화:")
            for change in changes['rank_changes'][:5]:
                direction = "↑" if change['rank_change'] > 0 else "↓"
                print(f"  {change['product_name'][:30]}: {change['old_rank']}위 → {change['new_rank']}위 {direction}")
        
        # 주요 가격 변화
        if changes['price_changes']:
            print(f"\n주요 가격 변화:")
            for change in changes['price_changes'][:5]:
                direction = "↑" if change['price_change'] > 0 else "↓"
                print(f"  {change['product_name'][:30]}: {change['old_price']:,}원 → {change['new_price']:,}원 {direction}")
    
    def log_changes(self, category_name, changes):
        """카테고리별 변화 이벤트 로깅"""
        # 순위 변화 로깅
        for change in changes['rank_changes']:
            self.db.log_change_event(
                change['product_id'], category_name, 'rank_change',
                change['old_rank'], change['new_rank'],
                f"순위 {abs(change['rank_change'])}단계 {'상승' if change['rank_change'] > 0 else '하락'}"
            )
        
        # 가격 변화 로깅
        for change in changes['price_changes']:
            self.db.log_change_event(
                change['product_id'], category_name, 'price_change',
                change['old_price'], change['new_price'],
                f"가격 {abs(change['price_change']):,}원 {'인상' if change['price_change'] > 0 else '인하'}"
            )
        
        # 신규 상품 로깅
        for product in changes['new_products']:
            self.db.log_change_event(
                product['product_id'], category_name, 'new_product',
                None, product['product_name'],
                f"신규 상품"
            )
    
    def close(self):
        """리소스 정리"""
        if self.browser:
            self.browser.close()


class MultiCategoryMonitoringSystem:
    """다중 카테고리 모니터링 시스템"""
    
    def __init__(self, categories_config, csv_baseline_path=None, db_path="page_monitoring.db", headless=True):
        self.categories_config = categories_config
        self.csv_baseline_path = csv_baseline_path
        self.db_path = db_path
        self.headless = headless
        
        print(f"다중 카테고리 모니터링 시스템 초기화")
        print(f"대상 카테고리: {len(categories_config)}개")
    
    def run_full_monitoring_cycle(self, cycles=1):
        """전체 카테고리 모니터링 사이클 실행"""
        print(f"\n🎯 다중 카테고리 모니터링 시작 ({cycles}회 반복)")
        print("="*80)
        
        for cycle in range(cycles):
            if cycles > 1:
                print(f"\n[사이클 {cycle + 1}/{cycles}]")
            
            # 각 카테고리별 모니터링
            for i, category_config in enumerate(self.categories_config, 1):
                print(f"\n[{i}/{len(self.categories_config)}] 📂 {category_config['name']} 모니터링")
                
                monitor = CategoryPageMonitor(
                    category_config=category_config,
                    csv_baseline_path=self.csv_baseline_path,
                    db_path=self.db_path,
                    headless=self.headless
                )
                
                try:
                    if not monitor.start_driver():
                        print(f"❌ {category_config['name']} 브라우저 시작 실패")
                        continue
                    
                    changes = monitor.run_monitoring_cycle()
                    
                    if changes:
                        print(f"✅ {category_config['name']} 모니터링 완료!")
                    else:
                        print(f"❌ {category_config['name']} 모니터링 실패")
                
                except KeyboardInterrupt:
                    print(f"\n⚠️ {category_config['name']} 모니터링 중단")
                    monitor.close()
                    return
                except Exception as e:
                    print(f"❌ {category_config['name']} 오류 발생: {e}")
                finally:
                    monitor.close()
                
                # 카테고리 간 대기 (봇 탐지 방지)
                if i < len(self.categories_config):
                    wait_time = 30 if cycle == 0 else 60
                    print(f"다음 카테고리까지 {wait_time}초 대기...")
                    time.sleep(wait_time)
            
            # 사이클 간 대기
            if cycle < cycles - 1:
                print(f"\n다음 사이클까지 10분 대기...")
                time.sleep(600)
        
        print(f"\n🎉 모든 모니터링 사이클 완료!")


def main():
    """메인 함수 - 다중 카테고리 모니터링"""
    
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
    
    csv_baseline = "coupang_iherb_products.csv"
    
    monitoring_system = MultiCategoryMonitoringSystem(
        categories_config=categories,
        csv_baseline_path=csv_baseline,
        headless=False
    )
    
    try:
        monitoring_system.run_full_monitoring_cycle(cycles=2)
        
    except KeyboardInterrupt:
        print("\n⚠️ 모니터링 중단됨")
    except Exception as e:
        print(f"❌ 시스템 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()