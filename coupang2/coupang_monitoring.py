#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
페이지 모니터링 시스템 - 카테고리별 독립 관리 + 중복 상품 처리
쿠팡 페이지의 모든 상품을 카테고리별로 무한 스크롤 수집하여 변화 추적
"""

import sys
import os
import time
import re
import sqlite3
import pandas as pd
import random
import json
from datetime import datetime
from bs4 import BeautifulSoup

# 기존 모듈 임포트 - 경로 수정
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)  # iherb_price 디렉토리
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
        """데이터베이스 테이블 초기화 - 카테고리별 독립 관리"""
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
        
        # 5. 중복 상품 추적 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS product_cross_category (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coupang_product_id TEXT NOT NULL,
                category_combinations TEXT NOT NULL,  -- JSON 배열
                category_count INTEGER NOT NULL,
                first_seen_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(coupang_product_id)
            )
        """)
        
        # 6. 상품 마스터 테이블 (통합 정보)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS product_master (
                coupang_product_id TEXT PRIMARY KEY,
                product_name TEXT,
                product_url TEXT,
                iherb_upc TEXT,
                iherb_part_number TEXT,
                total_categories INTEGER DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 7. 변화 이벤트 로그 테이블 (카테고리 정보 포함)
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
        
        print(f"데이터베이스 초기화 완료: {self.db_path}")
    
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
    
    def get_category_id(self, name):
        """카테고리 ID 조회"""
        conn = sqlite3.connect(self.db_path)
        try:
            result = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
            return result[0] if result else None
        finally:
            conn.close()
    
    def load_csv_baseline(self, csv_path):
        """CSV에서 베이스라인 로드"""
        if not os.path.exists(csv_path):
            print(f"베이스라인 파일 없음: {csv_path}")
            return
        
        print(f"CSV 베이스라인 로딩 시작: {csv_path}")
        
        conn = sqlite3.connect(self.db_path)
        
        # 기존 데이터 확인
        existing_count = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
        if existing_count > 0:
            print(f"베이스라인 이미 로드됨: {existing_count}개")
            conn.close()
            return
        
        # CSV 읽기
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            loaded_count = 0
            
            for _, row in df.iterrows():
                try:
                    # 상품 ID 추출
                    url = str(row.get('쿠팡_상품URL', ''))
                    product_id = self.extract_product_id_from_url(url)
                    
                    if not product_id:
                        continue
                    
                    conn.execute("""
                        INSERT OR IGNORE INTO matching_reference 
                        (coupang_product_id, coupang_product_name, coupang_product_url, 
                         original_category, iherb_upc, iherb_part_number)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        product_id,
                        str(row.get('쿠팡_제품명', '')),
                        url,
                        str(row.get('카테고리', '')),
                        str(row.get('아이허브_UPC', '')),
                        str(row.get('아이허브_파트넘버', ''))
                    ))
                    loaded_count += 1
                    
                except Exception as e:
                    continue
            
            conn.commit()
            print(f"베이스라인 로딩 완료: {loaded_count}개 상품")
            
        except Exception as e:
            print(f"베이스라인 로딩 오류: {e}")
        finally:
            conn.close()
    
    def extract_product_id_from_url(self, url):
        """URL에서 상품 ID 추출"""
        if not url:
            return None
        match = re.search(r'itemId=(\d+)', url)
        return match.group(1) if match else None
    
    def get_latest_snapshot_data(self, category_name):
        """카테고리별 최신 스냅샷 데이터 조회"""
        conn = sqlite3.connect(self.db_path)
        
        # 해당 카테고리의 최신 스냅샷 ID 조회
        snapshot_id = conn.execute("""
            SELECT id FROM page_snapshots 
            WHERE category_name = ? 
            ORDER BY snapshot_time DESC LIMIT 1
        """, (category_name,)).fetchone()
        
        if not snapshot_id:
            conn.close()
            return []
        
        # 해당 스냅샷의 상품 데이터 조회
        products = conn.execute("""
            SELECT coupang_product_id, category_rank, product_name, current_price, 
                   original_price, discount_rate, review_count, rating_score
            FROM product_states 
            WHERE snapshot_id = ?
            ORDER BY category_rank
        """, (snapshot_id[0],)).fetchall()
        
        conn.close()
        
        return [
            {
                'coupang_product_id': p[0],
                'category_rank': p[1],
                'product_name': p[2],
                'current_price': p[3],
                'original_price': p[4],
                'discount_rate': p[5],
                'review_count': p[6],
                'rating_score': p[7]
            }
            for p in products
        ]
    
    def save_snapshot(self, category_name, page_url, products, crawl_duration):
        """카테고리별 스냅샷 저장"""
        conn = sqlite3.connect(self.db_path)
        
        # 카테고리 ID 조회
        category_id = self.get_category_id(category_name)
        if not category_id:
            print(f"카테고리 '{category_name}' 등록되지 않음")
            return None
        
        # 스냅샷 레코드 생성
        cursor = conn.execute("""
            INSERT INTO page_snapshots (category_id, category_name, page_url, total_products, crawl_duration_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, (category_id, category_name, page_url, len(products), crawl_duration))
        
        snapshot_id = cursor.lastrowid
        
        # 상품 데이터 저장
        for rank, product in enumerate(products, 1):
            # 매칭 정보 조회
            matching_info = conn.execute("""
                SELECT iherb_upc, iherb_part_number 
                FROM matching_reference 
                WHERE coupang_product_id = ?
            """, (product['product_id'],)).fetchone()
            
            iherb_upc = matching_info[0] if matching_info else None
            iherb_part_number = matching_info[1] if matching_info else None
            matching_status = 'matched' if matching_info else 'unknown'
            
            conn.execute("""
                INSERT INTO product_states (
                    snapshot_id, coupang_product_id, category_rank,
                    product_name, product_url, current_price, original_price, discount_rate,
                    review_count, rating_score, is_rocket_delivery, is_free_shipping,
                    cashback_amount, delivery_info, iherb_upc, iherb_part_number, matching_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot_id, product['product_id'], rank,
                product['product_name'], product['product_url'], product['current_price'],
                product.get('original_price', 0), product.get('discount_rate', 0),
                product.get('review_count', 0), product.get('rating_score', 0.0),
                product.get('is_rocket_delivery', False), product.get('is_free_shipping', False),
                product.get('cashback_amount', 0), product.get('delivery_info', ''),
                iherb_upc, iherb_part_number, matching_status
            ))
            
            # 상품 마스터 정보 업데이트
            self._update_product_master(conn, product['product_id'], product)
        
        conn.commit()
        
        # 중복 상품 추적 업데이트
        self._update_cross_category_tracking(snapshot_id)
        
        conn.close()
        return snapshot_id
    
    def _update_product_master(self, conn, product_id, product_data):
        """상품 마스터 테이블 업데이트"""
        # 기존 레코드 확인
        existing = conn.execute("""
            SELECT total_categories FROM product_master WHERE coupang_product_id = ?
        """, (product_id,)).fetchone()
        
        if existing:
            # 기존 레코드 업데이트
            conn.execute("""
                UPDATE product_master 
                SET product_name = ?, product_url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE coupang_product_id = ?
            """, (product_data['product_name'], product_data['product_url'], product_id))
        else:
            # 새 레코드 생성
            conn.execute("""
                INSERT INTO product_master 
                (coupang_product_id, product_name, product_url, total_categories)
                VALUES (?, ?, ?, ?)
            """, (product_id, product_data['product_name'], product_data['product_url'], 1))
    
    def _update_cross_category_tracking(self, current_snapshot_id):
        """중복 상품 추적 업데이트"""
        conn = sqlite3.connect(self.db_path)
        
        # 현재 스냅샷의 카테고리 정보
        current_category = conn.execute("""
            SELECT category_name FROM page_snapshots WHERE id = ?
        """, (current_snapshot_id,)).fetchone()
        
        if not current_category:
            return
        
        current_category_name = current_category[0]
        
        # 현재 스냅샷의 모든 상품 조회
        current_products = conn.execute("""
            SELECT DISTINCT coupang_product_id FROM product_states WHERE snapshot_id = ?
        """, (current_snapshot_id,)).fetchall()
        
        for (product_id,) in current_products:
            # 해당 상품이 다른 카테고리에도 있는지 확인 (최신 스냅샷 기준)
            other_categories = conn.execute("""
                SELECT DISTINCT snap.category_name 
                FROM product_states ps
                JOIN page_snapshots snap ON ps.snapshot_id = snap.id
                WHERE ps.coupang_product_id = ? 
                AND snap.category_name != ?
                AND snap.id IN (
                    SELECT MAX(id) FROM page_snapshots GROUP BY category_name
                )
            """, (product_id, current_category_name)).fetchall()
            
            all_categories = [current_category_name] + [cat[0] for cat in other_categories]
            all_categories.sort()  # 일관된 순서
            
            if len(all_categories) > 1:
                # 중복 상품으로 기록
                category_json = json.dumps(all_categories, ensure_ascii=False)
                
                conn.execute("""
                    INSERT OR REPLACE INTO product_cross_category 
                    (coupang_product_id, category_combinations, category_count, last_updated)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """, (product_id, category_json, len(all_categories)))
                
                # 상품 마스터의 카테고리 수 업데이트
                conn.execute("""
                    UPDATE product_master 
                    SET total_categories = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE coupang_product_id = ?
                """, (len(all_categories), product_id))
        
        conn.commit()
        conn.close()
    
    def log_change_event(self, product_id, category_name, event_type, old_value, new_value, description):
        """변화 이벤트 로깅 (카테고리 정보 포함)"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO change_events (coupang_product_id, category_name, event_type, old_value, new_value, description)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (product_id, category_name, event_type, str(old_value), str(new_value), description))
        conn.commit()
        conn.close()


class CrossCategoryAnalyzer:
    """중복 상품 분석기"""
    
    def __init__(self, db_path="page_monitoring.db"):
        self.db_path = db_path
    
    def get_duplicate_products(self):
        """여러 카테고리에 나타나는 중복 상품 조회"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            pcc.coupang_product_id,
            pm.product_name,
            pcc.category_combinations,
            pcc.category_count,
            pcc.last_updated
        FROM product_cross_category pcc
        JOIN product_master pm ON pcc.coupang_product_id = pm.coupang_product_id
        WHERE pcc.category_count > 1
        ORDER BY pcc.category_count DESC, pcc.last_updated DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def compare_cross_category_ranks(self, product_id):
        """동일 상품의 카테고리별 순위 비교"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            snap.category_name,
            ps.category_rank,
            ps.current_price,
            snap.snapshot_time
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        WHERE ps.coupang_product_id = ?
        AND snap.id IN (
            SELECT MAX(id) FROM page_snapshots GROUP BY category_name
        )
        ORDER BY ps.category_rank
        """
        
        df = pd.read_sql_query(query, conn, params=[product_id])
        conn.close()
        
        return df
    
    def get_cross_category_trends(self, days=7):
        """중복 상품의 카테고리별 순위 트렌드"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            ps.coupang_product_id,
            pm.product_name,
            snap.category_name,
            ps.category_rank,
            ps.current_price,
            snap.snapshot_time
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        JOIN product_master pm ON ps.coupang_product_id = pm.coupang_product_id
        WHERE pm.total_categories > 1
        AND snap.snapshot_time >= datetime('now', '-{} days')
        ORDER BY ps.coupang_product_id, snap.category_name, snap.snapshot_time
        """.format(days)
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df


class InfiniteScrollExtractor:
    """무한 스크롤 상품 추출기"""
    
    def __init__(self, browser_manager):
        self.browser = browser_manager
        self.driver = browser_manager.driver
    
    def extract_all_products_with_scroll(self, page_url):
        """무한 스크롤로 모든 상품 추출"""
        print(f"무한 스크롤 크롤링 시작: {page_url}")
        
        self.driver.get(page_url)
        time.sleep(random.uniform(3, 5))
        
        # 판매량순 필터 적용
        self._click_sales_filter()
        
        all_products = []
        seen_product_ids = set()
        scroll_count = 0
        max_scrolls = 100  # 최대 스크롤 횟수
        no_new_products_count = 0
        max_no_new_attempts = 10
        
        print("📜 무한 스크롤로 모든 상품 수집 중...")
        
        while scroll_count < max_scrolls:
            scroll_count += 1
            
            # 현재 페이지에서 상품 추출
            new_products = self._extract_products_from_current_page(seen_product_ids)
            
            if new_products:
                all_products.extend(new_products)
                no_new_products_count = 0
                print(f"  [스크롤 {scroll_count}] 신규: {len(new_products)}개, 총: {len(all_products)}개")
            else:
                no_new_products_count += 1
                print(f"  [스크롤 {scroll_count}] 신규: 0개 (연속 {no_new_products_count}회)")
            
            # 더 이상 신규 상품이 없으면 종료
            if no_new_products_count >= max_no_new_attempts:
                print(f"🏁 더 이상 신규 상품이 없습니다 (연속 {max_no_new_attempts}회)")
                break
            
            # 스크롤 다운
            if not self._scroll_down_and_wait():
                print("🏁 페이지 끝에 도달했습니다")
                break
            
            time.sleep(random.uniform(1, 2))
        
        print(f"✅ 무한 스크롤 완료: 총 {len(all_products)}개 상품 수집")
        return all_products
    
    def _click_sales_filter(self):
        """판매량순 필터 클릭"""
        try:
            print("🔍 판매량순 필터 적용 중...")
            time.sleep(2)
            
            filter_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'li.sortkey')
            
            for button in filter_buttons:
                if '판매량순' in button.text:
                    button.click()
                    print("✅ 판매량순 필터 적용 완료")
                    time.sleep(3)
                    return
            
            print("⚠️ 판매량순 필터 없음 (기본 정렬 사용)")
        except Exception as e:
            print(f"⚠️ 필터 적용 오류: {e}")
    
    def _extract_products_from_current_page(self, seen_product_ids):
        """현재 페이지에서 신규 상품만 추출"""
        try:
            new_products = []
            
            # 상품 요소들 찾기
            product_elements = self.driver.find_elements(By.CSS_SELECTOR, 'li.product-wrap')
            
            for element in product_elements:
                try:
                    # 상품 링크 찾기
                    link_elem = element.find_element(By.CSS_SELECTOR, 'a.product-wrapper')
                    product_url = link_elem.get_attribute('href')
                    
                    # 상품 ID 추출
                    match = re.search(r'itemId=(\d+)', product_url)
                    if not match:
                        continue
                    
                    product_id = match.group(1)
                    
                    # 중복 체크
                    if product_id in seen_product_ids:
                        continue
                    
                    # 상품 정보 추출
                    product_info = self._extract_product_info(element, product_id, product_url)
                    if product_info:
                        new_products.append(product_info)
                        seen_product_ids.add(product_id)
                
                except Exception as e:
                    continue
            
            return new_products
            
        except Exception as e:
            print(f"상품 추출 오류: {e}")
            return []
    
    def _extract_product_info(self, element, product_id, product_url):
        """개별 상품 정보 추출"""
        try:
            # 상품명
            try:
                name_elem = element.find_element(By.CSS_SELECTOR, 'div.name')
                product_name = name_elem.text.strip()
            except:
                return None
            
            # 가격
            try:
                price_elem = element.find_element(By.CSS_SELECTOR, 'strong.price-value')
                price_text = price_elem.text.strip()
                current_price = self._extract_number(price_text)
            except:
                current_price = 0
            
            # 할인 정보
            try:
                discount_elem = element.find_element(By.CSS_SELECTOR, '.discount')
                discount_text = discount_elem.text.strip()
                discount_rate = self._extract_number(discount_text)
            except:
                discount_rate = 0
            
            # 리뷰 수
            try:
                review_elem = element.find_element(By.CSS_SELECTOR, '.review-count')
                review_text = review_elem.text.strip()
                review_count = self._extract_review_count(review_text)
            except:
                review_count = 0
            
            # 평점
            try:
                rating_elem = element.find_element(By.CSS_SELECTOR, '[data-rating]')
                rating_score = float(rating_elem.get_attribute('data-rating') or 0)
            except:
                rating_score = 0.0
            
            # 로켓배송 여부
            try:
                rocket_elem = element.find_element(By.CSS_SELECTOR, '.badge.rocket')
                is_rocket = True
            except:
                is_rocket = False
            
            return {
                'product_id': product_id,
                'product_name': product_name,
                'product_url': product_url,
                'current_price': current_price,
                'original_price': current_price + (current_price * discount_rate // 100) if discount_rate > 0 else current_price,
                'discount_rate': discount_rate,
                'review_count': review_count,
                'rating_score': rating_score,
                'is_rocket_delivery': is_rocket,
                'is_free_shipping': False,  # 기본값
                'cashback_amount': 0,  # 기본값
                'delivery_info': ''  # 기본값
            }
            
        except Exception as e:
            return None
    
    def _extract_number(self, text):
        """텍스트에서 숫자 추출"""
        if not text:
            return 0
        numbers = re.findall(r'[\d,]+', text)
        if numbers:
            return int(numbers[0].replace(',', ''))
        return 0
    
    def _extract_review_count(self, text):
        """리뷰 수 추출 - (170) 형태"""
        match = re.search(r'\((\d+)\)', text)
        return int(match.group(1)) if match else 0
    
    def _scroll_down_and_wait(self):
        """스크롤 다운 후 새 콘텐츠 로딩 대기"""
        try:
            # 현재 페이지 높이
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            # 천천히 단계적으로 스크롤 (coupang_251010.py 스타일)
            scroll_steps = random.randint(3, 5)
            for i in range(scroll_steps):
                scroll_amount = (last_height / scroll_steps) * (i + 1)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_amount});")
                time.sleep(0.3)
            
            # 완전히 끝까지 스크롤
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
    """카테고리별 변화 감지 엔진"""
    
    def detect_changes(self, old_products, new_products):
        """카테고리별 변화 감지"""
        if not old_products:
            # 첫 크롤링인 경우
            return {
                'new_products': new_products,
                'rank_changes': [],
                'price_changes': [],
                'removed_products': []
            }
        
        # 상품 ID별 매핑
        old_map = {p['coupang_product_id']: p for p in old_products}
        new_map = {p['product_id']: p for p in new_products}
        
        changes = {
            'new_products': [],
            'rank_changes': [],
            'price_changes': [],
            'removed_products': []
        }
        
        # 신규 상품
        new_ids = set(new_map.keys()) - set(old_map.keys())
        changes['new_products'] = [new_map[pid] for pid in new_ids]
        
        # 제거된 상품
        removed_ids = set(old_map.keys()) - set(new_map.keys())
        changes['removed_products'] = [old_map[pid] for pid in removed_ids]
        
        # 기존 상품의 변화
        common_ids = set(old_map.keys()) & set(new_map.keys())
        
        for pid in common_ids:
            old_product = old_map[pid]
            new_product = new_map[pid]
            
            # 순위 변화 (카테고리별 순위)
            old_rank = old_product['category_rank']
            # 새 상품 리스트에서 현재 순위 찾기
            new_rank = next((i+1 for i, p in enumerate(new_products) if p['product_id'] == pid), None)
            
            if new_rank and old_rank != new_rank:
                changes['rank_changes'].append({
                    'product_id': pid,
                    'product_name': new_product['product_name'],
                    'old_rank': old_rank,
                    'new_rank': new_rank,
                    'rank_change': old_rank - new_rank  # 양수면 순위 상승
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
        """
        Args:
            category_config: {'name': '헬스/건강식품', 'url': '...', 'description': '...'}
        """
        self.category_config = category_config
        self.db = CategoryMonitoringDatabase(db_path)
        self.browser = BrowserManager(headless=headless)
        self.extractor = InfiniteScrollExtractor(self.browser)
        self.change_detector = CategoryChangeDetector()
        self.cross_analyzer = CrossCategoryAnalyzer(db_path)
        
        # 카테고리 등록
        self.category_id = self.db.register_category(
            category_config['name'], 
            category_config['url']
        )
        
        # CSV 베이스라인 로드 (초기 1회만)
        if csv_baseline_path:
            self.db.load_csv_baseline(csv_baseline_path)
        
        print(f"카테고리 '{category_config['name']}' 모니터링 시스템 초기화 완료")
        print(f"대상 URL: {category_config['url']}")
    
    def start_driver(self):
        """브라우저 드라이버 시작"""
        print("Chrome 드라이버 시작 중... (macOS)")
        if self.browser.start_driver():
            print("Chrome 드라이버 시작 완료")
            return True
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
            # 1. 페이지 크롤링 (무한 스크롤)
            current_products = self.extractor.extract_all_products_with_scroll(page_url)
            
            # 2. 이전 데이터와 비교 (카테고리별)
            previous_products = self.db.get_latest_snapshot_data(category_name)
            changes = self.change_detector.detect_changes(previous_products, current_products)
            
            # 3. 변화 보고
            self.report_changes(category_name, changes)
            
            # 4. 변화 이벤트 로깅 (카테고리 정보 포함)
            self.log_changes(category_name, changes)
            
            # 5. 스냅샷 저장 (카테고리별)
            crawl_duration = time.time() - start_time
            snapshot_id = self.db.save_snapshot(category_name, page_url, current_products, crawl_duration)
            
            print(f"스냅샷 저장 완료: ID {snapshot_id}, {len(current_products)}개 상품")
            return changes
            
        except Exception as e:
            print(f"[{category_name}] 모니터링 오류: {e}")
            return None
    
    def report_changes(self, category_name, changes):
        """카테고리별 변화 보고"""
        print(f"[{category_name}] 변화 감지 결과:")
        print(f"  신규 상품: {len(changes['new_products'])}개")
        print(f"  제거된 상품: {len(changes['removed_products'])}개")
        print(f"  순위 변화: {len(changes['rank_changes'])}개")
        print(f"  가격 변화: {len(changes['price_changes'])}개")
        
        # 주요 변화 상세 출력
        if changes['rank_changes']:
            print(f"\n[{category_name}] 주요 순위 변화:")
            for change in changes['rank_changes'][:5]:
                direction = "상승" if change['rank_change'] > 0 else "하락"
                print(f"  {change['product_name'][:30]}...: {change['old_rank']}위 → {change['new_rank']}위 ({abs(change['rank_change'])}단계 {direction})")
        
        if changes['price_changes']:
            print(f"\n[{category_name}] 주요 가격 변화:")
            for change in changes['price_changes'][:5]:
                direction = "인상" if change['price_change'] > 0 else "인하"
                print(f"  {change['product_name'][:30]}...: {change['old_price']:,}원 → {change['new_price']:,}원 ({abs(change['price_change']):,}원 {direction})")
    
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
                f"신규 상품 등장"
            )
    
    def analyze_cross_category_products(self):
        """중복 상품 분석 보고"""
        print(f"\n{'='*60}")
        print(f"중복 상품 분석 (여러 카테고리에 동시 등장)")
        print(f"{'='*60}")
        
        # 중복 상품 조회
        duplicate_products = self.cross_analyzer.get_duplicate_products()
        
        if duplicate_products.empty:
            print("중복 상품이 발견되지 않았습니다.")
            return
        
        print(f"총 {len(duplicate_products)}개 상품이 여러 카테고리에 등장:")
        
        for _, product in duplicate_products.head(10).iterrows():
            categories = json.loads(product['category_combinations'])
            print(f"\n상품: {product['product_name'][:40]}...")
            print(f"  카테고리: {', '.join(categories)} ({product['category_count']}개)")
            
            # 카테고리별 순위 비교
            rank_comparison = self.cross_analyzer.compare_cross_category_ranks(product['coupang_product_id'])
            if not rank_comparison.empty:
                print("  카테고리별 순위:")
                for _, rank_info in rank_comparison.iterrows():
                    print(f"    {rank_info['category_name']}: {rank_info['category_rank']}위 (￦{rank_info['current_price']:,})")
    
    def close(self):
        """리소스 정리"""
        if self.browser:
            self.browser.close()


class MultiCategoryMonitoringSystem:
    """다중 카테고리 모니터링 시스템"""
    
    def __init__(self, categories_config, csv_baseline_path=None, db_path="page_monitoring.db", headless=True):
        """
        Args:
            categories_config: [
                {'name': '헬스/건강식품', 'url': '...'},
                {'name': '출산유아동', 'url': '...'}
            ]
        """
        self.categories_config = categories_config
        self.csv_baseline_path = csv_baseline_path
        self.db_path = db_path
        self.headless = headless
        self.cross_analyzer = CrossCategoryAnalyzer(db_path)
        
        print(f"다중 카테고리 모니터링 시스템 초기화")
        print(f"대상 카테고리: {len(categories_config)}개")
        for cat in categories_config:
            print(f"  - {cat['name']}")
    
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
                
                # 카테고리별 모니터 생성
                monitor = CategoryPageMonitor(
                    category_config=category_config,
                    csv_baseline_path=self.csv_baseline_path,
                    db_path=self.db_path,
                    headless=self.headless
                )
                
                try:
                    # 브라우저 시작
                    if not monitor.start_driver():
                        print(f"❌ {category_config['name']} 브라우저 시작 실패")
                        continue
                    
                    # 모니터링 실행
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
                    wait_time = 30 if cycle == 0 else 60  # 첫 사이클은 30초, 이후 60초
                    print(f"다음 카테고리까지 {wait_time}초 대기...")
                    time.sleep(wait_time)
            
            # 사이클 완료 후 중복 상품 분석
            if cycle == 0:  # 첫 사이클 후에만 분석
                self.analyze_cross_category_results()
            
            # 사이클 간 대기
            if cycle < cycles - 1:
                print(f"\n다음 사이클까지 10분 대기...")
                time.sleep(600)  # 10분 대기
        
        print(f"\n🎉 모든 모니터링 사이클 완료!")
    
    def analyze_cross_category_results(self):
        """전체 결과 분석 및 중복 상품 보고"""
        print(f"\n🔍 전체 결과 분석 시작")
        print("="*80)
        
        # 중복 상품 분석
        duplicate_products = self.cross_analyzer.get_duplicate_products()
        
        if not duplicate_products.empty:
            print(f"\n📊 중복 상품 발견: {len(duplicate_products)}개")
            print("상위 10개 중복 상품:")
            
            for _, product in duplicate_products.head(10).iterrows():
                categories = json.loads(product['category_combinations'])
                print(f"\n• {product['product_name'][:50]}...")
                print(f"  등장 카테고리: {', '.join(categories)} ({product['category_count']}개)")
                
                # 카테고리별 순위 비교
                rank_comparison = self.cross_analyzer.compare_cross_category_ranks(product['coupang_product_id'])
                if not rank_comparison.empty:
                    ranks = []
                    for _, rank_info in rank_comparison.iterrows():
                        ranks.append(f"{rank_info['category_name']}: {rank_info['category_rank']}위")
                    print(f"  순위: {', '.join(ranks)}")
        else:
            print("중복 상품이 발견되지 않았습니다.")
        
        # 최근 7일 중복 상품 트렌드
        trends = self.cross_analyzer.get_cross_category_trends(days=7)
        if not trends.empty:
            print(f"\n📈 최근 7일 중복 상품 트렌드: {len(trends)}개 데이터 포인트")


def main():
    """메인 함수 - 다중 카테고리 모니터링"""
    
    # 다중 카테고리 설정
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
    
    # 다중 카테고리 모니터링 시스템 생성
    monitoring_system = MultiCategoryMonitoringSystem(
        categories_config=categories,
        csv_baseline_path=csv_baseline,
        headless=False  # 테스트용으로 브라우저 표시
    )
    
    try:
        # 2회 모니터링 사이클 실행 (변화 감지 테스트)
        monitoring_system.run_full_monitoring_cycle(cycles=2)
        
    except KeyboardInterrupt:
        print("\n⚠️ 모니터링 중단됨")
    except Exception as e:
        print(f"❌ 시스템 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()