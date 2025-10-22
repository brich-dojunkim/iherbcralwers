#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
개선된 모니터링 데이터베이스 (최소 필수 버전)
- URL 기반 카테고리 식별
- 시계열 추적을 위한 핵심 기능만 유지
"""

import sqlite3
from datetime import datetime
from typing import List, Dict
import pandas as pd


class MonitoringDatabase:
    """개선된 카테고리별 모니터링 데이터베이스"""
    
    def __init__(self, db_path="improved_monitoring.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """데이터베이스 초기화"""
        conn = sqlite3.connect(self.db_path)
        
        # 1. 카테고리 테이블 (URL UNIQUE!)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 2. 페이지 스냅샷 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS page_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id INTEGER NOT NULL,
                page_url TEXT NOT NULL,
                snapshot_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_products INTEGER,
                crawl_duration_seconds REAL,
                status TEXT DEFAULT 'completed',
                FOREIGN KEY (category_id) REFERENCES categories (id)
            )
        """)
        
        # 3. 상품 상태 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS product_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL,
                coupang_product_id TEXT NOT NULL,
                category_rank INTEGER NOT NULL CHECK(category_rank > 0),
                
                product_name TEXT NOT NULL,
                product_url TEXT NOT NULL,
                current_price INTEGER DEFAULT 0,
                original_price INTEGER DEFAULT 0,
                discount_rate INTEGER DEFAULT 0,
                review_count INTEGER DEFAULT 0,
                rating_score REAL DEFAULT 0.0,
                
                is_rocket_delivery BOOLEAN DEFAULT FALSE,
                is_free_shipping BOOLEAN DEFAULT FALSE,
                
                FOREIGN KEY (snapshot_id) REFERENCES page_snapshots (id)
            )
        """)
        
        # 4. 변화 이벤트 테이블 (선택적)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS change_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL,
                coupang_product_id TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                
                event_type TEXT NOT NULL CHECK(event_type IN (
                    'new_product', 'removed_product', 
                    'rank_change', 'price_change', 'review_surge'
                )),
                
                old_value TEXT,
                new_value TEXT,
                change_magnitude REAL,
                description TEXT,
                event_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (snapshot_id) REFERENCES page_snapshots (id),
                FOREIGN KEY (category_id) REFERENCES categories (id)
            )
        """)
        
        # 5. 매칭 참조 테이블 (iHerb 매칭용, 선택적)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS matching_reference (
                coupang_product_id TEXT PRIMARY KEY,
                first_discovered_category TEXT,
                first_discovered_name TEXT,
                first_discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                iherb_upc TEXT,
                iherb_part_number TEXT,
                matched_at DATETIME,
                matching_confidence REAL DEFAULT 1.0,
                is_manually_verified BOOLEAN DEFAULT FALSE,
                notes TEXT
            )
        """)
        
        # 6. 인덱스 생성
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_categories_url ON categories(url)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_category ON page_snapshots(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_time ON page_snapshots(snapshot_time)",
            "CREATE INDEX IF NOT EXISTS idx_states_snapshot ON product_states(snapshot_id)",
            "CREATE INDEX IF NOT EXISTS idx_states_product ON product_states(coupang_product_id)",
            "CREATE INDEX IF NOT EXISTS idx_states_rank ON product_states(snapshot_id, category_rank)",
            "CREATE INDEX IF NOT EXISTS idx_events_snapshot ON change_events(snapshot_id)",
            "CREATE INDEX IF NOT EXISTS idx_events_category ON change_events(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_events_time ON change_events(event_time)"
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
        
        conn.commit()
        conn.close()
        
        print(f"✅ 데이터베이스 초기화 완료: {self.db_path}")
    
    def register_category(self, name: str, url: str) -> int:
        """
        카테고리 등록 또는 기존 ID 반환
        URL 기반 검색 (핵심!)
        """
        conn = sqlite3.connect(self.db_path)
        
        try:
            # URL로 기존 카테고리 확인
            existing = conn.execute("""
                SELECT id FROM categories WHERE url = ?
            """, (url,)).fetchone()
            
            if existing:
                category_id = existing[0]
                # name만 업데이트
                conn.execute("""
                    UPDATE categories SET name = ? WHERE id = ?
                """, (name, category_id))
                conn.commit()
                print(f"  ✅ 기존 카테고리 사용: {name} (ID: {category_id})")
            else:
                # 새 카테고리 생성
                conn.execute("""
                    INSERT INTO categories (name, url)
                    VALUES (?, ?)
                """, (name, url))
                conn.commit()
                
                category_id = conn.execute("""
                    SELECT id FROM categories WHERE url = ?
                """, (url,)).fetchone()[0]
                
                print(f"  ✅ 새 카테고리 생성: {name} (ID: {category_id})")
            
            return category_id
            
        except sqlite3.IntegrityError:
            # UNIQUE 제약조건 위반 (동시 실행 시)
            print(f"  ⚠️  카테고리 중복 감지, 재조회: {url}")
            category_id = conn.execute("""
                SELECT id FROM categories WHERE url = ?
            """, (url,)).fetchone()[0]
            return category_id
            
        finally:
            conn.close()
    
    def save_snapshot(self, category_id: int, page_url: str, 
                     products: List[Dict], crawl_duration: float) -> int:
        """스냅샷 저장"""
        conn = sqlite3.connect(self.db_path)
        
        # 순위 검증
        for product in products:
            if 'rank' not in product or product['rank'] <= 0:
                conn.close()
                raise ValueError(f"상품 {product.get('product_id')}의 순위가 올바르지 않습니다")
        
        # 스냅샷 생성
        cursor = conn.execute("""
            INSERT INTO page_snapshots 
            (category_id, page_url, total_products, crawl_duration_seconds)
            VALUES (?, ?, ?, ?)
        """, (category_id, page_url, len(products), crawl_duration))
        
        snapshot_id = cursor.lastrowid
        
        # 상품 상태 저장
        for product in products:
            conn.execute("""
                INSERT INTO product_states 
                (snapshot_id, coupang_product_id, category_rank,
                 product_name, product_url, current_price, original_price,
                 discount_rate, review_count, rating_score,
                 is_rocket_delivery, is_free_shipping)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot_id, 
                product['product_id'], 
                product['rank'],
                product['product_name'], 
                product['product_url'],
                product.get('current_price', 0), 
                product.get('original_price', 0),
                product.get('discount_rate', 0), 
                product.get('review_count', 0),
                product.get('rating_score', 0.0),
                product.get('is_rocket_delivery', False),
                product.get('is_free_shipping', False)
            ))
        
        conn.commit()
        conn.close()
        
        return snapshot_id
    
    def get_latest_snapshot_data(self, category_id: int) -> List[Dict]:
        """최신 스냅샷 데이터 조회 (변화 감지용)"""
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
        WHERE ps.snapshot_id = (
            SELECT MAX(snap.id) 
            FROM page_snapshots snap 
            WHERE snap.category_id = ?
        )
        ORDER BY ps.category_rank
        """
        
        cursor = conn.execute(query, (category_id,))
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
    
    def log_change_event(self, snapshot_id: int, product_id: str, 
                        category_id: int, event_type: str,
                        old_value, new_value, change_magnitude: float,
                        description: str):
        """변화 이벤트 로깅"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO change_events 
            (snapshot_id, coupang_product_id, category_id, event_type, 
             old_value, new_value, change_magnitude, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot_id, product_id, category_id, event_type,
            str(old_value) if old_value is not None else None,
            str(new_value) if new_value is not None else None,
            change_magnitude,
            description
        ))
        conn.commit()
        conn.close()
    
    def load_csv_baseline(self, csv_path: str) -> int:
        """CSV 베이스라인 로드 (iHerb 매칭용)"""
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            
            conn = sqlite3.connect(self.db_path)
            loaded_count = 0
            
            for _, row in df.iterrows():
                url = str(row.get('쿠팡_상품URL', ''))
                import re
                match = re.search(r'itemId=(\d+)', url)
                if not match:
                    continue
                
                product_id = match.group(1)
                category = str(row.get('카테고리', ''))
                product_name = str(row.get('쿠팡_제품명', ''))
                iherb_upc = str(row.get('아이허브_UPC', ''))
                iherb_part = str(row.get('아이허브_파트넘버', ''))
                
                if not iherb_upc and not iherb_part:
                    continue
                
                conn.execute("""
                    INSERT OR REPLACE INTO matching_reference 
                    (coupang_product_id, first_discovered_category, first_discovered_name,
                     iherb_upc, iherb_part_number, matched_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    product_id,
                    category,
                    product_name,
                    iherb_upc if iherb_upc else None,
                    iherb_part if iherb_part else None
                ))
                loaded_count += 1
            
            conn.commit()
            conn.close()
            
            print(f"✅ CSV 베이스라인 로드 완료: {loaded_count}개 매칭 상품")
            return loaded_count
            
        except Exception as e:
            print(f"❌ CSV 로드 실패: {e}")
            return 0