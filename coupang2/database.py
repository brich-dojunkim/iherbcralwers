#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
모니터링 데이터베이스
- DB 초기화
- 데이터 저장
"""

import sqlite3
from datetime import datetime
from typing import List, Dict


class MonitoringDatabase:
    """모니터링 데이터베이스"""
    
    def __init__(self, db_path="monitoring.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """데이터베이스 초기화"""
        conn = sqlite3.connect(self.db_path)
        
        # 1. sources 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                base_url TEXT NOT NULL
            )
        """)
        
        # 2. categories 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                coupang_category_id TEXT UNIQUE NOT NULL
            )
        """)
        
        # 3. snapshots 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                page_url TEXT NOT NULL,
                snapshot_time DATETIME NOT NULL,
                total_products INTEGER,
                crawl_duration_seconds REAL,
                status TEXT DEFAULT 'completed',
                error_message TEXT,
                FOREIGN KEY (source_id) REFERENCES sources (id),
                FOREIGN KEY (category_id) REFERENCES categories (id)
            )
        """)
        
        # 4. product_states 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS product_states (
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
                
                PRIMARY KEY (snapshot_id, coupang_product_id),
                FOREIGN KEY (snapshot_id) REFERENCES snapshots (id)
            )
        """)
        
        # 5. matching_reference 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS matching_reference (
                coupang_product_id TEXT PRIMARY KEY,
                first_discovered_category TEXT,
                first_discovered_name TEXT,
                first_discovered_at DATETIME,
                iherb_upc TEXT,
                iherb_part_number TEXT,
                matched_at DATETIME,
                matching_confidence REAL DEFAULT 1.0,
                is_manually_verified BOOLEAN DEFAULT FALSE,
                notes TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 6. 인덱스 생성
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_snapshots_source ON snapshots(source_id)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_category ON snapshots(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots(snapshot_time)",
            "CREATE INDEX IF NOT EXISTS idx_states_snapshot ON product_states(snapshot_id)",
            "CREATE INDEX IF NOT EXISTS idx_states_product ON product_states(coupang_product_id)",
            "CREATE INDEX IF NOT EXISTS idx_states_rank ON product_states(snapshot_id, category_rank)",
            "CREATE INDEX IF NOT EXISTS idx_matching_upc ON matching_reference(iherb_upc)",
            "CREATE INDEX IF NOT EXISTS idx_matching_part ON matching_reference(iherb_part_number)"
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
        
        conn.commit()
        conn.close()
        
        print(f"✅ DB 초기화 완료: {self.db_path}")
    
    def register_source(self, source_type: str, display_name: str, base_url: str) -> int:
        """소스 등록 또는 기존 ID 반환"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            existing = conn.execute("""
                SELECT id FROM sources WHERE source_type = ?
            """, (source_type,)).fetchone()
            
            if existing:
                source_id = existing[0]
            else:
                conn.execute("""
                    INSERT INTO sources (source_type, display_name, base_url)
                    VALUES (?, ?, ?)
                """, (source_type, display_name, base_url))
                conn.commit()
                
                source_id = conn.execute("""
                    SELECT id FROM sources WHERE source_type = ?
                """, (source_type,)).fetchone()[0]
            
            return source_id
            
        finally:
            conn.close()
    
    def register_category(self, name: str, coupang_category_id: str) -> int:
        """카테고리 등록 또는 기존 ID 반환"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            existing = conn.execute("""
                SELECT id FROM categories WHERE coupang_category_id = ?
            """, (coupang_category_id,)).fetchone()
            
            if existing:
                category_id = existing[0]
            else:
                conn.execute("""
                    INSERT INTO categories (name, coupang_category_id)
                    VALUES (?, ?)
                """, (name, coupang_category_id))
                conn.commit()
                
                category_id = conn.execute("""
                    SELECT id FROM categories WHERE coupang_category_id = ?
                """, (coupang_category_id,)).fetchone()[0]
            
            return category_id
            
        finally:
            conn.close()
    
    def save_snapshot(self, source_id: int, category_id: int, page_url: str,
                     products: List[Dict], crawl_duration: float,
                     snapshot_time: datetime = None,
                     error_message: str = None) -> int:
        """스냅샷 저장"""
        conn = sqlite3.connect(self.db_path)
        
        if snapshot_time is None:
            snapshot_time = datetime.now()
        
        # 순위 검증
        for product in products:
            if 'rank' not in product or product['rank'] <= 0:
                conn.close()
                raise ValueError(f"상품 {product.get('product_id')}의 순위가 올바르지 않습니다")
        
        # 스냅샷 생성
        cursor = conn.execute("""
            INSERT INTO snapshots 
            (source_id, category_id, page_url, snapshot_time, total_products, crawl_duration_seconds, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (source_id, category_id, page_url, snapshot_time.strftime("%Y-%m-%d %H:%M:%S"),
              len(products), crawl_duration, error_message))
        
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