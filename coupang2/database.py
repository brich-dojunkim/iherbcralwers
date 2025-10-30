#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
monitoring.db 스키마 관리
- 로켓직구 시계열 데이터 저장
- 스키마 정의 및 기본 CRUD만 포함
"""

import sqlite3
from datetime import datetime
from typing import List, Dict


class MonitoringDatabase:
    """모니터링 DB 관리"""
    
    def __init__(self, db_path: str = "monitoring.db"):
        self.db_path = db_path
    
    def init_database(self):
        """DB 초기화"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        # sources 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                base_url TEXT NOT NULL
            )
        """)
        
        # categories 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                coupang_category_id TEXT UNIQUE NOT NULL
            )
        """)
        
        # snapshots 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                page_url TEXT,
                snapshot_time DATETIME NOT NULL,
                total_products INTEGER DEFAULT 0,
                crawl_duration_seconds REAL,
                status TEXT DEFAULT 'completed',
                error_message TEXT,
                FOREIGN KEY (source_id) REFERENCES sources(id),
                FOREIGN KEY (category_id) REFERENCES categories(id)
            )
        """)
        
        # product_states 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS product_states (
                snapshot_id INTEGER NOT NULL,
                vendor_item_id TEXT NOT NULL,
                category_rank INTEGER NOT NULL,
                product_name TEXT NOT NULL,
                product_url TEXT NOT NULL,
                current_price INTEGER DEFAULT 0,
                original_price INTEGER DEFAULT 0,
                discount_rate REAL DEFAULT 0.0,
                rating_score REAL,
                review_count INTEGER DEFAULT 0,
                PRIMARY KEY (snapshot_id, vendor_item_id),
                FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
            )
        """)
        
        # matching_reference 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS matching_reference (
                vendor_item_id TEXT PRIMARY KEY,
                iherb_upc TEXT,
                iherb_part_number TEXT,
                matching_source TEXT NOT NULL,
                matching_confidence REAL DEFAULT 1.0,
                product_name TEXT
            )
        """)
        
        # 인덱스
        conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots(snapshot_time DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_product_states_vendor ON product_states(vendor_item_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_matching_upc ON matching_reference(iherb_upc)")
        
        conn.commit()
        conn.close()
    
    def add_source(self, source_type: str, display_name: str, base_url: str) -> int:
        """소스 등록"""
        conn = sqlite3.connect(self.db_path)
        
        existing = conn.execute("SELECT id FROM sources WHERE source_type = ?", (source_type,)).fetchone()
        if existing:
            conn.close()
            return existing[0]
        
        cursor = conn.execute(
            "INSERT INTO sources (source_type, display_name, base_url) VALUES (?, ?, ?)",
            (source_type, display_name, base_url)
        )
        conn.commit()
        source_id = cursor.lastrowid
        conn.close()
        return source_id
    
    def add_category(self, name: str, coupang_category_id: str) -> int:
        """카테고리 등록"""
        conn = sqlite3.connect(self.db_path)
        
        existing = conn.execute("SELECT id FROM categories WHERE coupang_category_id = ?", (coupang_category_id,)).fetchone()
        if existing:
            conn.close()
            return existing[0]
        
        cursor = conn.execute(
            "INSERT INTO categories (name, coupang_category_id) VALUES (?, ?)",
            (name, coupang_category_id)
        )
        conn.commit()
        category_id = cursor.lastrowid
        conn.close()
        return category_id
    
    def create_snapshot(self, source_id: int, category_id: int, page_url: str = None) -> int:
        """스냅샷 생성"""
        conn = sqlite3.connect(self.db_path)
        
        cursor = conn.execute(
            "INSERT INTO snapshots (source_id, category_id, page_url, snapshot_time, status) VALUES (?, ?, ?, ?, 'in_progress')",
            (source_id, category_id, page_url, datetime.now())
        )
        conn.commit()
        snapshot_id = cursor.lastrowid
        conn.close()
        return snapshot_id
    
    def complete_snapshot(self, snapshot_id: int, total_products: int, crawl_duration: float = None, error_message: str = None):
        """스냅샷 완료"""
        conn = sqlite3.connect(self.db_path)
        status = 'completed' if error_message is None else 'failed'
        
        conn.execute(
            "UPDATE snapshots SET total_products = ?, crawl_duration_seconds = ?, status = ?, error_message = ? WHERE id = ?",
            (total_products, crawl_duration, status, error_message, snapshot_id)
        )
        conn.commit()
        conn.close()
    
    def save_product_states(self, snapshot_id: int, products: List[Dict]):
        """제품 상태 저장"""
        conn = sqlite3.connect(self.db_path)
        
        for product in products:
            conn.execute(
                """INSERT OR REPLACE INTO product_states 
                   (snapshot_id, vendor_item_id, category_rank, product_name, product_url,
                    current_price, original_price, discount_rate, rating_score, review_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    snapshot_id,
                    product['vendor_item_id'],
                    product['category_rank'],
                    product['product_name'],
                    product['product_url'],
                    product.get('current_price', 0),
                    product.get('original_price', 0),
                    product.get('discount_rate', 0.0),
                    product.get('rating_score'),
                    product.get('review_count', 0)
                )
            )
        
        conn.commit()
        conn.close()