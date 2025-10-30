#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
monitoring.db 스키마 관리
- 로켓직구 시계열 데이터 저장
- 스키마 정의 및 기본 CRUD
"""

import sqlite3
from datetime import datetime
from typing import List, Dict


class MonitoringDatabase:
    """모니터링 DB 관리 (로켓직구 전용)"""
    
    def __init__(self, db_path: str = "/Users/brich/Desktop/iherb_price/coupang2/data/rocket/monitoring.db"):
        self.db_path = db_path
    
    def init_database(self):
        """DB 초기화"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        # sources 테이블 (로켓직구만 사용)
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
        
        # snapshots 테이블 (로켓직구 크롤링 기록)
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
        
        # product_states 테이블 (로켓직구 상품 상태)
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
        
        # matching_reference 테이블 (로켓직구 ↔ 아이허브 매칭)
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
        
        print("✅ DB 초기화 완료 (로켓직구 전용)")
    
    def register_source(self, source_type: str, display_name: str, base_url: str) -> int:
        """소스 등록 (로켓직구 전용)"""
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
    
    def register_category(self, name: str, coupang_category_id: str) -> int:
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
    
    def save_snapshot(self, source_id: int, category_id: int, page_url: str,
                     products: List[Dict], crawl_duration: float,
                     snapshot_time: datetime = None, error_message: str = None) -> int:
        """
        스냅샷 저장 (원자적 트랜잭션)
        
        Args:
            source_id: 소스 ID
            category_id: 카테고리 ID
            page_url: 페이지 URL
            products: 상품 리스트
            crawl_duration: 크롤링 소요시간
            snapshot_time: 스냅샷 시각 (None이면 현재)
            error_message: 에러 메시지 (필터 미적용 등)
        
        Returns:
            snapshot_id
        """
        if not products:
            raise ValueError("상품 리스트가 비어있습니다")
        
        # 순위 검증
        ranks = [p.get('rank', 0) for p in products]
        if min(ranks) != 1:
            raise ValueError(f"순위가 1부터 시작하지 않습니다: min={min(ranks)}")
        
        expected_ranks = set(range(1, len(products) + 1))
        actual_ranks = set(ranks)
        if expected_ranks != actual_ranks:
            missing = expected_ranks - actual_ranks
            raise ValueError(f"순위가 연속적이지 않습니다. 누락: {sorted(missing)}")
        
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        try:
            # 1. 스냅샷 생성
            status = 'completed' if error_message is None else 'completed_with_warning'
            snapshot_time = snapshot_time or datetime.now()
            
            cursor = conn.execute(
                """INSERT INTO snapshots 
                   (source_id, category_id, page_url, snapshot_time, 
                    total_products, crawl_duration_seconds, status, error_message)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (source_id, category_id, page_url, snapshot_time,
                 len(products), crawl_duration, status, error_message)
            )
            snapshot_id = cursor.lastrowid
            
            # 2. 제품 상태 저장
            for product in products:
                conn.execute(
                    """INSERT INTO product_states 
                       (snapshot_id, vendor_item_id, category_rank, product_name, product_url,
                        current_price, original_price, discount_rate, rating_score, review_count)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        snapshot_id,
                        product['product_id'],  # vendor_item_id
                        product['rank'],        # category_rank
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
            return snapshot_id
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_latest_snapshot_date(self) -> str:
        """최신 스냅샷 날짜 조회"""
        conn = sqlite3.connect(self.db_path)
        result = conn.execute(
            "SELECT DATE(MAX(snapshot_time)) FROM snapshots"
        ).fetchone()
        conn.close()
        return result[0] if result and result[0] else None
    
    def get_snapshots_by_date(self, target_date: str) -> List[Dict]:
        """특정 날짜의 스냅샷 목록"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            """SELECT s.id, s.snapshot_time, s.total_products, 
                      src.display_name as source, cat.name as category,
                      s.status, s.error_message
               FROM snapshots s
               JOIN sources src ON s.source_id = src.id
               JOIN categories cat ON s.category_id = cat.id
               WHERE DATE(s.snapshot_time) = ?
               ORDER BY s.snapshot_time DESC""",
            (target_date,)
        )
        
        snapshots = []
        for row in cursor.fetchall():
            snapshots.append({
                'id': row[0],
                'snapshot_time': row[1],
                'total_products': row[2],
                'source': row[3],
                'category': row[4],
                'status': row[5],
                'error_message': row[6]
            })
        
        conn.close()
        return snapshots