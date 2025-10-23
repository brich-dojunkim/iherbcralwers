#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
순수 모니터링 데이터베이스 (iHerb 매칭 제거)
- URL 기반 카테고리 식별
- 핵심 테이블만 유지
- iHerb 매칭은 별도 모듈에서 처리
"""

import sqlite3
from datetime import datetime
from typing import List, Dict


class MonitoringDatabase:
    """순수 모니터링 데이터베이스 (iHerb 매칭 없음)"""
    
    def __init__(self, db_path="monitoring.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """데이터베이스 초기화 (순수 버전)"""
        conn = sqlite3.connect(self.db_path)
        
        # 1. 카테고리 테이블
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
        
        # 3. 상품 상태 테이블 (핵심!)
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
                FOREIGN KEY (snapshot_id) REFERENCES page_snapshots (id)
            )
        """)
        
        # 4. 매칭 참조 테이블 (iHerb 매칭용, iherb_matcher.py에서 관리)
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
        
        # 5. 인덱스 생성 (필수 인덱스만)
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_categories_url ON categories(url)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_category ON page_snapshots(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_time ON page_snapshots(snapshot_time)",
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
        
        print(f"✅ 순수 모니터링 DB 초기화 완료: {self.db_path}")
        print(f"   💡 iHerb 매칭은 iherb_matcher.py로 별도 실행")
    
    def register_category(self, name: str, url: str) -> int:
        """카테고리 등록 또는 기존 ID 반환"""
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
        """스냅샷 저장 (순수 크롤링 데이터만)"""
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