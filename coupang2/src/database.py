#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
통합 DB 관리 (로켓직구 + 아이허브)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
첨부된 스키마 그대로 구현
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple


class IntegratedDatabase:
    """통합 DB 관리 (로켓직구 + 아이허브)"""
    
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    def init_database(self):
        """DB 초기화 (첨부된 스키마)"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        # snapshots 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date         DATE NOT NULL,
                rocket_category_url_1 TEXT,
                rocket_category_url_2 TEXT,
                rocket_category_url_3 TEXT,
                price_file_name       TEXT,
                insights_file_name    TEXT,
                reco_file_name        TEXT
            )
        """)
        
        # products 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                vendor_item_id TEXT PRIMARY KEY,
                product_id     TEXT,
                item_id        TEXT,
                part_number    TEXT,
                upc            TEXT,
                name           TEXT
            )
        """)
        
        # product_price 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS product_price (
                snapshot_id             INTEGER NOT NULL,
                vendor_item_id          TEXT    NOT NULL,
                rocket_price            INTEGER,
                rocket_original_price   INTEGER,
                iherb_price             INTEGER,
                iherb_original_price    INTEGER,
                iherb_recommended_price INTEGER,
                PRIMARY KEY (snapshot_id, vendor_item_id),
                FOREIGN KEY (snapshot_id)    REFERENCES snapshots(id),
                FOREIGN KEY (vendor_item_id) REFERENCES products(vendor_item_id)
            )
        """)
        
        # product_features 테이블
        conn.execute('''
            CREATE TABLE IF NOT EXISTS product_features (
                snapshot_id             INTEGER NOT NULL,
                vendor_item_id          TEXT    NOT NULL,
                rocket_rank             INTEGER,
                rocket_rating           REAL,
                rocket_reviews          INTEGER,
                rocket_category         TEXT,
                iherb_stock             INTEGER,
                iherb_stock_status      TEXT,
                iherb_revenue           INTEGER,
                iherb_sales_quantity    INTEGER,
                iherb_item_winner_ratio REAL,
                iherb_category          TEXT,
                PRIMARY KEY (snapshot_id, vendor_item_id),
                FOREIGN KEY (snapshot_id)    REFERENCES snapshots(id),
                FOREIGN KEY (vendor_item_id) REFERENCES products(vendor_item_id)
            )
        ''')
        
        # 기존 테이블에 컬럼 추가 (마이그레이션)
        try:
            conn.execute("ALTER TABLE product_features ADD COLUMN rocket_category TEXT")
        except:
            pass  # 이미 존재
        
        try:
            conn.execute("ALTER TABLE product_features ADD COLUMN iherb_category TEXT")
        except:
            pass  # 이미 존재
        
        # 인덱스
        conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_date ON snapshots(snapshot_date DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_product_price_snapshot ON product_price(snapshot_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_product_features_snapshot ON product_features(snapshot_id)")
        
        conn.commit()
        conn.close()
        
        print("✅ 통합 DB 초기화 완료")
    
    # ========================================
    # Snapshot 관리
    # ========================================
    
    def create_snapshot(self, snapshot_date: str, 
                       rocket_urls: Optional[Dict[str, str]] = None,
                       file_names: Optional[Dict[str, str]] = None) -> int:
        """새 snapshot 생성
        
        Args:
            snapshot_date: 'YYYY-MM-DD'
            rocket_urls: {'url_1': url, 'url_2': url, 'url_3': url}
            file_names: {'price': name, 'insights': name, 'reco': name}
        
        Returns:
            snapshot_id
        """
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        rocket_urls = rocket_urls or {}
        file_names = file_names or {}
        
        cursor = conn.execute(
            """INSERT INTO snapshots 
               (snapshot_date, rocket_category_url_1, rocket_category_url_2, rocket_category_url_3,
                price_file_name, insights_file_name, reco_file_name)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                snapshot_date,
                rocket_urls.get('url_1'),
                rocket_urls.get('url_2'),
                rocket_urls.get('url_3'),
                file_names.get('price'),
                file_names.get('insights'),
                file_names.get('reco')
            )
        )
        
        snapshot_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return snapshot_id
    
    def get_latest_snapshot_id(self) -> Optional[int]:
        """최신 snapshot ID 조회"""
        conn = sqlite3.connect(self.db_path)
        result = conn.execute(
            "SELECT id FROM snapshots ORDER BY snapshot_date DESC, id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        
        return result[0] if result else None
    
    def get_snapshot_by_date(self, target_date: str) -> Optional[int]:
        """특정 날짜의 snapshot ID 조회"""
        conn = sqlite3.connect(self.db_path)
        result = conn.execute(
            "SELECT id FROM snapshots WHERE snapshot_date = ? ORDER BY id DESC LIMIT 1",
            (target_date,)
        ).fetchone()
        conn.close()
        
        return result[0] if result else None
    
    # ========================================
    # Product 관리
    # ========================================
    
    def upsert_product(self, vendor_item_id: str, product_id: Optional[str] = None,
                      item_id: Optional[str] = None, part_number: Optional[str] = None,
                      upc: Optional[str] = None, name: Optional[str] = None):
        """상품 정보 추가/업데이트"""
        conn = sqlite3.connect(self.db_path)
        
        conn.execute(
            """INSERT INTO products (vendor_item_id, product_id, item_id, part_number, upc, name)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(vendor_item_id) DO UPDATE SET
                   product_id = COALESCE(excluded.product_id, product_id),
                   item_id = COALESCE(excluded.item_id, item_id),
                   part_number = COALESCE(excluded.part_number, part_number),
                   upc = COALESCE(excluded.upc, upc),
                   name = COALESCE(excluded.name, name)""",
            (vendor_item_id, product_id, item_id, part_number, upc, name)
        )
        
        conn.commit()
        conn.close()
    
    def batch_upsert_products(self, products: List[Dict]):
        """상품 일괄 추가/업데이트
        
        Args:
            products: [{'vendor_item_id': ..., 'product_id': ..., ...}, ...]
        """
        if not products:
            return
        
        conn = sqlite3.connect(self.db_path)
        
        for p in products:
            conn.execute(
                """
                INSERT INTO products (vendor_item_id, product_id, item_id, part_number, upc, name)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(vendor_item_id) DO UPDATE SET
                    product_id  = COALESCE(EXCLUDED.product_id,  products.product_id),
                    item_id     = COALESCE(EXCLUDED.item_id,     products.item_id),
                    part_number = COALESCE(EXCLUDED.part_number, products.part_number),
                    upc         = COALESCE(EXCLUDED.upc,         products.upc),
                    name        = COALESCE(EXCLUDED.name,        products.name)
                """,
                (
                    p.get('vendor_item_id'),
                    p.get('product_id'),
                    p.get('item_id'),
                    p.get('part_number'),
                    p.get('upc'),
                    p.get('name')
                )
            )
        
        conn.commit()
        conn.close()
    
    # ========================================
    # Price 관리
    # ========================================
    
    def save_product_price(self, snapshot_id: int, vendor_item_id: str,
                          rocket_price: Optional[int] = None,
                          rocket_original_price: Optional[int] = None,
                          iherb_price: Optional[int] = None,
                          iherb_original_price: Optional[int] = None,
                          iherb_recommended_price: Optional[int] = None):
        """상품 가격 저장"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        conn.execute(
            """INSERT OR REPLACE INTO product_price 
               (snapshot_id, vendor_item_id, rocket_price, rocket_original_price,
                iherb_price, iherb_original_price, iherb_recommended_price)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (snapshot_id, vendor_item_id, rocket_price, rocket_original_price,
             iherb_price, iherb_original_price, iherb_recommended_price)
        )
        
        conn.commit()
        conn.close()
    
    def batch_save_product_prices(self, snapshot_id: int, prices: List[Dict]):
        """가격 일괄 저장
        
        Args:
            prices: [{'vendor_item_id': ..., 'rocket_price': ..., ...}, ...]
        """
        if not prices:
            return
        
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        for p in prices:
            conn.execute(
                """
                INSERT INTO product_price
                  (snapshot_id, vendor_item_id, rocket_price, rocket_original_price,
                   iherb_price, iherb_original_price, iherb_recommended_price)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_id, vendor_item_id) DO UPDATE SET
                    rocket_price             = COALESCE(EXCLUDED.rocket_price,            product_price.rocket_price),
                    rocket_original_price    = COALESCE(EXCLUDED.rocket_original_price,   product_price.rocket_original_price),
                    iherb_price              = COALESCE(EXCLUDED.iherb_price,             product_price.iherb_price),
                    iherb_original_price     = COALESCE(EXCLUDED.iherb_original_price,    product_price.iherb_original_price),
                    iherb_recommended_price = COALESCE(EXCLUDED.iherb_recommended_price, product_price.iherb_recommended_price)
                """,
                (
                    snapshot_id,
                    p.get('vendor_item_id'),
                    p.get('rocket_price'),
                    p.get('rocket_original_price'),
                    p.get('iherb_price'),
                    p.get('iherb_original_price'),
                    p.get('iherb_recommended_price')
                )
            )
        
        conn.commit()
        conn.close()
    
    # ========================================
    # Features 관리
    # ========================================
    
    def save_product_features(self, snapshot_id: int, vendor_item_id: str,
                             rocket_rank: Optional[int] = None,
                             rocket_rating: Optional[float] = None,
                             rocket_reviews: Optional[int] = None,
                             iherb_stock: Optional[int] = None,
                             iherb_stock_status: Optional[str] = None,
                             iherb_revenue: Optional[int] = None,
                             iherb_sales_quantity: Optional[int] = None,
                             iherb_item_winner_ratio: Optional[float] = None):
        """상품 특성 저장"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        conn.execute(
            """INSERT OR REPLACE INTO product_features 
               (snapshot_id, vendor_item_id, rocket_rank, rocket_rating, rocket_reviews,
                iherb_stock, iherb_stock_status, iherb_revenue, iherb_sales_quantity,
                iherb_item_winner_ratio)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (snapshot_id, vendor_item_id, rocket_rank, rocket_rating, rocket_reviews,
             iherb_stock, iherb_stock_status, iherb_revenue, iherb_sales_quantity,
             iherb_item_winner_ratio)
        )
        
        conn.commit()
        conn.close()
    
    def batch_save_product_features(self, snapshot_id: int, features: List[Dict]):
        """특성 일괄 저장
        
        Args:
            features: [{'vendor_item_id': ..., 'rocket_rank': ..., ...}, ...]
        """
        if not features:
            return
        
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        
        for f in features:
            conn.execute(
                '''
                INSERT INTO product_features
                  (snapshot_id, vendor_item_id, rocket_rank, rocket_rating, rocket_reviews,
                   rocket_category, iherb_stock, iherb_stock_status, iherb_revenue,
                   iherb_sales_quantity, iherb_item_winner_ratio, iherb_category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_id, vendor_item_id) DO UPDATE SET
                    rocket_rank            = COALESCE(EXCLUDED.rocket_rank,            product_features.rocket_rank),
                    rocket_rating          = COALESCE(EXCLUDED.rocket_rating,          product_features.rocket_rating),
                    rocket_reviews         = COALESCE(EXCLUDED.rocket_reviews,         product_features.rocket_reviews),
                    rocket_category        = COALESCE(EXCLUDED.rocket_category,        product_features.rocket_category),
                    iherb_stock            = COALESCE(EXCLUDED.iherb_stock,            product_features.iherb_stock),
                    iherb_stock_status     = COALESCE(EXCLUDED.iherb_stock_status,     product_features.iherb_stock_status),
                    iherb_revenue          = COALESCE(EXCLUDED.iherb_revenue,          product_features.iherb_revenue),
                    iherb_sales_quantity   = COALESCE(EXCLUDED.iherb_sales_quantity,   product_features.iherb_sales_quantity),
                    iherb_item_winner_ratio= COALESCE(EXCLUDED.iherb_item_winner_ratio,product_features.iherb_item_winner_ratio),
                    iherb_category         = COALESCE(EXCLUDED.iherb_category,         product_features.iherb_category)
                ''',
                (
                    snapshot_id,
                    f.get('vendor_item_id'),
                    f.get('rocket_rank'),
                    f.get('rocket_rating'),
                    f.get('rocket_reviews'),
                    f.get('rocket_category'),
                    f.get('iherb_stock'),
                    f.get('iherb_stock_status'),
                    f.get('iherb_revenue'),
                    f.get('iherb_sales_quantity'),
                    f.get('iherb_item_winner_ratio'),
                    f.get('iherb_category')
                )
            )
        
        conn.commit()
        conn.close()


def main():
    """테스트"""
    db_path = "/home/claude/test_integrated.db"
    
    db = IntegratedDatabase(db_path)
    db.init_database()
    
    # Snapshot 생성
    snapshot_id = db.create_snapshot(
        snapshot_date='2025-01-15',
        rocket_urls={'url_1': 'https://example.com/cat1'},
        file_names={'price': 'price_20250115.xlsx'}
    )
    
    print(f"✅ Snapshot 생성: ID={snapshot_id}")
    
    # 상품 추가
    db.upsert_product(
        vendor_item_id='12345',
        product_id='67890',
        name='Test Product'
    )
    
    print("✅ 상품 추가 완료")
    
    # 가격 저장
    db.save_product_price(
        snapshot_id=snapshot_id,
        vendor_item_id='12345',
        rocket_price=10000,
        iherb_price=9000
    )
    
    print("✅ 가격 저장 완료")


if __name__ == "__main__":
    main()