#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
재설계된 모니터링 데이터베이스 (간소화 버전)
- 다중 소스 지원 (로켓직구 + iHerb 공식)
- matching_reference 테이블 간소화
"""

import sqlite3
from datetime import datetime
from typing import List, Dict, Optional


class MonitoringDatabase:
    """재설계된 모니터링 데이터베이스"""
    
    def __init__(self, db_path="monitoring.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """데이터베이스 초기화"""
        conn = sqlite3.connect(self.db_path)
        
        # 1. categories 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                coupang_category_id TEXT UNIQUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 2. sources 테이블 (간소화)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 초기 소스 데이터
        conn.execute("""
            INSERT OR IGNORE INTO sources 
            (source_type, display_name, base_url)
            VALUES 
            ('rocket_direct', '로켓직구', 'https://shop.coupang.com/coupangus/74511'),
            ('iherb_official', 'iHerb 공식', 'https://shop.coupang.com/iherb/135493')
        """)
        
        # 3. matching_reference 테이블 (간소화!)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS matching_reference (
                coupang_product_id TEXT PRIMARY KEY,
                iherb_upc TEXT,
                iherb_part_number TEXT
            )
        """)
        
        # 4. snapshots 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                source_id INTEGER NOT NULL,
                category_id INTEGER,
                
                page_url TEXT NOT NULL,
                snapshot_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_products INTEGER,
                crawl_duration_seconds REAL,
                
                status TEXT DEFAULT 'completed',
                error_message TEXT,
                
                FOREIGN KEY (source_id) REFERENCES sources(id),
                FOREIGN KEY (category_id) REFERENCES categories(id)
            )
        """)
        
        # 5. product_states 테이블
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
                FOREIGN KEY (snapshot_id) REFERENCES snapshots(id),
                FOREIGN KEY (coupang_product_id) REFERENCES matching_reference(coupang_product_id)
            )
        """)
        
        # 6. 인덱스 생성 (간소화)
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_categories_coupang_id ON categories(coupang_category_id)",
            "CREATE INDEX IF NOT EXISTS idx_matching_upc ON matching_reference(iherb_upc)",
            "CREATE INDEX IF NOT EXISTS idx_matching_part ON matching_reference(iherb_part_number)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_source ON snapshots(source_id)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_category ON snapshots(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots(snapshot_time)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_combo ON snapshots(source_id, category_id)",
            "CREATE INDEX IF NOT EXISTS idx_states_snapshot ON product_states(snapshot_id)",
            "CREATE INDEX IF NOT EXISTS idx_states_product ON product_states(coupang_product_id)",
            "CREATE INDEX IF NOT EXISTS idx_states_rank ON product_states(snapshot_id, category_rank)"
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
        
        conn.commit()
        conn.close()
        
        print(f"✅ DB 초기화 완료: {self.db_path}")
    
    def register_category(self, name: str, coupang_category_id: str) -> int:
        """카테고리 등록 또는 기존 ID 반환"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # 기존 카테고리 확인
            existing = conn.execute("""
                SELECT id FROM categories WHERE coupang_category_id = ?
            """, (coupang_category_id,)).fetchone()
            
            if existing:
                category_id = existing[0]
                # name만 업데이트
                conn.execute("""
                    UPDATE categories SET name = ? WHERE id = ?
                """, (name, category_id))
                conn.commit()
            else:
                # 새 카테고리 생성
                conn.execute("""
                    INSERT INTO categories (name, coupang_category_id)
                    VALUES (?, ?)
                """, (name, coupang_category_id))
                conn.commit()
                
                category_id = conn.execute("""
                    SELECT id FROM categories WHERE coupang_category_id = ?
                """, (coupang_category_id,)).fetchone()[0]
            
            return category_id
            
        except sqlite3.IntegrityError:
            # 중복 시 재조회
            category_id = conn.execute("""
                SELECT id FROM categories WHERE coupang_category_id = ?
            """, (coupang_category_id,)).fetchone()[0]
            return category_id
            
        finally:
            conn.close()
    
    def get_source_id(self, source_type: str) -> int:
        """소스 ID 조회"""
        conn = sqlite3.connect(self.db_path)
        
        source_id = conn.execute("""
            SELECT id FROM sources WHERE source_type = ?
        """, (source_type,)).fetchone()[0]
        
        conn.close()
        
        return source_id
    
    def save_snapshot(self, source_type: str, category_id: Optional[int],
                     page_url: str, products: List[Dict], 
                     crawl_duration: float) -> int:
        """스냅샷 저장"""
        conn = sqlite3.connect(self.db_path)
        
        # 순위 검증
        for product in products:
            if 'rank' not in product or product['rank'] <= 0:
                conn.close()
                raise ValueError(f"상품 {product.get('product_id')}의 순위가 올바르지 않습니다")
        
        # source_id 조회
        source_id = self.get_source_id(source_type)
        
        # 스냅샷 생성
        cursor = conn.execute("""
            INSERT INTO snapshots 
            (source_id, category_id, page_url, total_products, crawl_duration_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, (source_id, category_id, page_url, len(products), crawl_duration))
        
        snapshot_id = cursor.lastrowid
        
        # 상품 상태 저장
        for product in products:
            # matching_reference에 자동 생성 (INSERT OR IGNORE)
            conn.execute("""
                INSERT OR IGNORE INTO matching_reference (coupang_product_id)
                VALUES (?)
            """, (product['product_id'],))
            
            # product_states 저장
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
    
    def get_matching_info(self, coupang_product_id: str) -> Optional[Dict]:
        """제품의 매칭 정보 조회"""
        conn = sqlite3.connect(self.db_path)
        
        result = conn.execute("""
            SELECT coupang_product_id, iherb_upc, iherb_part_number
            FROM matching_reference
            WHERE coupang_product_id = ?
        """, (coupang_product_id,)).fetchone()
        
        conn.close()
        
        if result:
            return {
                'coupang_product_id': result[0],
                'iherb_upc': result[1],
                'iherb_part_number': result[2]
            }
        return None
    
    def check_matching_status(self):
        """매칭 상태 확인"""
        conn = sqlite3.connect(self.db_path)
        
        stats = {}
        
        # 전체 제품 수
        stats['total'] = conn.execute(
            "SELECT COUNT(*) FROM matching_reference"
        ).fetchone()[0]
        
        # UPC 있는 제품 수
        stats['with_upc'] = conn.execute(
            "SELECT COUNT(*) FROM matching_reference WHERE iherb_upc IS NOT NULL"
        ).fetchone()[0]
        
        # Part Number 있는 제품 수
        stats['with_part'] = conn.execute(
            "SELECT COUNT(*) FROM matching_reference WHERE iherb_part_number IS NOT NULL AND iherb_part_number != ''"
        ).fetchone()[0]
        
        conn.close()
        
        return stats


def main():
    """DB 초기화"""
    import os
    
    db_path = "monitoring.db"
    
    print(f"\n{'='*80}")
    print(f"🗄️  DB 초기화")
    print(f"{'='*80}\n")
    
    if os.path.exists(db_path):
        print(f"⚠️  기존 DB 파일이 있습니다: {db_path}")
        response = input("삭제하고 새로 만드시겠습니까? (y/n): ")
        
        if response.lower() == 'y':
            os.remove(db_path)
            print(f"✅ 기존 DB 삭제")
        else:
            print(f"❌ 초기화 취소")
            return
    
    # DB 생성
    db = MonitoringDatabase(db_path)
    
    # 통계
    stats = db.check_matching_status()
    
    print(f"\n{'='*80}")
    print(f"📊 초기 상태")
    print(f"{'='*80}\n")
    print(f"총 제품 수: {stats['total']:,}개")
    print(f"UPC 있는 제품: {stats['with_upc']:,}개")
    print(f"Part Number 있는 제품: {stats['with_part']:,}개")
    
    print(f"\n{'='*80}")
    print(f"다음 단계:")
    print(f"{'='*80}\n")
    print(f"1. python prepare_matching.py")
    print(f"2. python monitoring.py --source rocket")
    print(f"3. python monitoring.py --source official")
    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    main()