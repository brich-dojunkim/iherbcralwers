#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
기존 DB에서 개선된 DB로 데이터 마이그레이션
"""

import sqlite3
from datetime import datetime
import os
import sys

# improved_database를 import하기 위해 경로 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)


class DatabaseMigration:
    """DB 마이그레이션 클래스"""
    
    def __init__(self, old_db_path="page_monitoring.db", new_db_path="improved_monitoring.db"):
        self.old_db = old_db_path
        self.new_db = new_db_path
        
        if not os.path.exists(old_db_path):
            raise FileNotFoundError(f"기존 DB를 찾을 수 없습니다: {old_db_path}")
        
        # 기존 DB 테이블 검증
        self._verify_old_db()
    
    def _verify_old_db(self):
        """기존 DB의 테이블 구조 검증"""
        conn = sqlite3.connect(self.old_db)
        cursor = conn.cursor()
        
        # 필수 테이블 목록
        required_tables = ['categories', 'page_snapshots', 'product_states']
        
        # 존재하는 테이블 조회
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)
        existing_tables = {row[0] for row in cursor.fetchall()}
        
        conn.close()
        
        # 검증
        missing_tables = set(required_tables) - existing_tables
        if missing_tables:
            print(f"⚠️ 기존 DB에 다음 테이블이 없습니다: {missing_tables}")
            print(f"   존재하는 테이블: {existing_tables}")
            raise ValueError(f"기존 DB 구조가 올바르지 않습니다. 필요한 테이블: {required_tables}")
    
    def migrate(self):
        """전체 마이그레이션 실행"""
        print("="*70)
        print("🔄 데이터베이스 마이그레이션 시작")
        print("="*70)
        print(f"원본: {self.old_db}")
        print(f"대상: {self.new_db}")
        print("="*70 + "\n")
        
        # 0. 개선된 DB 초기화
        print("[0/5] 🔧 새 DB 초기화 중...")
        from database import MonitoringDatabase
        MonitoringDatabase(self.new_db)
        print("  ✅ 새 DB 초기화 완료\n")
        
        # 1. 카테고리 마이그레이션
        print("[1/5] 📂 카테고리 마이그레이션...")
        self._migrate_categories()
        
        # 2. 매칭 참조 마이그레이션
        print("[2/5] 🔗 매칭 참조 마이그레이션...")
        self._migrate_matching_reference()
        
        # 3. 스냅샷 마이그레이션
        print("[3/5] 📸 스냅샷 마이그레이션...")
        self._migrate_snapshots()
        
        # 4. 상품 상태 마이그레이션
        print("[4/5] 📦 상품 상태 마이그레이션...")
        self._migrate_product_states()
        
        # 5. 변화 이벤트 마이그레이션
        print("[5/5] 📝 변화 이벤트 마이그레이션...")
        self._migrate_change_events()
        
        print("\n" + "="*70)
        print("✅ 마이그레이션 완료!")
        print("="*70)
        
        # 검증
        self._verify_migration()
    
    def _migrate_categories(self):
        """카테고리 마이그레이션"""
        old_conn = sqlite3.connect(self.old_db)
        new_conn = sqlite3.connect(self.new_db)
        
        categories = old_conn.execute("""
            SELECT id, name, url, created_at FROM categories
        """).fetchall()
        
        for cat in categories:
            new_conn.execute("""
                INSERT OR REPLACE INTO categories (id, name, url, created_at)
                VALUES (?, ?, ?, ?)
            """, cat)
        
        new_conn.commit()
        old_conn.close()
        new_conn.close()
        
        print(f"  ✅ {len(categories)}개 카테고리 마이그레이션 완료")
    
    def _migrate_matching_reference(self):
        """매칭 참조 마이그레이션 (구조 변경)"""
        old_conn = sqlite3.connect(self.old_db)
        new_conn = sqlite3.connect(self.new_db)
        
        # 기존 matching_reference 조회
        matches = old_conn.execute("""
            SELECT 
                coupang_product_id,
                coupang_product_name,
                original_category,
                iherb_upc,
                iherb_part_number,
                created_time
            FROM matching_reference
            WHERE iherb_upc IS NOT NULL OR iherb_part_number IS NOT NULL
        """).fetchall()
        
        for match in matches:
            new_conn.execute("""
                INSERT OR REPLACE INTO matching_reference 
                (coupang_product_id, first_discovered_name, first_discovered_category,
                 iherb_upc, iherb_part_number, first_discovered_at, matched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                match[0],  # coupang_product_id
                match[1],  # first_discovered_name (기존 coupang_product_name)
                match[2],  # first_discovered_category (기존 original_category)
                match[3],  # iherb_upc
                match[4],  # iherb_part_number
                match[5],  # first_discovered_at (기존 created_time)
                match[5]   # matched_at (기존 created_time과 동일)
            ))
        
        new_conn.commit()
        old_conn.close()
        new_conn.close()
        
        print(f"  ✅ {len(matches)}개 매칭 참조 마이그레이션 완료")
    
    def _migrate_snapshots(self):
        """스냅샷 마이그레이션 (category_name 제거)"""
        old_conn = sqlite3.connect(self.old_db)
        new_conn = sqlite3.connect(self.new_db)
        
        snapshots = old_conn.execute("""
            SELECT 
                id, category_id, page_url, snapshot_time,
                total_products, crawl_duration_seconds, status
            FROM page_snapshots
        """).fetchall()
        
        for snap in snapshots:
            new_conn.execute("""
                INSERT OR REPLACE INTO page_snapshots 
                (id, category_id, page_url, snapshot_time, 
                 total_products, crawl_duration_seconds, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, snap)
        
        new_conn.commit()
        old_conn.close()
        new_conn.close()
        
        print(f"  ✅ {len(snapshots)}개 스냅샷 마이그레이션 완료")
    
    def _migrate_product_states(self):
        """상품 상태 마이그레이션 (매칭 정보 제거, 순위 검증)"""
        old_conn = sqlite3.connect(self.old_db)
        new_conn = sqlite3.connect(self.new_db)
        
        # 순위가 유효한 상품만 마이그레이션
        states = old_conn.execute("""
            SELECT 
                id, snapshot_id, coupang_product_id, category_rank,
                product_name, product_url, current_price, original_price,
                discount_rate, review_count, rating_score,
                is_rocket_delivery, is_free_shipping
            FROM product_states
            WHERE category_rank > 0
        """).fetchall()
        
        migrated = 0
        skipped = 0
        
        for state in states:
            try:
                new_conn.execute("""
                    INSERT OR REPLACE INTO product_states 
                    (id, snapshot_id, coupang_product_id, category_rank,
                     product_name, product_url, current_price, original_price,
                     discount_rate, review_count, rating_score,
                     is_rocket_delivery, is_free_shipping)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, state)
                migrated += 1
            except sqlite3.IntegrityError:
                skipped += 1
                continue
        
        new_conn.commit()
        old_conn.close()
        new_conn.close()
        
        print(f"  ✅ {migrated}개 상품 상태 마이그레이션 완료 (스킵: {skipped}개)")
    
    def _migrate_change_events(self):
        """변화 이벤트 마이그레이션 (snapshot_id 추가)"""
        old_conn = sqlite3.connect(self.old_db)
        new_conn = sqlite3.connect(self.new_db)
        
        # 기존 이벤트 조회
        events = old_conn.execute("""
            SELECT 
                ce.id,
                ce.coupang_product_id,
                ce.event_type,
                ce.old_value,
                ce.new_value,
                ce.description,
                ce.event_time,
                c.id as category_id
            FROM change_events ce
            JOIN categories c ON ce.category_name = c.name
        """).fetchall()
        
        migrated = 0
        
        for event in events:
            # snapshot_id 찾기 (이벤트 시간과 가장 가까운 스냅샷)
            snapshot_id = new_conn.execute("""
                SELECT id FROM page_snapshots
                WHERE category_id = ?
                AND snapshot_time <= ?
                ORDER BY snapshot_time DESC
                LIMIT 1
            """, (event[7], event[6])).fetchone()
            
            if not snapshot_id:
                continue
            
            # change_magnitude 계산
            change_magnitude = 0.0
            if event[2] in ['rank_change', 'price_change']:
                try:
                    old_val = float(event[3]) if event[3] else 0
                    new_val = float(event[4]) if event[4] else 0
                    if event[2] == 'rank_change':
                        change_magnitude = old_val - new_val  # 순위: 양수=상승
                    else:
                        change_magnitude = new_val - old_val  # 가격: 양수=인상
                except:
                    pass
            
            new_conn.execute("""
                INSERT OR REPLACE INTO change_events 
                (id, snapshot_id, coupang_product_id, category_id, event_type,
                 old_value, new_value, change_magnitude, description, event_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event[0],  # id
                snapshot_id[0],  # snapshot_id
                event[1],  # coupang_product_id
                event[7],  # category_id
                event[2],  # event_type
                event[3],  # old_value
                event[4],  # new_value
                change_magnitude,
                event[5],  # description
                event[6]   # event_time
            ))
            migrated += 1
        
        new_conn.commit()
        old_conn.close()
        new_conn.close()
        
        print(f"  ✅ {migrated}개 변화 이벤트 마이그레이션 완료")
    
    def _verify_migration(self):
        """마이그레이션 검증"""
        print("\n" + "="*70)
        print("🔍 마이그레이션 검증 중...")
        print("="*70)
        
        old_conn = sqlite3.connect(self.old_db)
        new_conn = sqlite3.connect(self.new_db)
        
        # 카테고리 수
        old_cat = old_conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        new_cat = new_conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        print(f"카테고리: {old_cat} → {new_cat} {'✅' if old_cat == new_cat else '⚠️'}")
        
        # 매칭 참조 수
        old_match = old_conn.execute("""
            SELECT COUNT(*) FROM matching_reference 
            WHERE iherb_upc IS NOT NULL OR iherb_part_number IS NOT NULL
        """).fetchone()[0]
        new_match = new_conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
        print(f"매칭 참조: {old_match} → {new_match} {'✅' if old_match == new_match else '⚠️'}")
        
        # 스냅샷 수
        old_snap = old_conn.execute("SELECT COUNT(*) FROM page_snapshots").fetchone()[0]
        new_snap = new_conn.execute("SELECT COUNT(*) FROM page_snapshots").fetchone()[0]
        print(f"스냅샷: {old_snap} → {new_snap} {'✅' if old_snap == new_snap else '⚠️'}")
        
        # 상품 상태 수
        old_state = old_conn.execute("SELECT COUNT(*) FROM product_states WHERE category_rank > 0").fetchone()[0]
        new_state = new_conn.execute("SELECT COUNT(*) FROM product_states").fetchone()[0]
        print(f"상품 상태: {old_state} → {new_state} {'✅' if old_state <= new_state else '⚠️'}")
        
        # 변화 이벤트 수
        old_event = old_conn.execute("SELECT COUNT(*) FROM change_events").fetchone()[0]
        new_event = new_conn.execute("SELECT COUNT(*) FROM change_events").fetchone()[0]
        print(f"변화 이벤트: {old_event} → {new_event} {'✅' if new_event > 0 else '⚠️'}")
        
        old_conn.close()
        new_conn.close()
        
        print("="*70)


def main():
    """마이그레이션 실행"""
    
    # 기존 DB 확인
    if not os.path.exists("page_monitoring.db"):
        print("❌ 기존 DB(page_monitoring.db)를 찾을 수 없습니다")
        return
    
    # 백업 생성
    import shutil
    backup_path = f"page_monitoring.db.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy("page_monitoring.db", backup_path)
    print(f"💾 백업 생성: {backup_path}\n")
    
    # 마이그레이션 실행
    try:
        migration = DatabaseMigration(
            old_db_path="page_monitoring.db",
            new_db_path="improved_monitoring.db"
        )
        migration.migrate()
        
        print("\n✅ 마이그레이션이 성공적으로 완료되었습니다!")
        print(f"📁 새 DB: improved_monitoring.db")
        print(f"💾 백업: {backup_path}")
        
    except Exception as e:
        print(f"\n❌ 마이그레이션 실패: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()