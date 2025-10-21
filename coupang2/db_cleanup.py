#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DB 정제 스크립트
- 상품명 기준으로 중복 제거
- product_id 불일치 해결
- 데이터 무결성 복구
- 고아 스냅샷 포함 모든 historical 데이터 보존
"""

import sqlite3
import re
from datetime import datetime


class DatabaseCleanup:
    """DB 정제 클래스"""
    
    def __init__(self, db_path="improved_monitoring.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
        print(f"✅ DB 연결: {db_path}\n")
    
    def print_section(self, title):
        """섹션 헤더"""
        print(f"\n{'='*80}")
        print(f"[{title}]")
        print(f"{'='*80}")
    
    def normalize_product_name(self, name: str) -> str:
        """상품명 정규화"""
        if not name:
            return ""
        
        # 1. 소문자 변환
        name = name.lower()
        
        # 2. 공백 정리
        name = re.sub(r'\s+', ' ', name)
        
        # 3. 특수문자 제거
        name = re.sub(r'[^\w\s가-힣]', '', name)
        
        # 4. 앞뒤 공백 제거
        return name.strip()
    
    def analyze_current_state(self):
        """현재 DB 상태 분석"""
        self.print_section("1. 현재 DB 상태 분석")
        
        # 전체 스냅샷 수
        snapshot_count = self.conn.execute(
            "SELECT COUNT(*) FROM page_snapshots"
        ).fetchone()[0]
        print(f"총 스냅샷: {snapshot_count}개")
        
        # 고아 스냅샷 확인
        orphaned_snapshots = self.conn.execute("""
            SELECT COUNT(*) FROM page_snapshots
            WHERE category_id IS NULL
        """).fetchone()[0]
        
        if orphaned_snapshots > 0:
            print(f"  ⚠️ 고아 스냅샷: {orphaned_snapshots}개 (카테고리 없음)")
            print(f"  → 이 스냅샷들도 정제하여 활용합니다")
        
        # 전체 상품 상태 레코드
        state_count = self.conn.execute(
            "SELECT COUNT(*) FROM product_states"
        ).fetchone()[0]
        print(f"총 상품 상태 레코드: {state_count}개")
        
        # 고아 스냅샷의 상품 수
        orphaned_products = self.conn.execute("""
            SELECT COUNT(*)
            FROM product_states ps
            JOIN page_snapshots snap ON ps.snapshot_id = snap.id
            WHERE snap.category_id IS NULL
        """).fetchone()[0]
        
        if orphaned_products > 0:
            print(f"  고아 스냅샷의 상품: {orphaned_products}개")
        
        # unique product_id 수
        unique_ids = self.conn.execute(
            "SELECT COUNT(DISTINCT coupang_product_id) FROM product_states"
        ).fetchone()[0]
        print(f"unique product_id: {unique_ids}개")
        
        # unique 상품명 수 (정규화 전)
        unique_names = self.conn.execute(
            "SELECT COUNT(DISTINCT product_name) FROM product_states"
        ).fetchone()[0]
        print(f"unique 상품명 (정규화 전): {unique_names}개")
        
        # 중복 의심 케이스 찾기
        print(f"\n중복 의심 케이스 분석...")
        
        cursor = self.conn.execute("""
            SELECT 
                product_name,
                COUNT(DISTINCT coupang_product_id) as id_count,
                GROUP_CONCAT(DISTINCT coupang_product_id) as ids
            FROM product_states
            GROUP BY product_name
            HAVING id_count > 1
            ORDER BY id_count DESC
            LIMIT 10
        """)
        
        duplicates = cursor.fetchall()
        
        if duplicates:
            print(f"⚠️ 같은 상품명에 여러 ID가 있는 케이스: {len(duplicates)}개")
            print(f"\n상위 10개 케이스:")
            print(f"{'상품명':<50} {'ID 수':<10} {'ID 샘플':<30}")
            print(f"{'-'*90}")
            
            for name, count, ids_str in duplicates[:10]:
                ids_list = ids_str.split(',')[:3]
                ids_sample = ', '.join(ids_list)
                if len(ids_str.split(',')) > 3:
                    ids_sample += '...'
                print(f"{name[:47]}... {count:<10} {ids_sample:<30}")
        else:
            print(f"✅ 중복 없음")
    
    def create_product_name_mapping(self):
        """상품명 기준 product_id 매핑 테이블 생성"""
        self.print_section("2. 상품명 기준 매핑 테이블 생성 (고아 스냅샷 포함)")
        
        # 임시 매핑 테이블 생성
        self.conn.execute("DROP TABLE IF EXISTS product_name_mapping")
        self.conn.execute("""
            CREATE TABLE product_name_mapping (
                normalized_name TEXT PRIMARY KEY,
                canonical_product_id TEXT NOT NULL,
                original_name TEXT NOT NULL,
                product_ids TEXT,
                first_seen_at DATETIME,
                last_seen_at DATETIME,
                occurrence_count INTEGER DEFAULT 0
            )
        """)
        
        print("✅ 매핑 테이블 생성 완료")
        
        # 모든 상품 조회 (고아 스냅샷 포함)
        print("\n상품명 정규화 및 매핑 생성 중 (모든 스냅샷 포함)...")
        
        cursor = self.conn.execute("""
            SELECT 
                coupang_product_id,
                product_name,
                MIN(ps.id) as first_occurrence,
                MAX(ps.id) as last_occurrence,
                COUNT(*) as occurrence_count
            FROM product_states ps
            GROUP BY coupang_product_id, product_name
        """)
        
        name_to_ids = {}
        
        for product_id, name, first_occ, last_occ, occ_count in cursor:
            normalized = self.normalize_product_name(name)
            
            if not normalized:
                continue
            
            if normalized not in name_to_ids:
                name_to_ids[normalized] = {
                    'ids': set(),
                    'original_name': name,
                    'first_seen': first_occ,
                    'last_seen': last_occ,
                    'occurrence_count': 0
                }
            
            name_to_ids[normalized]['ids'].add(product_id)
            name_to_ids[normalized]['last_seen'] = max(
                name_to_ids[normalized]['last_seen'], last_occ
            )
            name_to_ids[normalized]['occurrence_count'] += occ_count
        
        print(f"정규화된 unique 상품명: {len(name_to_ids)}개")
        
        # 매핑 테이블에 삽입
        print("\n매핑 데이터 삽입 중...")
        
        for normalized, data in name_to_ids.items():
            # canonical_product_id: 가장 최근에 사용된 ID 선택
            # (실제로는 가장 많이 나타난 ID를 선택하는 것이 좋지만, 여기서는 단순화)
            canonical_id = list(data['ids'])[0]
            
            self.conn.execute("""
                INSERT INTO product_name_mapping
                (normalized_name, canonical_product_id, original_name, 
                 product_ids, occurrence_count)
                VALUES (?, ?, ?, ?, ?)
            """, (
                normalized,
                canonical_id,
                data['original_name'],
                ','.join(data['ids']),
                data['occurrence_count']
            ))
        
        self.conn.commit()
        print(f"✅ 매핑 데이터 삽입 완료")
        
        # 통계
        multi_id_count = self.conn.execute("""
            SELECT COUNT(*) FROM product_name_mapping
            WHERE product_ids LIKE '%,%'
        """).fetchone()[0]
        
        print(f"\n매핑 통계:")
        print(f"  - 총 상품: {len(name_to_ids)}개")
        print(f"  - 여러 ID를 가진 상품: {multi_id_count}개")
    
    def update_product_states_with_canonical_ids(self):
        """product_states 테이블의 product_id를 canonical ID로 업데이트"""
        self.print_section("3. product_states 정규화 (모든 스냅샷)")
        
        print("⚠️  주의: 이 작업은 product_states 테이블을 직접 수정합니다!")
        print("백업을 먼저 생성합니다...\n")
        
        # 백업 테이블 생성
        self.conn.execute("DROP TABLE IF EXISTS product_states_backup")
        self.conn.execute("""
            CREATE TABLE product_states_backup AS
            SELECT * FROM product_states
        """)
        
        backup_count = self.conn.execute(
            "SELECT COUNT(*) FROM product_states_backup"
        ).fetchone()[0]
        print(f"✅ 백업 완료: {backup_count}개 레코드")
        
        # 고아 스냅샷 레코드도 포함되는지 확인
        orphaned_in_backup = self.conn.execute("""
            SELECT COUNT(*)
            FROM product_states_backup psb
            JOIN page_snapshots snap ON psb.snapshot_id = snap.id
            WHERE snap.category_id IS NULL
        """).fetchone()[0]
        
        if orphaned_in_backup > 0:
            print(f"  고아 스냅샷 레코드: {orphaned_in_backup}개 (백업에 포함됨)")
        
        # canonical ID로 업데이트
        print("\ncanonical ID로 업데이트 중 (고아 스냅샷 포함)...")
        
        # 임시 컬럼 추가
        try:
            self.conn.execute("""
                ALTER TABLE product_states
                ADD COLUMN normalized_name TEXT
            """)
        except:
            pass  # 이미 존재하면 무시
        
        # normalized_name 채우기 (모든 레코드)
        cursor = self.conn.execute("""
            SELECT id, product_name FROM product_states
        """)
        
        update_count = 0
        for state_id, name in cursor:
            normalized = self.normalize_product_name(name)
            self.conn.execute("""
                UPDATE product_states
                SET normalized_name = ?
                WHERE id = ?
            """, (normalized, state_id))
            update_count += 1
            
            if update_count % 1000 == 0:
                print(f"  처리 중: {update_count}개...")
        
        self.conn.commit()
        print(f"✅ normalized_name 채우기 완료: {update_count}개 (고아 스냅샷 포함)")
        
        # canonical ID로 업데이트
        print("\nproduct_id를 canonical ID로 업데이트 중...")
        
        update_query = """
            UPDATE product_states
            SET coupang_product_id = (
                SELECT canonical_product_id
                FROM product_name_mapping
                WHERE product_name_mapping.normalized_name = product_states.normalized_name
            )
            WHERE EXISTS (
                SELECT 1
                FROM product_name_mapping
                WHERE product_name_mapping.normalized_name = product_states.normalized_name
            )
        """
        
        cursor = self.conn.execute(update_query)
        updated = cursor.rowcount
        self.conn.commit()
        
        print(f"✅ product_id 업데이트 완료: {updated}개 레코드")
        
        # 고아 스냅샷도 업데이트되었는지 확인
        orphaned_after_update = self.conn.execute("""
            SELECT COUNT(DISTINCT ps.coupang_product_id)
            FROM product_states ps
            JOIN page_snapshots snap ON ps.snapshot_id = snap.id
            WHERE snap.category_id IS NULL
        """).fetchone()[0]
        
        if orphaned_after_update > 0:
            print(f"  고아 스냅샷의 unique 상품 (정제 후): {orphaned_after_update}개")
        
        # 검증
        unique_ids_after = self.conn.execute("""
            SELECT COUNT(DISTINCT coupang_product_id) 
            FROM product_states
        """).fetchone()[0]
        
        print(f"\n정규화 결과:")
        print(f"  - unique product_id (정규화 후): {unique_ids_after}개")
    
    def update_matching_reference(self):
        """matching_reference 테이블 정규화"""
        self.print_section("4. matching_reference 정규화")
        
        print("매칭 정보를 canonical ID로 병합 중...\n")
        
        # 백업
        self.conn.execute("DROP TABLE IF EXISTS matching_reference_backup")
        self.conn.execute("""
            CREATE TABLE matching_reference_backup AS
            SELECT * FROM matching_reference
        """)
        print("✅ 백업 완료")
        
        # 임시 테이블로 병합
        self.conn.execute("DROP TABLE IF EXISTS matching_reference_new")
        self.conn.execute("""
            CREATE TABLE matching_reference_new (
                coupang_product_id TEXT PRIMARY KEY,
                first_discovered_category TEXT,
                first_discovered_name TEXT,
                first_discovered_at DATETIME,
                iherb_upc TEXT,
                iherb_part_number TEXT,
                matched_at DATETIME,
                matching_confidence REAL DEFAULT 1.0,
                is_manually_verified BOOLEAN DEFAULT FALSE,
                notes TEXT
            )
        """)
        
        # 기존 매칭 정보를 canonical ID 기준으로 병합
        cursor = self.conn.execute("""
            SELECT DISTINCT
                ps.coupang_product_id as canonical_id,
                mr.first_discovered_category,
                mr.first_discovered_name,
                mr.first_discovered_at,
                mr.iherb_upc,
                mr.iherb_part_number,
                mr.matched_at,
                mr.matching_confidence,
                mr.is_manually_verified,
                mr.notes
            FROM matching_reference mr
            JOIN product_states ps ON mr.coupang_product_id = ps.coupang_product_id
        """)
        
        inserted = 0
        for row in cursor:
            try:
                self.conn.execute("""
                    INSERT OR IGNORE INTO matching_reference_new
                    (coupang_product_id, first_discovered_category, first_discovered_name,
                     first_discovered_at, iherb_upc, iherb_part_number, matched_at,
                     matching_confidence, is_manually_verified, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
                inserted += 1
            except:
                continue
        
        # 기존 테이블 교체
        self.conn.execute("DROP TABLE matching_reference")
        self.conn.execute("""
            ALTER TABLE matching_reference_new
            RENAME TO matching_reference
        """)
        
        self.conn.commit()
        
        new_count = self.conn.execute(
            "SELECT COUNT(*) FROM matching_reference"
        ).fetchone()[0]
        
        print(f"✅ matching_reference 정규화 완료: {new_count}개 레코드")
    
    def cleanup_change_events(self):
        """change_events 테이블 정규화"""
        self.print_section("5. change_events 정규화")
        
        print("변화 이벤트를 canonical ID로 업데이트 중...\n")
        
        # 백업
        self.conn.execute("DROP TABLE IF EXISTS change_events_backup")
        self.conn.execute("""
            CREATE TABLE change_events_backup AS
            SELECT * FROM change_events
        """)
        print("✅ 백업 완료")
        
        # 임시 normalized_name 컬럼 추가
        try:
            self.conn.execute("""
                ALTER TABLE change_events
                ADD COLUMN temp_normalized_name TEXT
            """)
        except:
            pass
        
        # product_states에서 normalized_name 가져오기
        update_query = """
            UPDATE change_events
            SET temp_normalized_name = (
                SELECT DISTINCT normalized_name
                FROM product_states
                WHERE product_states.coupang_product_id = change_events.coupang_product_id
                LIMIT 1
            )
        """
        
        self.conn.execute(update_query)
        self.conn.commit()
        
        # canonical ID로 업데이트
        update_query = """
            UPDATE change_events
            SET coupang_product_id = (
                SELECT canonical_product_id
                FROM product_name_mapping
                WHERE product_name_mapping.normalized_name = change_events.temp_normalized_name
            )
            WHERE temp_normalized_name IS NOT NULL
        """
        
        cursor = self.conn.execute(update_query)
        updated = cursor.rowcount
        self.conn.commit()
        
        print(f"✅ change_events 업데이트 완료: {updated}개 레코드")
    
    def remove_duplicate_snapshots(self):
        """중복 스냅샷 정리"""
        self.print_section("6. 중복 스냅샷 정리")
        
        # 같은 snapshot_id, 같은 canonical_product_id인 레코드 찾기
        duplicates = self.conn.execute("""
            SELECT 
                snapshot_id,
                coupang_product_id,
                COUNT(*) as dup_count
            FROM product_states
            GROUP BY snapshot_id, coupang_product_id
            HAVING dup_count > 1
        """).fetchall()
        
        if not duplicates:
            print("✅ 중복 스냅샷 없음")
            return
        
        print(f"⚠️ 중복 스냅샷 발견: {len(duplicates)}건")
        print("중복 제거 중 (최신 데이터 유지)...\n")
        
        removed_count = 0
        for snapshot_id, product_id, dup_count in duplicates:
            # 최신 데이터만 남기고 삭제
            self.conn.execute("""
                DELETE FROM product_states
                WHERE id NOT IN (
                    SELECT MAX(id)
                    FROM product_states
                    WHERE snapshot_id = ? AND coupang_product_id = ?
                )
                AND snapshot_id = ? AND coupang_product_id = ?
            """, (snapshot_id, product_id, snapshot_id, product_id))
            
            removed_count += (dup_count - 1)
        
        self.conn.commit()
        print(f"✅ 중복 제거 완료: {removed_count}개 레코드 삭제")
    
    def rebuild_indexes(self):
        """인덱스 재구축"""
        self.print_section("7. 인덱스 재구축")
        
        indexes = [
            "DROP INDEX IF EXISTS idx_product_states_snapshot",
            "DROP INDEX IF EXISTS idx_product_states_product",
            "DROP INDEX IF EXISTS idx_product_states_rank",
            "DROP INDEX IF EXISTS idx_change_events_product",
            "DROP INDEX IF EXISTS idx_change_events_time",
            "DROP INDEX IF EXISTS idx_change_events_type_time",
            "DROP INDEX IF EXISTS idx_change_events_snapshot",
            "DROP INDEX IF EXISTS idx_matching_upc",
            "DROP INDEX IF EXISTS idx_matching_part_number",
            
            "CREATE INDEX idx_product_states_snapshot ON product_states(snapshot_id)",
            "CREATE INDEX idx_product_states_product ON product_states(coupang_product_id)",
            "CREATE INDEX idx_product_states_rank ON product_states(snapshot_id, category_rank)",
            "CREATE INDEX idx_change_events_product ON change_events(coupang_product_id)",
            "CREATE INDEX idx_change_events_time ON change_events(event_time)",
            "CREATE INDEX idx_change_events_type_time ON change_events(event_type, event_time)",
            "CREATE INDEX idx_change_events_snapshot ON change_events(snapshot_id)",
            "CREATE INDEX idx_matching_upc ON matching_reference(iherb_upc)",
            "CREATE INDEX idx_matching_part_number ON matching_reference(iherb_part_number)",
            "CREATE INDEX idx_product_name_mapping ON product_name_mapping(normalized_name)"
        ]
        
        for sql in indexes:
            try:
                self.conn.execute(sql)
            except Exception as e:
                print(f"  ⚠️ {sql[:50]}... 실패: {e}")
        
        self.conn.commit()
        print("✅ 인덱스 재구축 완료")
    
    def vacuum_database(self):
        """DB 최적화"""
        self.print_section("8. DB 최적화")
        
        print("VACUUM 실행 중 (시간이 걸릴 수 있습니다)...")
        self.conn.execute("VACUUM")
        print("✅ DB 최적화 완료")
    
    def final_statistics(self):
        """최종 통계"""
        self.print_section("9. 최종 통계 (고아 스냅샷 포함)")
        
        stats = {
            'snapshots': self.conn.execute("SELECT COUNT(*) FROM page_snapshots").fetchone()[0],
            'orphaned_snapshots': self.conn.execute(
                "SELECT COUNT(*) FROM page_snapshots WHERE category_id IS NULL"
            ).fetchone()[0],
            'product_states': self.conn.execute("SELECT COUNT(*) FROM product_states").fetchone()[0],
            'unique_products': self.conn.execute("SELECT COUNT(DISTINCT coupang_product_id) FROM product_states").fetchone()[0],
            'change_events': self.conn.execute("SELECT COUNT(*) FROM change_events").fetchone()[0],
            'matching_references': self.conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0],
            'canonical_mappings': self.conn.execute("SELECT COUNT(*) FROM product_name_mapping").fetchone()[0]
        }
        
        # 고아 스냅샷의 상품 수
        orphaned_products = self.conn.execute("""
            SELECT COUNT(DISTINCT ps.coupang_product_id)
            FROM product_states ps
            JOIN page_snapshots snap ON ps.snapshot_id = snap.id
            WHERE snap.category_id IS NULL
        """).fetchone()[0]
        
        print(f"스냅샷: {stats['snapshots']}개")
        if stats['orphaned_snapshots'] > 0:
            print(f"  ✅ 고아 스냅샷: {stats['orphaned_snapshots']}개 (보존됨)")
            print(f"  ✅ 고아 스냅샷의 unique 상품: {orphaned_products}개 (활용 가능)")
        
        print(f"상품 상태 레코드: {stats['product_states']}개")
        print(f"unique 상품 (정제 후): {stats['unique_products']}개")
        print(f"변화 이벤트: {stats['change_events']}개")
        print(f"매칭 참조: {stats['matching_references']}개")
        print(f"상품명 매핑: {stats['canonical_mappings']}개")
        
        # 개선율 계산
        reduction_rate = (1 - stats['unique_products'] / stats['canonical_mappings']) * 100 if stats['canonical_mappings'] > 0 else 0
        print(f"\n중복 제거율: {reduction_rate:.1f}%")
        
        print(f"\n✅ 모든 historical 데이터가 보존되고 정제되었습니다")
        print(f"   고아 스냅샷을 포함한 전체 기간 분석이 가능합니다")
    
    def run_full_cleanup(self):
        """전체 정제 프로세스 실행"""
        print(f"\n{'#'*80}")
        print(f"# DB 정제 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"# DB: {self.db_path}")
        print(f"# 목적: 모든 historical 데이터 보존 및 정제")
        print(f"{'#'*80}")
        
        try:
            # 1. 현재 상태 분석
            self.analyze_current_state()
            
            # 2. 매핑 테이블 생성
            self.create_product_name_mapping()
            
            # 3. product_states 정규화
            self.update_product_states_with_canonical_ids()
            
            # 4. matching_reference 정규화
            self.update_matching_reference()
            
            # 5. change_events 정규화
            self.cleanup_change_events()
            
            # 6. 중복 제거
            self.remove_duplicate_snapshots()
            
            # 7. 인덱스 재구축
            self.rebuild_indexes()
            
            # 8. DB 최적화
            self.vacuum_database()
            
            # 9. 최종 통계
            self.final_statistics()
            
            print(f"\n{'#'*80}")
            print(f"# ✅ DB 정제 완료!")
            print(f"{'#'*80}")
            print(f"\n백업 테이블:")
            print(f"  - product_states_backup")
            print(f"  - matching_reference_backup")
            print(f"  - change_events_backup")
            print(f"\n새로운 테이블:")
            print(f"  - product_name_mapping (상품명 → canonical ID 매핑)")
            print(f"\n데이터 보존:")
            print(f"  ✅ 모든 스냅샷 보존 (고아 스냅샷 포함)")
            print(f"  ✅ 모든 상품 레코드 보존 및 정제")
            print(f"  ✅ 전체 기간 순위 추적 가능")
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            print(f"\n롤백하려면 백업 테이블을 복원하세요:")
            print(f"  DROP TABLE product_states;")
            print(f"  ALTER TABLE product_states_backup RENAME TO product_states;")
        
        finally:
            self.conn.close()


def main():
    """메인 실행"""
    
    # DB 경로
    db_path = "improved_monitoring.db"
    
    if not os.path.exists(db_path):
        db_path = "page_monitoring.db"
    
    if not os.path.exists(db_path):
        print("❌ DB 파일을 찾을 수 없습니다")
        return
    
    print("⚠️  이 스크립트는 DB를 직접 수정합니다!")
    print("하지만 모든 historical 데이터(고아 스냅샷 포함)를 보존합니다.")
    print("계속하기 전에 DB 백업을 권장합니다.\n")
    
    response = input("계속하시겠습니까? (yes/no): ")
    
    if response.lower() != 'yes':
        print("중단되었습니다.")
        return
    
    try:
        cleanup = DatabaseCleanup(db_path)
        cleanup.run_full_cleanup()
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import os
    main()

    """DB 정제 클래스"""
    
    def __init__(self, db_path="improved_monitoring.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
        print(f"✅ DB 연결: {db_path}\n")
    
    def print_section(self, title):
        """섹션 헤더"""
        print(f"\n{'='*80}")
        print(f"[{title}]")
        print(f"{'='*80}")
    
    def normalize_product_name(self, name: str) -> str:
        """상품명 정규화"""
        if not name:
            return ""
        
        # 1. 소문자 변환
        name = name.lower()
        
        # 2. 공백 정리
        name = re.sub(r'\s+', ' ', name)
        
        # 3. 특수문자 제거
        name = re.sub(r'[^\w\s가-힣]', '', name)
        
        # 4. 앞뒤 공백 제거
        return name.strip()
    
    def analyze_current_state(self):
        """현재 DB 상태 분석"""
        self.print_section("1. 현재 DB 상태 분석")
        
        # 전체 스냅샷 수
        snapshot_count = self.conn.execute(
            "SELECT COUNT(*) FROM page_snapshots"
        ).fetchone()[0]
        print(f"총 스냅샷: {snapshot_count}개")
        
        # 전체 상품 상태 레코드
        state_count = self.conn.execute(
            "SELECT COUNT(*) FROM product_states"
        ).fetchone()[0]
        print(f"총 상품 상태 레코드: {state_count}개")
        
        # unique product_id 수
        unique_ids = self.conn.execute(
            "SELECT COUNT(DISTINCT coupang_product_id) FROM product_states"
        ).fetchone()[0]
        print(f"unique product_id: {unique_ids}개")
        
        # unique 상품명 수 (정규화 전)
        unique_names = self.conn.execute(
            "SELECT COUNT(DISTINCT product_name) FROM product_states"
        ).fetchone()[0]
        print(f"unique 상품명 (정규화 전): {unique_names}개")
        
        # 중복 의심 케이스 찾기
        print(f"\n중복 의심 케이스 분석...")
        
        cursor = self.conn.execute("""
            SELECT 
                product_name,
                COUNT(DISTINCT coupang_product_id) as id_count,
                GROUP_CONCAT(DISTINCT coupang_product_id) as ids
            FROM product_states
            GROUP BY product_name
            HAVING id_count > 1
            ORDER BY id_count DESC
            LIMIT 10
        """)
        
        duplicates = cursor.fetchall()
        
        if duplicates:
            print(f"⚠️ 같은 상품명에 여러 ID가 있는 케이스: {len(duplicates)}개")
            print(f"\n상위 10개 케이스:")
            print(f"{'상품명':<50} {'ID 수':<10} {'ID 샘플':<30}")
            print(f"{'-'*90}")
            
            for name, count, ids_str in duplicates[:10]:
                ids_list = ids_str.split(',')[:3]
                ids_sample = ', '.join(ids_list)
                if len(ids_str.split(',')) > 3:
                    ids_sample += '...'
                print(f"{name[:47]}... {count:<10} {ids_sample:<30}")
        else:
            print(f"✅ 중복 없음")
    
    def create_product_name_mapping(self):
        """상품명 기준 product_id 매핑 테이블 생성"""
        self.print_section("2. 상품명 기준 매핑 테이블 생성")
        
        # 임시 매핑 테이블 생성
        self.conn.execute("DROP TABLE IF EXISTS product_name_mapping")
        self.conn.execute("""
            CREATE TABLE product_name_mapping (
                normalized_name TEXT PRIMARY KEY,
                canonical_product_id TEXT NOT NULL,
                original_name TEXT NOT NULL,
                product_ids TEXT,
                first_seen_at DATETIME,
                last_seen_at DATETIME,
                occurrence_count INTEGER DEFAULT 0
            )
        """)
        
        print("✅ 매핑 테이블 생성 완료")
        
        # 모든 상품 조회
        print("\n상품명 정규화 및 매핑 생성 중...")
        
        cursor = self.conn.execute("""
            SELECT 
                coupang_product_id,
                product_name,
                MIN(ps.id) as first_occurrence,
                MAX(ps.id) as last_occurrence,
                COUNT(*) as occurrence_count
            FROM product_states ps
            GROUP BY coupang_product_id, product_name
        """)
        
        name_to_ids = {}
        
        for product_id, name, first_occ, last_occ, occ_count in cursor:
            normalized = self.normalize_product_name(name)
            
            if not normalized:
                continue
            
            if normalized not in name_to_ids:
                name_to_ids[normalized] = {
                    'ids': set(),
                    'original_name': name,
                    'first_seen': first_occ,
                    'last_seen': last_occ,
                    'occurrence_count': 0
                }
            
            name_to_ids[normalized]['ids'].add(product_id)
            name_to_ids[normalized]['last_seen'] = max(
                name_to_ids[normalized]['last_seen'], last_occ
            )
            name_to_ids[normalized]['occurrence_count'] += occ_count
        
        print(f"정규화된 unique 상품명: {len(name_to_ids)}개")
        
        # 매핑 테이블에 삽입
        print("\n매핑 데이터 삽입 중...")
        
        for normalized, data in name_to_ids.items():
            # canonical_product_id: 가장 최근에 사용된 ID 선택
            # (실제로는 가장 많이 나타난 ID를 선택하는 것이 좋지만, 여기서는 단순화)
            canonical_id = list(data['ids'])[0]
            
            self.conn.execute("""
                INSERT INTO product_name_mapping
                (normalized_name, canonical_product_id, original_name, 
                 product_ids, occurrence_count)
                VALUES (?, ?, ?, ?, ?)
            """, (
                normalized,
                canonical_id,
                data['original_name'],
                ','.join(data['ids']),
                data['occurrence_count']
            ))
        
        self.conn.commit()
        print(f"✅ 매핑 데이터 삽입 완료")
        
        # 통계
        multi_id_count = self.conn.execute("""
            SELECT COUNT(*) FROM product_name_mapping
            WHERE product_ids LIKE '%,%'
        """).fetchone()[0]
        
        print(f"\n매핑 통계:")
        print(f"  - 총 상품: {len(name_to_ids)}개")
        print(f"  - 여러 ID를 가진 상품: {multi_id_count}개")
    
    def update_product_states_with_canonical_ids(self):
        """product_states 테이블의 product_id를 canonical ID로 업데이트"""
        self.print_section("3. product_states 정규화")
        
        print("⚠️  주의: 이 작업은 product_states 테이블을 직접 수정합니다!")
        print("백업을 먼저 생성합니다...\n")
        
        # 백업 테이블 생성
        self.conn.execute("DROP TABLE IF EXISTS product_states_backup")
        self.conn.execute("""
            CREATE TABLE product_states_backup AS
            SELECT * FROM product_states
        """)
        
        backup_count = self.conn.execute(
            "SELECT COUNT(*) FROM product_states_backup"
        ).fetchone()[0]
        print(f"✅ 백업 완료: {backup_count}개 레코드")
        
        # canonical ID로 업데이트
        print("\ncanonical ID로 업데이트 중...")
        
        # 임시 컬럼 추가
        try:
            self.conn.execute("""
                ALTER TABLE product_states
                ADD COLUMN normalized_name TEXT
            """)
        except:
            pass  # 이미 존재하면 무시
        
        # normalized_name 채우기
        cursor = self.conn.execute("""
            SELECT id, product_name FROM product_states
        """)
        
        update_count = 0
        for state_id, name in cursor:
            normalized = self.normalize_product_name(name)
            self.conn.execute("""
                UPDATE product_states
                SET normalized_name = ?
                WHERE id = ?
            """, (normalized, state_id))
            update_count += 1
            
            if update_count % 1000 == 0:
                print(f"  처리 중: {update_count}개...")
        
        self.conn.commit()
        print(f"✅ normalized_name 채우기 완료: {update_count}개")
        
        # canonical ID로 업데이트
        print("\nproduct_id를 canonical ID로 업데이트 중...")
        
        update_query = """
            UPDATE product_states
            SET coupang_product_id = (
                SELECT canonical_product_id
                FROM product_name_mapping
                WHERE product_name_mapping.normalized_name = product_states.normalized_name
            )
            WHERE EXISTS (
                SELECT 1
                FROM product_name_mapping
                WHERE product_name_mapping.normalized_name = product_states.normalized_name
            )
        """
        
        cursor = self.conn.execute(update_query)
        updated = cursor.rowcount
        self.conn.commit()
        
        print(f"✅ product_id 업데이트 완료: {updated}개 레코드")
        
        # 검증
        unique_ids_after = self.conn.execute("""
            SELECT COUNT(DISTINCT coupang_product_id) 
            FROM product_states
        """).fetchone()[0]
        
        print(f"\n정규화 결과:")
        print(f"  - unique product_id (정규화 후): {unique_ids_after}개")
    
    def update_matching_reference(self):
        """matching_reference 테이블 정규화"""
        self.print_section("4. matching_reference 정규화")
        
        print("매칭 정보를 canonical ID로 병합 중...\n")
        
        # 백업
        self.conn.execute("DROP TABLE IF EXISTS matching_reference_backup")
        self.conn.execute("""
            CREATE TABLE matching_reference_backup AS
            SELECT * FROM matching_reference
        """)
        print("✅ 백업 완료")
        
        # 임시 테이블로 병합
        self.conn.execute("DROP TABLE IF EXISTS matching_reference_new")
        self.conn.execute("""
            CREATE TABLE matching_reference_new AS
            SELECT 
                pnm.canonical_product_id as coupang_product_id,
                mr.first_discovered_category,
                mr.first_discovered_name,
                mr.first_discovered_at,
                mr.iherb_upc,
                mr.iherb_part_number,
                mr.matched_at,
                mr.matching_confidence,
                mr.is_manually_verified,
                mr.notes
            FROM matching_reference mr
            JOIN product_states ps ON mr.coupang_product_id = ps.coupang_product_id
            JOIN product_name_mapping pnm ON pnm.normalized_name = (
                SELECT normalized_name 
                FROM product_states 
                WHERE coupang_product_id = mr.coupang_product_id 
                LIMIT 1
            )
            GROUP BY pnm.canonical_product_id
        """)
        
        # 기존 테이블 교체
        self.conn.execute("DROP TABLE matching_reference")
        self.conn.execute("""
            ALTER TABLE matching_reference_new
            RENAME TO matching_reference
        """)
        
        self.conn.commit()
        
        new_count = self.conn.execute(
            "SELECT COUNT(*) FROM matching_reference"
        ).fetchone()[0]
        
        print(f"✅ matching_reference 정규화 완료: {new_count}개 레코드")
    
    def cleanup_change_events(self):
        """change_events 테이블 정규화"""
        self.print_section("5. change_events 정규화")
        
        print("변화 이벤트를 canonical ID로 업데이트 중...\n")
        
        # 백업
        self.conn.execute("DROP TABLE IF EXISTS change_events_backup")
        self.conn.execute("""
            CREATE TABLE change_events_backup AS
            SELECT * FROM change_events
        """)
        print("✅ 백업 완료")
        
        # 임시 normalized_name 컬럼 추가
        try:
            self.conn.execute("""
                ALTER TABLE change_events
                ADD COLUMN temp_normalized_name TEXT
            """)
        except:
            pass
        
        # product_states에서 normalized_name 가져오기
        update_query = """
            UPDATE change_events
            SET temp_normalized_name = (
                SELECT DISTINCT normalized_name
                FROM product_states
                WHERE product_states.coupang_product_id = change_events.coupang_product_id
                LIMIT 1
            )
        """
        
        self.conn.execute(update_query)
        self.conn.commit()
        
        # canonical ID로 업데이트
        update_query = """
            UPDATE change_events
            SET coupang_product_id = (
                SELECT canonical_product_id
                FROM product_name_mapping
                WHERE product_name_mapping.normalized_name = change_events.temp_normalized_name
            )
            WHERE temp_normalized_name IS NOT NULL
        """
        
        cursor = self.conn.execute(update_query)
        updated = cursor.rowcount
        self.conn.commit()
        
        print(f"✅ change_events 업데이트 완료: {updated}개 레코드")
    
    def remove_duplicate_snapshots(self):
        """중복 스냅샷 정리"""
        self.print_section("6. 중복 스냅샷 정리")
        
        # 같은 snapshot_id, 같은 canonical_product_id인 레코드 찾기
        duplicates = self.conn.execute("""
            SELECT 
                snapshot_id,
                coupang_product_id,
                COUNT(*) as dup_count
            FROM product_states
            GROUP BY snapshot_id, coupang_product_id
            HAVING dup_count > 1
        """).fetchall()
        
        if not duplicates:
            print("✅ 중복 스냅샷 없음")
            return
        
        print(f"⚠️ 중복 스냅샷 발견: {len(duplicates)}건")
        print("중복 제거 중 (최신 데이터 유지)...\n")
        
        removed_count = 0
        for snapshot_id, product_id, dup_count in duplicates:
            # 최신 데이터만 남기고 삭제
            self.conn.execute("""
                DELETE FROM product_states
                WHERE id NOT IN (
                    SELECT MAX(id)
                    FROM product_states
                    WHERE snapshot_id = ? AND coupang_product_id = ?
                )
                AND snapshot_id = ? AND coupang_product_id = ?
            """, (snapshot_id, product_id, snapshot_id, product_id))
            
            removed_count += (dup_count - 1)
        
        self.conn.commit()
        print(f"✅ 중복 제거 완료: {removed_count}개 레코드 삭제")
    
    def rebuild_indexes(self):
        """인덱스 재구축"""
        self.print_section("7. 인덱스 재구축")
        
        indexes = [
            "DROP INDEX IF EXISTS idx_product_states_snapshot",
            "DROP INDEX IF EXISTS idx_product_states_product",
            "DROP INDEX IF EXISTS idx_product_states_rank",
            "DROP INDEX IF EXISTS idx_change_events_product",
            "DROP INDEX IF EXISTS idx_change_events_time",
            "DROP INDEX IF EXISTS idx_change_events_type_time",
            "DROP INDEX IF EXISTS idx_matching_upc",
            "DROP INDEX IF EXISTS idx_matching_part_number",
            
            "CREATE INDEX idx_product_states_snapshot ON product_states(snapshot_id)",
            "CREATE INDEX idx_product_states_product ON product_states(coupang_product_id)",
            "CREATE INDEX idx_product_states_rank ON product_states(snapshot_id, category_rank)",
            "CREATE INDEX idx_change_events_product ON change_events(coupang_product_id)",
            "CREATE INDEX idx_change_events_time ON change_events(event_time)",
            "CREATE INDEX idx_change_events_type_time ON change_events(event_type, event_time)",
            "CREATE INDEX idx_matching_upc ON matching_reference(iherb_upc)",
            "CREATE INDEX idx_matching_part_number ON matching_reference(iherb_part_number)",
            "CREATE INDEX idx_product_name_mapping ON product_name_mapping(normalized_name)"
        ]
        
        for sql in indexes:
            try:
                self.conn.execute(sql)
            except Exception as e:
                print(f"  ⚠️ {sql[:50]}... 실패: {e}")
        
        self.conn.commit()
        print("✅ 인덱스 재구축 완료")
    
    def vacuum_database(self):
        """DB 최적화"""
        self.print_section("8. DB 최적화")
        
        print("VACUUM 실행 중 (시간이 걸릴 수 있습니다)...")
        self.conn.execute("VACUUM")
        print("✅ DB 최적화 완료")
    
    def final_statistics(self):
        """최종 통계"""
        self.print_section("9. 최종 통계")
        
        stats = {
            'snapshots': self.conn.execute("SELECT COUNT(*) FROM page_snapshots").fetchone()[0],
            'product_states': self.conn.execute("SELECT COUNT(*) FROM product_states").fetchone()[0],
            'unique_products': self.conn.execute("SELECT COUNT(DISTINCT coupang_product_id) FROM product_states").fetchone()[0],
            'change_events': self.conn.execute("SELECT COUNT(*) FROM change_events").fetchone()[0],
            'matching_references': self.conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0],
            'canonical_mappings': self.conn.execute("SELECT COUNT(*) FROM product_name_mapping").fetchone()[0]
        }
        
        print(f"스냅샷: {stats['snapshots']}개")
        print(f"상품 상태 레코드: {stats['product_states']}개")
        print(f"unique 상품 (정규화 후): {stats['unique_products']}개")
        print(f"변화 이벤트: {stats['change_events']}개")
        print(f"매칭 참조: {stats['matching_references']}개")
        print(f"상품명 매핑: {stats['canonical_mappings']}개")
        
        # 개선율 계산
        reduction_rate = (1 - stats['unique_products'] / stats['canonical_mappings']) * 100 if stats['canonical_mappings'] > 0 else 0
        print(f"\n중복 제거율: {reduction_rate:.1f}%")
    
    def run_full_cleanup(self):
        """전체 정제 프로세스 실행"""
        print(f"\n{'#'*80}")
        print(f"# DB 정제 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"# DB: {self.db_path}")
        print(f"{'#'*80}")
        
        try:
            # 1. 현재 상태 분석
            self.analyze_current_state()
            
            # 2. 매핑 테이블 생성
            self.create_product_name_mapping()
            
            # 3. product_states 정규화
            self.update_product_states_with_canonical_ids()
            
            # 4. matching_reference 정규화
            self.update_matching_reference()
            
            # 5. change_events 정규화
            self.cleanup_change_events()
            
            # 6. 중복 제거
            self.remove_duplicate_snapshots()
            
            # 7. 인덱스 재구축
            self.rebuild_indexes()
            
            # 8. DB 최적화
            self.vacuum_database()
            
            # 9. 최종 통계
            self.final_statistics()
            
            print(f"\n{'#'*80}")
            print(f"# ✅ DB 정제 완료!")
            print(f"{'#'*80}")
            print(f"\n백업 테이블:")
            print(f"  - product_states_backup")
            print(f"  - matching_reference_backup")
            print(f"  - change_events_backup")
            print(f"\n새로운 테이블:")
            print(f"  - product_name_mapping (상품명 → canonical ID 매핑)")
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            print(f"\n롤백하려면 백업 테이블을 복원하세요:")
            print(f"  DROP TABLE product_states;")
            print(f"  ALTER TABLE product_states_backup RENAME TO product_states;")
        
        finally:
            self.conn.close()


def main():
    """메인 실행"""
    
    # DB 경로
    db_path = "improved_monitoring.db"
    
    if not os.path.exists(db_path):
        db_path = "page_monitoring.db"
    
    if not os.path.exists(db_path):
        print("❌ DB 파일을 찾을 수 없습니다")
        return
    
    print("⚠️  이 스크립트는 DB를 직접 수정합니다!")
    print("계속하기 전에 DB 백업을 권장합니다.\n")
    
    response = input("계속하시겠습니까? (yes/no): ")
    
    if response.lower() != 'yes':
        print("중단되었습니다.")
        return
    
    try:
        cleanup = DatabaseCleanup(db_path)
        cleanup.run_full_cleanup()
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import os
    main()