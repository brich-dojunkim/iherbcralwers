#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DB 경량화 마이그레이션
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
목적: 아이허브 공식 스토어 크롤링 데이터 제거
- 로켓직구 데이터만 유지
- matching_reference는 유지 (아이허브 매칭 정보 필요)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import sqlite3
import sys
from datetime import datetime
from pathlib import Path


class DBMigration:
    """DB 마이그레이션"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.backup_path = None
    
    def create_backup(self):
        """백업 생성"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.backup_path = f"{self.db_path}.backup_{timestamp}"
        
        print(f"\n{'='*80}")
        print(f"💾 백업 생성 중...")
        print(f"{'='*80}\n")
        
        import shutil
        shutil.copy2(self.db_path, self.backup_path)
        
        print(f"✅ 백업 완료: {self.backup_path}\n")
    
    def analyze_current_state(self):
        """현재 상태 분석"""
        print(f"\n{'='*80}")
        print(f"📊 현재 DB 상태 분석")
        print(f"{'='*80}\n")
        
        conn = sqlite3.connect(self.db_path)
        
        # 1. 소스별 통계
        print("1️⃣ 소스별 데이터:")
        cursor = conn.execute("""
            SELECT 
                src.source_type,
                src.display_name,
                COUNT(DISTINCT snap.id) as snapshot_count,
                COUNT(DISTINCT ps.vendor_item_id) as product_count
            FROM sources src
            LEFT JOIN snapshots snap ON src.id = snap.source_id
            LEFT JOIN product_states ps ON snap.id = ps.snapshot_id
            GROUP BY src.source_type, src.display_name
        """)
        
        sources_data = {}
        for row in cursor.fetchall():
            source_type, display_name, snap_count, prod_count = row
            sources_data[source_type] = {
                'display_name': display_name,
                'snapshots': snap_count,
                'products': prod_count
            }
            print(f"   • {display_name} ({source_type})")
            print(f"     - 스냅샷: {snap_count:,}개")
            print(f"     - 제품: {prod_count:,}개")
        
        # 2. 테이블별 레코드 수
        print(f"\n2️⃣ 테이블별 레코드 수:")
        for table in ['sources', 'snapshots', 'product_states', 'matching_reference']:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"   • {table}: {count:,}개")
        
        # 3. DB 크기
        page_count = conn.execute("PRAGMA page_count").fetchone()[0]
        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
        db_size_mb = page_count * page_size / 1024 / 1024
        print(f"\n3️⃣ DB 크기: {db_size_mb:.2f} MB")
        
        conn.close()
        
        return sources_data
    
    def confirm_migration(self):
        """마이그레이션 확인"""
        print(f"\n{'='*80}")
        print(f"⚠️  마이그레이션 확인")
        print(f"{'='*80}\n")
        print("다음 작업을 수행합니다:")
        print("  1. iherb_official 소스 데이터 삭제")
        print("     - snapshots 테이블에서 iherb_official 스냅샷 삭제")
        print("     - product_states 테이블에서 해당 제품 상태 삭제")
        print("     - sources 테이블에서 iherb_official 소스 삭제")
        print("\n  2. 유지되는 데이터:")
        print("     - rocket_direct 모든 데이터")
        print("     - matching_reference 전체 (아이허브 매칭 정보)")
        print("     - categories 전체")
        print("\n  3. DB 최적화 (VACUUM)")
        print(f"\n⚠️  백업 파일: {self.backup_path}")
        print(f"{'='*80}\n")
        
        response = input("계속하시겠습니까? (yes/no): ").strip().lower()
        return response in ['yes', 'y']
    
    def execute_migration(self):
        """마이그레이션 실행"""
        print(f"\n{'='*80}")
        print(f"🔧 마이그레이션 실행 중...")
        print(f"{'='*80}\n")
        
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = OFF")  # 외래키 체크 일시 중단
        
        try:
            # 1. iherb_official source_id 찾기
            print("1️⃣ iherb_official source_id 조회...")
            iherb_source_id = conn.execute("""
                SELECT id FROM sources WHERE source_type = 'iherb_official'
            """).fetchone()
            
            if not iherb_source_id:
                print("   ⚠️  iherb_official 소스가 없습니다. 마이그레이션 불필요.")
                conn.close()
                return
            
            iherb_source_id = iherb_source_id[0]
            print(f"   ✓ source_id: {iherb_source_id}")
            
            # 2. iherb_official 스냅샷 찾기
            print(f"\n2️⃣ iherb_official 스냅샷 조회...")
            cursor = conn.execute("""
                SELECT id FROM snapshots WHERE source_id = ?
            """, (iherb_source_id,))
            iherb_snapshot_ids = [row[0] for row in cursor.fetchall()]
            print(f"   ✓ {len(iherb_snapshot_ids)}개 스냅샷 발견")
            
            # 3. product_states 삭제
            print(f"\n3️⃣ product_states에서 iherb_official 데이터 삭제...")
            if iherb_snapshot_ids:
                placeholders = ','.join('?' * len(iherb_snapshot_ids))
                before_count = conn.execute("SELECT COUNT(*) FROM product_states").fetchone()[0]
                
                conn.execute(f"""
                    DELETE FROM product_states 
                    WHERE snapshot_id IN ({placeholders})
                """, iherb_snapshot_ids)
                
                after_count = conn.execute("SELECT COUNT(*) FROM product_states").fetchone()[0]
                deleted_count = before_count - after_count
                print(f"   ✓ {deleted_count:,}개 레코드 삭제")
            
            # 4. snapshots 삭제
            print(f"\n4️⃣ snapshots에서 iherb_official 데이터 삭제...")
            before_count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            
            conn.execute("""
                DELETE FROM snapshots WHERE source_id = ?
            """, (iherb_source_id,))
            
            after_count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            deleted_count = before_count - after_count
            print(f"   ✓ {deleted_count:,}개 스냅샷 삭제")
            
            # 5. matching_reference에서 iherb_official 소스 데이터 삭제
            print(f"\n5️⃣ matching_reference에서 iherb_official 소스 삭제...")
            before_count = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
            
            conn.execute("""
                DELETE FROM matching_reference 
                WHERE matching_source = 'iherb_official'
            """)
            
            after_count = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
            deleted_count = before_count - after_count
            print(f"   ✓ {deleted_count:,}개 매칭 삭제")
            print(f"   ✓ 남은 매칭: {after_count:,}개 (로켓직구 매칭만 유지)")
            
            # 6. sources 테이블에서 iherb_official 삭제
            print(f"\n6️⃣ sources 테이블에서 iherb_official 삭제...")
            conn.execute("""
                DELETE FROM sources WHERE source_type = 'iherb_official'
            """)
            print(f"   ✓ 소스 삭제 완료")
            
            # 커밋
            conn.commit()
            print(f"\n✅ 마이그레이션 완료")
            
        except Exception as e:
            print(f"\n❌ 마이그레이션 실패: {e}")
            conn.rollback()
            raise
        
        finally:
            conn.execute("PRAGMA foreign_keys = ON")  # 외래키 체크 재활성화
            conn.close()
    
    def optimize_database(self):
        """DB 최적화 (VACUUM)"""
        print(f"\n{'='*80}")
        print(f"🔧 DB 최적화 (VACUUM) 실행 중...")
        print(f"{'='*80}\n")
        
        conn = sqlite3.connect(self.db_path)
        
        # 최적화 전 크기
        page_count_before = conn.execute("PRAGMA page_count").fetchone()[0]
        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
        size_before_mb = page_count_before * page_size / 1024 / 1024
        
        print(f"최적화 전 크기: {size_before_mb:.2f} MB")
        
        # VACUUM 실행
        conn.execute("VACUUM")
        
        # 최적화 후 크기
        page_count_after = conn.execute("PRAGMA page_count").fetchone()[0]
        size_after_mb = page_count_after * page_size / 1024 / 1024
        
        print(f"최적화 후 크기: {size_after_mb:.2f} MB")
        print(f"절약된 크기: {size_before_mb - size_after_mb:.2f} MB ({(size_before_mb - size_after_mb) / size_before_mb * 100:.1f}%)\n")
        
        conn.close()
    
    def verify_migration(self):
        """마이그레이션 결과 검증"""
        print(f"\n{'='*80}")
        print(f"✅ 마이그레이션 결과 검증")
        print(f"{'='*80}\n")
        
        conn = sqlite3.connect(self.db_path)
        
        # 1. 소스 확인
        print("1️⃣ 남은 소스:")
        cursor = conn.execute("SELECT source_type, display_name FROM sources")
        for source_type, display_name in cursor.fetchall():
            print(f"   • {display_name} ({source_type})")
        
        # 2. 스냅샷 수
        snapshot_count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        print(f"\n2️⃣ 남은 스냅샷: {snapshot_count:,}개")
        
        # 3. 제품 상태 수
        product_count = conn.execute("SELECT COUNT(*) FROM product_states").fetchone()[0]
        print(f"\n3️⃣ 남은 제품 상태: {product_count:,}개")
        
        # 4. 매칭 통계
        matching_count = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
        print(f"\n4️⃣ 남은 매칭: {matching_count:,}개")
        
        cursor = conn.execute("""
            SELECT matching_source, COUNT(*) 
            FROM matching_reference 
            GROUP BY matching_source
        """)
        for source, count in cursor.fetchall():
            print(f"   • {source}: {count:,}개")
        
        # 5. iherb_official 데이터 확인 (남아있으면 안됨)
        iherb_check = conn.execute("""
            SELECT COUNT(*) FROM sources WHERE source_type = 'iherb_official'
        """).fetchone()[0]
        
        if iherb_check > 0:
            print(f"\n⚠️  경고: iherb_official 소스가 여전히 존재합니다!")
        else:
            print(f"\n✅ 검증 완료: iherb_official 데이터 완전 제거됨")
        
        conn.close()
    
    def run(self):
        """전체 마이그레이션 실행"""
        print(f"\n{'='*80}")
        print(f"🎯 DB 경량화 마이그레이션")
        print(f"{'='*80}")
        print(f"DB: {self.db_path}")
        print(f"{'='*80}\n")
        
        # 1. DB 파일 존재 확인
        if not Path(self.db_path).exists():
            print(f"❌ DB 파일이 없습니다: {self.db_path}")
            return False
        
        # 2. 백업 생성
        self.create_backup()
        
        # 3. 현재 상태 분석
        sources_data = self.analyze_current_state()
        
        # iherb_official이 없으면 종료
        if 'iherb_official' not in sources_data:
            print(f"\n✅ 마이그레이션 불필요: iherb_official 데이터가 없습니다")
            return True
        
        # 4. 확인
        if not self.confirm_migration():
            print(f"\n⚠️  마이그레이션 취소됨")
            return False
        
        # 5. 마이그레이션 실행
        self.execute_migration()
        
        # 6. DB 최적화
        self.optimize_database()
        
        # 7. 결과 검증
        self.verify_migration()
        
        print(f"\n{'='*80}")
        print(f"🎉 마이그레이션 완료!")
        print(f"{'='*80}")
        print(f"\n💡 백업 파일 위치: {self.backup_path}")
        print(f"💡 문제 발생 시 백업으로 복구:")
        print(f"   cp {self.backup_path} {self.db_path}")
        print(f"{'='*80}\n")
        
        return True


def main():
    """메인 함수"""
    
    # DB 경로 (필요시 수정)
    DB_PATH = "monitoring.db"
    
    # 커맨드라인 인자로 경로 지정 가능
    if len(sys.argv) > 1:
        DB_PATH = sys.argv[1]
    
    # 마이그레이션 실행
    migration = DBMigration(DB_PATH)
    success = migration.run()
    
    if success:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()