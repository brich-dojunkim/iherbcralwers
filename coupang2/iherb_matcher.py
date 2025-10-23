#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
matching_reference 테이블 스키마 개선
- CSV 구조에 맞춰 최적화
- 더 나은 인덱싱
"""

import sqlite3
import os


def upgrade_matching_reference_schema(db_path: str):
    """matching_reference 테이블 스키마 업그레이드"""
    
    if not os.path.exists(db_path):
        print(f"❌ DB 파일을 찾을 수 없습니다: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    
    print(f"\n{'='*80}")
    print(f"matching_reference 테이블 스키마 업그레이드")
    print(f"{'='*80}\n")
    
    # 1. 기존 테이블 백업
    print("1. 기존 테이블 백업...")
    try:
        conn.execute("DROP TABLE IF EXISTS matching_reference_old")
        conn.execute("""
            CREATE TABLE matching_reference_old AS
            SELECT * FROM matching_reference
        """)
        
        old_count = conn.execute("SELECT COUNT(*) FROM matching_reference_old").fetchone()[0]
        print(f"   ✓ 백업 완료: {old_count:,}개 레코드")
    except:
        print("   - 기존 테이블 없음")
    
    # 2. 새 테이블 생성
    print("\n2. 새 테이블 생성...")
    
    conn.execute("DROP TABLE IF EXISTS matching_reference")
    
    conn.execute("""
        CREATE TABLE matching_reference (
            -- Primary Key
            coupang_product_id TEXT PRIMARY KEY,
            
            -- 쿠팡 상품 정보
            first_discovered_category TEXT,
            first_discovered_name TEXT,
            first_discovered_at DATETIME,
            
            -- iHerb 매칭 정보 (핵심!)
            iherb_upc TEXT,               -- 13자리 UPC 바코드
            iherb_part_number TEXT,       -- iHerb 파트넘버 (예: NOW-01648)
            
            -- 매칭 메타데이터
            matched_at DATETIME,
            matching_confidence REAL DEFAULT 1.0,
            is_manually_verified BOOLEAN DEFAULT FALSE,
            
            -- 추가 정보
            notes TEXT,
            
            -- 인덱스용 컬럼
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    print("   ✓ 새 테이블 생성 완료")
    
    # 3. 인덱스 생성
    print("\n3. 인덱스 생성...")
    
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_matching_upc ON matching_reference(iherb_upc)",
        "CREATE INDEX IF NOT EXISTS idx_matching_part_number ON matching_reference(iherb_part_number)",
        "CREATE INDEX IF NOT EXISTS idx_matching_category ON matching_reference(first_discovered_category)",
        "CREATE INDEX IF NOT EXISTS idx_matching_verified ON matching_reference(is_manually_verified)",
        "CREATE INDEX IF NOT EXISTS idx_matching_created ON matching_reference(created_at)"
    ]
    
    for idx_sql in indexes:
        conn.execute(idx_sql)
        print(f"   ✓ {idx_sql.split('ON')[0].split('INDEX')[1].strip()}")
    
    # 4. 기존 데이터 마이그레이션 (있다면)
    print("\n4. 기존 데이터 마이그레이션...")
    try:
        cursor = conn.execute("""
            SELECT COUNT(*) FROM matching_reference_old
        """)
        old_count = cursor.fetchone()[0]
        
        if old_count > 0:
            conn.execute("""
                INSERT INTO matching_reference
                SELECT * FROM matching_reference_old
            """)
            
            new_count = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
            print(f"   ✓ 마이그레이션 완료: {new_count:,}개 레코드")
        else:
            print("   - 마이그레이션할 데이터 없음")
    except:
        print("   - 마이그레이션 불필요")
    
    conn.commit()
    
    # 5. 스키마 확인
    print("\n5. 최종 스키마:")
    cursor = conn.execute("PRAGMA table_info(matching_reference)")
    
    print(f"\n{'컬럼명':<30} {'타입':<15} {'NULL 허용':<10}")
    print(f"{'-'*55}")
    for row in cursor.fetchall():
        col_name = row[1]
        col_type = row[2]
        not_null = "NOT NULL" if row[3] else "NULL"
        print(f"{col_name:<30} {col_type:<15} {not_null:<10}")
    
    conn.close()
    
    print(f"\n{'='*80}")
    print(f"✅ 스키마 업그레이드 완료!")
    print(f"{'='*80}\n")


def main():
    """메인 함수"""
    
    db_paths = [
        "monitoring.db"
    ]
    
    for db_path in db_paths:
        if os.path.exists(db_path):
            print(f"\n✅ DB 발견: {db_path}")
            upgrade_matching_reference_schema(db_path)
            break
    else:
        print("❌ DB 파일을 찾을 수 없습니다")


if __name__ == "__main__":
    main()