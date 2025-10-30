#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DB 구조 완전 출력 스크립트
- 모든 테이블, 컬럼, 인덱스, 외래키 정보 출력
"""

import sqlite3

DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/data/monitoring.db"  # 실제 경로


def print_db_structure():
    """DB 구조 완전 출력"""
    
    conn = sqlite3.connect(DB_PATH)
    
    print(f"\n{'='*100}")
    print(f"📊 DB 구조 분석: {DB_PATH}")
    print(f"{'='*100}\n")
    
    # 1. 테이블 목록
    cursor = conn.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    tables = [row[0] for row in cursor.fetchall()]
    
    print(f"📋 총 {len(tables)}개 테이블\n")
    
    # 2. 각 테이블 상세 정보
    for table in tables:
        print(f"{'='*100}")
        print(f"🗂️  테이블: {table}")
        print(f"{'='*100}\n")
        
        # 2-1. CREATE TABLE 문
        create_sql = conn.execute(f"""
            SELECT sql FROM sqlite_master 
            WHERE type='table' AND name='{table}'
        """).fetchone()[0]
        
        print(f"📝 CREATE TABLE 문:")
        print(create_sql)
        print()
        
        # 2-2. 컬럼 상세 정보
        print(f"📊 컬럼 상세:")
        cursor = conn.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        
        print(f"{'No':<5} {'컬럼명':<30} {'타입':<20} {'NotNull':<10} {'Default':<15} {'PK':<5}")
        print("-" * 100)
        
        for col in columns:
            cid, name, col_type, notnull, default_val, pk = col
            default_str = str(default_val) if default_val is not None else '-'
            print(f"{cid:<5} {name:<30} {col_type:<20} {notnull:<10} {default_str:<15} {pk:<5}")
        
        print()
        
        # 2-3. 레코드 수
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"📈 레코드 수: {count:,}개\n")
        
        # 2-4. 샘플 데이터 (최대 3개)
        if count > 0:
            print(f"🔍 샘플 데이터 (최대 3개):")
            cursor = conn.execute(f"SELECT * FROM {table} LIMIT 3")
            rows = cursor.fetchall()
            
            col_names = [desc[0] for desc in cursor.description]
            
            # 컬럼명 출력
            print("   " + " | ".join(f"{name:<20}" for name in col_names[:5]))  # 첫 5개 컬럼만
            print("   " + "-" * 100)
            
            # 데이터 출력
            for row in rows:
                values = [str(v)[:20] if v is not None else 'NULL' for v in row[:5]]
                print("   " + " | ".join(f"{val:<20}" for val in values))
            
            if len(col_names) > 5:
                print(f"   ... (나머지 {len(col_names) - 5}개 컬럼 생략)")
            
            print()
        
        # 2-5. 외래키
        cursor = conn.execute(f"PRAGMA foreign_key_list({table})")
        fks = cursor.fetchall()
        
        if fks:
            print(f"🔗 외래키:")
            for fk in fks:
                print(f"   • {fk[3]} → {fk[2]}.{fk[4]}")
            print()
        
        print()
    
    # 3. 인덱스 목록
    print(f"{'='*100}")
    print(f"🔍 인덱스")
    print(f"{'='*100}\n")
    
    cursor = conn.execute("""
        SELECT name, tbl_name, sql 
        FROM sqlite_master 
        WHERE type='index' AND name NOT LIKE 'sqlite_%'
        ORDER BY tbl_name, name
    """)
    
    indexes = cursor.fetchall()
    
    if indexes:
        current_table = None
        for idx_name, tbl_name, idx_sql in indexes:
            if current_table != tbl_name:
                if current_table is not None:
                    print()
                print(f"📊 테이블: {tbl_name}")
                current_table = tbl_name
            
            print(f"   • {idx_name}")
            if idx_sql:
                print(f"     {idx_sql}")
    else:
        print("인덱스 없음")
    
    print()
    
    # 4. 뷰 목록
    cursor = conn.execute("""
        SELECT name, sql 
        FROM sqlite_master 
        WHERE type='view'
        ORDER BY name
    """)
    
    views = cursor.fetchall()
    
    if views:
        print(f"{'='*100}")
        print(f"👁️  뷰")
        print(f"{'='*100}\n")
        
        for view_name, view_sql in views:
            print(f"📊 뷰: {view_name}")
            print(view_sql)
            print()
    
    conn.close()
    
    print(f"{'='*100}")
    print(f"✅ DB 구조 분석 완료")
    print(f"{'='*100}\n")


if __name__ == "__main__":
    try:
        print_db_structure()
    except Exception as e:
        print(f"\n❌ 오류: {e}")
        import traceback
        traceback.print_exc()