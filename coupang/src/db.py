#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rocket_iherb.db 구조 확인용 스크립트
- 모든 테이블 목록
- 각 테이블의 컬럼 정보 (타입, NOT NULL, PK 등)
- 각 테이블의 FK 정보
- 각 테이블의 대략적인 row 수
"""

import sqlite3
from pathlib import Path

DB_PATH = Path("/Users/brich/Desktop/iherb_price/coupang/data/rocket_iherb.db")  # <- 필요하면 여기 경로 수정

def get_tables(conn):
    """sqlite_master에서 user table 목록 가져오기"""
    cur = conn.cursor()
    cur.execute("""
        SELECT name 
        FROM sqlite_master 
        WHERE type='table' 
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
    """)
    return [row[0] for row in cur.fetchall()]

def print_table_info(conn, table_name):
    """PRAGMA로 테이블 구조 출력"""
    cur = conn.cursor()

    print("=" * 80)
    print(f"📌 Table: {table_name}")
    print("=" * 80)

    # 컬럼 정보
    print("\n[컬럼 정보]")
    cur.execute(f"PRAGMA table_info('{table_name}')")
    cols = cur.fetchall()
    # cols: cid, name, type, notnull, dflt_value, pk
    if not cols:
        print("  (컬럼 정보 없음)")
    else:
        print("  cid | name              | type         | notnull | pk | default")
        print("  ----+-------------------+--------------+---------+----+--------")
        for cid, name, col_type, notnull, dflt, pk in cols:
            print(f"  {cid:3d} | {name:17s} | {str(col_type or ''):12s} |"
                  f" {notnull:7d} | {pk:2d} | {dflt}")

    # FK 정보
    print("\n[외래키(FK) 정보]")
    cur.execute(f"PRAGMA foreign_key_list('{table_name}')")
    fks = cur.fetchall()
    # seq, id, table, from, to, on_update, on_delete, match
    if not fks:
        print("  (외래키 없음)")
    else:
        print("  seq | ref_table         | from_col          -> to_col           | on_update | on_delete")
        print("  ----+-------------------+-------------------+--------------------+-----------+----------")
        for seq, fk_id, ref_table, from_col, to_col, on_update, on_delete, match in fks:
            print(f"  {seq:3d} | {ref_table:17s} | {from_col:17s} -> {to_col:18s} |"
                  f" {on_update:9s} | {on_delete:8s}")

    # 인덱스 정보
    print("\n[인덱스 정보]")
    cur.execute(f"PRAGMA index_list('{table_name}')")
    indexes = cur.fetchall()
    # seq, name, unique, origin, partial
    if not indexes:
        print("  (인덱스 없음)")
    else:
        for seq, idx_name, unique, origin, partial in indexes:
            print(f"  - {idx_name} (unique={bool(unique)}, origin={origin}, partial={bool(partial)})")

    # 대략적인 row 수
    print("\n[row 수]")
    try:
        cur.execute(f"SELECT COUNT(*) FROM '{table_name}'")
        count = cur.fetchone()[0]
        print(f"  총 {count} rows")
    except Exception as e:
        print(f"  row 수 조회 중 오류: {e}")

    print()  # 한 줄 띄움


def main():
    if not DB_PATH.exists():
        print(f"❌ DB 파일을 찾을 수 없습니다: {DB_PATH}")
        return

    print("============================================================")
    print(f" DB 구조 분석: {DB_PATH}")
    print("============================================================\n")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        # 외래키 활성화 (혹시 몰라서)
        conn.execute("PRAGMA foreign_keys = ON;")

        tables = get_tables(conn)
        if not tables:
            print("❌ user 테이블이 하나도 없습니다.")
            return

        print(f"테이블 수: {len(tables)}")
        print("테이블 목록:")
        for t in tables:
            print(f"  - {t}")
        print("\n")

        # 각 테이블 구조 출력
        for table in tables:
            print_table_info(conn, table)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
