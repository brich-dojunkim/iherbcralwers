#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DB 구조 분석 도구
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
현재 DB의 상태를 분석하고 마이그레이션 계획을 수립합니다.
"""

import sqlite3
import sys
from pathlib import Path

def analyze_integrated_db(db_path: str):
    """통합 DB 분석"""
    print("\n" + "=" * 80)
    print("📊 통합 DB 분석")
    print("=" * 80)
    print(f"경로: {db_path}\n")
    
    if not Path(db_path).exists():
        print("❌ DB 파일이 존재하지 않습니다.")
        return
    
    conn = sqlite3.connect(db_path)
    
    # 1. 테이블 목록
    print("\n1️⃣ 테이블 목록:")
    print("-" * 80)
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    for table in tables:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  • {table}: {count:,}개")
    
    # 2. Snapshots 상세
    print("\n2️⃣ Snapshots:")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT id, snapshot_date, 
               rocket_category_url_1 IS NOT NULL as has_url1,
               rocket_category_url_2 IS NOT NULL as has_url2,
               rocket_category_url_3 IS NOT NULL as has_url3
        FROM snapshots
        ORDER BY id DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"  ID {row[0]:2d} | {row[1]} | URL1:{row[2]} URL2:{row[3]} URL3:{row[4]}")
    
    # 3. product_features 스키마
    print("\n3️⃣ product_features 스키마:")
    print("-" * 80)
    cursor = conn.execute("PRAGMA table_info(product_features)")
    for row in cursor.fetchall():
        print(f"  {row[0]:2d}. {row[1]:25s} {row[2]:10s}")
    
    # 4. 카테고리 컬럼 존재 확인
    cursor = conn.execute("PRAGMA table_info(product_features)")
    columns = [row[1] for row in cursor.fetchall()]
    has_rocket_category = 'rocket_category' in columns
    has_iherb_category = 'iherb_category' in columns
    
    print("\n4️⃣ 카테고리 컬럼 존재 여부:")
    print("-" * 80)
    print(f"  rocket_category: {'✅ 있음' if has_rocket_category else '❌ 없음'}")
    print(f"  iherb_category: {'✅ 있음' if has_iherb_category else '❌ 없음'}")
    
    # 5. 최신 snapshot의 카테고리 분포
    if has_rocket_category or has_iherb_category:
        cursor = conn.execute("SELECT MAX(id) FROM snapshots")
        latest_id = cursor.fetchone()[0]
        
        if latest_id:
            print(f"\n5️⃣ Snapshot {latest_id}의 카테고리 분포:")
            print("-" * 80)
            
            if has_rocket_category:
                print("  [로켓 카테고리]")
                cursor = conn.execute("""
                    SELECT rocket_category, COUNT(*) as cnt
                    FROM product_features
                    WHERE snapshot_id = ?
                    GROUP BY rocket_category
                """, (latest_id,))
                for row in cursor.fetchall():
                    cat = row[0] if row[0] else "(NULL)"
                    print(f"    • {cat}: {row[1]:,}개")
            
            if has_iherb_category:
                print("\n  [아이허브 카테고리]")
                cursor = conn.execute("""
                    SELECT iherb_category, COUNT(*) as cnt
                    FROM product_features
                    WHERE snapshot_id = ?
                    GROUP BY iherb_category
                """, (latest_id,))
                for row in cursor.fetchall():
                    cat = row[0] if row[0] else "(NULL)"
                    print(f"    • {cat}: {row[1]:,}개")
    
    conn.close()


def analyze_legacy_db(db_path: str):
    """구 DB 분석"""
    print("\n" + "=" * 80)
    print("📊 구 DB 분석 (monitoring.db)")
    print("=" * 80)
    print(f"경로: {db_path}\n")
    
    if not Path(db_path).exists():
        print("❌ DB 파일이 존재하지 않습니다.")
        return
    
    conn = sqlite3.connect(db_path)
    
    # 1. 테이블 목록
    print("\n1️⃣ 테이블 목록:")
    print("-" * 80)
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    for table in tables:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  • {table}: {count:,}개")
    
    # 2. Snapshots 상세
    print("\n2️⃣ Snapshots (최근 10개):")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT 
            s.id,
            DATE(s.snapshot_time) as date,
            c.name as category,
            COUNT(ps.vendor_item_id) as products
        FROM snapshots s
        LEFT JOIN categories c ON s.category_id = c.id
        LEFT JOIN product_states ps ON s.id = ps.snapshot_id
        WHERE s.source_id = (SELECT id FROM sources WHERE source_type = 'rocket_direct')
        GROUP BY s.id
        ORDER BY s.id DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"  ID {row[0]:4d} | {row[1]} | {row[2]:15s} | {row[3]:4d}개")
    
    # 3. 날짜별 통계
    print("\n3️⃣ 날짜별 snapshot 수:")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT 
            DATE(snapshot_time) as date,
            COUNT(*) as snapshot_count,
            SUM((SELECT COUNT(*) FROM product_states WHERE snapshot_id = s.id)) as total_products
        FROM snapshots s
        WHERE source_id = (SELECT id FROM sources WHERE source_type = 'rocket_direct')
        GROUP BY DATE(snapshot_time)
        ORDER BY date DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]} | Snapshots: {row[1]:2d}개 | 상품: {row[2]:,}개")
    
    conn.close()


def analyze_excel_files(excel_dir: str):
    """엑셀 파일 분석"""
    print("\n" + "=" * 80)
    print("📊 엑셀 파일 분석")
    print("=" * 80)
    print(f"경로: {excel_dir}\n")
    
    excel_path = Path(excel_dir)
    if not excel_path.exists():
        print("❌ 디렉토리가 존재하지 않습니다.")
        return
    
    # 파일 목록
    files = {
        'price_inventory': list(excel_path.glob('*price_inventory*.xlsx')),
        'seller_insights': list(excel_path.glob('*SELLER_INSIGHTS*.xlsx')),
        'coupang_price': list(excel_path.glob('Coupang_Price_*.xlsx')),
        'upc': list(excel_path.glob('20251024_*.xlsx'))
    }
    
    print("1️⃣ 파일 목록:")
    print("-" * 80)
    for file_type, file_list in files.items():
        print(f"\n  [{file_type}]")
        if file_list:
            for f in sorted(file_list)[:5]:
                size_mb = f.stat().st_size / 1024 / 1024
                print(f"    • {f.name} ({size_mb:.1f}MB)")
            if len(file_list) > 5:
                print(f"    ... 외 {len(file_list) - 5}개")
        else:
            print("    (파일 없음)")


def main():
    """메인"""
    print("\n" + "=" * 80)
    print("🔍 DB 마이그레이션 분석 도구")
    print("=" * 80)
    
    # 경로 설정
    project_root = Path(__file__).parent.parent
    integrated_db = project_root / "data" / "integrated" / "rocket_iherb.db"
    legacy_db = project_root / "data" / "rocket" / "monitoring.db"
    excel_dir = project_root / "data" / "iherb"
    
    # 분석 실행
    analyze_integrated_db(str(integrated_db))
    analyze_legacy_db(str(legacy_db))
    analyze_excel_files(str(excel_dir))
    
    print("\n" + "=" * 80)
    print("✅ 분석 완료")
    print("=" * 80)
    print("\n💡 다음 단계:")
    print("  1. 구 DB의 데이터를 통합 DB로 마이그레이션")
    print("  2. 엑셀 파일들을 일자별로 통합 DB에 적재")
    print("  3. 카테고리 정보 보완")
    print()


if __name__ == "__main__":
    main()