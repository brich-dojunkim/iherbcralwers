#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DB와 CSV 매칭 문제 디버깅 스크립트
"""

import sqlite3
import pandas as pd

print("\n" + "="*80)
print("🔍 DB와 CSV 매칭 문제 디버깅")
print("="*80 + "\n")

# 1. DB 확인
db_path = "/Users/brich/Desktop/iherb_price/coupang2/data/rocket/monitoring.db"
print(f"1️⃣ DB 확인: {db_path}\n")

try:
    conn = sqlite3.connect(db_path)
    
    # 전체 레코드 수
    total_count = pd.read_sql_query(
        "SELECT COUNT(*) as cnt FROM product_states", 
        conn
    ).iloc[0]['cnt']
    print(f"   • 전체 product_states 레코드: {total_count:,}개")
    
    # vendor_item_id 통계
    stats = pd.read_sql_query("""
        SELECT 
            COUNT(DISTINCT vendor_item_id) as 유니크_vendor_item_id,
            SUM(CASE WHEN vendor_item_id IS NULL THEN 1 ELSE 0 END) as NULL_개수,
            SUM(CASE WHEN vendor_item_id = '' THEN 1 ELSE 0 END) as 빈문자열_개수
        FROM product_states
    """, conn)
    print(f"   • 유니크 vendor_item_id: {stats.iloc[0]['유니크_vendor_item_id']:,}개")
    print(f"   • NULL: {stats.iloc[0]['NULL_개수']:,}개")
    print(f"   • 빈 문자열: {stats.iloc[0]['빈문자열_개수']:,}개")
    
    # vendor_item_id 샘플
    print(f"\n   📋 vendor_item_id 샘플 (10개):")
    sample = pd.read_sql_query(
        "SELECT vendor_item_id, product_name FROM product_states WHERE vendor_item_id IS NOT NULL LIMIT 10", 
        conn
    )
    for idx, row in sample.iterrows():
        print(f"      {row['vendor_item_id']}: {row['product_name'][:40]}")
    
    # 날짜별 데이터
    print(f"\n   📅 날짜별 데이터:")
    dates = pd.read_sql_query("""
        SELECT 
            DATE(snap.snapshot_time) as 날짜,
            COUNT(DISTINCT ps.vendor_item_id) as 상품수
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        WHERE ps.vendor_item_id IS NOT NULL
        GROUP BY DATE(snap.snapshot_time)
        ORDER BY DATE(snap.snapshot_time) DESC
        LIMIT 5
    """, conn)
    print(dates.to_string(index=False))
    
    conn.close()
    print(f"\n   ✅ DB 확인 완료")
    
except Exception as e:
    print(f"   ❌ DB 확인 실패: {e}")

# 2. CSV 확인
csv_path = "/Users/brich/Desktop/iherb_price/coupang2/data/rocket/rocket.csv"
print(f"\n2️⃣ CSV 확인: {csv_path}\n")

try:
    df_csv = pd.read_csv(csv_path)
    
    print(f"   • 전체 행: {len(df_csv):,}개")
    print(f"   • 컬럼: {', '.join(df_csv.columns)}")
    print(f"   • vendor_item_id 유니크: {df_csv['vendor_item_id'].nunique():,}개")
    print(f"   • part_number 있음: {(df_csv['iherb_part_number'].notna() & (df_csv['iherb_part_number'] != '')).sum():,}개")
    print(f"   • UPC 있음: {df_csv['iherb_upc'].notna().sum():,}개")
    
    print(f"\n   📋 vendor_item_id 샘플 (10개):")
    for vid in df_csv['vendor_item_id'].head(10):
        print(f"      {vid}")
    
    print(f"\n   ✅ CSV 확인 완료")
    
except Exception as e:
    print(f"   ❌ CSV 확인 실패: {e}")

# 3. 매칭 테스트
print(f"\n3️⃣ 매칭 테스트\n")

try:
    conn = sqlite3.connect(db_path)
    
    # DB에서 vendor_item_id 가져오기
    db_ids = pd.read_sql_query(
        "SELECT DISTINCT vendor_item_id FROM product_states WHERE vendor_item_id IS NOT NULL", 
        conn
    )['vendor_item_id'].astype(str).tolist()
    
    # CSV에서 vendor_item_id 가져오기
    csv_ids = df_csv['vendor_item_id'].astype(str).tolist()
    
    # 교집합
    common = set(db_ids) & set(csv_ids)
    
    print(f"   • DB에만 있음: {len(set(db_ids) - set(csv_ids)):,}개")
    print(f"   • CSV에만 있음: {len(set(csv_ids) - set(db_ids)):,}개")
    print(f"   • 공통 (매칭 가능): {len(common):,}개")
    
    if len(common) > 0:
        print(f"\n   ✅ 매칭 가능한 ID 샘플 (처음 5개):")
        for vid in list(common)[:5]:
            print(f"      {vid}")
    else:
        print(f"\n   ❌ 매칭 가능한 ID가 없습니다!")
        print(f"\n   🔍 원인 분석:")
        print(f"      - DB의 vendor_item_id 형식: {db_ids[:3] if db_ids else '없음'}")
        print(f"      - CSV의 vendor_item_id 형식: {csv_ids[:3] if csv_ids else '없음'}")
    
    conn.close()
    
except Exception as e:
    print(f"   ❌ 매칭 테스트 실패: {e}")

print(f"\n" + "="*80)
print("✅ 디버깅 완료")
print("="*80 + "\n")
