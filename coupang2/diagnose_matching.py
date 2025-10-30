#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
UPC 매칭 문제 진단 스크립트
"""

import sqlite3
import pandas as pd

DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/data/rocket/monitoring.db"
PRICE_INVENTORY_PATH = "/Users/brich/Desktop/iherb_price/coupang2/data/iherb/price_inventory_251028.xlsx"

print("="*80)
print("🔍 UPC 매칭 문제 진단")
print("="*80 + "\n")

# 1. matching_reference 테이블 확인
print("1️⃣ matching_reference 테이블 샘플 (상위 10개)")
print("-"*80)

conn = sqlite3.connect(DB_PATH)

query = """
SELECT 
    vendor_item_id,
    iherb_upc,
    iherb_part_number,
    matching_source,
    product_name
FROM matching_reference
LIMIT 10
"""

df_matching = pd.read_sql_query(query, conn)
print(df_matching.to_string(index=False))

# 2. matching_reference 통계
print("\n\n2️⃣ matching_reference 통계")
print("-"*80)

total_matching = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
with_upc = conn.execute("SELECT COUNT(*) FROM matching_reference WHERE iherb_upc IS NOT NULL AND iherb_upc != ''").fetchone()[0]

print(f"총 매칭 레코드: {total_matching:,}개")
print(f"UPC 있는 레코드: {with_upc:,}개")

# 3. product_states에서 실제 UPC 확인
print("\n\n3️⃣ product_states + matching_reference JOIN 결과 (최신 스냅샷)")
print("-"*80)

query = """
SELECT 
    ps.vendor_item_id,
    SUBSTR(ps.product_name, 1, 40) as product_name,
    mr.iherb_upc,
    mr.iherb_part_number
FROM product_states ps
LEFT JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
WHERE ps.snapshot_id = (SELECT MAX(id) FROM snapshots)
LIMIT 20
"""

df_joined = pd.read_sql_query(query, conn)
print(df_joined.to_string(index=False))

conn.close()

# 4. 아이허브 Excel 파일 확인
print("\n\n4️⃣ 아이허브 price_inventory UPC 샘플")
print("-"*80)

df_iherb = pd.read_excel(PRICE_INVENTORY_PATH, header=1, skiprows=[0])

print(f"전체 레코드: {len(df_iherb):,}개")
print(f"\n상위 10개:")
print(df_iherb[['옵션 ID', '쿠팡 노출 상품명', '바코드']].head(10).to_string(index=False))

# 5. UPC 데이터 타입 확인
print("\n\n5️⃣ UPC 데이터 타입 분석")
print("-"*80)

print("\n📊 아이허브 '바코드' 컬럼:")
print(f"  - 컬럼 타입: {df_iherb['바코드'].dtype}")
print(f"  - Null 개수: {df_iherb['바코드'].isna().sum():,}")
print(f"  - 유효한 값 개수: {df_iherb['바코드'].notna().sum():,}")
print(f"  - 샘플 값들:")

valid_barcodes = df_iherb['바코드'].dropna().head(5)
for i, val in enumerate(valid_barcodes):
    print(f"    {i+1}. {val} (타입: {type(val).__name__}, 길이: {len(str(val))})")

# 6. matching_reference의 UPC 형식 확인
print("\n\n6️⃣ matching_reference의 UPC 형식")
print("-"*80)

conn = sqlite3.connect(DB_PATH)
query = """
SELECT iherb_upc, LENGTH(iherb_upc) as upc_length, COUNT(*) as cnt
FROM matching_reference
WHERE iherb_upc IS NOT NULL AND iherb_upc != ''
GROUP BY LENGTH(iherb_upc)
ORDER BY cnt DESC
"""

df_upc_lengths = pd.read_sql_query(query, conn)
print(df_upc_lengths.to_string(index=False))

# 7. 실제 UPC 값 비교
print("\n\n7️⃣ UPC 값 직접 비교 (첫 5개)")
print("-"*80)

query = """
SELECT iherb_upc
FROM matching_reference
WHERE iherb_upc IS NOT NULL AND iherb_upc != ''
LIMIT 5
"""

db_upcs = pd.read_sql_query(query, conn)
conn.close()

print("DB의 UPC 값들:")
for i, upc in enumerate(db_upcs['iherb_upc']):
    print(f"  {i+1}. '{upc}' (타입: str, 길이: {len(upc)})")

print("\nExcel의 바코드 값들:")
excel_barcodes = df_iherb['바코드'].dropna().head(5)
for i, barcode in enumerate(excel_barcodes):
    barcode_str = str(int(barcode)) if isinstance(barcode, float) else str(barcode)
    print(f"  {i+1}. '{barcode_str}' (원본: {barcode}, 타입: {type(barcode).__name__}, 길이: {len(barcode_str)})")

# 8. 실제 매칭 테스트
print("\n\n8️⃣ 실제 매칭 가능성 테스트")
print("-"*80)

# DB UPC를 set으로
conn = sqlite3.connect(DB_PATH)
db_upc_set = set(pd.read_sql_query(
    "SELECT DISTINCT iherb_upc FROM matching_reference WHERE iherb_upc IS NOT NULL AND iherb_upc != ''",
    conn
)['iherb_upc'])
conn.close()

# Excel UPC를 set으로 (정규화)
excel_upc_set = set()
for barcode in df_iherb['바코드'].dropna():
    if isinstance(barcode, float):
        barcode_str = str(int(barcode))
    else:
        barcode_str = str(barcode)
    excel_upc_set.add(barcode_str)

print(f"DB UPC 고유값: {len(db_upc_set):,}개")
print(f"Excel 바코드 고유값: {len(excel_upc_set):,}개")

# 교집합 확인
matching_upcs = db_upc_set & excel_upc_set
print(f"매칭 가능한 UPC: {matching_upcs.__len__():,}개")

if len(matching_upcs) > 0:
    print(f"\n✅ 매칭 가능! 샘플 UPC:")
    for i, upc in enumerate(list(matching_upcs)[:5]):
        print(f"  {i+1}. {upc}")
else:
    print(f"\n❌ 매칭 불가능!")
    print(f"\nDB UPC 샘플 (첫 5개):")
    for i, upc in enumerate(list(db_upc_set)[:5]):
        print(f"  {i+1}. {upc}")
    
    print(f"\nExcel 바코드 샘플 (첫 5개):")
    for i, upc in enumerate(list(excel_upc_set)[:5]):
        print(f"  {i+1}. {upc}")

print("\n" + "="*80)
print("진단 완료")
print("="*80)