# verify_specific_snapshots.py
import sqlite3

conn = sqlite3.connect("improved_monitoring.db")

# 1. 스냅샷 17과 18의 카테고리 확인
print("스냅샷 카테고리 확인:")
result = conn.execute("""
    SELECT 
        snap.id,
        snap.category_id,
        c.name as category_name,
        snap.snapshot_time,
        COUNT(ps.id) as product_count
    FROM page_snapshots snap
    LEFT JOIN categories c ON snap.category_id = c.id
    LEFT JOIN product_states ps ON snap.id = ps.snapshot_id
    WHERE snap.id IN (17, 18)
    GROUP BY snap.id
""").fetchall()

for row in result:
    print(f"  스냅샷 {row[0]}: 카테고리ID={row[1]}, 카테고리명={row[2]}, 시간={row[3]}, 상품수={row[4]}")

# 2. 두 스냅샷의 상품명 샘플 비교
print("\n스냅샷 17 상품명 샘플 (TOP 5):")
samples_17 = conn.execute("""
    SELECT product_name, coupang_product_id, category_rank
    FROM product_states
    WHERE snapshot_id = 17
    ORDER BY category_rank
    LIMIT 5
""").fetchall()

for name, pid, rank in samples_17:
    print(f"  {rank}위: {name[:50]}... (ID: {pid})")

print("\n스냅샷 18 상품명 샘플 (TOP 5):")
samples_18 = conn.execute("""
    SELECT product_name, coupang_product_id, category_rank
    FROM product_states
    WHERE snapshot_id = 18
    ORDER BY category_rank
    LIMIT 5
""").fetchall()

for name, pid, rank in samples_18:
    print(f"  {rank}위: {name[:50]}... (ID: {pid})")

# 3. 상품명 기준으로 겹치는 상품 찾기
print("\n상품명 기준 겹치는 상품:")
overlap = conn.execute("""
    SELECT 
        ps17.product_name,
        ps17.coupang_product_id as id_17,
        ps18.coupang_product_id as id_18,
        ps17.category_rank as rank_17,
        ps18.category_rank as rank_18
    FROM product_states ps17
    JOIN product_states ps18 ON ps17.product_name = ps18.product_name
    WHERE ps17.snapshot_id = 17 AND ps18.snapshot_id = 18
    LIMIT 10
""").fetchall()

print(f"총 겹치는 상품명: {len(overlap)}개")
for name, id17, id18, rank17, rank18 in overlap[:5]:
    print(f"\n  상품명: {name[:50]}...")
    print(f"  스냅샷 17: ID={id17}, 순위={rank17}")
    print(f"  스냅샷 18: ID={id18}, 순위={rank18}")
    print(f"  ID 일치: {'✅' if id17 == id18 else '❌'}")

conn.close()