# analyze_orphaned_snapshots.py
import sqlite3

conn = sqlite3.connect("improved_monitoring.db")

# 고아 스냅샷의 상품명 샘플 조회
orphaned_snapshots = conn.execute("""
    SELECT DISTINCT ps.id
    FROM page_snapshots ps
    WHERE ps.category_id IS NULL
    ORDER BY ps.id
""").fetchall()

print(f"고아 스냅샷: {len(orphaned_snapshots)}개\n")

for (snapshot_id,) in orphaned_snapshots:
    # 각 스냅샷의 상위 10개 상품명
    products = conn.execute("""
        SELECT product_name, category_rank
        FROM product_states
        WHERE snapshot_id = ?
        ORDER BY category_rank
        LIMIT 10
    """, (snapshot_id,)).fetchall()
    
    print(f"스냅샷 {snapshot_id}:")
    for name, rank in products[:3]:
        print(f"  {rank}위: {name[:60]}...")
    print()

conn.close()