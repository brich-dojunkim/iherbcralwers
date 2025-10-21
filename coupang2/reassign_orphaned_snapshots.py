# reassign_orphaned_snapshots.py
import sqlite3

conn = sqlite3.connect("improved_monitoring.db")

print("=== 고아 스냅샷 카테고리 재배치 ===\n")

# 현재 유효한 카테고리
valid_categories = {
    29: '헬스/건강식품',
    30: '출산유아동',
    31: '스포츠레저'
}

print("유효한 카테고리:")
for cat_id, name in valid_categories.items():
    print(f"  {cat_id}: {name}")

# 스냅샷 10~18 분석
orphaned = [10, 11, 12, 13, 14, 15, 16, 17, 18]

print(f"\n고아 스냅샷 분석 (ID {orphaned[0]}~{orphaned[-1]}):\n")

# 각 스냅샷의 상위 5개 상품명 샘플링
snapshot_categories = {}

for snapshot_id in orphaned:
    products = conn.execute("""
        SELECT product_name
        FROM product_states
        WHERE snapshot_id = ?
        ORDER BY category_rank
        LIMIT 5
    """, (snapshot_id,)).fetchall()
    
    print(f"스냅샷 {snapshot_id}:")
    for i, (name,) in enumerate(products, 1):
        print(f"  {i}. {name[:70]}...")
    
    # 카테고리 추론
    sample_text = " ".join([p[0] for p in products])
    
    if any(keyword in sample_text for keyword in ['비타민', '오메가', '유산균', '프로바이오틱', '피쉬오일']):
        inferred_category = 29  # 헬스/건강식품
    elif any(keyword in sample_text for keyword in ['유아', '아기', '분유', '기저귀', '젖병']):
        inferred_category = 30  # 출산유아동
    elif any(keyword in sample_text for keyword in ['프로틴', '단백질', '크레아틴', '아르기닌', '게이너', 'BCAA']):
        inferred_category = 31  # 스포츠레저
    else:
        # 알 수 없음 - 리뷰 수 기반 추가 분석
        avg_review = conn.execute("""
            SELECT AVG(review_count)
            FROM product_states
            WHERE snapshot_id = ?
        """, (snapshot_id,)).fetchone()[0]
        
        # 임시: 리뷰 수가 높으면 헬스/건강식품으로 추정
        inferred_category = 29
    
    snapshot_categories[snapshot_id] = inferred_category
    print(f"  → 추론된 카테고리: {valid_categories[inferred_category]}")
    print()

# 사용자 확인
print("\n=== 재배치 계획 ===")
for snapshot_id, new_cat_id in snapshot_categories.items():
    old_cat_id = conn.execute("""
        SELECT category_id FROM page_snapshots WHERE id = ?
    """, (snapshot_id,)).fetchone()[0]
    
    print(f"스냅샷 {snapshot_id}: category_id {old_cat_id} → {new_cat_id} ({valid_categories[new_cat_id]})")

print("\n이 재배치를 실행하시겠습니까? (yes/no): ", end="")
confirm = input()

if confirm.lower() == 'yes':
    # 재배치 실행
    for snapshot_id, new_cat_id in snapshot_categories.items():
        conn.execute("""
            UPDATE page_snapshots
            SET category_id = ?
            WHERE id = ?
        """, (new_cat_id, snapshot_id))
    
    conn.commit()
    print("\n✅ 재배치 완료!")
    
    # 결과 확인
    print("\n=== 재배치 후 카테고리별 스냅샷 ===\n")
    result = conn.execute("""
        SELECT 
            c.id,
            c.name,
            COUNT(ps.id) as snapshot_count,
            GROUP_CONCAT(ps.id) as snapshot_ids
        FROM categories c
        LEFT JOIN page_snapshots ps ON c.id = ps.category_id
        GROUP BY c.id, c.name
        ORDER BY c.id
    """).fetchall()
    
    for cat_id, name, count, ids in result:
        print(f"{name}: {count}개 스냅샷")
        if ids:
            print(f"  스냅샷 ID: {ids}")
else:
    print("\n❌ 재배치 취소")

conn.close()