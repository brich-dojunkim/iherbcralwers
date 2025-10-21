# fix_category_ids.py
import sqlite3

conn = sqlite3.connect("improved_monitoring.db")

print("=== 카테고리 정리 시작 ===\n")

# 1. 모든 카테고리 조회
categories = conn.execute("""
    SELECT id, name, url, created_at
    FROM categories
    ORDER BY id
""").fetchall()

print("현재 카테고리:")
for cat_id, name, url, created in categories:
    print(f"  ID {cat_id}: {name} (생성: {created})")

# 2. 카테고리별로 그룹화 (이름 기준)
category_groups = {}
for cat_id, name, url, created in categories:
    if name not in category_groups:
        category_groups[name] = []
    category_groups[name].append({
        'id': cat_id,
        'url': url,
        'created': created
    })

print("\n중복 카테고리 분석:")
for name, versions in category_groups.items():
    if len(versions) > 1:
        print(f"\n'{name}': {len(versions)}개 버전")
        for v in versions:
            # 각 버전의 스냅샷 수
            snap_count = conn.execute("""
                SELECT COUNT(*) FROM page_snapshots
                WHERE category_id = ?
            """, (v['id'],)).fetchone()[0]
            print(f"  ID {v['id']}: {snap_count}개 스냅샷 (생성: {v['created']})")

# 3. 통합 계획 수립
print("\n\n=== 통합 계획 ===")
print("각 카테고리의 가장 오래된 ID로 통합합니다:\n")

for name, versions in category_groups.items():
    if len(versions) > 1:
        # 가장 오래된 ID (가장 작은 ID)
        canonical_id = min(v['id'] for v in versions)
        other_ids = [v['id'] for v in versions if v['id'] != canonical_id]
        
        print(f"{name}:")
        print(f"  → Canonical ID: {canonical_id}")
        print(f"  → 통합할 ID: {other_ids}")
        
        # 스냅샷 업데이트
        for old_id in other_ids:
            conn.execute("""
                UPDATE page_snapshots
                SET category_id = ?
                WHERE category_id = ?
            """, (canonical_id, old_id))
            print(f"     스냅샷 이동: {old_id} → {canonical_id}")
        
        # 중복 카테고리 삭제
        for old_id in other_ids:
            conn.execute("""
                DELETE FROM categories WHERE id = ?
            """, (old_id,))
            print(f"     카테고리 삭제: ID {old_id}")

conn.commit()

# 4. 결과 확인
print("\n\n=== 정리 후 결과 ===\n")
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
    print(f"ID {cat_id} ({name}): {count}개 스냅샷")
    if ids:
        id_list = ids.split(',')
        if len(id_list) <= 10:
            print(f"  스냅샷: {ids}")
        else:
            print(f"  스냅샷: {','.join(id_list[:5])}...{','.join(id_list[-5:])}")

conn.close()

print("\n✅ 카테고리 정리 완료!")