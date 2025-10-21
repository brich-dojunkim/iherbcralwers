# restore_category_ids.py
import sqlite3

conn = sqlite3.connect("improved_monitoring.db")

# 카테고리 ID 조회
categories = conn.execute("""
    SELECT id, name FROM categories
""").fetchall()

print("현재 카테고리:")
for cat_id, name in categories:
    print(f"  {cat_id}: {name}")

# 매핑 정의 (분석 결과 기반으로 수동 설정)
# 예: {snapshot_id: category_id}
mapping = {
    # 분석 후 채울 것
}

conn.close()