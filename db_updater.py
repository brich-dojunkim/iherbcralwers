import os
import sqlite3
from config import DatabaseConfig, PathConfig

brands = [
    ("now food", "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=353%2C35757%2C34529&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=%EB%82%98%EC%9A%B0%ED%91%B8%EB%93%9C"),
    ("thorne", "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=14420&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=thorne")
]

os.makedirs(PathConfig.DATA_ROOT, exist_ok=True)
db_path = os.path.join(PathConfig.DATA_ROOT, DatabaseConfig.DATABASE_NAME)

with sqlite3.connect(db_path) as conn:
    conn.execute("CREATE TABLE IF NOT EXISTS brand_info (brand_name TEXT PRIMARY KEY, coupang_url TEXT NOT NULL, last_updated TEXT, result_csv TEXT)")
    
    for brand_name, coupang_url in brands:
        # 기존 데이터가 있으면 URL만 업데이트, 없으면 새로 삽입
        conn.execute("""
            INSERT INTO brand_info (brand_name, coupang_url) 
            VALUES (?, ?)
            ON CONFLICT(brand_name) DO UPDATE SET coupang_url = excluded.coupang_url
        """, (brand_name, coupang_url))

print("브랜드 정보 업데이트 완료 (기존 데이터 유지)")