import sqlite3

conn = sqlite3.connect('updater.db')
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS brand_info (
        brand_name   TEXT PRIMARY KEY,
        coupang_url  TEXT NOT NULL,
        last_updated TEXT,
        result_csv   TEXT
    )
""")
cursor.execute("""
    INSERT OR REPLACE INTO brand_info (brand_name, coupang_url)
    VALUES (?, ?)
""", (
    "thorne",
    "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=14420&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=thorne"
))
conn.commit()
conn.close()
