-- ============================================
-- 브랜드-상품 가격 비교 DB 스키마
-- SQLite 3.x
-- ============================================

-- 1. 브랜드 메타데이터
CREATE TABLE IF NOT EXISTS brands (
    brand_name TEXT PRIMARY KEY,
    coupang_search_url TEXT NOT NULL,
    last_crawled_at TEXT,
    last_matched_at TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);

-- 2. 통합 상품 테이블 (핵심)
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- 식별자
    brand_name TEXT NOT NULL,
    coupang_product_id TEXT NOT NULL,
    iherb_product_code TEXT,
    
    -- 상품명
    coupang_product_name TEXT NOT NULL,
    coupang_product_name_english TEXT,
    iherb_product_name TEXT,
    
    -- URL
    coupang_url TEXT,
    iherb_product_url TEXT,
    
    -- 현재 가격 (빠른 조회용)
    coupang_current_price INTEGER,
    coupang_original_price INTEGER,
    coupang_discount_rate TEXT,
    iherb_discount_price INTEGER,
    iherb_list_price INTEGER,
    
    -- 파이프라인 상태
    pipeline_stage TEXT DEFAULT 'crawled' CHECK(pipeline_stage IN ('crawled', 'translated', 'matched', 'failed')),
    matching_status TEXT DEFAULT 'pending' CHECK(matching_status IN ('pending', 'success', 'not_found', 'error')),
    
    -- 재시작 지원
    processing_lock TEXT,
    last_error TEXT,
    
    -- 타임스탬프
    first_seen_at TEXT NOT NULL,
    last_crawled_at TEXT NOT NULL,
    last_matched_at TEXT,
    price_updated_at TEXT,
    
    -- 제약조건
    UNIQUE(brand_name, coupang_product_id)
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_pipeline ON products(brand_name, pipeline_stage);
CREATE INDEX IF NOT EXISTS idx_processing ON products(processing_lock);
CREATE INDEX IF NOT EXISTS idx_matching ON products(brand_name, matching_status);
CREATE INDEX IF NOT EXISTS idx_last_crawled ON products(brand_name, last_crawled_at);

-- 3. 가격 변동 이력 (변경 시에만 기록)
CREATE TABLE IF NOT EXISTS price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    price_type TEXT NOT NULL CHECK(price_type IN ('coupang', 'iherb')),
    
    old_price INTEGER,
    new_price INTEGER,
    
    changed_at TEXT NOT NULL DEFAULT (datetime('now')),
    
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_price_history_product ON price_history(product_id, changed_at DESC);

-- 4. 쿠팡 추가 정보 (1:1 관계)
CREATE TABLE IF NOT EXISTS coupang_details (
    product_id INTEGER PRIMARY KEY,
    
    stock_status TEXT,
    delivery_badge TEXT,
    origin_country TEXT,
    unit_price TEXT,
    rating REAL,
    review_count INTEGER,
    is_rocket INTEGER,
    
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- 5. 아이허브 추가 정보 (1:1 관계)
CREATE TABLE IF NOT EXISTS iherb_details (
    product_id INTEGER PRIMARY KEY,
    
    discount_percent TEXT,
    subscription_discount TEXT,
    price_per_unit TEXT,
    is_in_stock INTEGER DEFAULT 1,
    stock_message TEXT,
    back_in_stock_date TEXT,
    
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- 6. 파이프라인 에러 로그
CREATE TABLE IF NOT EXISTS pipeline_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    stage TEXT NOT NULL CHECK(stage IN ('crawl', 'translate', 'match')),
    error_type TEXT,
    error_message TEXT,
    occurred_at TEXT NOT NULL DEFAULT (datetime('now')),
    
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_errors_product ON pipeline_errors(product_id, occurred_at DESC);

-- 7. 이미지 정보 (선택적)
CREATE TABLE IF NOT EXISTS product_images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    source TEXT NOT NULL CHECK(source IN ('coupang', 'iherb')),
    
    image_url TEXT,
    local_path TEXT,
    file_size INTEGER,
    width INTEGER,
    height INTEGER,
    
    downloaded_at TEXT,
    
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_images_product ON product_images(product_id, source);

-- ============================================
-- 뷰 (편의 조회용)
-- ============================================

-- 전체 상품 정보 (JOIN 결과)
CREATE VIEW IF NOT EXISTS v_products_full AS
SELECT 
    p.*,
    cd.stock_status, cd.delivery_badge, cd.origin_country, cd.unit_price,
    cd.rating, cd.review_count, cd.is_rocket,
    id.discount_percent, id.subscription_discount, id.price_per_unit,
    id.is_in_stock, id.stock_message, id.back_in_stock_date
FROM products p
LEFT JOIN coupang_details cd ON p.id = cd.product_id
LEFT JOIN iherb_details id ON p.id = id.product_id;

-- 가격 비교 뷰
CREATE VIEW IF NOT EXISTS v_price_comparison AS
SELECT 
    p.id,
    p.brand_name,
    p.coupang_product_name,
    p.iherb_product_name,
    p.coupang_current_price,
    p.iherb_discount_price,
    CASE 
        WHEN p.coupang_current_price IS NULL OR p.iherb_discount_price IS NULL THEN NULL
        WHEN p.coupang_current_price > p.iherb_discount_price THEN 'iherb'
        WHEN p.coupang_current_price < p.iherb_discount_price THEN 'coupang'
        ELSE 'same'
    END AS cheaper_platform,
    ABS(COALESCE(p.coupang_current_price, 0) - COALESCE(p.iherb_discount_price, 0)) AS price_difference,
    p.matching_status,
    cd.stock_status AS coupang_stock,
    id.is_in_stock AS iherb_in_stock
FROM products p
LEFT JOIN coupang_details cd ON p.id = cd.product_id
LEFT JOIN iherb_details id ON p.id = id.product_id
WHERE p.matching_status = 'success';

-- ============================================
-- 초기 데이터 (옵션)
-- ============================================

-- 기존 브랜드 추가
INSERT OR IGNORE INTO brands (brand_name, coupang_search_url) VALUES
('nowfood', 'https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=353%2C35757%2C34529&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=%EB%82%98%EC%9A%B0%ED%91%B8%EB%93%9C'),
('thorne', 'https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=14420&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=thorne');