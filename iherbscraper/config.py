"""
iHerb 스크래퍼 설정 관리
"""

class Config:
    """스크래퍼 전역 설정"""
    
    # 브라우저 설정
    DEFAULT_DELAY_RANGE = (2, 4)
    MAX_RETRIES = 3
    BROWSER_RESTART_INTERVAL = 30
    PAGE_LOAD_TIMEOUT = 30
    IMPLICIT_WAIT = 10
    
    # 검색 설정
    MAX_PRODUCTS_TO_COMPARE = 4
    BASE_URL = "https://www.iherb.com"
    KOREA_URL = "https://kr.iherb.com"
    
    # 유사도 계산 가중치
    SIMILARITY_WEIGHTS = {
        'english': 0.7,
        'korean': 0.2,
        'brand': 0.1
    }
    
    # 매칭 임계값
    MATCHING_THRESHOLDS = {
        'min_similarity': 0.7,
        'success_threshold': 0.6
    }
    
    # 브라우저 옵션
    CHROME_OPTIONS = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-images",
        "--disable-blink-features=AutomationControlled",
        "--disable-extensions",
        "--disable-plugins",
        "--disable-web-security",
        "--allow-running-insecure-content",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--disable-features=TranslateUI",
        "--disable-ipc-flooding-protection",
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "--window-size=1920,1080",
        "--memory-pressure-off",
        "--max_old_space_size=4096",
        "--page-load-strategy=eager"
    ]
    
    # CSS 선택자
    SELECTORS = {
        # 설정 관련
        'settings_button': '.selected-country-wrapper',
        'english_option': '[data-val="en-US"]',  # English 옵션
        'save_button': '.save-selection',
        
        # 검색 결과
        'product_containers': '.product-cell-container',
        'product_link': '.absolute-link.product-link',
        'product_title': '.product-title',
        
        # 상품 페이지
        'product_name': [
            'h1#name[data-testid="product-name"]',
            'h1#name',
            '.product-title h1',
            'h1'
        ],
        'product_specs': '#product-specs-list',
        'part_number': '[data-part-number]',
        
        # 가격 정보
        'list_price': [
            '.list-price',
            '.original-price-wrapper .list-price',
            '.strike-through-price-wrapper .list-price',
            'span[class*="list-price"]',
            '.price-original'
        ],
        'discount_price': [
            '.discount-price',
            '.strike-through-price-wrapper .discount-price',
            'b[class*="discount-price"]',
            '.price-inner b',
            'b[style*="color: rgb(211, 47, 47)"]'
        ],
        'discount_percent': '.percent-off',
        'subscription_discount': '.auto-ship-message-item',
        'price_per_unit': '.discount-price-per-unit'
    }
    
    # 정규표현식 패턴
    PATTERNS = {
        'item_code': r'item\s*code:\s*([A-Z0-9-]+)',
        'product_code_url': r'/pr/([A-Z0-9-]+)',
        'english_count': r'(\d+)\s*(?:count|ct|tablets?|capsules?|softgels?)',
        'dosage_mg': r'(\d+(?:,\d+)*)\s*mg',
        'discount_percent': r'(\d+)%',
        'subscription_discount': r'(\d+)%.*?off',
        'price_usd': r'\$([\d,]+\.?\d*)',
        'price_pattern': r'"price"[:\s]*"?\$?([\d,]+\.?\d*)"?'
    }
    
    # 제형 매핑
    FORM_MAPPING = {
        'tablet': ['tablet', 'tablets', 'tab'],
        'capsule': ['capsule', 'capsules', 'caps', 'vcaps'],
        'softgel': ['softgel', 'softgels', 'soft gel'],
        'gummy': ['gummy', 'gummies'],
        'powder': ['powder', 'pwd'],
        'liquid': ['liquid', 'drops']
    }
    
    # 공통 브랜드
    COMMON_BRANDS = [
        'now foods',
        'nature\'s way', 
        'solgar', 
        'life extension', 
        'jarrow formulas',
        'country life',
        'source naturals',
        'nordic naturals'
    ]
    
    # CSV 컬럼 구조
    OUTPUT_COLUMNS = [
        'coupang_product_id', 'coupang_product_name', 'coupang_product_name_english',
        'iherb_product_name', 'iherb_product_code', 'status', 'similarity_score',
        'coupang_url', 'iherb_product_url',
        'coupang_current_price_krw', 'coupang_original_price_krw', 'coupang_discount_rate',
        'iherb_list_price_usd', 'iherb_discount_price_usd', 'iherb_discount_percent',
        'iherb_subscription_discount', 'iherb_price_per_unit',
        'price_difference_note', 'processed_at', 'actual_index', 'search_language'
    ]