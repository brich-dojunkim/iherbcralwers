"""
iHerb 스크래퍼 설정 관리
"""

class Config:
    """스크래퍼 전역 설정"""
    
    # 브라우저 설정
    DEFAULT_DELAY_RANGE = (1, 2)
    MAX_RETRIES = 3
    BROWSER_RESTART_INTERVAL = 30
    PAGE_LOAD_TIMEOUT = 30
    IMPLICIT_WAIT = 3
    
    # 검색 설정
    MAX_PRODUCTS_TO_COMPARE = 4
    BASE_URL = "https://www.iherb.com"
    KOREA_URL = "https://kr.iherb.com"
    
    # 유사도 계산 가중치 (영어만 사용)
    SIMILARITY_WEIGHTS = {
        'english': 0.8,
        'brand': 0.2
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
        
        # 가격 정보 (원화 기준)
        'subscription_discount_price': [
            '.strike-through-price-wrapper.show .discount-price',
            '.discount-price-wrapper .discount-price',
            'b.discount-price[style*="color: rgb(211, 47, 47)"]',
            '.auto-ship-first .discount-price'
        ],
        'onetime_list_price': [
            '.original-price-config.show .list-price',
            '.one-time-second .list-price',
            'span.list-price'
        ],
        'discount_percent': [
            '.percent-off',
            'span.percent-off',
            '.strike-through-price-wrapper .percent-off'
        ],
        'subscription_discount': [
            '.auto-ship-message-item',
            '.subscription-off-message',
            '.auto-ship-message'
        ],
        'price_per_unit': [
            '.discount-price-per-unit',
            '.list-price-per-unit',
            'span[class*="per-unit"]'
        ]
    }
    
    # 정규표현식 패턴
    PATTERNS = {
        'item_code': r'item\s*code:\s*([A-Z0-9-]+)',
        'product_code_url': r'/pr/([A-Z0-9-]+)',
        'english_count': r'(\d+)\s*(?:count|ct|tablets?|capsules?|softgels?|veg\s*capsules?|vcaps?|pieces?|servings?)',
        'dosage_mg': r'(\d+(?:,\d+)*)\s*mg',
        'discount_percent': r'(\d+)%',
        'subscription_discount': r'(\d+)%.*?off',
        'krw_price': r'₩([\d,]+)',  # 원화 패턴
        'krw_price_quoted': r'"₩([\d,]+)"',  # 따옴표 있는 원화
        'krw_price_spaced': r'₩\s*([\d,]+)'  # 공백 있는 원화
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
    
    OUTPUT_COLUMNS = [
        'iherb_product_name', 'coupang_product_name_english', 'coupang_product_name', 
        'similarity_score', 'matching_reason', 'coupang_url', 'iherb_product_url', 
        'coupang_product_id', 'iherb_product_code',
        'status', 'coupang_current_price_krw', 'coupang_original_price_krw', 'coupang_discount_rate',
        'iherb_list_price_krw', 'iherb_discount_price_krw', 'iherb_discount_percent',
        'iherb_subscription_discount', 'iherb_price_per_unit',
        'price_difference_krw', 'cheaper_platform', 'savings_amount', 'savings_percentage',
        'price_difference_note', 'processed_at', 'actual_index', 'search_language'
    ]