"""
iHerb 스크래퍼 설정 관리 - Gemini 2.5 Flash 적용 (최종 수정 버전)
주요 변경사항:
1. 매칭 관련 4개 컬럼 제거 (similarity_score, matching_reason, gemini_confidence, failure_type)
2. 쿠팡 재고 관련 5개 컬럼 추가
3. DOSAGE_MISMATCH 제거
4. 최종 34개 컬럼 구조
"""

class FailureType:
    """실패 유형 분류 - 정리된 버전"""
    
    # 시스템 오류 (재시도 필요)
    BROWSER_ERROR = "BROWSER_ERROR"
    NETWORK_ERROR = "NETWORK_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    WEBDRIVER_ERROR = "WEBDRIVER_ERROR"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    UNPROCESSED = "UNPROCESSED"
    
    # Gemini API 관련 오류
    GEMINI_API_ERROR = "GEMINI_API_ERROR"
    GEMINI_TIMEOUT = "GEMINI_TIMEOUT"
    GEMINI_QUOTA_EXCEEDED = "GEMINI_QUOTA_EXCEEDED"
    
    # 정당한 실패 (재시도 불필요)
    NO_SEARCH_RESULTS = "NO_SEARCH_RESULTS"
    NO_MATCHING_PRODUCT = "NO_MATCHING_PRODUCT"
    COUNT_MISMATCH = "COUNT_MISMATCH"
    GEMINI_NO_MATCH = "GEMINI_NO_MATCH"
    
    # 성공
    SUCCESS = "SUCCESS"

    @classmethod
    def is_system_error(cls, failure_type):
        """시스템 오류 여부 판단"""
        system_errors = {
            cls.BROWSER_ERROR, cls.NETWORK_ERROR, cls.TIMEOUT_ERROR,
            cls.WEBDRIVER_ERROR, cls.PROCESSING_ERROR, cls.UNPROCESSED,
            cls.GEMINI_API_ERROR, cls.GEMINI_TIMEOUT
        }
        return failure_type in system_errors
    
    @classmethod
    def get_description(cls, failure_type):
        """실패 유형 설명"""
        descriptions = {
            cls.BROWSER_ERROR: "브라우저 연결/크래시 오류",
            cls.NETWORK_ERROR: "네트워크 연결 오류", 
            cls.TIMEOUT_ERROR: "타임아웃 오류",
            cls.WEBDRIVER_ERROR: "웹드라이버 오류",
            cls.PROCESSING_ERROR: "처리 중 오류",
            cls.UNPROCESSED: "처리되지 않음",
            cls.GEMINI_API_ERROR: "Gemini API 일반 오류",
            cls.GEMINI_TIMEOUT: "Gemini API 타임아웃",
            cls.GEMINI_QUOTA_EXCEEDED: "Gemini API 할당량 초과",
            cls.NO_SEARCH_RESULTS: "검색 결과 없음",
            cls.NO_MATCHING_PRODUCT: "매칭되는 상품 없음",
            cls.COUNT_MISMATCH: "개수 불일치",
            cls.GEMINI_NO_MATCH: "Gemini 판단: 동일 제품 없음",
            cls.SUCCESS: "성공"
        }
        return descriptions.get(failure_type, "알 수 없는 오류")


class Config:
    """스크래퍼 전역 설정 - Gemini 2.5 Flash 최적화 버전"""
    
    # ========== Gemini 2.5 Flash 설정 ==========
    GEMINI_API_KEY = "AIzaSyA2r-_8ePWcmP-5o9esScT2pcOgj_57J3M"
    GEMINI_TEXT_MODEL = "models/gemini-2.0-flash"
    GEMINI_VISION_MODEL = "models/gemini-2.0-flash"
    GEMINI_MAX_RETRIES = 3
    GEMINI_TIMEOUT = 25
    GEMINI_RATE_LIMIT_DELAY = 5
    
    # ========== 이미지 비교 설정 ==========
    COUPANG_IMAGES_DIR = "/Users/brich/Desktop/iherb_price/coupang/coupang_images"
    IHERB_IMAGES_DIR = "/Users/brich/Desktop/iherb_price/iherbscraper/iherb_images"
    IMAGE_COMPARISON_ENABLED = True
    IMAGE_DOWNLOAD_TIMEOUT = 12
    MAX_IMAGE_SIZE_MB = 8
    
    # ========== 브라우저 설정 ==========
    DEFAULT_DELAY_RANGE = (1.5, 3)
    MAX_RETRIES = 3
    BROWSER_RESTART_INTERVAL = 25
    PAGE_LOAD_TIMEOUT = 18
    IMPLICIT_WAIT = 2.5
    
    # ========== 검색 설정 ==========
    MAX_PRODUCTS_TO_COMPARE = 4
    BASE_URL = "https://www.iherb.com"
    KOREA_URL = "https://kr.iherb.com"
    
    # ========== 브라우저 옵션 ==========
    CHROME_OPTIONS = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
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
    
    # ========== CSS 선택자 ==========
    SELECTORS = {
        'settings_button': '.selected-country-wrapper',
        'english_option': '[data-val="en-US"]',
        'save_button': '.save-selection',
        'product_containers': '.product-cell-container',
        'product_link': '.absolute-link.product-link',
        'product_title': '.product-title',
        'product_name': [
            'h1#name[data-testid="product-name"]',
            'h1#name',
            '.product-title h1',
            'h1'
        ],
        'product_specs': '#product-specs-list',
        'part_number': '[data-part-number]',
    }
    
    # ========== 정규표현식 패턴 ==========
    PATTERNS = {
        'item_code': r'item\s*code:\s*([A-Z0-9-]+)',
        'product_code_url': r'/pr/([A-Z0-9-]+)',
        'english_count': r'(\d+)\s+(?:count|ct|tablets?|capsules?|softgels?|veg\s*capsules?|vcaps?|pieces?|servings?|tab)(?!\w)',
        
        # 정밀한 가격 패턴들
        'krw_discount_price_red': r'<b[^>]*class="[^"]*discount-price[^"]*"[^>]*style="[^"]*color:\s*rgb\(211,\s*47,\s*47\)[^"]*"[^>]*>₩([\d,]+)</b>',
        'krw_discount_price_simple': r'<b[^>]*discount-price[^>]*>₩([\d,]+)</b>',
        'krw_out_of_stock_price': r'data-testid="product-price"[^>]*>\s*<p>\s*₩([\d,]+)\s*</p>',
        'krw_list_price_span': r'<span[^>]*class="[^"]*list-price[^"]*"[^>]*>₩([\d,]+)</span>',
        'krw_list_price_general': r'class="list-price[^"]*"[^>]*>₩([\d,]+)',
        
        'percent_off_bracket': r'<span[^>]*percent-off[^>]*>\((\d+)%\s*off\)</span>',
        'percent_off_simple': r'(\d+)%\s*off',
        
        'price_per_unit_span': r'<span[^>]*price-per-unit[^>]*>₩(\d+)/(serving|unit|tablet|capsule)</span>',
        'price_per_unit_text': r'₩(\d+)/(serving|unit|tablet|capsule)(?![^<]*</[^>]*>)',
        'price_per_serving_direct': r'₩(\d+)/serving',
        
        'subscription_discount_future': r'(\d+)%\s*off\s+on.*?future.*?orders',
        'subscription_discount_autoship': r'(\d+)%\s*off.*?autoship',
        
        'out_of_stock_testid': r'data-testid="product-stock-status"[^>]*>[^<]*out\s+of\s+stock',
        'out_of_stock_text': r'out\s+of\s+stock',
        'notify_me_button': r'notify\s+me',
        
        'back_in_stock_date_testid': r'data-testid="product-stock-status-text"[^>]*>([^<]+)',
        'back_in_stock_general': r'back\s+in\s+stock\s+date\s*:?\s*([^<\n]+)',
        
        'krw_price_general': r'₩([\d,]+)',
        'krw_price_quoted': r'"₩([\d,]+)"',
    }
    
    # ========== 출력 컬럼 (최종 34개) - 매칭 컬럼 제거 + 쿠팡 재고 추가 ==========
    OUTPUT_COLUMNS = [
        # 상품정보 (5개)
        'iherb_product_name', 
        'coupang_product_name_english', 
        'coupang_product_name',
        'coupang_product_id', 
        'iherb_product_code',
        
        # URL정보 (2개)
        'coupang_url', 
        'iherb_product_url',
        
        # 상태정보 (1개)
        'status',
        
        # 쿠팡가격 (3개)
        'coupang_current_price_krw', 
        'coupang_original_price_krw', 
        'coupang_discount_rate',
        
        # 아이허브가격 (4개)
        'iherb_list_price_krw', 
        'iherb_discount_price_krw', 
        'iherb_discount_percent',
        'iherb_subscription_discount',
        
        # 쿠팡재고 (5개) - 🆕 추가
        'coupang_stock_status',      # 쿠팡 재고 상태
        'coupang_delivery_badge',    # 배송 정보
        'coupang_origin_country',    # 원산지
        'coupang_unit_price',        # 단위가격
        
        # 아이허브재고 (4개)
        'iherb_price_per_unit',
        'is_in_stock', 
        'stock_message', 
        'back_in_stock_date',
        
        # 가격비교 (5개)
        'price_difference_krw', 
        'cheaper_platform', 
        'savings_amount', 
        'savings_percentage',
        'price_difference_note'
    ]
