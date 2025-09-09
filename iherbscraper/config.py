"""
iHerb 스크래퍼 설정 관리 - 정규식 기반 가격 추출 최적화
"""

class FailureType:
    """실패 유형 분류"""
    
    # 시스템 오류 (재시도 필요)
    BROWSER_ERROR = "BROWSER_ERROR"              # 브라우저 연결/크래시 오류
    NETWORK_ERROR = "NETWORK_ERROR"              # 네트워크 연결 오류
    TIMEOUT_ERROR = "TIMEOUT_ERROR"              # 타임아웃 오류
    WEBDRIVER_ERROR = "WEBDRIVER_ERROR"          # 웹드라이버 오류
    PROCESSING_ERROR = "PROCESSING_ERROR"        # 기타 처리 오류
    UNPROCESSED = "UNPROCESSED"                  # 처리되지 않음
    
    # 정당한 실패 (재시도 불필요)
    NO_SEARCH_RESULTS = "NO_SEARCH_RESULTS"      # 검색 결과 없음
    NO_MATCHING_PRODUCT = "NO_MATCHING_PRODUCT"  # 매칭되는 상품 없음
    COUNT_MISMATCH = "COUNT_MISMATCH"            # 개수 불일치
    DOSAGE_MISMATCH = "DOSAGE_MISMATCH"          # 용량 불일치
    LOW_SIMILARITY = "LOW_SIMILARITY"            # 유사도 부족
    
    # 성공
    SUCCESS = "SUCCESS"                          # 성공

    @classmethod
    def is_system_error(cls, failure_type):
        """시스템 오류 여부 판단"""
        system_errors = {
            cls.BROWSER_ERROR, cls.NETWORK_ERROR, cls.TIMEOUT_ERROR,
            cls.WEBDRIVER_ERROR, cls.PROCESSING_ERROR, cls.UNPROCESSED
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
            cls.NO_SEARCH_RESULTS: "검색 결과 없음",
            cls.NO_MATCHING_PRODUCT: "매칭되는 상품 없음",
            cls.COUNT_MISMATCH: "개수 불일치",
            cls.DOSAGE_MISMATCH: "용량 불일치", 
            cls.LOW_SIMILARITY: "유사도 부족",
            cls.SUCCESS: "성공"
        }
        return descriptions.get(failure_type, "알 수 없는 오류")


class Config:
    """스크래퍼 전역 설정"""
    
    # 브라우저 설정
    DEFAULT_DELAY_RANGE = (0.5, 1)
    MAX_RETRIES = 3
    BROWSER_RESTART_INTERVAL = 30
    PAGE_LOAD_TIMEOUT = 15
    IMPLICIT_WAIT = 2
    
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
    
    # CSS 선택자 - 기본 상품 정보용 (가격 정보는 정규식 사용)
    SELECTORS = {
        # 설정 관련
        'settings_button': '.selected-country-wrapper',
        'english_option': '[data-val="en-US"]',
        'save_button': '.save-selection',
        
        # 검색 결과
        'product_containers': '.product-cell-container',
        'product_link': '.absolute-link.product-link',
        'product_title': '.product-title',
        
        # 상품 페이지 - 가격 외 정보
        'product_name': [
            'h1#name[data-testid="product-name"]',
            'h1#name',
            '.product-title h1',
            'h1'
        ],
        'product_specs': '#product-specs-list',
        'part_number': '[data-part-number]',
    }
    
    # 정규표현식 패턴 - 정규식 기반 가격 추출 최적화
    PATTERNS = {
        # 기본 패턴들
        'item_code': r'item\s*code:\s*([A-Z0-9-]+)',
        'product_code_url': r'/pr/([A-Z0-9-]+)',
        'english_count': r'(\d+)\s+(?:count|ct|tablets?|capsules?|softgels?|veg\s*capsules?|vcaps?|pieces?|servings?|tab)(?!\w)',
        'dosage_mg': r'(\d+(?:,\d+)*)\s*mg',
        
        # 정밀한 가격 패턴들 - HTML 구조 기반
        'krw_discount_price_red': r'<b[^>]*class="[^"]*discount-price[^"]*"[^>]*style="[^"]*color:\s*rgb\(211,\s*47,\s*47\)[^"]*"[^>]*>₩([\d,]+)</b>',
        'krw_discount_price_simple': r'<b[^>]*discount-price[^>]*>₩([\d,]+)</b>',
        'krw_out_of_stock_price': r'data-testid="product-price"[^>]*>\s*<p>\s*₩([\d,]+)\s*</p>',
        'krw_list_price_span': r'<span[^>]*class="[^"]*list-price[^"]*"[^>]*>₩([\d,]+)</span>',
        'krw_list_price_general': r'class="list-price[^"]*"[^>]*>₩([\d,]+)',
        
        # 할인율 패턴
        'percent_off_bracket': r'<span[^>]*percent-off[^>]*>\((\d+)%\s*off\)</span>',
        'percent_off_simple': r'(\d+)%\s*off',
        
        # 단위당 가격 패턴
        'price_per_unit_span': r'<span[^>]*price-per-unit[^>]*>₩(\d+)/(serving|unit|tablet|capsule)</span>',
        'price_per_unit_text': r'₩(\d+)/(serving|unit|tablet|capsule)(?![^<]*</[^>]*>)',
        'price_per_serving_direct': r'₩(\d+)/serving',
        
        # 정기배송 할인 패턴
        'subscription_discount_future': r'(\d+)%\s*off\s+on.*?future.*?orders',
        'subscription_discount_autoship': r'(\d+)%\s*off.*?autoship',
        
        # 품절 상태 패턴
        'out_of_stock_testid': r'data-testid="product-stock-status"[^>]*>[^<]*out\s+of\s+stock',
        'out_of_stock_text': r'out\s+of\s+stock',
        'notify_me_button': r'notify\s+me',
        
        # 재입고 날짜 패턴
        'back_in_stock_date_testid': r'data-testid="product-stock-status-text"[^>]*>([^<]+)',
        'back_in_stock_general': r'back\s+in\s+stock\s+date\s*:?\s*([^<\n]+)',
        
        # 백업용 일반 원화 패턴
        'krw_price_general': r'₩([\d,]+)',
        'krw_price_quoted': r'"₩([\d,]+)"',
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
    
    # 출력 컬럼
    OUTPUT_COLUMNS = [
        'iherb_product_name', 'coupang_product_name_english', 'coupang_product_name', 
        'similarity_score', 'matching_reason', 'failure_type',
        'coupang_url', 'iherb_product_url', 'coupang_product_id', 'iherb_product_code',
        'status', 'coupang_current_price_krw', 'coupang_original_price_krw', 'coupang_discount_rate',
        'iherb_list_price_krw', 'iherb_discount_price_krw', 'iherb_discount_percent',
        'iherb_subscription_discount', 'iherb_price_per_unit',
        'is_in_stock', 'stock_message', 'back_in_stock_date',
        'price_difference_krw', 'cheaper_platform', 'savings_amount', 'savings_percentage',
        'price_difference_note', 'processed_at', 'actual_index', 'search_language'
    ]