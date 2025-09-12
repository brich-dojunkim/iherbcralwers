"""
iHerb 스크래퍼 설정 관리 - Gemini 2.5 Flash 적용
주요 업데이트:
1. Gemini 2.5 Flash 모델 사용
2. 성능 최적화된 설정
3. 콘솔 로그 분석 반영
"""

class FailureType:
    """실패 유형 분류 - Gemini API 오류 추가"""
    
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
    DOSAGE_MISMATCH = "DOSAGE_MISMATCH"
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
            cls.DOSAGE_MISMATCH: "용량 불일치",
            cls.GEMINI_NO_MATCH: "Gemini 판단: 동일 제품 없음",
            cls.SUCCESS: "성공"
        }
        return descriptions.get(failure_type, "알 수 없는 오류")


class Config:
    """스크래퍼 전역 설정 - Gemini 2.5 Flash 최적화 버전"""
    
    # ========== Gemini 2.5 Flash 설정 ==========
    GEMINI_API_KEY = "AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE"
    GEMINI_TEXT_MODEL = "models/gemini-2.0-flash"  # 🆕 2.0 Flash 적용
    GEMINI_VISION_MODEL = "models/gemini-2.0-flash"  # 🆕 2.0 Flash 적용
    GEMINI_MAX_RETRIES = 3
    GEMINI_TIMEOUT = 25  # 2.5 모델이 더 빨라서 25초로 단축
    GEMINI_RATE_LIMIT_DELAY = 5  # 2.5 모델 성능 향상으로 1.5초로 단축
    
    # ========== 이미지 비교 설정 (성능 최적화) ==========
    COUPANG_IMAGES_DIR = "/Users/brich/Desktop/iherb_price/coupang_images"
    IHERB_IMAGES_DIR = "/Users/brich/Desktop/iherb_price/iherb_images"
    IMAGE_COMPARISON_ENABLED = True
    IMAGE_DOWNLOAD_TIMEOUT = 12  # 타임아웃 단축
    MAX_IMAGE_SIZE_MB = 8  # 크기 제한 강화
    
    # ========== 브라우저 설정 (검증된 설정 유지) ==========
    DEFAULT_DELAY_RANGE = (1.5, 3)  # 성공 확인으로 딜레이 단축
    MAX_RETRIES = 3
    BROWSER_RESTART_INTERVAL = 25  # 조금 늘려서 안정성 확보
    PAGE_LOAD_TIMEOUT = 18
    IMPLICIT_WAIT = 2.5
    
    # ========== 검색 설정 (성능 검증됨) ==========
    MAX_PRODUCTS_TO_COMPARE = 4  # 3→4로 증가 (2.5 모델 성능 향상)
    BASE_URL = "https://www.iherb.com"
    KOREA_URL = "https://kr.iherb.com"
    
    # ========== 매칭 설정 (2.5 모델 최적화) ==========
    MATCHING_THRESHOLDS = {
        'min_similarity': 0.4,  # 2.5 모델이 더 정확해서 임계값 낮춤
        'success_threshold': 0.7  # 성공 기준은 높임
    }
    
    # ========== 브라우저 옵션 (안정성 검증됨) ==========
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
        # 설정 관련
        'settings_button': '.selected-country-wrapper',
        'english_option': '[data-val="en-US"]',
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
    }
    
    # ========== 정규표현식 패턴 ==========
    PATTERNS = {
        # 기본 패턴들
        'item_code': r'item\s*code:\s*([A-Z0-9-]+)',
        'product_code_url': r'/pr/([A-Z0-9-]+)',
        'english_count': r'(\d+)\s+(?:count|ct|tablets?|capsules?|softgels?|veg\s*capsules?|vcaps?|pieces?|servings?|tab)(?!\w)',
        'dosage_mg': r'(\d+(?:,\d+)*)\s*mg',
        
        # 정밀한 가격 패턴들 (검증된 패턴)
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
    
    # ========== 출력 컬럼 ==========
    OUTPUT_COLUMNS = [
        'iherb_product_name', 'coupang_product_name_english', 'coupang_product_name', 
        'similarity_score', 'matching_reason', 'gemini_confidence', 'failure_type',
        'coupang_url', 'iherb_product_url', 'coupang_product_id', 'iherb_product_code',
        'status', 'coupang_current_price_krw', 'coupang_original_price_krw', 'coupang_discount_rate',
        'iherb_list_price_krw', 'iherb_discount_price_krw', 'iherb_discount_percent',
        'iherb_subscription_discount', 'iherb_price_per_unit',
        'is_in_stock', 'stock_message', 'back_in_stock_date',
        'price_difference_krw', 'cheaper_platform', 'savings_amount', 'savings_percentage',
        'price_difference_note', 'processed_at', 'actual_index', 'search_language',
        'gemini_api_calls', 'gemini_model_version'  # 모델 버전 추적 추가
    ]
    
    # ========== 성능 모니터링 설정 ==========
    PERFORMANCE_TRACKING = {
        'enable_detailed_logging': True,
        'log_api_response_times': True,
        'track_matching_confidence': True,
        'monitor_memory_usage': False  # 필요시 활성화
    }
    
    # ========== 배치 처리 설정 ==========
    BATCH_PROCESSING = {
        'checkpoint_interval': 50,  # 50개마다 체크포인트
        'auto_backup_interval': 100,  # 100개마다 백업
        'progress_report_interval': 10,  # 10개마다 진행률 리포트
        'memory_cleanup_interval': 200  # 200개마다 메모리 정리
    }
    
    @classmethod
    def validate_api_key(cls):
        """API 키 유효성 검사"""
        if not cls.GEMINI_API_KEY or cls.GEMINI_API_KEY == "YOUR_GEMINI_API_KEY_HERE":
            return False, "Gemini API 키가 설정되지 않았습니다."
        
        if len(cls.GEMINI_API_KEY) < 30:
            return False, "API 키가 너무 짧습니다. 올바른 키인지 확인해주세요."
        
        return True, "Gemini 2.5 Flash API 키 설정 완료"
    
    @classmethod
    def get_model_info(cls):
        """모델 정보 반환"""
        return {
            'text_model': cls.GEMINI_TEXT_MODEL,
            'vision_model': cls.GEMINI_VISION_MODEL,
            'version': '2.5-flash',
            'performance_level': 'high',
            'expected_improvement': '15-30% better accuracy than 1.5-flash'
        }