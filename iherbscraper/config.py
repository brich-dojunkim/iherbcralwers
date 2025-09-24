"""
Configuration + Pattern Matcher - 설정과 패턴 매칭 통합
"""

import re


class Config:
    """설정 및 패턴 매칭 통합 클래스"""
    
    # ========== API 설정 ==========
    GEMINI_API_KEY = "AIzaSyA2r-_8ePWcmP-5o9esScT2pcOgj_57J3M"
    GEMINI_TEXT_MODEL = "models/gemini-2.0-flash"
    GEMINI_VISION_MODEL = "models/gemini-2.0-flash"
    GEMINI_MAX_RETRIES = 3
    GEMINI_TIMEOUT = 25
    
    # ========== 경로 설정 ==========
    COUPANG_IMAGES_DIR = "/Users/brich/Desktop/iherb_price/coupang/coupang_images"
    IHERB_IMAGES_DIR = "/Users/brich/Desktop/iherb_price/iherbscraper/iherb_images"
    
    # ========== 브라우저 설정 ==========
    DEFAULT_DELAY_RANGE = (1.5, 3)
    MAX_RETRIES = 3
    PAGE_LOAD_TIMEOUT = 18
    
    # ========== URL 설정 ==========
    BASE_URL = "https://www.iherb.com"
    KOREA_URL = "https://kr.iherb.com"
    
    # ========== 이미지 설정 ==========
    IMAGE_DOWNLOAD_TIMEOUT = 12
    MAX_IMAGE_SIZE_MB = 8
    
    # ========== 브라우저 옵션 ==========
    CHROME_OPTIONS = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-blink-features=AutomationControlled",
        "--disable-extensions",
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "--window-size=1920,1080"
    ]
    
    # ========== 선택자 설정 ==========
    SELECTORS = {
        # 언어 설정
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
    }
    
    # ========== 출력 컬럼 ==========
    OUTPUT_COLUMNS = [
        'iherb_product_name', 'coupang_product_name_english', 'coupang_product_name', 
        'similarity_score', 'matching_reason', 'gemini_confidence',
        'coupang_url', 'iherb_product_url', 'coupang_product_id', 'iherb_product_code',
        'status', 'coupang_current_price_krw', 'coupang_original_price_krw', 'coupang_discount_rate',
        'iherb_list_price_krw', 'iherb_discount_price_krw', 'iherb_discount_percent',
        'iherb_subscription_discount', 'iherb_price_per_unit',
        'is_in_stock', 'stock_message', 'back_in_stock_date',
        'price_difference_krw', 'cheaper_platform', 'savings_amount', 'savings_percentage',
        'price_difference_note'
    ]
    
    # ========== 정규표현식 패턴 ==========
    PATTERNS = {
        # 기본 패턴들
        'item_code': r'item\s*code:\s*([A-Z0-9-]+)',
        'product_code_url': r'/pr/([A-Z0-9-]+)',
        'english_count': r'(\d+)\s+(?:count|ct|tablets?|capsules?|softgels?|veg\s*capsules?|vcaps?|pieces?|servings?|tab)(?!\w)',
        'dosage_mg': r'(\d+(?:,\d+)*)\s*mg',
        
        # 가격 패턴들 (우선순위 순서로 정렬)
        'krw_discount_price_red': r'<b[^>]*class="[^"]*discount-price[^"]*"[^>]*style="[^"]*color:\s*rgb\(211,\s*47,\s*47\)[^"]*"[^>]*>₩([\d,]+)</b>',
        'krw_discount_price_inline': r'<b[^>]*style="[^"]*color:\s*rgb\(211,\s*47,\s*47\)[^"]*"[^>]*>₩([\d,]+)</b>',
        'krw_discount_price_simple': r'<b[^>]*discount-price[^>]*>₩([\d,]+)</b>',
        'krw_out_of_stock_price': r'data-testid="product-price"[^>]*>\s*<p>\s*₩([\d,]+)\s*</p>',
        'krw_list_price_span': r'<span[^>]*class="[^"]*list-price[^"]*"[^>]*>₩([\d,]+)</span>',
        'krw_list_price_general': r'class="list-price[^"]*"[^>]*>₩([\d,]+)',
        
        # 할인율 패턴들
        'percent_off_bracket': r'<span[^>]*percent-off[^>]*>\((\d+)%\s*off\)</span>',
        'percent_off_parentheses': r'\((\d+)%\s*할인\)',
        'percent_off_simple': r'(\d+)%\s*off',
        
        # 단위당 가격 패턴들
        'price_per_unit_small': r'<div[^>]*class="[^"]*small[^"]*price-per-unit[^"]*"[^>]*>₩(\d+)/(\w+)</div>',
        'discount_price_per_unit': r'<span[^>]*discount-price-per-unit[^>]*>₩(\d+)/(\w+)</span>',
        'list_price_per_unit': r'<div[^>]*list-price-per-unit[^>]*>₩(\d+)/(\w+)</div>',
        'price_per_unit_korean': r'₩([\d,]+)/(제공량|정|캡슐)',
        'price_per_unit_span': r'<span[^>]*price-per-unit[^>]*>₩(\d+)/(serving|unit|tablet|capsule)</span>',
        'price_per_serving_direct': r'₩(\d+)/serving',
        'price_per_unit_text': r'₩(\d+)/(serving|unit|tablet|capsule)(?![^<]*</[^>]*>)',
        
        # 정기배송 할인 패턴들
        'subscription_discount_future': r'(\d+)%\s*off\s+on.*?future.*?orders',
        'subscription_discount_autoship': r'(\d+)%\s*off.*?autoship',
        
        # 품절 상태 패턴들
        'out_of_stock_testid_korean': r'data-testid="product-stock-status"[^>]*>[^<]*품절',
        'out_of_stock_testid': r'data-testid="product-stock-status"[^>]*>[^<]*out\s+of\s+stock',
        'out_of_stock_korean': r'품절',
        'out_of_stock_text': r'out\s+of\s+stock',
        'notify_me_button_korean': r'알림\s*받기',
        'notify_me_button': r'notify\s+me',
        
        # 재입고 날짜 패턴들
        'back_in_stock_date_testid': r'data-testid="product-stock-status-text"[^>]*>([^<]+)',
        'back_in_stock_korean': r'재입고\s*일자\s*([^<\n]+)',
        'back_in_stock_general': r'back\s+in\s+stock\s+date\s*:?\s*([^<\n]+)',
        
        # 백업용 일반 패턴들
        'krw_price_general': r'₩([\d,]+)',
        'krw_price_quoted': r'"₩([\d,]+)"',
    }
    
    # ========== 패턴 우선순위 ==========
    PATTERN_PRIORITIES = {
        'price': [
            'krw_discount_price_red',
            'krw_discount_price_inline',
            'krw_discount_price_simple',
            'krw_out_of_stock_price',
            'krw_list_price_span',
            'krw_list_price_general'
        ],
        'discount': [
            'percent_off_bracket',
            'percent_off_parentheses',
            'percent_off_simple'
        ],
        'unit_price': [
            'price_per_unit_small',
            'discount_price_per_unit',
            'list_price_per_unit',
            'price_per_unit_korean',
            'price_per_unit_span',
            'price_per_serving_direct',
            'price_per_unit_text'
        ],
        'stock': [
            'out_of_stock_testid_korean',
            'out_of_stock_testid',
            'out_of_stock_korean',
            'out_of_stock_text'
        ],
        'notify': [
            'notify_me_button_korean',
            'notify_me_button'
        ],
        'restock': [
            'back_in_stock_date_testid',
            'back_in_stock_korean',
            'back_in_stock_general'
        ]
    }
    
    @classmethod
    def validate_api_key(cls):
        """API 키 유효성 검사"""
        if not cls.GEMINI_API_KEY or cls.GEMINI_API_KEY == "YOUR_GEMINI_API_KEY_HERE":
            return False, "Gemini API 키가 설정되지 않았습니다."
        
        if len(cls.GEMINI_API_KEY) < 30:
            return False, "API 키가 너무 짧습니다. 올바른 키인지 확인해주세요."
        
        return True, "Gemini 2.5 Flash API 키 설정 완료"
    
    # ========== 패턴 매칭 메서드들 ==========
    
    @classmethod
    def extract_product_code(cls, html_content):
        """상품 코드 추출"""
        # URL에서 먼저 시도
        url_match = re.search(cls.PATTERNS['product_code_url'], html_content)
        if url_match:
            return url_match.group(1)
        
        # item code에서 시도
        item_code_match = re.search(cls.PATTERNS['item_code'], html_content, re.IGNORECASE)
        if item_code_match:
            return item_code_match.group(1)
        
        return ""
    
    @classmethod
    def extract_current_price(cls, html_content):
        """현재 가격 추출 - 우선순위 기반"""
        for pattern_name in cls.PATTERN_PRIORITIES['price']:
            pattern = cls.PATTERNS[pattern_name]
            match = re.search(pattern, html_content)
            if match:
                return int(match.group(1).replace(',', ''))
        return 0
    
    @classmethod
    def extract_list_price(cls, html_content, current_price=0):
        """정가 추출 - 현재가보다 높은 가격"""
        for pattern_name in ['krw_list_price_span', 'krw_list_price_general']:
            pattern = cls.PATTERNS[pattern_name]
            match = re.search(pattern, html_content)
            if match:
                list_price = int(match.group(1).replace(',', ''))
                if list_price > current_price:
                    return list_price
        return 0
    
    @classmethod
    def extract_discount_percent(cls, html_content):
        """할인율 추출"""
        for pattern_name in cls.PATTERN_PRIORITIES['discount']:
            pattern = cls.PATTERNS[pattern_name]
            match = re.search(pattern, html_content)
            if match:
                return int(match.group(1))
        return 0
    
    @classmethod
    def extract_subscription_discount(cls, html_content):
        """정기배송 할인 추출"""
        for pattern_name in ['subscription_discount_future', 'subscription_discount_autoship']:
            pattern = cls.PATTERNS[pattern_name]
            match = re.search(pattern, html_content)
            if match:
                return int(match.group(1))
        return 0
    
    @classmethod
    def extract_price_per_unit(cls, html_content):
        """단위당 가격 추출"""
        for pattern_name in cls.PATTERN_PRIORITIES['unit_price']:
            pattern = cls.PATTERNS[pattern_name]
            match = re.search(pattern, html_content)
            if match:
                if len(match.groups()) >= 2:
                    return f"₩{match.group(1)}/{match.group(2)}"
                else:
                    return match.group(0)
        return ""
    
    @classmethod
    def extract_stock_status(cls, html_content):
        """재고 상태 추출"""
        # 품절 상태 확인
        for pattern_name in cls.PATTERN_PRIORITIES['stock']:
            pattern = cls.PATTERNS[pattern_name]
            if re.search(pattern, html_content, re.IGNORECASE):
                return False  # 품절
        
        # 알림 버튼으로 재확인
        for pattern_name in cls.PATTERN_PRIORITIES['notify']:
            pattern = cls.PATTERNS[pattern_name]
            if re.search(pattern, html_content, re.IGNORECASE):
                return False  # 품절
        
        return True  # 재고 있음
    
    @classmethod
    def extract_stock_message(cls, html_content, is_in_stock):
        """재고 메시지 추출"""
        if is_in_stock:
            return ""
        
        # 한국어 우선
        if re.search(cls.PATTERNS['out_of_stock_korean'], html_content):
            return "품절"
        elif re.search(cls.PATTERNS['out_of_stock_text'], html_content):
            return "Out of Stock"
        
        return "재고 없음"
    
    @classmethod
    def extract_back_in_stock_date(cls, html_content):
        """재입고 날짜 추출"""
        for pattern_name in cls.PATTERN_PRIORITIES['restock']:
            pattern = cls.PATTERNS[pattern_name]
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return ""
    
    @classmethod
    def extract_all_price_info(cls, html_content):
        """모든 가격 정보를 한 번에 추출"""
        current_price = cls.extract_current_price(html_content)
        list_price = cls.extract_list_price(html_content, current_price)
        discount_percent = cls.extract_discount_percent(html_content)
        subscription_discount = cls.extract_subscription_discount(html_content)
        price_per_unit = cls.extract_price_per_unit(html_content)
        
        return {
            'current_price_krw': current_price,
            'list_price_krw': list_price,
            'discount_percent': discount_percent,
            'subscription_discount': subscription_discount,
            'price_per_unit': price_per_unit
        }
    
    @classmethod
    def extract_all_stock_info(cls, html_content):
        """모든 재고 정보를 한 번에 추출"""
        is_in_stock = cls.extract_stock_status(html_content)
        stock_message = cls.extract_stock_message(html_content, is_in_stock)
        back_in_stock_date = cls.extract_back_in_stock_date(html_content) if not is_in_stock else ""
        
        return {
            'is_in_stock': is_in_stock,
            'stock_message': stock_message,
            'back_in_stock_date': back_in_stock_date
        }
    
    @classmethod
    def extract_all_info(cls, html_content):
        """모든 정보를 한 번에 추출 - 메인 API"""
        product_code = cls.extract_product_code(html_content)
        price_info = cls.extract_all_price_info(html_content)
        stock_info = cls.extract_all_stock_info(html_content)
        
        return {
            'product_code': product_code,
            **price_info,
            **stock_info
        }