"""
쿠팡 크롤러 전용 설정 - 실제 작동하는 설정만 유지
"""

import sys
import os

# 전역 설정 import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import APIConfig, PathConfig


class CoupangConfig:
    """쿠팡 크롤러 전용 설정 - 정제된 버전"""
    
    # ========== 전역 설정에서 가져오는 것들 ==========
    GEMINI_API_KEY = APIConfig.GEMINI_API_KEY
    GEMINI_TEXT_MODEL = APIConfig.GEMINI_TEXT_MODEL
    
    # 이미지 저장 기본 디렉토리
    DEFAULT_IMAGE_DIR_NAME = PathConfig.COUPANG_IMAGES_DEFAULT_DIR
    
    # ========== 실제 작동하는 브라우저 설정만 유지 ==========
    # 성공했던 "단순 옵션"들만
    CHROME_OPTIONS_SIMPLE = [
        '--no-sandbox',
        '--disable-dev-shm-usage'
        '--disable-blink-features=AutomationControlled',
        '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    # JavaScript 웹드라이버 숨기기 스크립트
    WEBDRIVER_STEALTH_SCRIPT = """
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        
        Object.defineProperty(navigator, 'languages', {
            get: () => ['ko-KR', 'ko', 'en-US', 'en']
        });
        
        window.chrome = {
            runtime: {}
        };
    """
    
    # ========== 쿠팡 전용 스크래핑 설정 ==========
    DEFAULT_DELAY_RANGE = (2, 5)
    PAGE_LOAD_TIMEOUT = 20
    
    # ========== 쿠팡 전용 이미지 다운로드 설정 ==========
    IMAGE_DOWNLOAD_TIMEOUT = 10
    MIN_IMAGE_SIZE = (50, 50)
    
    # ========== 쿠팡 전용 HTTP 헤더 ==========
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Referer': 'https://www.coupang.com/',
        'Accept': 'image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
    }
    
    # ========== 쿠팡 전용 CSS 선택자 ==========
    SELECTORS = {
        # 페이지네이션
        'next_button': 'a.Pagination_nextBtn__TUY5t',
        'pagination_disabled': 'disabled',
        
        # 상품 컨테이너
        'product_list': '#product-list',
        'product_items': 'li.ProductUnit_productUnit__Qd6sv, li[class*="ProductUnit"], li[data-id]',
        'no_result': '.search-no-result',
        
        # 상품 정보
        'product_link': 'a',
        
        # 상품명
        'product_name_v2': 'div.ProductUnit_productNameV2__cV9cw',
        'product_name_legacy': 'div.ProductUnit_productName__gre7e',
        'product_name_general': '[class*="ProductUnit_productName"]',
        
        # 가격 영역
        'price_area': 'div.PriceArea_priceArea__NntJz',
        
        # 이미지
        'product_image_main': 'figure.ProductUnit_productImage__Mqcg1 img[src*="320x320"]',
        'product_image_figure': 'figure.ProductUnit_productImage__Mqcg1 img',
        'product_image_cdn': 'img[src*="coupangcdn.com"]',
        'product_image_fallback': 'img'
    }
    
    # ========== 쿠팡 전용 필수 컬럼 ==========
    REQUIRED_COLUMNS = [
        'product_id', 'product_name', 'product_url',
        'current_price', 'original_price', 'discount_rate',
        'unit_price', 'rating', 'review_count', 'delivery_badge',
        'is_rocket', 'stock_status', 'origin_country',
        'image_url', 'image_local_path', 'image_filename', 'crawled_at'
    ]
    
    # ========== 쿠팡 전용 번역 설정 ==========
    TRANSLATION_BATCH_SIZE = 10
    TRANSLATION_GENERATION_CONFIG = {
        'temperature': 0.1,
        'max_output_tokens': 100
    }

# ========== 삭제된 설정들 (사용하지 않음) ==========
# 이런 것들은 제거됨:
# - CHROME_OPTIONS_MAC (너무 복잡해서 봇 탐지됨)
# - 복잡한 브라우저 옵션들
# - 불필요한 실험적 설정들