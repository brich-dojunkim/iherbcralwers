"""
시스템 설정 파일
"""

# ========================================
# API 설정
# ========================================

# Gemini API 키 (고급 이미지 비교용)
# 없으면 단순 이미지 해시 비교 사용
GEMINI_API_KEY = None  # 또는 "your-api-key-here"

# ========================================
# 크롤링 설정
# ========================================

# 브라우저 설정
HEADLESS_MODE = False  # True: 백그라운드, False: GUI 표시
WINDOW_SIZE = "1920,1080"

# 대기 시간 (초)
SEARCH_DELAY = 2  # 검색 간 대기
PAGE_LOAD_WAIT = 3  # 페이지 로드 대기
DETAIL_PAGE_WAIT = 3  # 상세 페이지 대기

# ========================================
# 검색 설정
# ========================================

# 쿠팡 검색 후보 개수
TOP_N_PRODUCTS = 5

# 필터 설정
APPLY_SINGLE_ITEM_FILTER = True  # 낱개상품 필터 적용

# ========================================
# 매칭 로직 설정
# ========================================

# 정수 추출 범위
MIN_COUNT = 10
MAX_COUNT = 1000

# 매칭 우선순위 가중치
MATCH_WEIGHTS = {
    'count_match': 10.0,  # 정수 일치 (최우선)
    'brand_match': 5.0,   # 브랜드 일치
    'price_low': 1.0,     # 낮은 가격
}

# ========================================
# 이미지 비교 설정
# ========================================

# 이미지 해시 임계값 (단순 비교 모드)
HASH_THRESHOLD_HIGH = 5    # 5 이하: 매우 유사
HASH_THRESHOLD_MEDIUM = 10  # 10 이하: 유사
HASH_THRESHOLD_LOW = 15     # 15 이하: 다소 유사

# ========================================
# 저장 설정
# ========================================

# 중간 저장 주기
SAVE_INTERVAL = 10  # 10개 처리마다 저장

# 출력 파일 인코딩
OUTPUT_ENCODING = 'utf-8-sig'  # Excel 호환

# ========================================
# 로깅 설정
# ========================================

# 로그 레벨
LOG_LEVEL = 'INFO'  # DEBUG, INFO, WARNING, ERROR

# 상세 출력
VERBOSE = True

# ========================================
# 재시도 설정
# ========================================

# 최대 재시도 횟수
MAX_RETRIES = 3

# 재시도 대기 시간 (초)
RETRY_DELAY = 5

# ========================================
# 알려진 브랜드 리스트
# ========================================

KNOWN_BRANDS = [
    'GNC', '지앤씨',
    '나우푸드', 'NOW', 'Now Foods',
    '닥터스베스트', "Doctor's Best",
    '쏜리서치', 'Thorne', 'Thorne Research',
    '솔가', 'Solgar',
    '라이프익스텐션', 'Life Extension',
    '재로우', 'Jarrow', 'Jarrow Formulas',
    '가든오브라이프', 'Garden of Life',
    '스포츠리서치', 'Sports Research',
    '뉴트리콜로지', 'Nutricology',
    '네이처스웨이', "Nature's Way",
    '블루보넷', 'Bluebonnet',
]

# ========================================
# CSS 선택자 (사이트 변경 시 수정)
# ========================================

SELECTORS = {
    # GNC
    'gnc_product_link': 'a.thumb-link',
    'gnc_product_name': 'h1.product-name',
    'gnc_product_image': 'img.product-tile-img',
    
    # 쿠팡
    'coupang_product_list': "li[class*='search-product']",
    'coupang_product_name': 'div.name',
    'coupang_product_price': 'strong.price-value',
    'coupang_product_link': 'a',
    'coupang_shipping': '.shipping-fee',
}