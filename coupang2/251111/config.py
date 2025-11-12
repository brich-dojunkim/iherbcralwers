"""
쿠팡 제품 매칭 시스템 설정 파일
"""

# Gemini API 키 (여기에 직접 입력)
GEMINI_API_KEY = "your-api-key-here"  # 실제 API 키로 변경하세요

# CSV 파일 경로 (실제 사용 시 run_matcher.py에서 자동 설정됨)
# 예상 위치: coupang2/251111/iHerb_쿠팡 추가 가격조사_20251111.csv
CSV_PATH = "/Users/brich/Desktop/iherb_price/coupang2/251111/iHerb_쿠팡_추가_가격조사_대조결과_20251112.csv"

# Gemini API 설정
GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_TEMPERATURE = 0.1
GEMINI_MAX_TOKENS = 500

# 검색 설정
TOP_N_PRODUCTS = 4  # 검색 결과 상위 N개
SEARCH_DELAY = 2  # 검색 간 대기 시간(초)
PAGE_LOAD_WAIT = 3  # 페이지 로드 대기(초)
FILTER_APPLY_WAIT = 2  # 필터 적용 대기(초)

# 브라우저 설정
HEADLESS_MODE = False  # True: 백그라운드 실행, False: GUI 표시
WINDOW_SIZE = "1920,1080"

# 정수 추출 설정
MIN_COUNT = 10  # 최소 정수
MAX_COUNT = 1000  # 최대 정수

# 신뢰도 지표 가중치 (참고용)
INDICATOR_WEIGHTS = {
    '정수일치': 0.30,  # 가장 중요
    '브랜드일치': 0.25,
    '타입일치': 0.15,
    '제품명유사도_높음': 0.15,
    '1순위제품': 0.05,
    '리뷰100개이상': 0.05,
    '평점4점이상': 0.05,
}

# 신뢰도 임계값 (자동 판정용)
CONFIDENCE_THRESHOLDS = {
    'high': 80,  # 자동 승인
    'medium': 60,  # 우선 검토
    'low': 0,  # 필수 수동 확인
}

# 출력 파일 설정
OUTPUT_DIR = "./outputs"  # 현재 디렉토리의 outputs 폴더
OUTPUT_FILENAME = "matching_results.csv"
ENCODING = "utf-8-sig"  # Excel 호환성

# 로그 설정
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# 재시도 설정
MAX_RETRIES = 3  # 최대 재시도 횟수
RETRY_DELAY = 5  # 재시도 대기(초)

# 예외 처리 설정
FALLBACK_TO_FIRST_RESULT = True  # API 실패 시 1순위 선택
SKIP_ON_NO_RESULTS = True  # 검색 결과 없으면 스킵

# 필터링 설정
REQUIRE_COUNT_MATCH = True  # 정수 일치 필수
ALLOW_PARTIAL_MATCH = False  # 부분 매칭 허용 여부

# 알려진 브랜드 목록 (ProductParser에서 사용)
KNOWN_BRANDS = [
    '쏜리서치', 'thorne', 'thorne research',
    '나우푸드', 'now', 'now foods',
    '닥터스베스트', 'doctors best', "doctor's best",
    '라이프익스텐션', 'life extension',
    '재로우', 'jarrow', 'jarrow formulas',
    '가든오브라이프', 'garden of life',
    '스포츠리서치', 'sports research',
    '솔가', 'solgar',
    '뉴트리콜로지', 'nutricology',
    '네이처스웨이', "nature's way",
    '블루보넷', 'bluebonnet',
]

# 제품 타입 목록
PRODUCT_TYPES = [
    '캡슐', '베지캡', '베지 캡', '베지테리안 캡슐', 'veggie cap', 'vcap',
    '소프트젤', '소프트겔', 'softgel', 'soft gel',
    '타블렛', '정', 'tablet',
    '알', 'pill',
    '분말', 'powder',
    '액상', 'liquid',
]

# CSS 셀렉터 (업데이트 필요 시 수정)
SELECTORS = {
    'product_list': 'li.ProductUnit_productUnit__Qd6sv',
    'product_name': 'div.ProductUnit_productNameV2__cV9cw',
    'product_link': 'a',
    'price_current': 'div.custom-oos.fw-text-\\[20px\\]\\/\\[24px\\]',
    'unit_price': 'span.custom-oos.fw-text-\\[12px\\]\\/\\[15px\\]',
    'rating': 'span.ProductRating_rating__lMxS9',
    'review_count': 'span.ProductRating_ratingCount__R0Vhz',
    'filter_label': 'label',
}

# Gemini 프롬프트 템플릿
GEMINI_PROMPT_TEMPLATE = """
당신은 제품 매칭 전문가입니다. 원본 제품과 가장 유사한 쿠팡 제품을 선택해주세요.

**원본 제품:**
- 제품명: {original_name}
- 정수: {original_count}
- 브랜드: {original_brand}
- 타입: {original_type}

**후보 제품들 (정수 일치 확인됨):**
{candidates_text}

**선택 기준:**
1. 브랜드명 일치 (최우선)
2. 제품 타입 일치 (캡슐, 소프트젤 등)
3. 제품명 핵심 키워드 일치
4. 가격 합리성

**응답 형식 (반드시 JSON만 출력):**
{{
    "selected_rank": 1,
    "match_score": 85,
    "reason": "브랜드와 제품명이 정확히 일치함",
    "brand_match": true,
    "type_match": true,
    "name_similarity": "high"
}}

JSON만 출력하세요:
"""