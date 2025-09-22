"""
Product Updater 설정 파일 (수정된 버전)
기존 모듈들의 설정을 재활용하면서 업데이터 전용 설정 + 쿠팡 크롤링 설정 추가
"""

import os
import sys
from datetime import datetime

# 기존 모듈 경로 추가
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COUPANG_MODULE_PATH = os.path.join(BASE_DIR, 'coupang')
IHERB_MODULE_PATH = os.path.join(BASE_DIR, 'iherbscraper')

# 경로를 맨 앞에 추가하여 우선순위 확보
sys.path.insert(0, IHERB_MODULE_PATH)
sys.path.insert(0, COUPANG_MODULE_PATH)

# 기존 모듈 설정 임포트 (명시적 모듈명 사용)
try:
    # iherbscraper.config에서 임포트
    import importlib.util
    
    # iherbscraper config 모듈 직접 로드
    iherb_config_path = os.path.join(IHERB_MODULE_PATH, 'config.py')
    if os.path.exists(iherb_config_path):
        spec = importlib.util.spec_from_file_location("iherb_config", iherb_config_path)
        iherb_config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(iherb_config_module)
        
        IHerbConfig = iherb_config_module.Config
        FailureType = iherb_config_module.FailureType
        print("✓ iHerb 모듈 설정 로드 성공")
    else:
        raise ImportError("iHerb config.py not found")
        
except Exception as e:
    print(f"⚠️ iHerbScraper 모듈 로드 실패: {e}")
    IHerbConfig = None
    FailureType = None


class UpdaterConfig:
    """업데이터 전용 설정"""
    
    # ========== 파일 경로 설정 ==========
    UPDATER_DIR = os.path.dirname(os.path.abspath(__file__))
    OUTPUT_DIR = os.path.join(UPDATER_DIR, 'output')
    BACKUP_DIR = os.path.join(UPDATER_DIR, 'backups')
    
    # 기본 마스터 파일 패턴
    MASTER_FILE_PATTERN = "master_products_{timestamp}.csv"
    BACKUP_FILE_PATTERN = "backup_master_{timestamp}.csv"
    
    # ========== 업데이트 설정 ==========
    MAX_NEW_PRODUCTS_PER_RUN = 100  # 한 번에 처리할 신규 상품 수
    PRICE_UPDATE_BATCH_SIZE = 20    # 가격 업데이트 배치 크기
    ENABLE_PRICE_HISTORY = False    # 가격 이력 추적 (간소화를 위해 비활성화)
    
    # ========== 쿠팡 크롤링 설정 (신규 추가) ==========
    BRAND_SEARCH_CONFIGS = {
        'now_foods': {
            'search_url': 'https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2331823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=%EB%82%98%EC%9A%B0%ED%91%B8%EB%93%9C',
            'max_pages': 10,
            'delay_range': (2, 4),
            'headless': True,
            'description': '나우푸드(NOW Foods) 브랜드 검색'
        },
        'doctors_best': {
            'search_url': 'https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2331823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=%EB%8B%A5%ED%84%B0%EC%8A%A4%EB%B2%A0%EC%8A%A4%ED%8A%B8',
            'max_pages': 5,
            'delay_range': (2, 4),
            'headless': True,
            'description': '닥터스베스트(Doctor\'s Best) 브랜드 검색'
        },
        'life_extension': {
            'search_url': 'https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2331823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=%EB%9D%BC%EC%9D%B4%ED%94%84+%EC%9D%B5%EC%8A%A4%ED%85%90%EC%85%98',
            'max_pages': 8,
            'delay_range': (3, 5),
            'headless': True,
            'description': '라이프 익스텐션(Life Extension) 브랜드 검색'
        },
        'jarrow': {
            'search_url': 'https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2331823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=%EC%9E%90%EB%A1%9C%EC%9A%B0',
            'max_pages': 5,
            'delay_range': (2, 4),
            'headless': True,
            'description': '자로우(Jarrow) 브랜드 검색'
        },
        'natrol': {
            'search_url': 'https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2331823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=%EB%82%98%ED%8A%B8%EB%A1%A4',
            'max_pages': 5,
            'delay_range': (2, 4),
            'headless': True,
            'description': '나트롤(Natrol) 브랜드 검색'
        }
    }
    
    # 기본 쿠팡 크롤링 설정
    DEFAULT_COUPANG_SETTINGS = {
        'headless': True,
        'delay_range': (2, 4),
        'download_images': False,  # 업데이터에서는 이미지 다운로드 비활성화
        'max_pages': 5,
        'timeout': 30
    }
    
    # ========== 마스터 CSV 스키마 ==========
    # 기존 iHerb 결과 스키마 + 최소 메타 필드
    if IHerbConfig:
        MASTER_COLUMNS = IHerbConfig.OUTPUT_COLUMNS + [
            'last_updated',    # 마지막 업데이트 시간
            'data_source',     # 'initial', 'update', 'new_product'
            'update_count'     # 업데이트 횟수
        ]
    else:
        # 기본 스키마 (iHerb 모듈 없을 때)
        MASTER_COLUMNS = [
            'iherb_product_name', 'coupang_product_name_english', 'coupang_product_name',
            'similarity_score', 'matching_reason', 'gemini_confidence', 'failure_type',
            'coupang_url', 'iherb_product_url', 'coupang_product_id', 'iherb_product_code',
            'status', 'coupang_current_price_krw', 'coupang_original_price_krw', 'coupang_discount_rate',
            'iherb_list_price_krw', 'iherb_discount_price_krw', 'iherb_discount_percent',
            'iherb_subscription_discount', 'iherb_price_per_unit',
            'is_in_stock', 'stock_message', 'back_in_stock_date',
            'price_difference_krw', 'cheaper_platform', 'savings_amount', 'savings_percentage',
            'price_difference_note', 'processed_at', 'actual_index', 'search_language',
            'gemini_api_calls', 'gemini_model_version',
            'last_updated', 'data_source', 'update_count'
        ]
    
    # ========== 기존 모듈 설정 재활용 ==========
    @classmethod
    def get_iherb_config(cls):
        """iHerb 모듈 설정 반환"""
        return IHerbConfig if IHerbConfig else None
    
    @classmethod
    def get_failure_types(cls):
        """실패 타입 반환"""
        return FailureType if FailureType else None
    
    @classmethod
    def get_brand_config(cls, brand_name: str):
        """특정 브랜드의 쿠팡 검색 설정 반환"""
        return cls.BRAND_SEARCH_CONFIGS.get(brand_name.lower())
    
    @classmethod
    def get_all_brands(cls):
        """모든 브랜드 목록 반환"""
        return list(cls.BRAND_SEARCH_CONFIGS.keys())
    
    # ========== 디렉토리 생성 ==========
    @classmethod
    def ensure_directories(cls):
        """필요한 디렉토리 생성"""
        for directory in [cls.OUTPUT_DIR, cls.BACKUP_DIR]:
            os.makedirs(directory, exist_ok=True)
    
    # ========== 파일명 생성 헬퍼 ==========
    @classmethod
    def generate_master_filename(cls):
        """새로운 마스터 파일명 생성"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return cls.MASTER_FILE_PATTERN.format(timestamp=timestamp)
    
    @classmethod
    def generate_backup_filename(cls):
        """백업 파일명 생성"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return cls.BACKUP_FILE_PATTERN.format(timestamp=timestamp)
    
    # ========== 로깅 설정 ==========
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # ========== 성능 설정 ==========
    PARALLEL_PROCESSING = False  # 간소화를 위해 비활성화
    MAX_WORKERS = 2
    
    # ========== 데이터 검증 설정 ==========
    VALIDATE_PRICES = True
    VALIDATE_URLS = False  # 성능을 위해 비활성화
    
    # ========== 업데이트 통계 ==========
    TRACK_UPDATE_STATS = True
    STATS_COLUMNS = [
        'total_products',
        'existing_products', 
        'new_products',
        'successful_updates',
        'failed_updates',
        'price_changes_detected',
        'new_matches_found',
        'processing_time_seconds',
        'coupang_crawl_count',
        'brands_processed'
    ]


# 🔧 핵심 수정: Config 클래스 alias 추가
# 다른 모듈들이 "from config import Config"를 사용할 수 있도록
Config = IHerbConfig if IHerbConfig else UpdaterConfig

# 전역 설정 인스턴스 (기존 이름 유지)
CONFIG = UpdaterConfig

# 초기화
CONFIG.ensure_directories()

print("Product Updater 설정 로드 완료 (쿠팡 크롤링 통합)")
print(f"  출력 디렉토리: {CONFIG.OUTPUT_DIR}")
print(f"  백업 디렉토리: {CONFIG.BACKUP_DIR}")
print(f"  마스터 컬럼 수: {len(CONFIG.MASTER_COLUMNS)}")
print(f"  iHerb 모듈 연동: {'✓' if IHerbConfig else '✗'}")
print(f"  쿠팡 브랜드 설정: {len(CONFIG.BRAND_SEARCH_CONFIGS)}개")

# 브랜드 목록 출력
print(f"  지원 브랜드: {', '.join(CONFIG.get_all_brands())}")

# Config alias 정보
if IHerbConfig:
    print(f"  Config 클래스 alias: iHerbConfig → Config")
    print(f"  다른 모듈에서 'from config import Config' 사용 가능")

# 디버그 정보
if IHerbConfig:
    print(f"  iHerb Config 클래스: {IHerbConfig}")
    print(f"  FailureType 클래스: {FailureType}")
    print(f"  베이스 URL: {getattr(IHerbConfig, 'BASE_URL', 'N/A')}")