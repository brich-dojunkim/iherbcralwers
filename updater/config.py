"""
Product Updater 설정 파일
기존 모듈들의 설정을 재활용하면서 업데이터 전용 설정 추가
"""

import os
import sys
from datetime import datetime

# 기존 모듈 경로 추가
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COUPANG_MODULE_PATH = os.path.join(BASE_DIR, 'coupang')
IHERB_MODULE_PATH = os.path.join(BASE_DIR, 'iherbscraper')

sys.path.append(COUPANG_MODULE_PATH)
sys.path.append(IHERB_MODULE_PATH)

# 기존 모듈 설정 임포트
try:
    from config import Config as IHerbConfig
    from config import FailureType
except ImportError:
    print("⚠️ iHerbScraper 모듈을 찾을 수 없습니다.")
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
    
    # ========== 마스터 CSV 스키마 ==========
    # 기존 iHerb 결과 스키마 + 최소 메타 필드
    if IHerbConfig:
        MASTER_COLUMNS = IHerbConfig.OUTPUT_COLUMNS + [
            'last_updated',    # 마지막 업데이트 시간
            'data_source',     # 'initial' 또는 'update'
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
        'processing_time_seconds'
    ]


# 전역 설정 인스턴스
CONFIG = UpdaterConfig

# 초기화
CONFIG.ensure_directories()

print("Product Updater 설정 로드 완료")
print(f"  출력 디렉토리: {CONFIG.OUTPUT_DIR}")
print(f"  백업 디렉토리: {CONFIG.BACKUP_DIR}")
print(f"  마스터 컬럼 수: {len(CONFIG.MASTER_COLUMNS)}")
print(f"  iHerb 모듈 연동: {'✓' if IHerbConfig else '✗'}")