import os

class APIConfig:
    """API 관련 전역 설정 - 여러 모듈에서 공통 사용"""
    
    # Gemini API Key (환경변수 우선, 기본값 제공)
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyBe9EQB8cvXko5fWoU309sYpkiUkQLe2ZM")
    
    # Gemini 모델 (iherbscraper, coupang/translator 공통)
    GEMINI_TEXT_MODEL = "models/gemini-2.0-flash"
    GEMINI_VISION_MODEL = "models/gemini-2.0-flash"

class PathConfig:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # 기존 (하위 호환)
    OUTPUTS_DIR = "./outputs"
    COUPANG_IMAGES_DEFAULT_DIR = "coupang_images"
    IHERB_IMAGES_DEFAULT_DIR = "iherb_images"
    
    # 새로운 통일된 경로들
    DATA_ROOT = os.path.join(PROJECT_ROOT, "data")
    UNIFIED_OUTPUTS_DIR = os.path.join(DATA_ROOT, "outputs")
    UNIFIED_IMAGES_DIR = os.path.join(DATA_ROOT, "images")
    UNIFIED_COUPANG_IMAGES_DIR = os.path.join(UNIFIED_IMAGES_DIR, "coupang")
    UNIFIED_IHERB_IMAGES_DIR = os.path.join(UNIFIED_IMAGES_DIR, "iherb")

class DatabaseConfig:
    """데이터베이스 관련 전역 설정"""
    
    DATABASE_NAME = "updater.db"  # sqlite_brand_updater.py 등에서 사용

# 환경 확인 유틸리티
def is_development():
    """개발 환경 여부"""
    return os.getenv("ENVIRONMENT", "development") == "development"

def ensure_directories():
    """필요한 디렉토리들 생성"""
    os.makedirs(PathConfig.OUTPUTS_DIR, exist_ok=True)
    os.makedirs(PathConfig.UNIFIED_OUTPUTS_DIR, exist_ok=True)
    os.makedirs(PathConfig.UNIFIED_COUPANG_IMAGES_DIR, exist_ok=True)
    os.makedirs(PathConfig.UNIFIED_IHERB_IMAGES_DIR, exist_ok=True)

# 설정 검증
def validate_config():
    """필수 설정값 검증"""
    if not APIConfig.GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY가 설정되지 않았습니다.")
    
    ensure_directories()
    return True