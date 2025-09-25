"""
전역 공통 설정 파일
여러 모듈에서 공통으로 사용되는 핵심 설정만 중앙화
"""

import os


class APIConfig:
    """API 관련 전역 설정 - 여러 모듈에서 공통 사용"""
    
    # Gemini API Key (환경변수 우선, 기본값 제공)
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyBe9EQB8cvXko5fWoU309sYpkiUkQLe2ZM")
    
    # Gemini 모델 (iherbscraper, coupang/translator 공통)
    GEMINI_TEXT_MODEL = "models/gemini-2.0-flash"
    GEMINI_VISION_MODEL = "models/gemini-2.0-flash"


class PathConfig:
    """경로 관련 전역 설정 - 프로젝트 구조 공통"""
    
    # 프로젝트 루트 (모든 모듈에서 기준점으로 사용)
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # 공통 디렉토리들
    OUTPUTS_DIR = "./outputs"  # 모든 실행 스크립트에서 공통 사용
    COUPANG_IMAGES_DEFAULT_DIR = "coupang_images"  # coupang 모듈 내 상대 경로
    IHERB_IMAGES_DEFAULT_DIR = "iherb_images"  # iherbscraper 모듈 내 상대 경로


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


# 설정 검증
def validate_config():
    """필수 설정값 검증"""
    if not APIConfig.GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY가 설정되지 않았습니다.")
    
    ensure_directories()
    return True