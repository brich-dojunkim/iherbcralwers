"""
전역 공통 설정 파일
여러 모듈에서 공통으로 사용되는 핵심 설정만 중앙화
"""

import os
import glob
import re
from datetime import datetime


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

    @classmethod
    def find_latest_completed_data(cls, brand_name: str):
        """브랜드의 가장 최근 완성된 데이터 파일 찾기"""
        pattern = os.path.join(cls.UNIFIED_OUTPUTS_DIR, f"final_{brand_name}_*_completed.csv")
        files = glob.glob(pattern)
        
        if not files:
            return None
        
        # 파일명에서 타임스탬프 추출하여 정렬
        file_timestamps = []
        for file_path in files:
            filename = os.path.basename(file_path)
            # final_브랜드_YYYYMMDD_HHMMSS_completed.csv 패턴
            match = re.search(rf"final_{brand_name}_(\d{{8}}_\d{{6}})_completed\.csv", filename)
            if match:
                timestamp_str = match.group(1)
                try:
                    timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    file_timestamps.append((timestamp, file_path))
                except:
                    continue
        
        if file_timestamps:
            # 가장 최근 파일 반환
            latest_file = max(file_timestamps, key=lambda x: x[0])[1]
            return latest_file
        
        return None

    @classmethod
    def find_partial_data(cls, brand_name: str):
        """브랜드의 가장 최근 부분 완성된 데이터 파일 찾기"""
        pattern = os.path.join(cls.UNIFIED_OUTPUTS_DIR, f"*_{brand_name}_*_partial.csv")
        files = glob.glob(pattern)
        
        if not files:
            return None
        
        # 파일명에서 타임스탬프 추출하여 정렬
        file_timestamps = []
        for file_path in files:
            filename = os.path.basename(file_path)
            # 타입_브랜드_YYYYMMDD_HHMMSS_partial.csv 패턴
            match = re.search(rf".*_{brand_name}_(\d{{8}}_\d{{6}})_partial\.csv", filename)
            if match:
                timestamp_str = match.group(1)
                try:
                    timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    file_timestamps.append((timestamp, file_path))
                except:
                    continue
        
        if file_timestamps:
            # 가장 최근 파일 반환
            latest_file = max(file_timestamps, key=lambda x: x[0])[1]
            return latest_file
        
        return None

    @classmethod
    def extract_brand_from_url(cls, search_url: str):
        """쿠팡 검색 URL에서 브랜드명 추출"""
        # URL에서 q= 파라미터 추출
        match = re.search(r'[?&]q=([^&]+)', search_url)
        if match:
            brand = match.group(1).lower()
            return brand
        return "unknown"

    @classmethod
    def mark_file_completed(cls, partial_file_path: str):
        """부분 완성 파일을 완성 파일로 변경"""
        if not os.path.exists(partial_file_path):
            return None
        
        completed_path = partial_file_path.replace('_partial.csv', '_completed.csv')
        
        try:
            os.rename(partial_file_path, completed_path)
            return completed_path
        except Exception as e:
            print(f"파일 완성 표시 실패: {e}")
            return None


class DatabaseConfig:
    """데이터베이스 관련 전역 설정"""
    
    DATABASE_NAME = "updater.db"


# 환경 확인 유틸리티
def is_development():
    """개발 환경 여부"""
    return os.getenv("ENVIRONMENT", "development") == "development"


def ensure_directories():
    """필요한 디렉토리들 생성"""
    os.makedirs(PathConfig.OUTPUTS_DIR, exist_ok=True)
    # 새로운 통일된 디렉토리들 추가
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