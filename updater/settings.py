"""
통합 설정 관리 (config.py에서 settings.py로 변경하여 충돌 방지)
"""

import os
from pathlib import Path

# 프로젝트 루트 경로
PROJECT_ROOT = Path(__file__).parent.parent
COUPANG_PATH = PROJECT_ROOT / 'coupang'
IHERB_PATH = PROJECT_ROOT / 'iherbscraper'

# 통합 설정
UPDATER_CONFIG = {
    'GEMINI_API_KEY': "AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE",
    'GEMINI_TEXT_MODEL': "models/gemini-2.0-flash",
    'GEMINI_VISION_MODEL': "models/gemini-2.0-flash",
    
    'BRAND_SEARCH_URLS': {
        'NOW Foods': "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&q=%EB%82%98%EC%9A%B0%ED%91%B8%EB%93%9C",
        'Doctors Best': "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&q=%EB%8B%A5%ED%84%B0%EC%8A%A4%EB%B2%A0%EC%8A%A4%ED%8A%B8",
        'Garden of Life': "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&q=%EA%B0%80%EB%93%A0%EC%98%A4%EB%B8%8C%EB%9D%BC%EC%9D%B4%ED%94%84",
        'Natures Way': "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&q=%EB%84%A4%EC%9D%B4%EC%B2%98%EC%8A%A4%EC%9B%A8%EC%9D%B4"
    },
    
    'TRANSLATION_BATCH_SIZE': 10,
    'DELAY_RANGE': (2, 4),
    'MAX_PRODUCTS_TO_COMPARE': 4,
    'CHECKPOINT_INTERVAL': 10,
    'RESTART_METADATA_FILE': 'restart_metadata.json'
}

# 쿠팡 모듈 호환성을 위한 추가 설정
BASE_DIR = PROJECT_ROOT

PATHS = {
    'images': COUPANG_PATH / 'coupang_images',
    'outputs': COUPANG_PATH / 'outputs'
}

def ensure_directories():
    """디렉토리 생성 (쿠팡 모듈 호환성)"""
    for path in PATHS.values():
        os.makedirs(path, exist_ok=True)

def validate_config():
    """설정 유효성 검사"""
    if not UPDATER_CONFIG['GEMINI_API_KEY']:
        raise ValueError("Gemini API 키가 설정되지 않았습니다.")
    
    if not COUPANG_PATH.exists():
        raise FileNotFoundError(f"쿠팡 모듈 경로를 찾을 수 없습니다: {COUPANG_PATH}")
    
    if not IHERB_PATH.exists():
        raise FileNotFoundError(f"아이허브 모듈 경로를 찾을 수 없습니다: {IHERB_PATH}")
    
    return True