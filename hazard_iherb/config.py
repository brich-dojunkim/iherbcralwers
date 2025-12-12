#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공통 설정
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# .env 로드
load_dotenv()

# API 설정
MFDS_API_KEY = "ec1d322edd5e4fc2a3f6"
SERVICE_ID = "I2715"
BASE_URL = "http://openapi.foodsafetykorea.go.kr/api"
PAGE_SIZE = 1000

# Gemini API
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# 경로 설정
PROJECT_DIR = Path(__file__).parent
IMG_DIR = PROJECT_DIR / "hazard_images"
OUTPUT_CSV = PROJECT_DIR / "csv" / "hazard_iherb_matched_final.csv"
API_CACHE_CSV = PROJECT_DIR / "csv" / "api_cache_hazard_data.csv"

# 생성
IMG_DIR.mkdir(exist_ok=True)

# 상태 상수
class Status:
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    NO_IMAGE = "NO_IMAGE"
    DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
    VERIFIED_MATCH = "VERIFIED_MATCH"
    VERIFIED_MISMATCH = "VERIFIED_MISMATCH"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"

# CSV 컬럼 정의
CSV_COLUMNS = [
    'SELF_IMPORT_SEQ',
    'PRDT_NM',
    'MUFC_NM',
    'MUFC_CNTRY_NM',
    'INGR_NM_LST',
    'CRET_DTM',
    'IMAGE_URL_MFDS',
    'IHERB_URL',
    'STATUS',
    'IHERB_PRODUCT_IMAGES',
    'GEMINI_VERIFIED',
    'GEMINI_REASON',
    'VERIFIED_DTM'
]
