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

# 정규화된 CSV 경로
HAZARD_BASE_CSV = PROJECT_DIR / "csv" / "hazard_base.csv"
IHERB_SCRAPED_CSV = PROJECT_DIR / "csv" / "iherb_scraped.csv"
GEMINI_VERIFICATIONS_CSV = PROJECT_DIR / "csv" / "gemini_verifications.csv"

# 디렉토리 생성
IMG_DIR.mkdir(exist_ok=True)
(PROJECT_DIR / "csv").mkdir(exist_ok=True)

# 상태 상수
class Status:
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    NO_IMAGE = "NO_IMAGE"
    DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
    SCRAPE_FAILED = "SCRAPE_FAILED"

# CSV 컬럼 정의
HAZARD_BASE_COLUMNS = [
    'SELF_IMPORT_SEQ',
    'PRDT_NM',
    'MUFC_NM',
    'MUFC_CNTRY_NM',
    'INGR_NM_LST',
    'CRET_DTM',
    'IMAGE_URL_MFDS'
]

IHERB_SCRAPED_COLUMNS = [
    'SELF_IMPORT_SEQ',
    'IHERB_URL',
    'product_code',
    'IHERB_제품명',
    'IHERB_제조사',
    'IHERB_PRODUCT_IMAGES',
    'STATUS',
    'SCRAPED_AT'
]

GEMINI_VERIFICATIONS_COLUMNS = [
    'SELF_IMPORT_SEQ',
    'GEMINI_VERIFIED',
    'GEMINI_REASON',
    'VERIFIED_AT'
]