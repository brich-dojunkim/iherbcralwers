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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 통합 CSV (단일 파일로 모든 상태 관리)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
UNIFIED_CSV = PROJECT_DIR / "csv" / "unified_results.csv"

# 통합 컬럼 정의
UNIFIED_COLUMNS = [
    # 기본 정보 (식약처 API)
    'SELF_IMPORT_SEQ',      # 고유 ID
    'PRDT_NM',              # 위해식품 제품명
    'MUFC_NM',              # 제조사
    'MUFC_CNTRY_NM',        # 제조국
    'INGR_NM_LST',          # 성분
    'CRET_DTM',             # 등록일시
    'IMAGE_URL',            # 위해식품 이미지 URL
    'STT_YMD',              # 적용시작일
    'END_YMD',              # 적용종료일
    'LAST_UPDT_DTM',        # 최종수정일
    'BARCD_CTN',            # 바코드번호
    
    # iHerb 정보
    'IHERB_URL',            # iHerb 제품 URL
    'product_code',         # iHerb 상품코드 (URL에서 추출)
    'IHERB_CODE',           # iHerb 상품코드 (이미지 URL에서 추출)
    'IHERB_제품명',          # iHerb 제품명
    'IHERB_제조사',          # iHerb 제조사
    'IHERB_PRODUCT_IMAGES', # iHerb 이미지 (JSON)
    'STATUS',               # 처리 상태
    'SCRAPED_AT',           # 스크래핑 시각
    
    # Gemini 검증
    'GEMINI_VERIFIED',      # 매칭 여부 (True/False)
    'GEMINI_REASON',        # 매칭 이유
    'VERIFIED_AT'           # 검증 시각
]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 레거시 CSV (하위 호환성 유지, deprecated)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HAZARD_BASE_CSV = PROJECT_DIR / "csv" / "hazard_base.csv"
IHERB_SCRAPED_CSV = PROJECT_DIR / "csv" / "iherb_scraped.csv"
GEMINI_VERIFICATIONS_CSV = PROJECT_DIR / "csv" / "gemini_verifications.csv"

HAZARD_BASE_COLUMNS = [
    'SELF_IMPORT_SEQ',
    'PRDT_NM',
    'MUFC_NM',
    'MUFC_CNTRY_NM',
    'INGR_NM_LST',
    'CRET_DTM',
    'IMAGE_URL',
    'STT_YMD',          # 적용시작일
    'END_YMD',          # 적용종료일
    'LAST_UPDT_DTM',    # 최종수정일
    'BARCD_CTN'         # 바코드번호
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

# 디렉토리 생성
IMG_DIR.mkdir(exist_ok=True)
(PROJECT_DIR / "csv").mkdir(exist_ok=True)

# 상태 상수
class Status:
    # Google 검색 단계
    FOUND = "FOUND"                       # iHerb URL 발견
    NOT_FOUND = "NOT_FOUND"               # iHerb URL 미발견
    NO_IMAGE = "NO_IMAGE"                 # 이미지 없음
    DOWNLOAD_FAILED = "DOWNLOAD_FAILED"   # 이미지 다운로드 실패
    
    # 스크래핑 단계
    SCRAPE_FAILED = "SCRAPE_FAILED"       # iHerb 스크래핑 실패
    
    # 검증 단계
    VERIFIED_MATCH = "VERIFIED_MATCH"     # ✅ Gemini 매칭 성공
    VERIFIED_MISMATCH = "VERIFIED_MISMATCH"  # ❌ Gemini 매칭 실패