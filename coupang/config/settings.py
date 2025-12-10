#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
중앙 설정 관리
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
모든 경로와 설정을 여기서 관리
"""

from pathlib import Path
from typing import Optional


class Config:
    """프로젝트 설정"""
    
    # ========================================
    # 프로젝트 루트
    # ========================================
    PROJECT_ROOT = Path(__file__).parent.parent.resolve()
    
    # ========================================
    # 데이터 경로
    # ========================================
    DATA_DIR = PROJECT_ROOT / "data"
    
    # 기존 DB (legacy)
    MATCHING_CSV_PATH = DATA_DIR / "rocket" / "rocket.csv"
    
    # 통합 DB (신규)
    INTEGRATED_DB_PATH = DATA_DIR / "rocket_iherb.db"
    
    # 엑셀 디렉토리
    IHERB_EXCEL_DIR = DATA_DIR / "iherb"
    
    # 출력 경로
    OUTPUT_DIR = PROJECT_ROOT / "output"
    
    # ========================================
    # 엑셀 파일 패턴
    # ========================================
    EXCEL_PATTERNS = {
        'price_inventory': '*price_inventory*.xlsx',
        'seller_insights': '*SELLER_INSIGHTS*.xlsx',
        'coupang_price': 'Coupang_Price_*.xlsx',
        'upc': '20251024_*.xlsx'
    }
    
    # ========================================
    # 로켓직구 카테고리 설정
    # ========================================
    ROCKET_CATEGORIES = [
        {
            'name': '헬스/건강식품',
            'category_id': '305433',
            'url_path': '?category=305433&platform=p&brandId=0',
            'url_column': 'rocket_category_url_1'
        },
        {
            'name': '출산유아동',
            'category_id': '219079',
            'url_path': '?category=219079&platform=p&brandId=0',
            'url_column': 'rocket_category_url_2'
        },
        {
            'name': '스포츠레저',
            'category_id': '317675',
            'url_path': '?category=317675&platform=p&brandId=0',
            'url_column': 'rocket_category_url_3'
        },
        {
            'name': '식품',
            'category_id': '189408',
            'url_path': '?category=189408&platform=p',
            'url_column': 'rocket_category_url_4'
        },
        {
            'name': '뷰티',
            'category_id': '174637',
            'url_path': '?category=174637&platform=p&brandId=0',
            'url_column': 'rocket_category_url_5'
        }
    ]
    
    # 로켓직구 기본 URL
    ROCKET_BASE_URL = 'https://shop.coupang.com/coupangus/74511'
    
    # ========================================
    # 헬퍼 메서드
    # ========================================
    @classmethod
    def ensure_directories(cls):
        """필요한 디렉토리 생성"""
        cls.DATA_DIR.mkdir(exist_ok=True)
        (cls.DATA_DIR / "rocket").mkdir(exist_ok=True)
        (cls.DATA_DIR / "integrated").mkdir(exist_ok=True)
        (cls.DATA_DIR / "iherb").mkdir(exist_ok=True)
        cls.OUTPUT_DIR.mkdir(exist_ok=True)
    
    @classmethod
    def get_category_by_name(cls, name: str) -> Optional[dict]:
        """카테고리 이름으로 설정 찾기"""
        for cat in cls.ROCKET_CATEGORIES:
            if cat['name'] == name:
                return cat
        return None
    
    @classmethod
    def get_category_by_id(cls, category_id: str) -> Optional[dict]:
        """카테고리 ID로 설정 찾기"""
        for cat in cls.ROCKET_CATEGORIES:
            if cat['category_id'] == category_id:
                return cat
        return None
    
    @classmethod
    def get_latest_excel(cls, pattern_key: str) -> Optional[Path]:
        """특정 패턴의 최신 엑셀 파일 찾기
        
        Args:
            pattern_key: 'price_inventory', 'seller_insights', 'coupang_price', 'upc'
        
        Returns:
            최신 파일 경로 또는 None
        """
        pattern = cls.EXCEL_PATTERNS.get(pattern_key)
        if not pattern:
            return None
        
        files = list(cls.IHERB_EXCEL_DIR.glob(pattern))
        if not files:
            return None
        
        # 수정 시간 기준 최신 파일
        return sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
    
    @classmethod
    def get_all_excel_files(cls) -> dict:
        """모든 엑셀 파일의 최신 버전 찾기
        
        Returns:
            {'price_inventory': Path, 'seller_insights': Path, ...}
        """
        return {
            key: cls.get_latest_excel(key)
            for key in cls.EXCEL_PATTERNS.keys()
        }