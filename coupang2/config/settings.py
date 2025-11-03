#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
중앙 설정 관리
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
모든 경로와 설정을 여기서 관리
"""

from pathlib import Path


class Config:
    """프로젝트 설정"""
    
    # 프로젝트 루트
    PROJECT_ROOT = Path(__file__).parent.parent.resolve()
    
    # 데이터 경로
    DATA_DIR = PROJECT_ROOT / "data"
    DB_PATH = DATA_DIR / "rocket" / "monitoring.db"
    MATCHING_CSV_PATH = DATA_DIR / "rocket" / "rocket.csv"
    IHERB_EXCEL_DIR = DATA_DIR / "iherb"
    
    # 출력 경로
    OUTPUT_DIR = PROJECT_ROOT / "output"
    
    # 로켓직구 카테고리 설정
    ROCKET_CATEGORIES = [
        {
            'name': '헬스/건강식품',
            'category_id': '305433',  # 카테고리 ID만 (URL 파라미터 제외)
            'url_path': '?category=305433&platform=p&brandId=0'
        },
        {
            'name': '출산유아동',
            'category_id': '219079',
            'url_path': '?category=219079&platform=p&brandId=0'
        },
        {
            'name': '스포츠레저',
            'category_id': '317675',
            'url_path': '?category=317675&platform=p&brandId=0'
        }
    ]
    
    # 로켓직구 기본 URL
    ROCKET_BASE_URL = 'https://shop.coupang.com/coupangus/74511'
    
    @classmethod
    def ensure_directories(cls):
        """필요한 디렉토리 생성"""
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.OUTPUT_DIR.mkdir(exist_ok=True)
    
    @classmethod
    def get_category_by_name(cls, name: str):
        """카테고리 이름으로 설정 찾기"""
        for cat in cls.ROCKET_CATEGORIES:
            if cat['name'] == name:
                return cat
        return None