#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
이미지 처리 유틸리티
"""

import uuid
import requests
from pathlib import Path
from typing import Optional

import pandas as pd


def download_image(url: str, save_path: Path = None, save_dir: Path = None) -> Optional[Path]:
    """
    이미지 다운로드
    
    Args:
        url: 이미지 URL
        save_path: 저장 경로 (지정 시)
        save_dir: 저장 디렉토리 (save_path 없을 때)
    """
    # ★★★ 수정: URL 파라미터의 쉼표와 구분자 쉼표 구분 ★★★
    # http로 시작하지 않는 부분이 있으면 여러 URL
    if ',' in url and ' http' in url.lower():
        # 여러 URL이 쉼표로 구분된 경우 (예: "url1, url2, url3")
        urls = [u.strip() for u in url.split(',')]
        # http로 시작하는 첫 번째 URL 사용
        for u in urls:
            if u.startswith('http'):
                url = u
                break
    
    if save_path is None:
        if save_dir is None:
            save_dir = Path.cwd()
        
        save_dir.mkdir(exist_ok=True, parents=True)
        
        ext = ".jpg"
        if ".png" in url.lower():
            ext = ".png"
        
        save_path = save_dir / f"{uuid.uuid4().hex}{ext}"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Referer': 'https://www.foodsafetykorea.go.kr/'
        }
        
        resp = requests.get(url, timeout=15, headers=headers)
        resp.raise_for_status()
        
        if len(resp.content) == 0:
            return None
        
        save_path.write_bytes(resp.content)
        return save_path
        
    except Exception as e:
        print(f"  [ERROR] 이미지 다운로드 실패: {e}")
        return None
    

def extract_product_code(url: str) -> Optional[str]:
    """URL에서 상품코드 추출"""
    if pd.isna(url) or not url:
        return None
    try:
        parts = url.rstrip('/').split('/')
        if len(parts) > 0:
            last_part = parts[-1]
            code = ''.join(filter(str.isdigit, last_part))
            return code if code else None
    except:
        return None