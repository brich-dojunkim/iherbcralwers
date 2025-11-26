#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Utility Functions
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
상품명 파싱, 정규화 등 공통 유틸리티
"""

import re
import numpy as np
from typing import Optional


def extract_pack_count(name: str) -> Optional[int]:
    """상품명에서 '... 2개', '... 1개' 등 마지막 등장하는 '~개'의 숫자 부분을 추출"""
    if not isinstance(name, str):
        return np.nan
    matches = re.findall(r'(\d+)\s*개', name)
    if not matches:
        return np.nan
    try:
        return int(matches[-1])
    except Exception:
        return np.nan


def extract_unit_count(name: str) -> Optional[int]:
    """상품명에서 '200정', '100정' 등의 개수를 추출"""
    if not isinstance(name, str):
        return np.nan
    
    # '200정', '100정' 패턴
    matches = re.findall(r'(\d+)\s*정', name)
    if matches:
        try:
            return int(matches[0])
        except:
            pass
    
    # '200베지캡슐', '100캡슐' 패턴
    matches = re.findall(r'(\d+)\s*(?:베지)?캡슐', name)
    if matches:
        try:
            return int(matches[0])
        except:
            pass
    
    return np.nan


def extract_weight(name: str) -> Optional[float]:
    """상품명에서 용량을 추출하여 g 단위로 반환"""
    if not isinstance(name, str):
        return np.nan
    
    text = name.replace(',', '')
    match = re.search(r'(\d+(?:\.\d+)?)\s*(kg|g|lbs?|lb|oz|파운드)', text, flags=re.I)
    
    if not match:
        return np.nan

    value = float(match.group(1))
    unit = match.group(2).lower()

    if unit == 'kg':
        return value * 1000
    if unit == 'g':
        return value
    if unit in ('lb', 'lbs', '파운드'):
        return value * 453.59237
    if unit == 'oz':
        return value * 28.3495231

    return np.nan


def normalize_part_number(pn: str) -> str:
    """품번 정규화: 대문자, 하이픈/공백 제거"""
    if not isinstance(pn, str):
        return ''
    return re.sub(r'[-\s]', '', str(pn).upper().strip())