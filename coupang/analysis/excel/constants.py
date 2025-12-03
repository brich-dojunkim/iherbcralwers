#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Constants
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
색상 팔레트 및 서식 상수
"""

from typing import Dict

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 색상 팔레트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COLORS: Dict[str, str] = {
    # 기본
    "INFO_DARK": "0F172A",
    "INFO_MID": "475569",
    "INFO_LIGHT": "E2E8F0",
    
    # 주요
    "PRIMARY_DARK": "5E2A8A",
    "PRIMARY_MID": "7A3EB1",
    "PRIMARY_LIGHT": "D2B7E5",
    
    # 보조
    "SECONDARY_DARK": "305496",
    "SECONDARY_MID": "4472C4",
    "SECONDARY_LIGHT": "B4C7E7",
    
    # 삼차
    "TERTIARY_DARK": "C55A11",
    "TERTIARY_MID": "F4B084",
    "TERTIARY_LIGHT": "FBE5D6",
    
    # 성공
    "SUCCESS_DARK": "375623",
    "SUCCESS_MID": "548235",
    "SUCCESS_LIGHT": "A8D08D",
    
    # 강조
    "GREEN": "C6EFCE",
    "RED": "FFC7CE",
    "YELLOW": "FFEB9C",
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 숫자 서식
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FORMATS: Dict[str, str] = {
    "text": "@",
    "integer": "#,##0",
    "float": "0.0",
    "currency": "#,##0",
    "percentage": '0.0"%"',
    "rank": "0",
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 색상 조합 (3단 헤더용)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COLOR_SCHEMES: Dict[str, Dict[str, str]] = {
    "info": {
        "top": COLORS["INFO_DARK"],
        "mid": COLORS["INFO_MID"],
        "bottom": COLORS["INFO_LIGHT"],
    },
    "primary": {
        "top": COLORS["PRIMARY_DARK"],
        "mid": COLORS["PRIMARY_MID"],
        "bottom": COLORS["PRIMARY_LIGHT"],
    },
    "secondary": {
        "top": COLORS["SECONDARY_DARK"],
        "mid": COLORS["SECONDARY_MID"],
        "bottom": COLORS["SECONDARY_LIGHT"],
    },
    "tertiary": {
        "top": COLORS["TERTIARY_DARK"],
        "mid": COLORS["TERTIARY_MID"],
        "bottom": COLORS["TERTIARY_LIGHT"],
    },
    "success": {
        "top": COLORS["SUCCESS_DARK"],
        "mid": COLORS["SUCCESS_MID"],
        "bottom": COLORS["SUCCESS_LIGHT"],
    },
}