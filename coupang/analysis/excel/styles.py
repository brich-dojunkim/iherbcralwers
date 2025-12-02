#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Styles & Types
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Excel 스타일 상수 + 타입 정의
"""

from dataclasses import dataclass
from typing import List, Callable, Any, Optional, Dict

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 색상 팔레트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COLORS = {
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

FORMATS = {
    "text": "@",
    "integer": "#,##0",
    "float": "0.0",
    "currency": "#,##0",
    "percentage": '0.0"%"',
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 타입 정의
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class SubGroup:
    """2단 헤더 서브그룹"""
    name: str
    cols: List[str]


@dataclass
class HeaderGroup:
    """3단 헤더 그룹"""
    name: str
    color_top: str
    color_mid: str
    color_bottom: str
    sub_groups: List[SubGroup]


@dataclass
class ConditionalFormat:
    """조건부 서식"""
    column: str
    condition: Callable[[Any], bool]
    color: str


@dataclass
class ExcelConfig:
    """Excel 렌더링 설정 (단순화)"""
    header_groups: List[HeaderGroup]
    column_widths: Dict[str, float]
    column_formats: Dict[str, str] = None
    conditional_formats: List[ConditionalFormat] = None
    databar_columns: List[str] = None
    link_columns: List[str] = None
    freeze_panes: tuple = None
    auto_filter: bool = True