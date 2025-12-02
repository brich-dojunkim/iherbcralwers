#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Rendering Module
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Excel 렌더링 + 시멘틱 스펙
"""

from .styles import (
    COLORS,
    FORMATS,
    HeaderGroup,
    SubGroup,
    ConditionalFormat,
    ExcelConfig
)

from .renderer import ExcelRenderer, infer_format

from .semantic import (
    # 타입
    ColumnSpec,
    GroupSpec,
    # 상수
    SEMANTIC_TYPES,
    DATA_TYPES,
    COLOR_SEMANTIC,
    # 함수
    build_header_groups,
    build_column_widths,
    build_column_formats,
    build_conditional_formats,
    extract_link_columns,
    extract_all_column_names,
)

__all__ = [
    # 스타일
    'COLORS',
    'FORMATS',
    'HeaderGroup',
    'SubGroup',
    'ConditionalFormat',
    'ExcelConfig',
    # 렌더러
    'ExcelRenderer',
    'infer_format',
    # 시멘틱
    'ColumnSpec',
    'GroupSpec',
    'SEMANTIC_TYPES',
    'DATA_TYPES',
    'COLOR_SEMANTIC',
    'build_header_groups',
    'build_column_widths',
    'build_column_formats',
    'build_conditional_formats',
    'extract_link_columns',
    'extract_all_column_names',
]