#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Rendering Module
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
표준화된 Excel 렌더링

🔥 리팩토링 완료:
  - utils.py 삭제 (builders.py로 통합)
  - specs.py 삭제 (패턴 매칭으로 대체)
  - builders.py: DataFrame 변환 + Config 생성 통합
  - renderer.py: 컬럼-값 자동 매핑 추가
"""

# Types
from .types import ColumnSpec, GroupSpec, SubGroup, ConditionalRule, ExcelConfig

# Constants
from .constants import COLORS, FORMATS, COLOR_SCHEMES

# Builders (통합)
from .builders import ExcelConfigBuilder, quick_build

# Rules
from .rules import (
    make_delta_rule,
    make_winner_rule,
    make_cheaper_source_rule,
    make_confidence_rule,
    make_positive_red_rule
)

# Renderer
from .renderer import ExcelRenderer


__all__ = [
    # Types
    'ColumnSpec',
    'GroupSpec',
    'SubGroup',
    'ConditionalRule',
    'ExcelConfig',
    
    # Constants
    'COLORS',
    'FORMATS',
    'COLOR_SCHEMES',
    
    # Builders (통합)
    'ExcelConfigBuilder',
    'quick_build',
    
    # Rules
    'make_delta_rule',
    'make_winner_rule',
    'make_cheaper_source_rule',
    'make_confidence_rule',
    'make_positive_red_rule',
    
    # Renderer
    'ExcelRenderer',
]