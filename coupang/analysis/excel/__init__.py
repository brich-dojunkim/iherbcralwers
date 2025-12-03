#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Rendering Module
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
표준화된 Excel 렌더링
"""

from .types import ColumnSpec, GroupSpec, SubGroup, ConditionalRule, ExcelConfig
from .constants import COLORS, FORMATS, COLOR_SCHEMES
from .specs import STANDARD_COLUMNS, get_column_spec, get_timestamped_spec, get_delta_spec
from .rules import (
    make_delta_rule,
    make_winner_rule,
    make_cheaper_source_rule,
    make_confidence_rule,
    make_positive_red_rule
)
from .renderer import ExcelRenderer
from .utils import safe_get, build_output_dataframe, apply_formula_column

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
    # Specs
    'STANDARD_COLUMNS',
    'get_column_spec',
    'get_timestamped_spec',
    'get_delta_spec',
    # Rules
    'make_delta_rule',
    'make_winner_rule',
    'make_cheaper_source_rule',
    'make_confidence_rule',
    'make_positive_red_rule',
    # Renderer
    'ExcelRenderer',
    # Utils
    'safe_get',
    'build_output_dataframe',
    'apply_formula_column',
]