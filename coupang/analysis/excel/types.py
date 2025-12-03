#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Types
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
타입 정의
"""

from dataclasses import dataclass, field
from typing import List, Callable, Any, Optional, Tuple


@dataclass
class ColumnSpec:
    """컬럼 스펙"""
    name: str
    width: float = 12.0
    number_format: str = "@"
    alignment: str = "center"  # left, center, right
    conditional_rules: List[str] = field(default_factory=list)


@dataclass
class SubGroup:
    """서브그룹 (2단 헤더)"""
    name: str
    columns: List[str]


@dataclass
class GroupSpec:
    """그룹 스펙 (3단 헤더)"""
    name: str
    color_scheme: str  # 'info', 'primary', 'secondary', 'tertiary', 'success'
    sub_groups: List[SubGroup]


@dataclass
class ConditionalRule:
    """조건부 서식 규칙"""
    column: str
    condition: Callable[[Any], bool]
    fill_color: Optional[str] = None
    font_color: Optional[str] = None


@dataclass
class ExcelConfig:
    """Excel 렌더링 설정"""
    groups: List[GroupSpec]
    columns: List[ColumnSpec]
    conditional_rules: List[ConditionalRule] = field(default_factory=list)
    freeze_panes: Optional[Tuple[int, int]] = None  # (row, col)
    auto_filter: bool = True