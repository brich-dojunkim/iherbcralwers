#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Conditional Format Rules
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
조건부 서식 규칙 팩토리
"""

from typing import List
from .types import ConditionalRule
from .constants import COLORS


def make_delta_rule(col_name: str) -> List[ConditionalRule]:
    """Δ 컬럼 규칙: 양수=초록, 음수=빨강
    
    Args:
        col_name: 컬럼명
    
    Returns:
        List[ConditionalRule]
    """
    return [
        ConditionalRule(
            column=col_name,
            condition=lambda v: v is not None and float(v) > 0,
            fill_color=COLORS["GREEN"]
        ),
        ConditionalRule(
            column=col_name,
            condition=lambda v: v is not None and float(v) < 0,
            fill_color=COLORS["RED"]
        ),
    ]


def make_winner_rule(col_name: str, threshold: float = 30.0) -> List[ConditionalRule]:
    """위너비율 규칙: >= threshold = 초록
    
    Args:
        col_name: 컬럼명
        threshold: 임계값 (기본 30%)
    
    Returns:
        List[ConditionalRule]
    """
    return [
        ConditionalRule(
            column=col_name,
            condition=lambda v: v is not None and float(v) >= threshold,
            fill_color=COLORS["GREEN"]
        ),
    ]


def make_cheaper_source_rule(col_name: str) -> List[ConditionalRule]:
    """유리한곳 규칙: 아이허브=초록, 로켓직구=빨강
    
    Args:
        col_name: 컬럼명
    
    Returns:
        List[ConditionalRule]
    """
    return [
        ConditionalRule(
            column=col_name,
            condition=lambda v: v == "아이허브",
            fill_color=COLORS["GREEN"]
        ),
        ConditionalRule(
            column=col_name,
            condition=lambda v: v == "로켓직구",
            fill_color=COLORS["RED"]
        ),
    ]


def make_confidence_rule(col_name: str) -> List[ConditionalRule]:
    """신뢰도 규칙: High=초록, Medium=노랑, Low=빨강
    
    Args:
        col_name: 컬럼명
    
    Returns:
        List[ConditionalRule]
    """
    return [
        ConditionalRule(
            column=col_name,
            condition=lambda v: v == "High",
            fill_color=COLORS["GREEN"]
        ),
        ConditionalRule(
            column=col_name,
            condition=lambda v: v == "Medium",
            fill_color=COLORS["YELLOW"]
        ),
        ConditionalRule(
            column=col_name,
            condition=lambda v: v == "Low",
            fill_color=COLORS["RED"]
        ),
    ]


def make_positive_red_rule(col_name: str) -> List[ConditionalRule]:
    """양수=빨강 (할인율 등)
    
    Args:
        col_name: 컬럼명
    
    Returns:
        List[ConditionalRule]
    """
    return [
        ConditionalRule(
            column=col_name,
            condition=lambda v: v is not None and float(v) > 0,
            fill_color=COLORS["RED"]
        ),
    ]