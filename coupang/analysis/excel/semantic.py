#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Semantic Excel Specification
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
시멘틱 기반 Excel 스펙 정의 및 자동 변환
"""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Dict, Callable
from .styles import COLORS, FORMATS, HeaderGroup, SubGroup, ConditionalFormat


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 시멘틱 타입 정의
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SEMANTIC_TYPES = {
    'identity',      # 식별 정보 (매칭상태, 품번, ID)
    'meta',          # 메타 정보 (제품명, 카테고리, 링크)
    'action',        # 액션 지표 (할인전략)
    'performance',   # 성과 지표 (판매, 위너)
    'price',         # 가격 정보
    'change',        # 변화(Δ) 지표
}

DATA_TYPES = {
    'text',          # 텍스트/ID
    'integer',       # 정수 (천단위)
    'currency',      # 금액 (천단위)
    'percentage',    # 백분율 (소수점 1자리)
    'percentage_int',# 백분율 (정수)
    'float',         # 실수 (소수점 1자리)
    'rank',          # 순위 (정수, 천단위 없음)
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 시멘틱 색상 매핑
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COLOR_SEMANTIC = {
    'identity': {
        'top': COLORS["INFO_DARK"],
        'mid': COLORS["INFO_MID"],
        'bottom': COLORS["INFO_LIGHT"],
    },
    'meta': {
        'top': COLORS["INFO_DARK"],
        'mid': COLORS["INFO_MID"],
        'bottom': COLORS["INFO_LIGHT"],
    },
    'action': {
        'top': COLORS["PRIMARY_DARK"],
        'mid': COLORS["PRIMARY_MID"],
        'bottom': COLORS["PRIMARY_LIGHT"],
    },
    'performance': {
        'top': COLORS["SUCCESS_DARK"],
        'mid': COLORS["SUCCESS_MID"],
        'bottom': COLORS["SUCCESS_LIGHT"],
    },
    'price': {
        'top': COLORS["TERTIARY_DARK"],
        'mid': COLORS["TERTIARY_MID"],
        'bottom': COLORS["TERTIARY_LIGHT"],
    },
    'change': {
        'top': COLORS["SECONDARY_DARK"],
        'mid': COLORS["SECONDARY_MID"],
        'bottom': COLORS["SECONDARY_LIGHT"],
    },
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 컬럼 스펙
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class ColumnSpec:
    """컬럼 스펙 (시멘틱 + 시각화)"""
    
    name: str                                    # 표시명
    data_type: str                               # DATA_TYPES 중 하나
    width: Optional[float] = None                # None이면 자동 추론
    number_format: Optional[str] = None          # None이면 data_type에서 자동
    conditional_rules: List[str] = field(default_factory=list)  # 규칙 이름 목록
    
    def __post_init__(self):
        """자동 추론"""
        if self.width is None:
            self.width = infer_width(self.name, self.data_type)
        
        if self.number_format is None:
            self.number_format = infer_format_from_datatype(self.data_type)


@dataclass
class GroupSpec:
    """그룹 스펙 (시멘틱 계층)"""
    
    name: str                                    # 그룹명
    semantic_type: str                           # SEMANTIC_TYPES 중 하나
    subgroups: List[Tuple[str, List[str]]]       # [(서브그룹명, [컬럼명들])]
    columns: List[ColumnSpec]                    # 전체 컬럼 스펙
    
    def get_colors(self) -> Dict[str, str]:
        """시멘틱 타입에 따른 색상"""
        return COLOR_SEMANTIC[self.semantic_type]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 자동 추론 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def infer_width(column_name: str, data_type: str) -> float:
    """컬럼명과 데이터 타입으로 너비 자동 추론"""
    
    # 제품명
    if '제품명' in column_name or 'product_name' in column_name.lower():
        return 60.0
    
    # 날짜 포함 (줄바꿈)
    if '\n' in column_name:
        # 날짜 형식: "판매가\n(2025-11-24)"
        return 14.0
    
    # ID류
    if any(keyword in column_name for keyword in ['Product_ID', 'Vendor', 'Item', 'UPC']):
        if 'Product_ID' in column_name:
            return 14.0
        elif 'UPC' in column_name:
            return 15.0
        else:
            return 17.0
    
    # 링크
    if '링크' in column_name or 'url' in column_name.lower():
        return 10.0
    
    # 카테고리
    if '카테고리' in column_name or 'category' in column_name.lower():
        if '로켓' in column_name:
            return 13.0
        else:
            return 11.0
    
    # 컬럼명 길이 기반
    name_clean = column_name.replace('\n', '').replace('_', '')
    name_len = len(name_clean)
    
    if name_len <= 2:
        return 8.0
    elif name_len <= 5:
        return 10.0
    elif name_len <= 8:
        return 12.0
    elif name_len <= 12:
        return 14.0
    else:
        return 16.0


def infer_format_from_datatype(data_type: str) -> str:
    """데이터 타입에서 숫자 포맷 자동 결정"""
    
    format_map = {
        'text': FORMATS['text'],
        'integer': FORMATS['integer'],
        'currency': FORMATS['currency'],
        'percentage': FORMATS['percentage'],
        'percentage_int': '0"%"',
        'float': FORMATS['float'],
        'rank': '0',
    }
    
    return format_map.get(data_type, FORMATS['text'])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 조건부 서식 규칙 팩토리
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CONDITIONAL_RULE_FACTORIES = {}

def register_rule(name: str):
    """규칙 등록 데코레이터"""
    def decorator(func: Callable):
        CONDITIONAL_RULE_FACTORIES[name] = func
        return func
    return decorator


@register_rule('delta')
def create_delta_rule(column_name: str) -> List[ConditionalFormat]:
    """Δ 컬럼: 양수=초록, 음수=빨강"""
    return [
        ConditionalFormat(
            column=column_name,
            condition=lambda v: float(v or 0) > 0,
            color=COLORS["GREEN"]
        ),
        ConditionalFormat(
            column=column_name,
            condition=lambda v: float(v or 0) < 0,
            color=COLORS["RED"]
        ),
    ]


@register_rule('positive_red')
def create_positive_red_rule(column_name: str) -> List[ConditionalFormat]:
    """양수=빨강 (할인율)"""
    return [
        ConditionalFormat(
            column=column_name,
            condition=lambda v: float(v or 0) > 0,
            color=COLORS["RED"]
        ),
    ]


@register_rule('confidence')
def create_confidence_rule(column_name: str) -> List[ConditionalFormat]:
    """신뢰도: High=초록, Medium=노랑, Low=빨강"""
    return [
        ConditionalFormat(
            column=column_name,
            condition=lambda v: v == 'High',
            color=COLORS["GREEN"]
        ),
        ConditionalFormat(
            column=column_name,
            condition=lambda v: v == 'Medium',
            color=COLORS["YELLOW"]
        ),
        ConditionalFormat(
            column=column_name,
            condition=lambda v: v == 'Low',
            color=COLORS["RED"]
        ),
    ]


@register_rule('winner_100')
def create_winner_100_rule(column_name: str) -> List[ConditionalFormat]:
    """위너비율 100% = 초록"""
    return [
        ConditionalFormat(
            column=column_name,
            condition=lambda v: abs(float(v or 0) - 100.0) < 0.0001,
            color=COLORS["GREEN"]
        ),
    ]


@register_rule('winner_threshold')
def create_winner_threshold_rule(column_name: str) -> List[ConditionalFormat]:
    """위너비율 >= 30% = 초록"""
    return [
        ConditionalFormat(
            column=column_name,
            condition=lambda v: float(v or 0) >= 30,
            color=COLORS["GREEN"]
        ),
    ]


@register_rule('source_comparison')
def create_source_comparison_rule(column_name: str) -> List[ConditionalFormat]:
    """유리한곳: 아이허브=초록, 로켓직구=빨강"""
    return [
        ConditionalFormat(
            column=column_name,
            condition=lambda v: v == '아이허브',
            color=COLORS["GREEN"]
        ),
        ConditionalFormat(
            column=column_name,
            condition=lambda v: v == '로켓직구',
            color=COLORS["RED"]
        ),
    ]


def build_conditional_formats(columns: List[ColumnSpec]) -> List[ConditionalFormat]:
    """컬럼 스펙에서 조건부 서식 자동 생성"""
    
    formats = []
    
    for col in columns:
        for rule_name in col.conditional_rules:
            if rule_name in CONDITIONAL_RULE_FACTORIES:
                factory = CONDITIONAL_RULE_FACTORIES[rule_name]
                formats.extend(factory(col.name))
            else:
                print(f"⚠️ 알 수 없는 규칙: {rule_name}")
    
    return formats


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 변환 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_header_groups(group_specs: List[GroupSpec]) -> List[HeaderGroup]:
    """GroupSpec → HeaderGroup 변환"""
    
    header_groups = []
    
    for group_spec in group_specs:
        colors = group_spec.get_colors()
        
        sub_groups = []
        for sub_name, col_names in group_spec.subgroups:
            sub_groups.append(SubGroup(name=sub_name, cols=col_names))
        
        header_groups.append(
            HeaderGroup(
                name=group_spec.name,
                color_top=colors['top'],
                color_mid=colors['mid'],
                color_bottom=colors['bottom'],
                sub_groups=sub_groups
            )
        )
    
    return header_groups


def build_column_widths(columns: List[ColumnSpec]) -> Dict[str, float]:
    """ColumnSpec → 너비 딕셔너리"""
    return {col.name: col.width for col in columns}


def build_column_formats(columns: List[ColumnSpec]) -> Dict[str, str]:
    """ColumnSpec → 포맷 딕셔너리"""
    return {col.name: col.number_format for col in columns}


def extract_link_columns(columns: List[ColumnSpec]) -> List[str]:
    """링크 컬럼 추출"""
    return [col.name for col in columns if '링크' in col.name or 'url' in col.name.lower()]


def extract_all_column_names(group_specs: List[GroupSpec]) -> List[str]:
    """모든 컬럼명 추출 (순서 보장)"""
    all_names = []
    for group_spec in group_specs:
        for _, col_names in group_spec.subgroups:
            all_names.extend(col_names)
    return all_names