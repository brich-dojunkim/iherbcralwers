#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Config Builder (통합 버전)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
column_map → ExcelConfig + DataFrame 한 번에 생성

🔥 핵심 수정: GROUP_PATTERNS - 할인율을 가격상태로 올바르게 분류
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Tuple, Optional

from .types import ColumnSpec, GroupSpec, SubGroup, ConditionalRule, ExcelConfig
from .constants import FORMATS, COLORS, COLOR_SCHEMES


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 패턴 정의
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FORMAT_PATTERNS = [
    (r'(할인|비율|위너|점유|비중)', FORMATS['percentage']),
    (r'(가격|매출|격차|정가|판매가|추천가)', FORMATS['currency']),
    (r'(판매량|재고|리뷰|수량)', FORMATS['integer']),
    (r'(평점|rating)', FORMATS['float']),
    (r'(순위|rank)', FORMATS['rank']),
    (r'Δ$', FORMATS['float']),
]

ALIGNMENT_PATTERNS = [
    (r'(가격|할인|비율|판매량|매출|재고|순위|평점|리뷰|비중|Δ)', 'right'),
    (r'(제품명|카테고리)', 'left'),
]

WIDTH_PATTERNS = [
    (r'제품명', 60.0),
    (r'(카테고리|Vendor)', 16.0),
    (r'(링크|url)', 10.0),
    (r'품번', 12.0),
    (r'(할인율|비율)', 14.0),
]

# 🔥 수정: 더 구체적인 패턴을 먼저 체크하도록 순서 조정
GROUP_PATTERNS = [
    # 1. 가장 구체적인 패턴부터
    (r'(상태|품번|Product_ID|UPC|신뢰)', 'info', '코어'),
    
    # 2. 할인전략 그룹 (구체화)
    (r'(요청|추천|손익).*할인', 'primary', '할인전략'),  # "요청할인율", "추천할인율", "손익분기할인율"
    (r'(격차|유리|쿠팡추천)', 'primary', '할인전략'),  # "가격격차", "유리한곳", "쿠팡추천가"
    
    # 3. 변화
    (r'Δ$', 'secondary', '변화'),
    
    # 4. 가격상태 그룹 (할인율 포함)
    (r'할인율', 'tertiary', '가격상태'),  # "로켓할인율", "아이허브할인율" ✅
    (r'(가격|정가|판매가)', 'tertiary', '가격상태'),
    
    # 5. 나머지
    (r'(판매량|위너|재고|매출|비중)', 'success', '판매/위너'),
    (r'제품명', 'info', '제품명'),
    (r'카테고리', 'info', '카테고리'),
    (r'링크', 'info', '링크'),
    (r'(Vendor|Item|ID)', 'info', 'ID'),
    (r'순위', 'info', '순위'),
    (r'(평점|리뷰|주문|전환|취소)', 'info', '평가'),
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ExcelConfigBuilder 클래스
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ExcelConfigBuilder:
    """column_map → ExcelConfig + DataFrame 자동 생성"""
    
    @classmethod
    def from_column_map(
        cls,
        source_df: pd.DataFrame,
        column_map: Dict[str, tuple],
        auto_groups: bool = True,
        auto_rules: bool = True,
        freeze_panes: Optional[Tuple[int, int]] = (4, 4),
        auto_filter: bool = True,
    ) -> Tuple[ExcelConfig, pd.DataFrame]:
        """column_map에서 ExcelConfig + DataFrame 자동 생성
        
        Args:
            source_df: 원본 DataFrame
            column_map: {
                'Excel컬럼명': ('source_column',),
                'Excel컬럼명': ('source_column', dtype),
                'Excel컬럼명': ('source_column', dtype, default),
            }
            
            🆕 지원 dtype:
                - 'share': 전체 대비 비중 (%) 자동 계산
                - 'rank': 순위 (내림차순) 자동 계산
                - 'Int64', float, str: 기본 타입 변환
            
            auto_groups: 자동 그룹 생성
            auto_rules: 자동 조건부 서식
            freeze_panes: 틀 고정 위치
            auto_filter: 자동 필터
        
        Returns:
            (ExcelConfig, output_df)
        """
        
        # 1. DataFrame 변환
        output_df = cls._build_dataframe(source_df, column_map)
        
        # 2. ColumnSpec 생성
        columns = cls._build_columns(list(column_map.keys()))
        
        # 3. GroupSpec 생성
        if auto_groups:
            groups = cls._infer_groups(list(column_map.keys()))
        else:
            groups = []
        
        # 4. ConditionalRule 생성
        if auto_rules:
            rules = cls._auto_rules(list(column_map.keys()))
        else:
            rules = []
        
        # 5. ExcelConfig 생성
        config = ExcelConfig(
            groups=groups,
            columns=columns,
            conditional_rules=rules,
            freeze_panes=freeze_panes,
            auto_filter=auto_filter,
        )
        
        return config, output_df
    
    @staticmethod
    def _build_dataframe(
        source_df: pd.DataFrame,
        column_map: Dict[str, tuple]
    ) -> pd.DataFrame:
        """column_map → DataFrame 변환 + 🆕 동적 계산 지원"""
        
        output_data = {}
        share_specs = []
        rank_specs = []
        
        for excel_col, spec in column_map.items():
            # Tuple 파싱
            if len(spec) == 1:
                source_col, dtype, default = spec[0], None, np.nan
            elif len(spec) == 2:
                source_col, dtype, default = spec[0], spec[1], np.nan
            else:
                source_col, dtype, default = spec
            
            # 🆕 동적 계산 타입
            if dtype == 'share':
                share_specs.append((excel_col, source_col))
                continue
            
            if dtype == 'rank':
                rank_specs.append((excel_col, source_col))
                continue
            
            # 기존 로직
            if source_col is None:
                continue
            
            if source_col not in source_df.columns:
                if dtype == 'Int64':
                    output_data[excel_col] = pd.Series([pd.NA] * len(source_df), dtype='Int64')
                else:
                    output_data[excel_col] = pd.Series([default] * len(source_df))
            else:
                series = source_df[source_col]
                
                if dtype == 'Int64':
                    output_data[excel_col] = pd.to_numeric(series, errors='coerce').astype('Int64')
                elif dtype:
                    output_data[excel_col] = series.astype(dtype)
                else:
                    output_data[excel_col] = series
        
        df = pd.DataFrame(output_data)
        
        # 비중 계산
        for excel_col, source_col in share_specs:
            if source_col in source_df.columns:
                total = pd.to_numeric(source_df[source_col], errors='coerce').fillna(0).sum()
                if total > 0:
                    df[excel_col] = (
                        pd.to_numeric(source_df[source_col], errors='coerce').fillna(0) / total * 100
                    ).round(0).astype('Int64')
                else:
                    df[excel_col] = pd.Series([pd.NA] * len(df), dtype='Int64')
            else:
                df[excel_col] = pd.Series([pd.NA] * len(df), dtype='Int64')
        
        # 순위 계산
        for excel_col, source_col in rank_specs:
            if source_col in source_df.columns:
                df[excel_col] = source_df[source_col].rank(method='min', ascending=False).astype('Int64')
            else:
                df[excel_col] = pd.Series([pd.NA] * len(df), dtype='Int64')
        
        return df
    
    @staticmethod
    def _build_columns(column_names: List[str]) -> List[ColumnSpec]:
        """컬럼명 패턴으로 ColumnSpec 자동 생성"""
        
        columns = []
        
        for name in column_names:
            # 서식
            number_format = FORMATS['text']
            for pattern, fmt in FORMAT_PATTERNS:
                if re.search(pattern, name):
                    number_format = fmt
                    break
            
            # 정렬
            alignment = 'center'
            for pattern, align in ALIGNMENT_PATTERNS:
                if re.search(pattern, name):
                    alignment = align
                    break
            
            # 너비
            width = 12.0
            for pattern, w in WIDTH_PATTERNS:
                if re.search(pattern, name):
                    width = w
                    break
            
            columns.append(ColumnSpec(
                name=name,
                width=width,
                number_format=number_format,
                alignment=alignment,
            ))
        
        return columns
    
    @staticmethod
    def _infer_groups(column_names: List[str]) -> List[GroupSpec]:
        """컬럼명 패턴으로 GroupSpec 자동 생성 - 🔥 원래 순서 완전 보존
        
        핵심: column_map 순서를 절대 변경하지 않음!
        연속된 같은 그룹만 하나로 묶음
        """
        
        # 1단계: 각 컬럼의 그룹 결정 (순서 유지)
        col_groups = []
        for col_name in column_names:
            matched = False
            for pattern, scheme, group_name in GROUP_PATTERNS:
                if re.search(pattern, col_name):
                    col_groups.append((col_name, scheme, group_name))
                    matched = True
                    break
            
            if not matched:
                col_groups.append((col_name, 'info', '기타'))
        
        # 2단계: 연속된 같은 그룹끼리만 묶음 (순서 유지)
        groups = []
        
        if not col_groups:
            return groups
        
        # 첫 번째 그룹 시작
        current_group_name = col_groups[0][2]
        current_scheme = col_groups[0][1]
        current_columns = [col_groups[0][0]]
        
        for i in range(1, len(col_groups)):
            col_name, scheme, group_name = col_groups[i]
            
            if group_name == current_group_name:
                # 같은 그룹 → 추가
                current_columns.append(col_name)
            else:
                # 다른 그룹 → 이전 그룹 저장하고 새 그룹 시작
                groups.append(GroupSpec(
                    name=current_group_name,
                    color_scheme=current_scheme,
                    sub_groups=[SubGroup(name='', columns=current_columns)]
                ))
                
                # 새 그룹 시작
                current_group_name = group_name
                current_scheme = scheme
                current_columns = [col_name]
        
        # 마지막 그룹 추가
        groups.append(GroupSpec(
            name=current_group_name,
            color_scheme=current_scheme,
            sub_groups=[SubGroup(name='', columns=current_columns)]
        ))
        
        return groups
    
    @staticmethod
    def _auto_rules(column_names: List[str]) -> List[ConditionalRule]:
        """컬럼명으로 조건부 서식 자동 생성"""
        
        rules = []
        
        for col_name in column_names:
            if col_name.endswith('Δ'):
                rules.extend([
                    ConditionalRule(
                        column=col_name,
                        condition=lambda v, c=col_name: v is not None and float(v) > 0,
                        fill_color=COLORS['GREEN']
                    ),
                    ConditionalRule(
                        column=col_name,
                        condition=lambda v, c=col_name: v is not None and float(v) < 0,
                        fill_color=COLORS['RED']
                    ),
                ])
            
            elif '위너' in col_name:
                rules.append(ConditionalRule(
                    column=col_name,
                    condition=lambda v, c=col_name: v is not None and float(v) >= 30,
                    fill_color=COLORS['GREEN']
                ))
            
            elif '유리' in col_name:
                rules.extend([
                    ConditionalRule(
                        column=col_name,
                        condition=lambda v, c=col_name: v == '아이허브',
                        fill_color=COLORS['GREEN']
                    ),
                    ConditionalRule(
                        column=col_name,
                        condition=lambda v, c=col_name: v == '로켓직구',
                        fill_color=COLORS['RED']
                    ),
                ])
            
            elif '격차' in col_name:
                rules.extend([
                    ConditionalRule(
                        column=col_name,
                        condition=lambda v, c=col_name: v is not None and float(v) < 0,
                        fill_color=COLORS['GREEN']
                    ),
                    ConditionalRule(
                        column=col_name,
                        condition=lambda v, c=col_name: v is not None and float(v) > 0,
                        fill_color=COLORS['RED']
                    ),
                ])
            
            elif any(k in col_name for k in ['손익', '추천', '요청']) and '할인' in col_name:
                rules.append(ConditionalRule(
                    column=col_name,
                    condition=lambda v, c=col_name: v is not None and float(v) > 0,
                    fill_color=COLORS['RED']
                ))
            
            elif '신뢰' in col_name:
                rules.extend([
                    ConditionalRule(
                        column=col_name,
                        condition=lambda v, c=col_name: v == 'High',
                        fill_color=COLORS['GREEN']
                    ),
                    ConditionalRule(
                        column=col_name,
                        condition=lambda v, c=col_name: v == 'Medium',
                        fill_color=COLORS['YELLOW']
                    ),
                    ConditionalRule(
                        column=col_name,
                        condition=lambda v, c=col_name: v == 'Low',
                        fill_color=COLORS['RED']
                    ),
                ])
        
        return rules


def quick_build(
    source_df: pd.DataFrame,
    column_map: Dict[str, tuple],
    **kwargs
) -> Tuple[ExcelConfig, pd.DataFrame]:
    """빠른 생성 헬퍼"""
    return ExcelConfigBuilder.from_column_map(source_df, column_map, **kwargs)