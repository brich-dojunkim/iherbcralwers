#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Config Builder (통합 버전)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
column_map → ExcelConfig + DataFrame 한 번에 생성

🔥 통합 기능:
  - DataFrame 변환 (기존 utils.py)
  - ColumnSpec 자동 생성 (패턴 매칭)
  - GroupSpec 자동 생성 (패턴 매칭)
  - ConditionalRule 자동 생성 (패턴 매칭)
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
    (r'(할인|비율|위너|점유)', FORMATS['percentage']),
    (r'(가격|매출|격차|정가|판매가|추천가)', FORMATS['currency']),
    (r'(판매량|재고|리뷰|수량)', FORMATS['integer']),
    (r'(평점|rating)', FORMATS['float']),
    (r'(순위|rank)', FORMATS['rank']),
    (r'Δ$', FORMATS['float']),
]

ALIGNMENT_PATTERNS = [
    (r'(가격|할인|비율|판매량|매출|재고|순위|평점|리뷰|Δ)', 'right'),
    (r'(제품명|카테고리)', 'left'),
]

WIDTH_PATTERNS = [
    (r'제품명', 60.0),
    (r'(카테고리|Vendor)', 16.0),
    (r'(링크|url)', 10.0),
    (r'품번', 12.0),
    (r'(할인율|비율)', 14.0),
]

GROUP_PATTERNS = [
    (r'(상태|품번|Product_ID|UPC)', 'info', '코어'),
    (r'(요청|추천|손익|할인|격차|유리)', 'primary', '할인전략'),
    (r'Δ$', 'secondary', '변화'),
    (r'(가격|정가|판매가|추천가)', 'tertiary', '가격상태'),
    (r'(판매량|위너|재고|매출)', 'success', '판매/위너'),
    (r'제품명', 'info', '제품명'),
    (r'카테고리', 'info', '카테고리'),
    (r'링크', 'info', '링크'),
    (r'(Vendor|Item|ID)', 'info', 'ID'),
    (r'순위', 'info', '순위'),
    (r'(평점|리뷰)', 'info', '평가'),
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
        """column_map → DataFrame 변환 (기존 utils.build_output_dataframe)
        
        🔥 통합: utils.py의 build_output_dataframe 로직
        """
        
        output_data = {}
        
        for excel_col, spec in column_map.items():
            # Tuple 파싱
            if len(spec) == 1:
                source_col, dtype, default = spec[0], None, np.nan
            elif len(spec) == 2:
                source_col, dtype, default = spec[0], spec[1], np.nan
            else:
                source_col, dtype, default = spec
            
            # 소스 컬럼이 None이면 스킵 (동적 계산 컬럼)
            if source_col is None:
                continue
            
            # 컬럼 추출
            if source_col not in source_df.columns:
                # 없으면 기본값
                if dtype == 'Int64':
                    output_data[excel_col] = pd.Series([pd.NA] * len(source_df), dtype='Int64')
                else:
                    output_data[excel_col] = pd.Series([default] * len(source_df))
            else:
                series = source_df[source_col]
                
                # 타입 변환
                if dtype == 'Int64':
                    output_data[excel_col] = pd.to_numeric(series, errors='coerce').astype('Int64')
                elif dtype:
                    output_data[excel_col] = series.astype(dtype)
                else:
                    output_data[excel_col] = series
        
        return pd.DataFrame(output_data)
    
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
        """컬럼명 패턴으로 GroupSpec 자동 생성"""
        
        # 컬럼 → 그룹 매핑
        col_to_group = {}
        
        for col_name in column_names:
            for pattern, scheme, group_name in GROUP_PATTERNS:
                if re.search(pattern, col_name):
                    col_to_group[col_name] = (scheme, group_name)
                    break
            
            if col_name not in col_to_group:
                col_to_group[col_name] = ('info', '기타')
        
        # 그룹별로 묶기 (순서 유지)
        groups_dict = {}
        group_order = []
        
        for col_name in column_names:
            scheme, group_name = col_to_group[col_name]
            
            if group_name not in groups_dict:
                groups_dict[group_name] = {
                    'scheme': scheme,
                    'columns': []
                }
                group_order.append(group_name)
            
            groups_dict[group_name]['columns'].append(col_name)
        
        # GroupSpec 생성 (순서 보장)
        groups = []
        for group_name in group_order:
            info = groups_dict[group_name]
            groups.append(GroupSpec(
                name=group_name,
                color_scheme=info['scheme'],
                sub_groups=[SubGroup(name='', columns=info['columns'])]
            ))
        
        return groups
    
    @staticmethod
    def _auto_rules(column_names: List[str]) -> List[ConditionalRule]:
        """컬럼명으로 조건부 서식 자동 생성"""
        
        rules = []
        
        for col_name in column_names:
            # Δ 컬럼: 양수=초록, 음수=빨강
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
            
            # 위너비율: >= 30% = 초록
            elif '위너' in col_name:
                rules.append(ConditionalRule(
                    column=col_name,
                    condition=lambda v, c=col_name: v is not None and float(v) >= 30,
                    fill_color=COLORS['GREEN']
                ))
            
            # 유리한곳: 아이허브=초록, 로켓직구=빨강
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
            
            # 가격격차: 음수(아이허브 저렴)=초록, 양수=빨강
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
            
            # 할인율 (손익/추천/요청): 양수=빨강
            elif any(k in col_name for k in ['손익', '추천', '요청']) and '할인' in col_name:
                rules.append(ConditionalRule(
                    column=col_name,
                    condition=lambda v, c=col_name: v is not None and float(v) > 0,
                    fill_color=COLORS['RED']
                ))
            
            # 신뢰도: High=초록, Medium=노랑, Low=빨강
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 헬퍼 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def quick_build(
    source_df: pd.DataFrame,
    column_map: Dict[str, tuple],
    **kwargs
) -> Tuple[ExcelConfig, pd.DataFrame]:
    """빠른 생성 헬퍼
    
    Args:
        source_df: 원본 DataFrame
        column_map: 컬럼 매핑
        **kwargs: ExcelConfigBuilder.from_column_map 옵션
    
    Returns:
        (ExcelConfig, output_df)
    
    Example:
        config, output_df = quick_build(df, column_map)
        renderer.render(output_df, config)
    """
    return ExcelConfigBuilder.from_column_map(source_df, column_map, **kwargs)