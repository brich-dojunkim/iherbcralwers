#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Utils
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DataFrame → Excel 변환 유틸리티
"""

import pandas as pd
import numpy as np
from typing import Any, Dict, Optional, Union


def safe_get(
    df: pd.DataFrame, 
    col_name: str, 
    dtype: Optional[str] = None, 
    default: Any = np.nan
) -> pd.Series:
    """안전하게 DataFrame 컬럼 가져오기
    
    DataFrame.get()은 존재하지 않으므로, 안전한 컬럼 접근 제공
    
    Args:
        df: DataFrame
        col_name: 컬럼명
        dtype: 변환할 타입 ('Int64', 'float', 'str' 등)
        default: 컬럼이 없을 때 기본값
    
    Returns:
        pd.Series
    
    Examples:
        >>> safe_get(df, 'price', dtype='Int64')
        >>> safe_get(df, 'name', default='Unknown')
    """
    # 컬럼이 없는 경우
    if col_name not in df.columns:
        if dtype == 'Int64':
            return pd.Series([pd.NA] * len(df), dtype='Int64')
        return pd.Series([default] * len(df))
    
    series = df[col_name]
    
    # 타입 변환
    if dtype == 'Int64':
        return pd.to_numeric(series, errors='coerce').astype('Int64')
    elif dtype:
        return series.astype(dtype)
    
    return series


def build_output_dataframe(
    df: pd.DataFrame, 
    column_map: Dict[str, tuple]
) -> pd.DataFrame:
    """Excel 출력용 DataFrame 생성
    
    column_map의 각 항목:
    - 2-tuple: (source_column, dtype)
    - 3-tuple: (source_column, dtype, default)
    
    Args:
        df: 원본 DataFrame
        column_map: {
            'Excel컬럼명': ('source_column', dtype),
            'Excel컬럼명': ('source_column', dtype, default),
            ...
        }
    
    Returns:
        변환된 DataFrame
    
    Examples:
        >>> column_map = {
        ...     '매칭상태': ('matching_status',),
        ...     'UPC': ('iherb_upc', 'Int64'),
        ...     '판매량': ('iherb_sales_quantity', 'Int64', 0),
        ... }
        >>> output_df = build_output_dataframe(df, column_map)
    """
    output_data = {}
    
    for excel_col, spec in column_map.items():
        # Tuple 길이에 따라 파싱
        if len(spec) == 1:
            source_col, dtype, default = spec[0], None, np.nan
        elif len(spec) == 2:
            source_col, dtype, default = spec[0], spec[1], np.nan
        else:  # len == 3
            source_col, dtype, default = spec
        
        output_data[excel_col] = safe_get(df, source_col, dtype=dtype, default=default)
    
    return pd.DataFrame(output_data)


def apply_formula_column(
    output_df: pd.DataFrame,
    col_name: str,
    formula: callable,
    **kwargs
) -> pd.DataFrame:
    """계산 컬럼 추가 (비중, 순위 등)
    
    Args:
        output_df: 대상 DataFrame
        col_name: 추가할 컬럼명
        formula: 계산 함수 (df를 받아 Series 반환)
        **kwargs: formula에 전달할 추가 인자
    
    Returns:
        컬럼이 추가된 DataFrame
    
    Examples:
        >>> # 매출 비중 계산
        >>> def revenue_share(df):
        ...     total = df['매출(원)'].sum()
        ...     return (df['매출(원)'] / total * 100).round(0).astype('Int64')
        >>> 
        >>> output_df = apply_formula_column(output_df, '매출비중', revenue_share)
    """
    output_df[col_name] = formula(output_df, **kwargs)
    return output_df