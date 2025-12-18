#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV 처리 유틸리티
"""

import csv
from pathlib import Path
from typing import List, Optional

import pandas as pd


def load_csv(path: Path, columns: Optional[List[str]] = None, dtype: dict = None) -> pd.DataFrame:
    """CSV 로드"""
    if not path.exists():
        return pd.DataFrame()
    
    try:
        if dtype is None and columns:
            dtype = {col: str for col in columns}
        
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=dtype)
        
        if columns:
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            df = df[columns]
        
        return df
    except Exception as e:
        print(f"[ERROR] CSV 로드 실패 ({path}): {e}")
        return pd.DataFrame()


def save_csv(df: pd.DataFrame, path: Path, columns: Optional[List[str]] = None):
    """CSV 저장"""
    path.parent.mkdir(parents=True, exist_ok=True)
    
    if columns:
        for col in columns:
            if col not in df.columns:
                df[col] = None
        df = df[columns]
    
    df.to_csv(path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_NONNUMERIC)


def append_to_csv(new_df: pd.DataFrame, path: Path, columns: List[str], dedupe_col: str = None):
    """
    CSV에 데이터 추가 (append)
    
    Args:
        new_df: 추가할 데이터
        path: CSV 경로
        columns: 컬럼 순서
        dedupe_col: 중복 제거 기준 컬럼
    """
    if path.exists():
        existing_df = load_csv(path, columns)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        if dedupe_col:
            combined_df = combined_df.drop_duplicates(subset=[dedupe_col], keep='last')
    else:
        combined_df = new_df
    
    save_csv(combined_df, path, columns)