#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
식약처 API 서비스
"""

import time
import requests
from typing import Optional

import pandas as pd

from config import (
    MFDS_API_KEY, SERVICE_ID, BASE_URL, PAGE_SIZE,
    HAZARD_BASE_CSV, HAZARD_BASE_COLUMNS
)
from utils.csv_utils import save_csv


def fetch_hazard_data(limit: Optional[int] = None, use_cache: bool = True) -> pd.DataFrame:
    """식약처 위해식품 데이터 수집"""
    print("=== 식약처 위해식품 데이터 수집 시작 ===")
    
    cached_df = pd.DataFrame()
    has_cache = False
    
    if use_cache and HAZARD_BASE_CSV.exists():
        try:
            cached_df = pd.read_csv(HAZARD_BASE_CSV, encoding="utf-8-sig")
            print(f"[CACHE] 기존 캐시 로드: {len(cached_df)}건")
            has_cache = True
        except Exception as e:
            print(f"[WARN] 캐시 로드 실패: {e}")
    
    all_rows = []
    
    if limit:
        url = f"{BASE_URL}/{MFDS_API_KEY}/{SERVICE_ID}/json/1/{limit}"
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            if SERVICE_ID in data:
                all_rows = data[SERVICE_ID].get('row', [])
                print(f"[API] 수집: {len(all_rows)}건")
        except Exception as e:
            print(f"[ERROR] API 요청 실패: {e}")
    else:
        # 전체 수집
        start_idx = 1
        while True:
            end_idx = start_idx + PAGE_SIZE - 1
            url = f"{BASE_URL}/{MFDS_API_KEY}/{SERVICE_ID}/json/{start_idx}/{end_idx}"
            
            try:
                resp = requests.get(url, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                
                if SERVICE_ID not in data:
                    break
                
                rows = data[SERVICE_ID].get('row', [])
                if not rows:
                    break
                
                all_rows.extend(rows)
                if len(all_rows) % 1000 == 0:
                    print(f"[API] 수집: {len(all_rows)}건")
                
                start_idx = end_idx + 1
                time.sleep(0.3)
                
            except Exception as e:
                print(f"[ERROR] API 요청 실패: {e}")
                break
    
    new_df = pd.DataFrame(all_rows)
    
    if has_cache and not new_df.empty:
        cached_df['SELF_IMPORT_SEQ'] = cached_df['SELF_IMPORT_SEQ'].astype(str)
        new_df['SELF_IMPORT_SEQ'] = new_df['SELF_IMPORT_SEQ'].astype(str)
        
        combined_df = pd.concat([new_df, cached_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['SELF_IMPORT_SEQ'], keep='first')
        df = combined_df
    else:
        if not new_df.empty:
            new_df['SELF_IMPORT_SEQ'] = new_df['SELF_IMPORT_SEQ'].astype(str)
        df = new_df
    
    if not df.empty and 'CRET_DTM' in df.columns:
        df['CRET_DTM'] = pd.to_numeric(df['CRET_DTM'], errors='coerce').fillna(0).astype(int)
        df = df.sort_values('CRET_DTM', ascending=False).reset_index(drop=True)
    
    if not df.empty and use_cache:
        save_csv(df, HAZARD_BASE_CSV, HAZARD_BASE_COLUMNS)
        print(f"[CACHE] hazard_base.csv 저장: {len(df)}건")
    
    print(f"=== 수집 완료: {len(df)}건 ===\n")
    return df