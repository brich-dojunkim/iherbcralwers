#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
데이터 관리 서비스
"""

from pathlib import Path
from typing import Set, Tuple

import pandas as pd

from config import (
    HAZARD_BASE_CSV,
    IHERB_SCRAPED_CSV,
    GEMINI_VERIFICATIONS_CSV,
    Status
)
from utils.csv_utils import load_csv


def get_unprocessed_items(stage: str) -> Tuple[pd.DataFrame, Set[str]]:
    """
    미처리 항목 조회
    
    Args:
        stage: 'phase1' or 'phase2'
    
    Returns:
        (처리할 DataFrame, 이미 처리된 SEQ 셋)
    """
    if stage == 'phase1':
        # hazard_base에서 로드
        df_base = load_csv(HAZARD_BASE_CSV)
        
        # 기존 스크래핑 결과
        df_scraped = load_csv(IHERB_SCRAPED_CSV)
        processed_seqs = set(df_scraped['SELF_IMPORT_SEQ']) if not df_scraped.empty else set()
        
        # 미처리만
        df_todo = df_base[~df_base['SELF_IMPORT_SEQ'].isin(processed_seqs)]
        
        return df_todo, processed_seqs
    
    elif stage == 'phase2':
        # 스크래핑 완료 & STATUS=FOUND
        df_scraped = load_csv(IHERB_SCRAPED_CSV)
        df_found = df_scraped[df_scraped['STATUS'] == Status.FOUND]
        
        # 기존 검증 결과
        df_verified = load_csv(GEMINI_VERIFICATIONS_CSV)
        verified_seqs = set(df_verified['SELF_IMPORT_SEQ']) if not df_verified.empty else set()
        
        # 미검증만
        df_todo = df_found[~df_found['SELF_IMPORT_SEQ'].isin(verified_seqs)]
        
        # hazard_base와 조인 (이미지 URL 필요)
        df_base = load_csv(HAZARD_BASE_CSV)
        df_todo = df_todo.merge(
            df_base[['SELF_IMPORT_SEQ', 'PRDT_NM', 'MUFC_NM', 'IMAGE_URL_MFDS']],
            on='SELF_IMPORT_SEQ',
            how='left'
        )
        
        return df_todo, verified_seqs
    
    else:
        raise ValueError(f"Unknown stage: {stage}")


def join_all_data() -> pd.DataFrame:
    """3개 CSV를 JOIN하여 전체 데이터 반환"""
    df_base = load_csv(HAZARD_BASE_CSV)
    df_scraped = load_csv(IHERB_SCRAPED_CSV)
    df_gemini = load_csv(GEMINI_VERIFICATIONS_CSV)
    
    df_matched = df_base \
        .merge(df_scraped, on='SELF_IMPORT_SEQ', how='left') \
        .merge(df_gemini, on='SELF_IMPORT_SEQ', how='left')
    
    return df_matched