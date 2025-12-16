#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 3: 증분 업데이트
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. hazard_base.csv에서 최신 데이터 확인
2. 신규 데이터만 Phase 1 실행
3. 미검증 데이터 Phase 2 실행
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from config import (
    HAZARD_BASE_CSV,
    IHERB_SCRAPED_CSV,
    GEMINI_VERIFICATIONS_CSV
)
from services.mfds_api import fetch_hazard_data

# Phase 1, 2의 main 함수 import
import phase1_collect_urls
import phase2_verify_images


def get_new_items(days: int = 7) -> pd.DataFrame:
    """
    신규 위해식품 데이터 수집
    
    Args:
        days: 최근 N일 데이터
    """
    print("\n" + "="*70)
    print(f"최근 {days}일 신규 데이터 확인")
    print("="*70 + "\n")
    
    # 기존 스크래핑 결과 로드
    existing_seqs = set()
    if Path(IHERB_SCRAPED_CSV).exists():
        try:
            existing_df = pd.read_csv(IHERB_SCRAPED_CSV, encoding="utf-8-sig", dtype=str)
            existing_seqs = set(existing_df['SELF_IMPORT_SEQ'].astype(str))
            print(f"[INFO] 기존 스크래핑: {len(existing_seqs)}건")
        except Exception as e:
            print(f"[WARN] 기존 CSV 로드 실패: {e}")
    
    # hazard_base에서 로드
    if not Path(HAZARD_BASE_CSV).exists():
        print("[INFO] hazard_base.csv 없음, API에서 가져옵니다")
        all_df = fetch_hazard_data(limit=None, use_cache=True)
    else:
        all_df = pd.read_csv(HAZARD_BASE_CSV, encoding="utf-8-sig", dtype=str)
        print(f"[INFO] hazard_base.csv 로드: {len(all_df)}건")
    
    if all_df.empty:
        print("[ERROR] 데이터 수집 실패")
        return pd.DataFrame()
    
    # 날짜 필터링
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    cutoff_int = int(cutoff_date)
    
    all_df['CRET_DTM'] = pd.to_numeric(all_df['CRET_DTM'], errors='coerce').fillna(0).astype(int)
    recent_df = all_df[all_df['CRET_DTM'] >= cutoff_int].copy()
    print(f"[INFO] 최근 {days}일 데이터: {len(recent_df)}건")
    
    # 신규 데이터 필터링
    recent_df['SELF_IMPORT_SEQ'] = recent_df['SELF_IMPORT_SEQ'].astype(str)
    new_df = recent_df[~recent_df['SELF_IMPORT_SEQ'].isin(existing_seqs)]
    
    print(f"[INFO] 신규 데이터: {len(new_df)}건")
    
    return new_df


def main():
    parser = argparse.ArgumentParser(description="Phase 3: 증분 업데이트")
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="최근 N일 데이터 확인 (기본: 7일)"
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="헤드리스 모드"
    )
    parser.add_argument(
        "--skip-phase1",
        action="store_true",
        help="Phase 1 스킵 (URL 수집 제외)"
    )
    parser.add_argument(
        "--skip-phase2",
        action="store_true",
        help="Phase 2 스킵 (검증 제외)"
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("Phase 3: 증분 업데이트")
    print("="*70 + "\n")
    
    # Step 1: 신규 데이터 확인
    new_df = get_new_items(days=args.days)
    
    has_new = not new_df.empty
    
    if has_new:
        print(f"\n[INFO] 신규 데이터 {len(new_df)}건 발견")
    else:
        print("\n[INFO] 신규 데이터 없음")
    
    # Step 2: Phase 1 - URL 수집 + 스크래핑
    if not args.skip_phase1 and has_new:
        print("\n" + "="*70)
        print("Phase 1: 신규 데이터 URL 수집 + 스크래핑")
        print("="*70 + "\n")
        
        # Phase 1 실행 (신규 데이터는 자동으로 처리됨)
        phase1_collect_urls.main(max_items=None)
    
    # Step 3: Phase 2 - 이미지 검증 (미검증 항목)
    if not args.skip_phase2:
        print("\n" + "="*70)
        print("Phase 2: 미검증 항목 검증")
        print("="*70 + "\n")
        
        phase2_verify_images.main(
            headless=args.headless,
            limit=None,
            start_seq=None
        )
    
    # 최종 통계
    print("\n" + "="*70)
    print("최종 통계")
    print("="*70 + "\n")
    
    if Path(HAZARD_BASE_CSV).exists():
        df_base = pd.read_csv(HAZARD_BASE_CSV, encoding="utf-8-sig", dtype=str)
        print(f"전체 위해식품: {len(df_base)}건")
    
    if Path(IHERB_SCRAPED_CSV).exists():
        df_scraped = pd.read_csv(IHERB_SCRAPED_CSV, encoding="utf-8-sig", dtype=str)
        print(f"\n[스크래핑 통계]")
        print(f"전체: {len(df_scraped)}건")
        if 'STATUS' in df_scraped.columns:
            print(df_scraped['STATUS'].value_counts().to_string())
    
    if Path(GEMINI_VERIFICATIONS_CSV).exists():
        df_gemini = pd.read_csv(GEMINI_VERIFICATIONS_CSV, encoding="utf-8-sig", dtype=str)
        print(f"\n[검증 통계]")
        print(f"검증 완료: {len(df_gemini)}건")
        
        if 'GEMINI_VERIFIED' in df_gemini.columns:
            match_count = (df_gemini['GEMINI_VERIFIED'] == 'True').sum()
            mismatch_count = (df_gemini['GEMINI_VERIFIED'] == 'False').sum()
            print(f"매칭 성공: {match_count}건")
            print(f"매칭 실패: {mismatch_count}건")


if __name__ == "__main__":
    main()