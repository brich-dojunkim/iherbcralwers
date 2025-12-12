#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 3: 증분 업데이트
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. 기존 CSV에서 최신 SEQ 확인
2. 식약처 API에서 신규 데이터만 수집
3. Phase 1 실행 (신규 데이터만)
4. Phase 2 실행 (신규 + 미검증)
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from config import OUTPUT_CSV, Status
from phase1_collect_urls import fetch_hazard_data, process_hazard_items
from phase2_verify_images import process_verification


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 신규 데이터 확인
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_new_items(days: int = 7) -> pd.DataFrame:
    """
    신규 위해식품 데이터 수집
    
    Args:
        days: 최근 N일 데이터
    """
    print("\n" + "="*70)
    print(f"최근 {days}일 신규 데이터 확인")
    print("="*70 + "\n")
    
    # 기존 CSV 로드
    existing_seqs = set()
    if Path(OUTPUT_CSV).exists():
        try:
            existing_df = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig")
            existing_seqs = set(existing_df['SELF_IMPORT_SEQ'].astype(str))
            print(f"[INFO] 기존 데이터: {len(existing_seqs)}건")
        except Exception as e:
            print(f"[WARN] 기존 CSV 로드 실패: {e}")
    
    # 전체 데이터 수집
    all_df = fetch_hazard_data(limit=None, use_cache=True)
    
    if all_df.empty:
        print("[ERROR] 데이터 수집 실패")
        return pd.DataFrame()
    
    # 날짜 필터링
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    cutoff_int = int(cutoff_date)
    
    recent_df = all_df[all_df['CRET_DTM'] >= cutoff_int].copy()
    print(f"[INFO] 최근 {days}일 데이터: {len(recent_df)}건")
    
    # 신규 데이터 필터링
    recent_df['SELF_IMPORT_SEQ'] = recent_df['SELF_IMPORT_SEQ'].astype(str)
    new_df = recent_df[~recent_df['SELF_IMPORT_SEQ'].isin(existing_seqs)]
    
    print(f"[INFO] 신규 데이터: {len(new_df)}건")
    
    return new_df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메인
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
    
    if new_df.empty:
        print("\n[INFO] 신규 데이터 없음")
    else:
        # Step 2: Phase 1 - URL 수집 (신규 데이터만)
        if not args.skip_phase1:
            print("\n" + "="*70)
            print("Phase 1: 신규 데이터 URL 수집")
            print("="*70 + "\n")
            
            process_hazard_items(
                hazard_df=new_df,
                out_csv=OUTPUT_CSV,
                max_items=None,
                headless=args.headless
            )
    
    # Step 3: Phase 2 - 이미지 검증 (미검증 항목)
    if not args.skip_phase2:
        print("\n" + "="*70)
        print("Phase 2: 미검증 항목 검증")
        print("="*70 + "\n")
        
        process_verification(
            input_csv=OUTPUT_CSV,
            headless=args.headless,
            revalidate_all=False,  # 미검증만
            specific_seq=None
        )
    
    # 최종 통계
    print("\n" + "="*70)
    print("최종 통계")
    print("="*70 + "\n")
    
    if Path(OUTPUT_CSV).exists():
        final_df = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig")
        
        print(f"전체 데이터: {len(final_df)}건")
        print(f"\n[상태별 분포]")
        print(final_df['STATUS'].value_counts().to_string())
        
        verified_count = final_df['GEMINI_VERIFIED'].notna().sum()
        match_count = (final_df['GEMINI_VERIFIED'] == True).sum()
        
        print(f"\n[검증 통계]")
        print(f"검증 완료: {verified_count}건")
        print(f"매칭 성공: {match_count}건")
        print(f"매칭 실패: {verified_count - match_count}건")


if __name__ == "__main__":
    main()
