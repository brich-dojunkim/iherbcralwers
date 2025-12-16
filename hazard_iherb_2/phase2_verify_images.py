#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2: Gemini 이미지 검증
"""

import json
import time
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import GEMINI_VERIFICATIONS_CSV, GEMINI_VERIFICATIONS_COLUMNS
from utils.selenium_utils import create_driver, save_debug_info
from utils.csv_utils import append_to_csv
from scrapers.gemini_verifier import GeminiVerifier
from services.data_manager import get_unprocessed_items


# 설정
DEBUG_DIR = Path(__file__).parent / "debug"
TEMP_DIR = Path(__file__).parent / "temp_images"
MAX_MESSAGES_PER_CHAT = 5
WAIT_AFTER_VERIFY = 15


def main(headless: bool = False, limit: int = None, start_seq: str = None):
    print("\n" + "="*70)
    print("Phase 2: Gemini 이미지 검증")
    print("="*70 + "\n")
    
    # 1. 미검증 항목 로드
    df_todo, verified_seqs = get_unprocessed_items('phase2')

    if start_seq:
        start_idx = df_todo[df_todo['SELF_IMPORT_SEQ'] == str(start_seq)].index
        if len(start_idx) > 0:
            df_todo = df_todo.loc[start_idx[0]:]
            print(f"[INFO] SEQ {start_seq}부터 시작: {len(df_todo)}건")

    if limit:
        df_todo = df_todo.head(limit)
    
    print(f"[INFO] 검증 대상: {len(df_todo)}건\n")
    
    if df_todo.empty:
        print("[INFO] 검증할 항목 없음")
        return
    
    # 2. Gemini 브라우저 생성
    print("[BROWSER] Gemini 브라우저 생성...")
    driver = create_driver(headless=headless, profile_name="GeminiBot")
    driver.get("https://gemini.google.com")
    time.sleep(3)
    
    save_debug_info(driver, "initial", "login", DEBUG_DIR)
    
    print("\n[LOGIN] Gemini 로그인 확인")
    print("준비되면 Enter를 눌러주세요...")
    input()
    print("[LOGIN] ✓ 계속 진행\n")
    
    # 3. 검증 시작
    gemini = GeminiVerifier(driver, TEMP_DIR, DEBUG_DIR)
    
    try:
        results = []
        chat_count = 0
        success = 0
        
        for idx, row in df_todo.iterrows():
            # 새 채팅
            if chat_count >= MAX_MESSAGES_PER_CHAT and chat_count > 0:
                print(f"\n[CHAT] {chat_count}개 처리 → 새 채팅\n")
                gemini.start_new_chat()
                chat_count = 0
            
            seq = str(row['SELF_IMPORT_SEQ'])
            prdt_nm = row['PRDT_NM']
            
            # 데이터 준비
            hazard_image = row['IMAGE_URL_MFDS']
            if ',' in str(hazard_image):
                hazard_image = hazard_image.split(',')[0].strip()
            
            iherb_images = json.loads(row['IHERB_PRODUCT_IMAGES'])
            iherb_image = iherb_images[0] if iherb_images else None
            
            print(f"\n{'='*70}")
            print(f"[{idx+1}/{len(df_todo)}] {prdt_nm}")
            print(f"SEQ: {seq}")
            print(f"{'='*70}")
            
            if not iherb_image:
                print("  [SKIP] iHerb 이미지 없음")
                continue
            
            # 검증
            is_match, reason = gemini.verify_images_with_retry(
                hazard_image_url=hazard_image,
                iherb_image_url=iherb_image,
                hazard_name=prdt_nm,
                hazard_brand=row['MUFC_NM'],
                iherb_name=row['IHERB_제품명'],
                iherb_brand=row['IHERB_제조사'],
                seq=seq
            )
            
            results.append({
                'SELF_IMPORT_SEQ': seq,
                'GEMINI_VERIFIED': str(is_match),
                'GEMINI_REASON': str(reason),
                'VERIFIED_AT': datetime.now().strftime("%Y%m%d%H%M%S")
            })
            
            if is_match:
                print(f"  [SUCCESS] ✓ 매칭 성공")
                success += 1
            else:
                print(f"  [INFO] ✗ 매칭 실패")
            
            # 저장
            append_to_csv(
                pd.DataFrame(results),
                GEMINI_VERIFICATIONS_CSV,
                GEMINI_VERIFICATIONS_COLUMNS,
                dedupe_col='SELF_IMPORT_SEQ'
            )
            results = []
            
            chat_count += 1
            time.sleep(WAIT_AFTER_VERIFY)
        
        print(f"\n{'='*70}")
        print(f"[DONE] 검증 완료!")
        print(f"매칭 성공: {success}건")
        print(f"{'='*70}")
    
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] 중단")
    
    finally:
        driver.quit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--start-seq", type=str, help="시작 SEQ")
    args = parser.parse_args()
    
    main(headless=args.headless, limit=args.limit)