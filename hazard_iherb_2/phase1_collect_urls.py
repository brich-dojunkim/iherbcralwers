#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 1: iHerb URL 수집 + 스크래핑
"""

import os
import time
import json
from datetime import datetime

from config import IMG_DIR, IHERB_SCRAPED_CSV, IHERB_SCRAPED_COLUMNS, Status
from utils.selenium_utils import create_driver
from utils.image_utils import download_image, extract_product_code
from scrapers.google_search import GoogleImageSearch
from utils.csv_utils import append_to_csv
from scrapers.iherb_scraper import IHerbScraper
from services.mfds_api import fetch_hazard_data
from services.data_manager import get_unprocessed_items

import pandas as pd


def main(max_items: int = None, headless: bool = False):
    print("\n" + "="*70)
    print("Phase 1: iHerb URL 수집 + 스크래핑")
    print("="*70 + "\n")
    
    # 1. 데이터 로드
    hazard_df = fetch_hazard_data(limit=None)
    if hazard_df.empty:
        print("[ERROR] 데이터 없음")
        return
    
    # 2. 미처리 항목 확인
    df_todo, processed_seqs = get_unprocessed_items('phase1')
    
    if max_items:
        df_todo = df_todo.head(max_items)
        print(f"[INFO] {max_items}건으로 제한")
    
    print(f"[INFO] 처리 대상: {len(df_todo)}건\n")
    
    if df_todo.empty:
        print("[INFO] 처리할 항목 없음")
        return
    
    # 3. 드라이버 생성
    driver = create_driver(headless=headless)
    google_search = GoogleImageSearch(driver)
    iherb_scraper = IHerbScraper(driver)
    
    IMG_DIR.mkdir(exist_ok=True)
    
    # 통계
    stats = {
        'success': 0,
        'not_found': 0,
        'no_image': 0,
        'download_failed': 0,
        'scrape_failed': 0
    }
    
    try:
        total = len(df_todo)
        
        for idx, (_, row) in enumerate(df_todo.iterrows(), start=1):
            seq = str(row['SELF_IMPORT_SEQ'])
            prdt_nm = row['PRDT_NM']
            image_url = row.get('IMAGE_URL', '')
            
            # 첫 줄 출력: [진행률] SEQ | 제품명
            print(f"[{idx}/{total}] {seq} | {prdt_nm[:50]}...")
            
            results = []
            
            # 이미지 없음
            if not image_url or pd.isna(image_url):
                print(f"         ⊘ NO_IMAGE\n")
                stats['no_image'] += 1
                results.append({
                    "SELF_IMPORT_SEQ": seq,
                    "IHERB_URL": None,
                    "product_code": None,
                    "IHERB_제품명": None,
                    "IHERB_제조사": None,
                    "IHERB_PRODUCT_IMAGES": None,
                    "STATUS": Status.NO_IMAGE,
                    "SCRAPED_AT": datetime.now().strftime("%Y%m%d%H%M%S")
                })
                append_to_csv(pd.DataFrame(results), IHERB_SCRAPED_CSV, IHERB_SCRAPED_COLUMNS)
                continue
            
            # 이미지 다운로드
            image_path = download_image(str(image_url), save_dir=IMG_DIR)
            if not image_path:
                print(f"         ⏬ DOWNLOAD_FAILED\n")
                stats['download_failed'] += 1
                results.append({
                    "SELF_IMPORT_SEQ": seq,
                    "IHERB_URL": None,
                    "product_code": None,
                    "IHERB_제품명": None,
                    "IHERB_제조사": None,
                    "IHERB_PRODUCT_IMAGES": None,
                    "STATUS": Status.DOWNLOAD_FAILED,
                    "SCRAPED_AT": datetime.now().strftime("%Y%m%d%H%M%S")
                })
                append_to_csv(pd.DataFrame(results), IHERB_SCRAPED_CSV, IHERB_SCRAPED_COLUMNS)
                continue
            
            # Google 검색
            iherb_url = google_search.find_iherb_url(image_path)
            
            if not iherb_url:
                print(f"         ✗ NOT_FOUND\n")
                stats['not_found'] += 1
                results.append({
                    "SELF_IMPORT_SEQ": seq,
                    "IHERB_URL": None,
                    "product_code": None,
                    "IHERB_제품명": None,
                    "IHERB_제조사": None,
                    "IHERB_PRODUCT_IMAGES": None,
                    "STATUS": Status.NOT_FOUND,
                    "SCRAPED_AT": datetime.now().strftime("%Y%m%d%H%M%S")
                })
            else:
                # iHerb 스크래핑
                scraped = iherb_scraper.scrape_product(iherb_url)
                
                if scraped['image_url'] and scraped['product_name'] and scraped['brand']:
                    print(f"         ✓ FOUND → {scraped['brand']}\n")
                    status = Status.FOUND
                    stats['success'] += 1
                else:
                    print(f"         ⚠ SCRAPE_FAILED → URL found but scraping incomplete\n")
                    status = Status.SCRAPE_FAILED
                    stats['scrape_failed'] += 1
                
                results.append({
                    "SELF_IMPORT_SEQ": seq,
                    "IHERB_URL": iherb_url,
                    "product_code": extract_product_code(iherb_url),
                    "IHERB_제품명": scraped['product_name'],
                    "IHERB_제조사": scraped['brand'],
                    "IHERB_PRODUCT_IMAGES": json.dumps([scraped['image_url']]) if scraped['image_url'] else None,
                    "STATUS": status,
                    "SCRAPED_AT": datetime.now().strftime("%Y%m%d%H%M%S")
                })
            
            # 저장
            append_to_csv(pd.DataFrame(results), IHERB_SCRAPED_CSV, IHERB_SCRAPED_COLUMNS)
            
            # 이미지 삭제
            try:
                os.remove(image_path)
            except:
                pass
            
            time.sleep(1)
        
        # 최종 통계
        print(f"{'='*70}")
        print(f"[DONE] 처리 완료!")
        print(f"{'='*70}")
        print(f"성공: {stats['success']}건")
        print(f"미발견: {stats['not_found']}건")
        print(f"이미지 없음: {stats['no_image']}건")
        print(f"다운로드 실패: {stats['download_failed']}건")
        print(f"스크래핑 실패: {stats['scrape_failed']}건")
        print(f"{'='*70}")
    
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] 중단")
    
    finally:
        driver.quit()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Phase 1: URL 수집 + 스크래핑")
    parser.add_argument("--max-items", type=int, help="처리 개수 제한")
    parser.add_argument("--headless", action="store_true", help="헤드리스 모드")
    args = parser.parse_args()
    
    main(max_items=args.max_items, headless=args.headless)