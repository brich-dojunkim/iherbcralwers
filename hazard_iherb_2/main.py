#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
통합 Phase: URL 찾기 + 즉시 검증
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. Google 역검색 → iHerb URL 찾기
2. AI Overview 추출 (선택)
3. iHerb 페이지 스크래핑 (같은 브라우저 사용)
4. Gemini 즉시 검증
5. 결과 CSV 저장
"""

import os
import json
import time
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from config_2 import OUTPUT_CSV, IMG_DIR, Status, CSV_COLUMNS
from utils.selenium_utils import create_driver, save_debug_info
from utils.image_utils import download_image, extract_product_code
from utils.csv_utils import append_to_csv
from scrapers.google_search import GoogleImageSearch
from scrapers.iherb_scraper import IHerbScraper
from scrapers.gemini_verifier import GeminiVerifier
from services.mfds_api import fetch_hazard_data
from services.data_manager import get_unprocessed_items


# 설정
DEBUG_DIR = Path(__file__).parent / "debug"
TEMP_DIR = Path(__file__).parent / "temp_images"
MAX_MESSAGES_PER_CHAT = 5
WAIT_AFTER_VERIFY = 15


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 통합 Matcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class UnifiedMatcher:
    """통합 매칭 (검색 + 검증)"""
    
    def __init__(self, headless: bool = False):
        print(f"\n{'='*70}")
        print(f"브라우저 초기화 중...")
        print(f"{'='*70}\n")
        
        # Google/iHerb 겸용 브라우저
        print(f"[1/2] Google/iHerb 브라우저 생성...")
        self.main_driver = create_driver(headless, "MainBot")
        self.google_search = GoogleImageSearch(self.main_driver)
        self.iherb_scraper = IHerbScraper(self.main_driver)
        print(f"✓ Google/iHerb 브라우저 준비 완료\n")
        time.sleep(2)
        
        # Gemini 검증용 브라우저
        print(f"[2/2] Gemini 브라우저 생성...")
        self.gemini_driver = create_driver(headless, "GeminiBot")
        self.gemini_driver.get("https://gemini.google.com")
        time.sleep(3)
        self.gemini = GeminiVerifier(self.gemini_driver, TEMP_DIR, DEBUG_DIR)
        print(f"✓ Gemini 브라우저 준비 완료\n")
        
        # Gemini 로그인 확인
        print(f"{'='*70}")
        print(f"Gemini 로그인 확인")
        print(f"{'='*70}\n")
        print(f"Gemini 브라우저에서 로그인 후 Enter를 눌러주세요...")
        print(f"⚠️  중요: Enter 누른 후 브라우저 창을 절대 닫지 마세요!\n")
        input(f"준비되면 Enter...")
        print(f"✓ 계속 진행합니다\n")
        time.sleep(2)
        
        # 채팅 카운터
        self.chat_count = 0
    
    def process_item(self, row: pd.Series, idx: int, total: int) -> dict:
        """한 항목 처리"""
        
        # 새 채팅 시작
        if self.chat_count >= MAX_MESSAGES_PER_CHAT and self.chat_count > 0:
            print(f"\n[CHAT] {self.chat_count}개 처리 → 새 채팅\n")
            self.gemini.start_new_chat()
            self.chat_count = 0
        
        seq = str(row['SELF_IMPORT_SEQ'])
        prdt_nm = row['PRDT_NM']
        mufc_nm = row.get('MUFC_NM', '')
        cret_dtm = row.get('CRET_DTM', '')
        image_url = row['IMAGE_URL']
        
        # 날짜 포맷팅 (YYYYMMDD → YYYY-MM-DD)
        if cret_dtm and len(str(cret_dtm)) == 8:
            date_str = str(cret_dtm)
            date_display = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        else:
            date_display = str(cret_dtm)
        
        # 제품명 짧게 표시
        prdt_display = prdt_nm[:50] + "..." if len(prdt_nm) > 50 else prdt_nm
        
        print(f"\n[{idx}/{total}] {date_display} | {prdt_display}")
        
        # 1. 이미지 확인
        if not image_url or pd.isna(image_url):
            print(f"         ⊘ NO_IMAGE")
            return self._create_result(row, Status.NO_IMAGE)
        
        # 2. 이미지 다운로드
        image_path = download_image(str(image_url))
        if not image_path:
            print(f"         ⏬ DOWNLOAD_FAILED")
            return self._create_result(row, Status.DOWNLOAD_FAILED)
        
        # 3. Google 역검색 (main_driver 사용)
        iherb_url = self.google_search.find_iherb_url(image_path)
        
        # 이미지 삭제
        try:
            os.remove(image_path)
        except:
            pass
        
        if not iherb_url:
            print(f"         ✗ NOT_FOUND")
            return self._create_result(row, Status.NOT_FOUND)
        
        print(f"  [URL] {iherb_url}")
        
        # 4. AI Overview 추출
        ai_overview = self.google_search.extract_ai_overview()
        if ai_overview:
            print(f"  [AI] {ai_overview['product_name'][:50]}...")
        
        # 5. iHerb 스크래핑 (같은 main_driver 사용!)
        print(f"  [SCRAPE] iHerb 페이지 접속...")
        scraped = self.iherb_scraper.scrape_product(iherb_url)
        
        if not scraped['image_url'] or not scraped['product_name']:
            print(f"         ⚠ SCRAPE_FAILED")
            return self._create_result(
                row, 
                Status.SCRAPE_FAILED,
                iherb_url=iherb_url
            )
        
        print(f"  [IHERB] {scraped['brand']} - {scraped['product_name'][:40]}")
        
        # 6. Gemini 검증 (gemini_driver 사용)
        print(f"  [VERIFY] Gemini 검증 중...")
        is_match, reason = self.gemini.verify_images_with_retry(
            hazard_image_url=str(image_url),
            iherb_image_url=scraped['image_url'],
            hazard_name=prdt_nm,
            hazard_brand=mufc_nm,
            iherb_name=scraped['product_name'],
            iherb_brand=scraped['brand'],
            seq=seq,
            ai_overview_text=ai_overview['full_text'] if ai_overview else None
        )
        
        self.chat_count += 1
        
        if is_match:
            print(f"         ✓ VERIFIED_MATCH")
            print(f"  [REASON] {reason[:80]}...")
            status = Status.VERIFIED_MATCH
        else:
            print(f"         ✗ VERIFIED_MISMATCH")
            print(f"  [REASON] {reason[:80]}...")
            status = Status.VERIFIED_MISMATCH
        
        return self._create_result(
            row,
            status,
            iherb_url=iherb_url,
            product_code=extract_product_code(iherb_url),
            iherb_brand=scraped['brand'],
            iherb_name=scraped['product_name'],
            iherb_images=json.dumps([scraped['image_url']]),
            gemini_verified=is_match,
            gemini_reason=reason
        )
    
    def _create_result(self, row: pd.Series, status: str, **kwargs) -> dict:
        """결과 딕셔너리 생성"""
        result = {
            'SELF_IMPORT_SEQ': str(row['SELF_IMPORT_SEQ']),
            'PRDT_NM': row['PRDT_NM'],
            'MUFC_NM': row.get('MUFC_NM', ''),
            'MUFC_CNTRY_NM': row.get('MUFC_CNTRY_NM', ''),
            'INGR_NM_LST': row.get('INGR_NM_LST', ''),
            'CRET_DTM': row.get('CRET_DTM', ''),
            'IMAGE_URL': row.get('IMAGE_URL', ''),
            'IHERB_URL': kwargs.get('iherb_url', ''),
            'product_code': kwargs.get('product_code', ''),
            'IHERB_제품명': kwargs.get('iherb_name', ''),
            'IHERB_제조사': kwargs.get('iherb_brand', ''),
            'IHERB_PRODUCT_IMAGES': kwargs.get('iherb_images', ''),
            'STATUS': status,
            'GEMINI_VERIFIED': kwargs.get('gemini_verified', ''),
            'GEMINI_REASON': kwargs.get('gemini_reason', ''),
            'VERIFIED_AT': datetime.now().strftime("%Y%m%d%H%M%S")
        }
        return result
    
    def close(self):
        """브라우저 종료"""
        try:
            self.main_driver.quit()
            self.gemini_driver.quit()
        except:
            pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메인
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    parser = argparse.ArgumentParser(description="통합 Phase: 검색 + 검증")
    parser.add_argument("--max-items", type=int, help="처리 개수 제한")
    parser.add_argument("--headless", action="store_true", help="헤드리스 모드")
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("통합 Phase: URL 찾기 + 즉시 검증")
    print("="*70 + "\n")
    
    # 데이터 로드
    hazard_df = fetch_hazard_data()
    
    if hazard_df.empty:
        print("[ERROR] 데이터 없음")
        return
    
    if args.max_items:
        hazard_df = hazard_df.head(args.max_items)
    
    print(f"[INFO] 처리 대상: {len(hazard_df)}건\n")
    
    # 기존 결과 로드
    processed_seqs = set()
    results = []
    
    if Path(OUTPUT_CSV).exists():
        try:
            existing_df = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig")
            processed_seqs = set(existing_df['SELF_IMPORT_SEQ'].astype(str))
            results = existing_df.to_dict('records')
            print(f"[INFO] 기존 결과: {len(processed_seqs)}건\n")
        except:
            pass
    
    # 미처리 항목만
    hazard_df = hazard_df[~hazard_df['SELF_IMPORT_SEQ'].astype(str).isin(processed_seqs)]
    print(f"[INFO] 미처리 항목: {len(hazard_df)}건\n")
    
    if hazard_df.empty:
        print("[INFO] 처리할 항목 없음")
        return
    
    # Matcher 초기화
    matcher = UnifiedMatcher(headless=args.headless)
    
    try:
        total = len(hazard_df)
        success = 0
        
        for idx, (_, row) in enumerate(hazard_df.iterrows(), 1):
            result = matcher.process_item(row, idx, total)
            results.append(result)
            
            if result['STATUS'] == Status.VERIFIED_MATCH:
                success += 1
            
            # 저장
            save_results(results, OUTPUT_CSV)
            
            # Rate limit
            time.sleep(WAIT_AFTER_VERIFY)
        
        # 최종 통계
        print(f"\n{'='*70}")
        print(f"완료!")
        print(f"{'='*70}")
        print(f"총 처리: {total}건")
        print(f"매칭 성공: {success}건")
        print(f"성공률: {success/total*100:.1f}%")
        print(f"{'='*70}")
        
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] 중단")
        save_results(results, OUTPUT_CSV)
    
    finally:
        matcher.close()


def save_results(results: list, output_path: str):
    """결과 저장"""
    df = pd.DataFrame(results)
    
    # 컬럼 순서
    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = ''
    
    df = df[CSV_COLUMNS]
    df.to_csv(output_path, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()