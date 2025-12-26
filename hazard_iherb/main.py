#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
통합 Phase: 검색 → 스크래핑 → 검증 (한번에)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
브라우저 2개로 모든 단계를 순차적으로 처리
- 브라우저 1: Google 검색 + iHerb 스크래핑
- 브라우저 2: Gemini 검증
- 결과: 단일 unified_results.csv로 저장
"""

import os
import json
import time
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import (
    IMG_DIR,
    Status,
    PROJECT_DIR,
    UNIFIED_CSV,
    UNIFIED_COLUMNS
)
from utils.selenium_utils import create_driver
from utils.image_utils import download_image, extract_product_code, extract_iherb_code
from scrapers.google_search import GoogleImageSearch
from scrapers.iherb_scraper import IHerbScraper
from scrapers.gemini_verifier import GeminiVerifier
from services.mfds_api import fetch_hazard_data


# 설정
DEBUG_DIR = Path(__file__).parent / "debug"
TEMP_DIR = Path(__file__).parent / "temp_images"
MAX_MESSAGES_PER_CHAT = 5
WAIT_AFTER_GOOGLE = 2
WAIT_AFTER_SCRAPE = 2
WAIT_AFTER_VERIFY = 5


class UnifiedMatcher:
    """통합 매칭 (검색 + 스크래핑 + 검증)"""
    
    def __init__(self, headless: bool = False):
        print(f"\n{'='*70}")
        print(f"브라우저 초기화")
        print(f"{'='*70}\n")
        
        # 브라우저 1: Google/iHerb
        print(f"[1/2] Google/iHerb 브라우저 생성...")
        self.main_driver = create_driver(headless, "MainBot")
        self.google_search = GoogleImageSearch(self.main_driver)
        self.iherb_scraper = IHerbScraper(self.main_driver)
        print(f"      ✓ 준비 완료\n")
        time.sleep(2)
        
        # 브라우저 2: Gemini
        print(f"[2/2] Gemini 브라우저 생성...")
        self.gemini_driver = create_driver(headless, "GeminiBot")
        self.gemini_driver.get("https://gemini.google.com")
        time.sleep(3)
        self.gemini = GeminiVerifier(self.gemini_driver, TEMP_DIR, DEBUG_DIR)
        print(f"      ✓ 준비 완료\n")
        
        # Gemini 로그인 확인
        print(f"{'='*70}")
        print(f"Gemini 로그인 확인")
        print(f"{'='*70}\n")
        print(f"Gemini 브라우저에서 로그인 후 Enter를 눌러주세요...")
        print(f"⚠️  중요: Enter 누른 후 브라우저 창을 절대 닫지 마세요!\n")
        input(f"준비되면 Enter...")
        print(f"✓ 계속 진행\n")
        time.sleep(2)
        
        # 채팅 카운터
        self.chat_count = 0
        self.headless = headless
    
    def process_item(self, row: pd.Series, idx: int, total: int) -> dict:
        """
        한 항목을 처음부터 끝까지 처리
        
        Returns:
            결과 딕셔너리
        """
        # 새 채팅 시작 (5개마다)
        if self.chat_count >= MAX_MESSAGES_PER_CHAT and self.chat_count > 0:
            print(f"\n[CHAT] {self.chat_count}개 처리 → 새 채팅 시작\n")
            self.gemini.start_new_chat()
            self.chat_count = 0
            time.sleep(3)
        
        seq = str(row['SELF_IMPORT_SEQ'])
        prdt_nm = row['PRDT_NM']
        mufc_nm = row.get('MUFC_NM', '')
        image_url = row.get('IMAGE_URL', '')
        
        # 날짜 포맷팅
        cret_dtm = row.get('CRET_DTM', '')
        if cret_dtm and len(str(cret_dtm)) == 8:
            date_str = str(cret_dtm)
            date_display = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        else:
            date_display = str(cret_dtm)
        
        # 제품명 짧게
        prdt_display = prdt_nm[:50] + "..." if len(prdt_nm) > 50 else prdt_nm
        
        print(f"\n{'='*70}")
        print(f"[{idx}/{total}] {date_display} | {prdt_display}")
        print(f"SEQ: {seq}")
        print(f"{'='*70}")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 1단계: 이미지 확인 및 다운로드
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        if not image_url or pd.isna(image_url):
            print(f"  [STEP 1] ⊘ NO_IMAGE\n")
            return self._create_result(row, Status.NO_IMAGE)
        
        image_path = download_image(str(image_url), save_dir=IMG_DIR)
        if not image_path:
            print(f"  [STEP 1] ⏬ DOWNLOAD_FAILED\n")
            return self._create_result(row, Status.DOWNLOAD_FAILED)
        
        print(f"  [STEP 1] ✓ 이미지 다운로드 완료")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 2단계: Google 역검색으로 iHerb URL 찾기
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        print(f"  [STEP 2] Google 역검색 중...")
        
        # 세션 클리어 (검색 전에)
        self.google_search.clear_session()
        
        # 검색 수행
        iherb_url = self.google_search.find_iherb_url(image_path)
        
        # 이미지 삭제
        try:
            os.remove(image_path)
        except:
            pass
        
        if not iherb_url:
            print(f"  [STEP 2] ✗ NOT_FOUND\n")
            return self._create_result(row, Status.NOT_FOUND)
        
        print(f"  [STEP 2] ✓ URL 발견: {iherb_url}")
        
        # Google 검색 후 대기
        time.sleep(WAIT_AFTER_GOOGLE)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 3단계: iHerb 페이지 스크래핑
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        print(f"  [STEP 3] iHerb 스크래핑 중...")
        scraped = self.iherb_scraper.scrape_product(iherb_url)
        
        if not scraped['image_url'] or not scraped['product_name'] or not scraped['brand']:
            print(f"  [STEP 3] ⚠ SCRAPE_FAILED\n")
            return self._create_result(
                row,
                Status.SCRAPE_FAILED,
                iherb_url=iherb_url,
                product_code=extract_product_code(iherb_url)
            )
        
        print(f"  [STEP 3] ✓ 스크래핑 완료")
        print(f"           제조사: {scraped['brand']}")
        print(f"           제품명: {scraped['product_name'][:40]}...")
        
        # 스크래핑 후 대기
        time.sleep(WAIT_AFTER_SCRAPE)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 4단계: Gemini 이미지 검증
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        print(f"  [STEP 4] Gemini 검증 중...")
        
        try:
            is_match, reason = self.gemini.verify_images_with_retry(
                hazard_image_url=str(image_url),
                iherb_image_url=scraped['image_url'],
                hazard_name=prdt_nm,
                hazard_brand=mufc_nm,
                iherb_name=scraped['product_name'],
                iherb_brand=scraped['brand'],
                seq=seq
            )
            
            self.chat_count += 1
            
            if is_match:
                print(f"  [STEP 4] ✓ VERIFIED_MATCH")
                print(f"           이유: {reason[:80]}...")
                status = Status.VERIFIED_MATCH
            else:
                print(f"  [STEP 4] ✗ VERIFIED_MISMATCH")
                print(f"           이유: {reason[:80]}...")
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
            
        except Exception as e:
            print(f"  [STEP 4] ⚠ 검증 실패: {e}\n")
            return self._create_result(
                row,
                Status.FOUND,  # URL은 찾았지만 검증 실패
                iherb_url=iherb_url,
                product_code=extract_product_code(iherb_url),
                iherb_brand=scraped['brand'],
                iherb_name=scraped['product_name'],
                iherb_images=json.dumps([scraped['image_url']])
            )
    
    def _create_result(self, row: pd.Series, status: str, **kwargs) -> dict:
        """결과 딕셔너리 생성"""
        return {
            'SELF_IMPORT_SEQ': str(row['SELF_IMPORT_SEQ']),
            'PRDT_NM': row['PRDT_NM'],
            'MUFC_NM': row.get('MUFC_NM', ''),
            'MUFC_CNTRY_NM': row.get('MUFC_CNTRY_NM', ''),
            'INGR_NM_LST': row.get('INGR_NM_LST', ''),
            'CRET_DTM': row.get('CRET_DTM', ''),
            'IMAGE_URL': row.get('IMAGE_URL', ''),
            'IHERB_URL': kwargs.get('iherb_url', ''),
            'product_code': kwargs.get('product_code', ''),
            'IHERB_IMAGE_CODE': extract_iherb_code(kwargs.get('iherb_images', '')) or '',
            'IHERB_제품명': kwargs.get('iherb_name', ''),
            'IHERB_제조사': kwargs.get('iherb_brand', ''),
            'IHERB_PRODUCT_IMAGES': kwargs.get('iherb_images', ''),
            'STATUS': status,
            'SCRAPED_AT': datetime.now().strftime("%Y%m%d%H%M%S"),
            'GEMINI_VERIFIED': str(kwargs.get('gemini_verified', '')),
            'GEMINI_REASON': str(kwargs.get('gemini_reason', '')),
            'VERIFIED_AT': datetime.now().strftime("%Y%m%d%H%M%S") if kwargs.get('gemini_verified') is not None else ''
        }

    
    
    def close(self):
        """브라우저 종료"""
        try:
            self.main_driver.quit()
        except:
            pass
        
        try:
            self.gemini_driver.quit()
        except:
            pass


def save_result_to_unified_csv(result: dict):
    """
    결과를 unified_results.csv에 저장 (append 방식)
    
    Args:
        result: 처리 결과 딕셔너리
    """
    # 기존 데이터 로드
    if UNIFIED_CSV.exists():
        try:
            df_existing = pd.read_csv(UNIFIED_CSV, encoding="utf-8-sig", dtype=str)
        except:
            df_existing = pd.DataFrame()
    else:
        df_existing = pd.DataFrame()
    
    # 새 데이터 추가
    df_new = pd.DataFrame([result])
    
    # 중복 제거 (SELF_IMPORT_SEQ 기준)
    if not df_existing.empty:
        df_existing = df_existing[df_existing['SELF_IMPORT_SEQ'] != result['SELF_IMPORT_SEQ']]
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new
    
    # 컬럼 순서 맞추기
    for col in UNIFIED_COLUMNS:
        if col not in df_combined.columns:
            df_combined[col] = ''
    
    df_combined = df_combined[UNIFIED_COLUMNS]
    
    # 저장
    df_combined.to_csv(UNIFIED_CSV, index=False, encoding="utf-8-sig")


def main():
    parser = argparse.ArgumentParser(description="통합 Phase: 검색 → 스크래핑 → 검증")
    parser.add_argument("--max-items", type=int, help="처리 개수 제한")
    parser.add_argument("--headless", action="store_true", help="헤드리스 모드")
    parser.add_argument("--start-seq", type=str, help="시작 SEQ (이어서 진행)")
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("통합 Phase: 검색 → 스크래핑 → 검증")
    print("="*70 + "\n")
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 1. 데이터 로드
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    print("[INFO] 데이터 로드 중...")
    hazard_df = fetch_hazard_data(limit=None, use_cache=True)
    
    if hazard_df.empty:
        print("[ERROR] 데이터 없음")
        return
    
    print(f"[INFO] 전체 위해식품: {len(hazard_df)}건\n")
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 2. 미처리 항목 필터링
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # 기존 처리 결과 로드
    if UNIFIED_CSV.exists():
        try:
            unified_df = pd.read_csv(UNIFIED_CSV, encoding="utf-8-sig", dtype=str)
            processed_seqs = set(unified_df['SELF_IMPORT_SEQ'].astype(str))
            print(f"[INFO] 기존 처리: {len(processed_seqs)}건")
        except:
            processed_seqs = set()
            print(f"[INFO] 기존 처리: 0건")
    else:
        processed_seqs = set()
        print(f"[INFO] 기존 처리: 0건")
    
    # 미처리만 필터링
    hazard_df['SELF_IMPORT_SEQ'] = hazard_df['SELF_IMPORT_SEQ'].astype(str)
    df_todo = hazard_df[~hazard_df['SELF_IMPORT_SEQ'].isin(processed_seqs)].copy()
    
    # start-seq 옵션
    if args.start_seq:
        start_idx = df_todo[df_todo['SELF_IMPORT_SEQ'] == args.start_seq].index
        if len(start_idx) > 0:
            df_todo = df_todo.loc[start_idx[0]:].copy()
            print(f"[INFO] SEQ {args.start_seq}부터 시작")
    
    # max-items 제한
    if args.max_items:
        df_todo = df_todo.head(args.max_items)
        print(f"[INFO] {args.max_items}건으로 제한")
    
    print(f"[INFO] 미처리 항목: {len(df_todo)}건")
    print(f"[INFO] 처리 대상: {len(df_todo)}건\n")
    
    if df_todo.empty:
        print("[INFO] 처리할 항목 없음")
        return
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 3. Matcher 초기화 및 처리
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    matcher = UnifiedMatcher(headless=args.headless)
    
    try:
        total = len(df_todo)
        
        for idx, (_, row) in enumerate(df_todo.iterrows(), 1):
            # 한 항목 처리 (검색 → 스크래핑 → 검증)
            result = matcher.process_item(row, idx, total)
            
            # 결과 즉시 저장
            save_result_to_unified_csv(result)
            
            # Rate limit 대응 (Gemini 사용한 경우만 15초 대기)
            if result['STATUS'] in [Status.VERIFIED_MATCH, Status.VERIFIED_MISMATCH]:
                time.sleep(WAIT_AFTER_VERIFY)
            else:
                time.sleep(1)  # NOT_FOUND, NO_IMAGE 등은 1초만
        
        # 최종 통계
        print_final_stats()
        
        # 매칭 성공 항목만 추출
        export_matched_only()
        
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] 중단됨")
        print_final_stats()
        export_matched_only()
    
    except Exception as e:
        print(f"\n\n[ERROR] 예외 발생: {e}")
        print_final_stats()
        export_matched_only()
    
    finally:
        matcher.close()


def print_final_stats():
    """최종 통계 출력"""
    if not UNIFIED_CSV.exists():
        return
    
    try:
        df = pd.read_csv(UNIFIED_CSV, encoding="utf-8-sig", dtype=str)
        
        print(f"\n{'='*70}")
        print(f"최종 통계")
        print(f"{'='*70}")
        print(f"총 처리:             {len(df)}건")
        print(f"{'━'*70}")
        
        if 'STATUS' in df.columns:
            status_counts = df['STATUS'].value_counts()
            
            match = status_counts.get(Status.VERIFIED_MATCH, 0)
            mismatch = status_counts.get(Status.VERIFIED_MISMATCH, 0)
            not_found = status_counts.get(Status.NOT_FOUND, 0)
            scrape_failed = status_counts.get(Status.SCRAPE_FAILED, 0)
            no_image = status_counts.get(Status.NO_IMAGE, 0)
            download_failed = status_counts.get(Status.DOWNLOAD_FAILED, 0)
            
            print(f"매칭 성공:           {match}건 ✓")
            print(f"매칭 실패:           {mismatch}건")
            print(f"URL 미발견:          {not_found}건")
            print(f"스크래핑 실패:       {scrape_failed}건")
            print(f"이미지 없음:         {no_image}건")
            print(f"다운로드 실패:       {download_failed}건")
            print(f"{'━'*70}")
            
            total_processed = match + mismatch + not_found + scrape_failed + no_image + download_failed
            if total_processed > 0:
                success_rate = (match / total_processed) * 100
                print(f"성공률:             {success_rate:.1f}%")
        
        print(f"{'='*70}\n")
        
    except Exception as e:
        print(f"[ERROR] 통계 출력 실패: {e}")


def export_matched_only():
    """매칭 성공한 항목만 추출"""
    if not UNIFIED_CSV.exists():
        return
    
    try:
        df = pd.read_csv(UNIFIED_CSV, encoding="utf-8-sig", dtype=str)
        df_matched = df[df['STATUS'] == Status.VERIFIED_MATCH].copy()
        
        if df_matched.empty:
            return
        
        matched_csv = PROJECT_DIR / "csv" / "matched_only.csv"
        df_matched.to_csv(matched_csv, index=False, encoding="utf-8-sig")
        
        print(f"[EXPORT] 매칭 성공 항목만 추출: {len(df_matched)}건")
        print(f"[SAVE] {matched_csv}\n")
        
    except Exception as e:
        print(f"[ERROR] 매칭 항목 추출 실패: {e}")


if __name__ == "__main__":
    main()