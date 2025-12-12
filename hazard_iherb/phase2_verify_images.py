#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2: 이미지 검증
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. CSV에서 STATUS='FOUND' 필터링
2. iHerb 상세페이지 크롤링
3. Gemini Vision으로 이미지 비교
4. 결과 업데이트
"""

import os
import json
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import undetected_chromedriver as uc

from config import (
    OUTPUT_CSV, IMG_DIR, Status, CSV_COLUMNS, GEMINI_API_KEY
)
from iherb_scraper import IHerbScraper

# Gemini 설정
if GEMINI_API_KEY:
    import google.generativeai as genai
    genai.configure(api_key=GEMINI_API_KEY)
    GEMINI_ENABLED = True
else:
    GEMINI_ENABLED = False
    print("[WARN] GEMINI_API_KEY 없음 - 검증 비활성화")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Gemini 검증
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def verify_with_gemini(
    hazard_image_url: str,
    iherb_image_url: str,
    hazard_name: str,
    hazard_brand: str,
    iherb_name: str,
    iherb_brand: str
) -> tuple:
    """
    Gemini Vision으로 두 이미지 URL 비교
    
    Returns:
        (is_match: bool, reason: str)
    """
    if not GEMINI_ENABLED:
        return False, "API key not configured"
    
    try:
        import base64
        import requests
        
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        # 이미지 다운로드 및 base64 인코딩
        def download_and_encode(url: str) -> tuple:
            """이미지 다운로드 후 base64로 인코딩"""
            try:
                resp = requests.get(url, timeout=10, headers={
                    'User-Agent': 'Mozilla/5.0'
                })
                resp.raise_for_status()
                
                # MIME type 결정
                content_type = resp.headers.get('content-type', 'image/jpeg')
                if 'image' not in content_type:
                    content_type = 'image/jpeg'
                
                # base64 인코딩
                encoded = base64.b64encode(resp.content).decode('utf-8')
                return content_type, encoded
            except Exception as e:
                raise Exception(f"Image download failed: {e}")
        
        # 두 이미지 다운로드
        hazard_mime, hazard_data = download_and_encode(hazard_image_url)
        iherb_mime, iherb_data = download_and_encode(iherb_image_url)
        
        prompt = f"""Compare these two product images and metadata:

IMAGE 1 (Hazard Product):
- Name: {hazard_name}
- Brand: {hazard_brand}

IMAGE 2 (iHerb Product):
- Name: {iherb_name}
- Brand: {iherb_brand}

Are they the SAME product?
- Same brand name (consider variations like "NOW Foods" vs "NOW")?
- Same product name (consider translations)?
- Similar packaging/design?

Answer format: Start with YES or NO, then explain briefly.
Example: "YES: Same brand 'NOW Foods' and product name visible"
Example: "NO: Different brand logos - Jarrow vs NOW"
"""
        
        # Gemini API 호출 - base64 데이터 사용
        response = model.generate_content([
            prompt,
            {
                'mime_type': hazard_mime,
                'data': hazard_data
            },
            {
                'mime_type': iherb_mime,
                'data': iherb_data
            }
        ])
        
        result = response.text.strip()
        first_word = result.split()[0].upper().rstrip(':,.')
        
        if first_word == 'YES':
            is_match = True
        elif first_word == 'NO':
            is_match = False
        else:
            result_lower = result.lower()
            is_match = result_lower.startswith('yes')
        
        return is_match, result
        
    except Exception as e:
        error_msg = str(e)
        print(f"  [GEMINI ERROR] {error_msg}")
        
        # API 할당량/속도 제한 에러 감지
        error_keywords = [
            '429',
            'quota',
            'rate limit',
            'resource_exhausted',
            'too many requests',
            'quota exceeded'
        ]
        
        if any(keyword in error_msg.lower() for keyword in error_keywords):
            print(f"\n{'='*70}")
            print(f"[CRITICAL] API 할당량 초과 또는 Rate Limit 감지")
            print(f"에러 내용: {error_msg[:200]}")
            print(f"")
            print(f"해결 방법:")
            print(f"1. 잠시 대기 후 재실행 (자동으로 이어서 실행됩니다)")
            print(f"2. Gemini API 할당량 확인: https://aistudio.google.com/")
            print(f"3. 유료 플랜 업그레이드 고려")
            print(f"{'='*70}\n")
            raise SystemExit("API quota/rate limit - 프로그램 종료")
        
        return False, error_msg


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Selenium 드라이버
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def create_driver(headless: bool = False):
    """undetected-chromedriver 생성"""
    options = uc.ChromeOptions()
    
    if headless:
        options.add_argument("--headless=new")
    
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1400,1000")
    options.add_argument("--lang=en-US")
    
    driver = uc.Chrome(options=options, version_main=None)
    driver.set_page_load_timeout(60)
    
    return driver


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메인 처리
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_verification(
    input_csv: str = OUTPUT_CSV,
    headless: bool = False,
    revalidate_all: bool = False,
    specific_seq: Optional[str] = None
):
    """이미지 검증 처리"""
    
    if not Path(input_csv).exists():
        print(f"[ERROR] CSV 파일 없음: {input_csv}")
        return
    
    # CSV 로드
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    print(f"[INFO] 전체 데이터: {len(df)}건")
    
    # 필터링
    if specific_seq:
        df = df[df['SELF_IMPORT_SEQ'].astype(str) == str(specific_seq)]
        print(f"[INFO] 특정 SEQ 필터링: {len(df)}건")
    elif revalidate_all:
        df = df[df['STATUS'] == Status.FOUND]
        print(f"[INFO] 전체 재검증: {len(df)}건")
    else:
        # 미검증 항목만
        df = df[
            (df['STATUS'] == Status.FOUND) &
            (df['GEMINI_VERIFIED'].isna())
        ]
        print(f"[INFO] 미검증 항목: {len(df)}건")
    
    if df.empty:
        print("[INFO] 검증할 항목 없음")
        return
    
    driver = create_driver(headless=headless)
    scraper = IHerbScraper(driver)
    
    try:
        total = len(df)
        processed = 0
        success = 0
        
        for idx, row in df.iterrows():
            seq = str(row['SELF_IMPORT_SEQ'])
            prdt_nm = row['PRDT_NM']
            mufc_nm = row['MUFC_NM']
            iherb_url = row['IHERB_URL']
            hazard_image_url = row['IMAGE_URL_MFDS']
            
            # 쉼표로 구분된 여러 URL 중 첫 번째만 사용
            if pd.notna(hazard_image_url) and ',' in str(hazard_image_url):
                hazard_image_url = str(hazard_image_url).split(',')[0].strip()
                print(f"  [INFO] 여러 이미지 중 첫 번째 URL 사용")
            
            print(f"\n{'='*70}")
            print(f"[{processed+1}/{total}] {prdt_nm}")
            print(f"SEQ: {seq}")
            print(f"iHerb: {iherb_url}")
            print(f"{'='*70}")
            
            # iHerb 페이지 크롤링
            scraped_data = scraper.scrape_product_page(iherb_url)
            
            if not scraped_data:
                print("  [ERROR] 크롤링 실패")
                df.at[idx, 'STATUS'] = Status.VERIFICATION_FAILED
                df.at[idx, 'GEMINI_REASON'] = str("Scraping failed")
                processed += 1
                save_dataframe(df, input_csv)
                continue
            
            # 이미지 URL 저장
            images_json = json.dumps(scraped_data['all_images'])
            df.at[idx, 'IHERB_PRODUCT_IMAGES'] = str(images_json)
            
            # Gemini 검증
            comparison_image = scraper.get_best_comparison_image(scraped_data)
            
            if not comparison_image:
                print("  [ERROR] 비교 이미지 없음")
                df.at[idx, 'STATUS'] = Status.VERIFICATION_FAILED
                df.at[idx, 'GEMINI_REASON'] = str("No comparison image")
                processed += 1
                save_dataframe(df, input_csv)
                continue
            
            print(f"  [VERIFY] 식약처: {hazard_image_url[:80]}...")
            print(f"  [VERIFY] iHerb: {comparison_image[:80]}...")
            
            is_match, reason = verify_with_gemini(
                hazard_image_url=hazard_image_url,
                iherb_image_url=comparison_image,
                hazard_name=prdt_nm,
                hazard_brand=mufc_nm,
                iherb_name=scraped_data['product_name'],
                iherb_brand=scraped_data['brand']
            )
            
            # 결과 저장 - 모든 값을 명시적으로 타입 변환
            df.at[idx, 'GEMINI_VERIFIED'] = bool(is_match)
            df.at[idx, 'GEMINI_REASON'] = str(reason)
            df.at[idx, 'VERIFIED_DTM'] = str(datetime.now().strftime("%Y%m%d%H%M%S"))
            
            if is_match:
                df.at[idx, 'STATUS'] = Status.VERIFIED_MATCH
                print(f"  [SUCCESS] ✓ 매칭 성공")
                success += 1
            else:
                df.at[idx, 'STATUS'] = Status.VERIFIED_MISMATCH
                print(f"  [INFO] ✗ 매칭 실패")
            
            print(f"  [REASON] {reason[:100]}...")
            
            processed += 1
            save_dataframe(df, input_csv)
            
            time.sleep(2)  # Rate limiting
        
        print(f"\n{'='*70}")
        print(f"[DONE] 검증 완료!")
        print(f"총 처리: {processed}건")
        print(f"매칭 성공: {success}건")
        print(f"매칭 실패: {processed - success}건")
        print(f"{'='*70}")
    
    except SystemExit as e:
        # API quota/rate limit 에러
        print(f"\n[API LIMIT] {e}")
        save_dataframe(df, input_csv)
        print(f"현재까지 처리: {processed}건 저장됨")
        raise  # 종료
    
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] 중단")
        save_dataframe(df, input_csv)
    
    finally:
        try:
            driver.quit()
        except:
            pass


def save_dataframe(df: pd.DataFrame, output_path: str):
    """DataFrame 저장 - 타입 명시적 변환"""
    # 컬럼 순서 정렬
    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = None
    
    # 타입 명시적 변환 (경고 방지)
    df['IHERB_PRODUCT_IMAGES'] = df['IHERB_PRODUCT_IMAGES'].astype(str)
    df['GEMINI_VERIFIED'] = df['GEMINI_VERIFIED'].astype(object)
    df['GEMINI_REASON'] = df['GEMINI_REASON'].astype(str)
    df['VERIFIED_DTM'] = df['VERIFIED_DTM'].astype(str)
    
    df = df[CSV_COLUMNS]
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"  [SAVE] 저장 완료")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    parser = argparse.ArgumentParser(description="Phase 2: iHerb 이미지 검증")
    parser.add_argument(
        "--csv",
        default=OUTPUT_CSV,
        help="입력 CSV 파일 경로"
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="헤드리스 모드"
    )
    parser.add_argument(
        "--revalidate-all",
        action="store_true",
        help="전체 재검증 (STATUS=FOUND 모두)"
    )
    parser.add_argument(
        "--seq",
        type=str,
        help="특정 SELF_IMPORT_SEQ만 검증"
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("Phase 2: iHerb 이미지 검증")
    print("="*70 + "\n")
    
    if not GEMINI_ENABLED:
        print("[ERROR] GEMINI_API_KEY 설정 필요")
        print("export GEMINI_API_KEY='your-key'")
        return
    
    process_verification(
        input_csv=args.csv,
        headless=args.headless,
        revalidate_all=args.revalidate_all,
        specific_seq=args.seq
    )


if __name__ == "__main__":
    main()