#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 1: iHerb URL 수집
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. 식약처 API에서 위해식품 목록 수집
2. 이미지 다운로드
3. Google Images 역검색
4. iHerb URL 추출 (Gemini 검증 제외)
5. CSV 저장
"""

import os
import time
import uuid
import requests
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs, unquote

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config import (
    MFDS_API_KEY, SERVICE_ID, BASE_URL, PAGE_SIZE,
    IMG_DIR, OUTPUT_CSV, API_CACHE_CSV, Status, CSV_COLUMNS
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API 데이터 수집
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def fetch_hazard_data(limit: Optional[int] = None, use_cache: bool = True) -> pd.DataFrame:
    """식약처 위해식품 데이터 수집"""
    print("=== 식약처 위해식품 데이터 수집 시작 ===")
    
    cached_df = pd.DataFrame()
    has_cache = False
    
    if use_cache and Path(API_CACHE_CSV).exists():
        try:
            cached_df = pd.read_csv(API_CACHE_CSV, encoding="utf-8-sig")
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
        df.to_csv(API_CACHE_CSV, index=False, encoding="utf-8-sig")
        print(f"[CACHE] 캐시 저장: {len(df)}건")
    
    print(f"=== 수집 완료: {len(df)}건 ===\n")
    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 이미지 다운로드
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def download_image(url: str) -> Optional[Path]:
    """이미지 다운로드"""
    IMG_DIR.mkdir(exist_ok=True)
    
    if ',' in url:
        urls = [u.strip() for u in url.split(',')]
        url = urls[0]
    
    ext = ".jpg"
    if ".png" in url.lower():
        ext = ".png"
    
    filepath = IMG_DIR / f"{uuid.uuid4().hex}{ext}"
    
    try:
        resp = requests.get(url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://www.foodsafetykorea.go.kr/'
        })
        resp.raise_for_status()
        
        if len(resp.content) == 0:
            return None
        
        filepath.write_bytes(resp.content)
        return filepath
        
    except Exception as e:
        print(f"  [ERROR] 다운로드 실패: {e}")
        return None


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
# Google 이미지 검색
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def extract_real_url(google_redirect_url: str) -> str:
    """구글 redirect URL에서 실제 URL 추출"""
    try:
        if "/url?q=" in google_redirect_url:
            parsed = urlparse(google_redirect_url)
            params = parse_qs(parsed.query)
            if 'q' in params:
                return unquote(params['q'][0])
        return google_redirect_url
    except:
        return google_redirect_url


def find_iherb_url_by_image(driver, image_path: Path) -> Optional[str]:
    """Google Images 역검색으로 iHerb URL 찾기"""
    wait = WebDriverWait(driver, 25)

    try:
        driver.get("https://images.google.com/")
        time.sleep(1.5)

        # 카메라 버튼
        try:
            camera_btn = wait.until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "div[aria-label*='Search by image' i], div[aria-label*='이미지로 검색' i]"
                ))
            )
            camera_btn.click()
        except:
            return None

        time.sleep(1)

        # 이미지 업로드
        try:
            file_input = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']"))
            )
            file_input.send_keys(str(image_path.resolve()))
        except:
            return None

        time.sleep(1)
        
        # 스크롤 트릭
        driver.execute_script("window.scrollTo(0, 800);")
        time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)

        # 검색창에 'iherb' 입력
        try:
            search_box = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='q'], textarea[name='q']"))
            )
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='q'], textarea[name='q']")))
            search_box.click()
            search_box.clear()
            search_box.send_keys("iherb")
            search_box.send_keys(Keys.ENTER)
        except:
            return None

        # 결과 대기
        try:
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "a")))
            time.sleep(2)
        except:
            time.sleep(3)

        # 첫 번째 유효한 iHerb 링크 찾기
        links = driver.find_elements(By.CSS_SELECTOR, "a")
        
        for a in links:
            href = a.get_attribute("href")
            if not href:
                continue
            
            real_url = extract_real_url(href)
            
            # 제외 도메인
            exclude = [
                "google.com", "google.co.kr", "gstatic.com",
                "youtube.com", "googleusercontent.com", "about/products"
            ]
            if any(x in real_url.lower() for x in exclude):
                continue
            
            if len(real_url) < 10 or not real_url.startswith("http"):
                continue
            
            # iHerb URL 발견
            if "iherb.com" in real_url.lower():
                print(f"  [SUCCESS] iHerb 발견: {real_url}")
                return real_url
        
        return None
            
    except Exception as e:
        print(f"  [ERROR] 검색 실패: {e}")
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메인 처리
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_hazard_items(
    hazard_df: pd.DataFrame,
    out_csv: str = OUTPUT_CSV,
    max_items: Optional[int] = None,
    headless: bool = False
):
    """위해식품 URL 수집"""
    
    if hazard_df.empty:
        print("[WARN] 데이터 없음")
        return

    if max_items is not None:
        hazard_df = hazard_df.head(max_items)

    IMG_DIR.mkdir(exist_ok=True)

    # 기존 결과 로드
    processed_seqs = set()
    results = []
    
    if Path(out_csv).exists():
        try:
            existing_df = pd.read_csv(out_csv, encoding="utf-8-sig")
            processed_seqs = set(existing_df['SELF_IMPORT_SEQ'].astype(str))
            results = existing_df.to_dict('records')
            print(f"[INFO] 기존 결과: {len(processed_seqs)}건\n")
        except Exception as e:
            print(f"[WARN] 기존 결과 로드 실패: {e}\n")

    driver = create_driver(headless=headless)

    try:
        total = len(hazard_df)
        processed_count = 0
        skipped_count = 0
        
        for idx, (_, row) in enumerate(hazard_df.iterrows(), start=1):
            seq = str(row.get("SELF_IMPORT_SEQ", ""))
            prdt_nm = row.get("PRDT_NM", "")
            mufc_nm = row.get("MUFC_NM", "")
            mufc_cntry_nm = row.get("MUFC_CNTRY_NM", "")
            ingr_nm_lst = row.get("INGR_NM_LST", "")
            cret_dtm = row.get("CRET_DTM", "")
            image_url = row.get("IMAGE_URL", "")

            print(f"\n{'='*70}")
            print(f"[{idx}/{total}] {prdt_nm}")
            print(f"SEQ: {seq}")
            print(f"{'='*70}")

            if seq in processed_seqs:
                print("  [SKIP] 이미 처리됨")
                skipped_count += 1
                continue

            # 이미지 확인
            if not image_url or (isinstance(image_url, float) and pd.isna(image_url)):
                print("  [SKIP] 이미지 없음")
                results.append({
                    "SELF_IMPORT_SEQ": seq,
                    "PRDT_NM": prdt_nm,
                    "MUFC_NM": mufc_nm,
                    "MUFC_CNTRY_NM": mufc_cntry_nm,
                    "INGR_NM_LST": ingr_nm_lst,
                    "CRET_DTM": cret_dtm,
                    "IMAGE_URL_MFDS": image_url,
                    "IHERB_URL": None,
                    "STATUS": Status.NO_IMAGE,
                    "IHERB_PRODUCT_IMAGES": None,
                    "GEMINI_VERIFIED": None,
                    "GEMINI_REASON": None,
                    "VERIFIED_DTM": None
                })
                processed_seqs.add(seq)
                save_results(results, out_csv)
                continue

            # 이미지 다운로드
            image_path = download_image(str(image_url))
            if not image_path:
                results.append({
                    "SELF_IMPORT_SEQ": seq,
                    "PRDT_NM": prdt_nm,
                    "MUFC_NM": mufc_nm,
                    "MUFC_CNTRY_NM": mufc_cntry_nm,
                    "INGR_NM_LST": ingr_nm_lst,
                    "CRET_DTM": cret_dtm,
                    "IMAGE_URL_MFDS": image_url,
                    "IHERB_URL": None,
                    "STATUS": Status.DOWNLOAD_FAILED,
                    "IHERB_PRODUCT_IMAGES": None,
                    "GEMINI_VERIFIED": None,
                    "GEMINI_REASON": None,
                    "VERIFIED_DTM": None
                })
                processed_seqs.add(seq)
                save_results(results, out_csv)
                continue

            # Google 이미지 검색
            iherb_url = find_iherb_url_by_image(driver, image_path)

            results.append({
                "SELF_IMPORT_SEQ": seq,
                "PRDT_NM": prdt_nm,
                "MUFC_NM": mufc_nm,
                "MUFC_CNTRY_NM": mufc_cntry_nm,
                "INGR_NM_LST": ingr_nm_lst,
                "CRET_DTM": cret_dtm,
                "IMAGE_URL_MFDS": image_url,
                "IHERB_URL": iherb_url,
                "STATUS": Status.FOUND if iherb_url else Status.NOT_FOUND,
                "IHERB_PRODUCT_IMAGES": None,
                "GEMINI_VERIFIED": None,
                "GEMINI_REASON": None,
                "VERIFIED_DTM": None
            })
            processed_seqs.add(seq)
            processed_count += 1

            # 이미지 삭제
            try:
                os.remove(image_path)
            except:
                pass

            save_results(results, out_csv)
            time.sleep(1)

        print(f"\n{'='*70}")
        print(f"[DONE] 처리 완료!")
        print(f"총 처리: {processed_count}건 (스킵: {skipped_count}건)")
        print(f"iHerb 발견: {sum(1 for r in results if r['STATUS'] == Status.FOUND)}건")
        print(f"{'='*70}")

    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] 중단")
        if results:
            save_results(results, out_csv)
    
    finally:
        try:
            driver.quit()
        except:
            pass


def save_results(results, out_csv):
    """결과 저장"""
    df = pd.DataFrame(results)
    # 컬럼 순서 정렬
    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[CSV_COLUMNS]
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메인
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    print("\n" + "="*70)
    print("Phase 1: iHerb URL 수집")
    print("="*70 + "\n")
    
    hazard_df = fetch_hazard_data(limit=None)

    if hazard_df.empty:
        print("[ERROR] 데이터 없음")
        return

    process_hazard_items(
        hazard_df,
        out_csv=OUTPUT_CSV,
        max_items=None,
        headless=False
    )


if __name__ == "__main__":
    main()
