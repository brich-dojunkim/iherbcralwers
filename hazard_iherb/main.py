#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
식약처 해외직구 위해식품 × iHerb 자동 매칭 시스템 (Gemini 검증 통합)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. 식약처 OpenAPI(I2715)에서 위해식품 목록 수집
2. 각 제품 이미지 다운로드 (쉼표로 구분된 여러 URL 처리)
3. Google Images 역검색 (이미지 업로드 + 스크롤 트릭 + "iherb" 검색)
4. iHerb 상품 URL 추출
5. [신규] Gemini Vision으로 검색 결과 썸네일 검증
6. CSV 저장
"""

import os
import time
import uuid
import requests
from pathlib import Path
from typing import Optional, List, Dict
from urllib.parse import urlparse, parse_qs, unquote

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ===== [추가] Gemini 검증 =====
try:
    from gemini_verifier import verify_with_gemini
    GEMINI_ENABLED = True
except:
    GEMINI_ENABLED = False
    print("[WARN] Gemini 검증 모듈 없음. 검증 없이 진행")
# =============================


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 설정
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

API_KEY = "ec1d322edd5e4fc2a3f6"
SERVICE_ID = "I2715"
BASE_URL = "http://openapi.foodsafetykorea.go.kr/api"
PAGE_SIZE = 1000

IMG_DIR = Path("hazard_images")
OUTPUT_CSV = "hazard_iherb_matched_final.csv"
API_CACHE_CSV = "api_cache_hazard_data.csv"  # API 응답 캐시 파일


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 식약처 API 데이터 수집
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def fetch_hazard_data(limit: Optional[int] = None, use_cache: bool = True) -> pd.DataFrame:
    """
    식약처 해외직구 위해식품 데이터 수집
    
    Args:
        limit: 수집할 최대 건수 (None = 전체)
        use_cache: True면 캐시 파일 사용, False면 항상 새로 요청
    """
    print("=== 식약처 위해식품 데이터 수집 시작 ===")
    
    # 캐시 파일 확인
    cached_df = pd.DataFrame()
    has_cache = False
    if use_cache and Path(API_CACHE_CSV).exists():
        try:
            cached_df = pd.read_csv(API_CACHE_CSV, encoding="utf-8-sig")
            print(f"[CACHE] 기존 캐시 파일 로드: {len(cached_df)}건")
            has_cache = True
            
            # 캐시가 있으면 신규 데이터만 확인
            latest_seq = cached_df['SELF_IMPORT_SEQ'].astype(str).max() if not cached_df.empty else "0"
            print(f"[CACHE] 최신 SEQ: {latest_seq} - 신규 데이터 확인 중...")
            
        except Exception as e:
            print(f"[WARN] 캐시 로드 실패: {e}")
            cached_df = pd.DataFrame()
            has_cache = False
    
    all_rows = []
    
    # limit이 설정되어 있으면 정확히 그만큼만 요청
    if limit:
        end_idx = limit
        url = f"{BASE_URL}/{API_KEY}/{SERVICE_ID}/json/1/{end_idx}"
        
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
        # 캐시가 있으면 최신 데이터만 API에서 확인
        if has_cache:
            # 최근 7일간 수정된 데이터만 확인 (신규 데이터 추가용)
            from datetime import datetime, timedelta
            
            # 7일 전 날짜 계산
            week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
            print(f"[API] 최근 7일 수정 데이터 확인 중 (기준일: {week_ago})...")
            
            # LAST_UPDT_DTM 파라미터로 필터링
            url_latest = f"{BASE_URL}/{API_KEY}/{SERVICE_ID}/json/1/1000/LAST_UPDT_DTM={week_ago}"
            try:
                resp = requests.get(url_latest, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                if SERVICE_ID in data:
                    latest_rows = data[SERVICE_ID].get('row', [])
                    all_rows.extend(latest_rows)
                    print(f"[API] 최근 수정 데이터 수집: {len(latest_rows)}건")
            except Exception as e:
                print(f"[WARN] 최신 데이터 요청 실패: {e}")
        
        # 캐시가 없으면 전체 데이터 수집
        else:
            print("[API] 전체 데이터 수집 시작...")
            start_idx = 1
            while True:
                end_idx = start_idx + PAGE_SIZE - 1
                url = f"{BASE_URL}/{API_KEY}/{SERVICE_ID}/json/{start_idx}/{end_idx}"
                
                try:
                    resp = requests.get(url, timeout=20)
                    resp.raise_for_status()
                    data = resp.json()
                    
                    if SERVICE_ID not in data:
                        print(f"[ERROR] API 응답 오류: {data}")
                        break
                    
                    body = data[SERVICE_ID]
                    rows = body.get('row', [])
                    
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
    
    # 새로 가져온 데이터를 DataFrame으로 변환
    new_df = pd.DataFrame(all_rows)
    
    # 캐시와 병합
    if has_cache:
        # 캐시가 있는 경우: 신규 데이터를 캐시에 추가
        if not new_df.empty:
            # SELF_IMPORT_SEQ를 문자열로 통일 (타입 불일치 방지)
            cached_df['SELF_IMPORT_SEQ'] = cached_df['SELF_IMPORT_SEQ'].astype(str)
            new_df['SELF_IMPORT_SEQ'] = new_df['SELF_IMPORT_SEQ'].astype(str)
            
            # 중복 제거 (SELF_IMPORT_SEQ 기준)
            combined_df = pd.concat([new_df, cached_df], ignore_index=True)
            before_dedup = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=['SELF_IMPORT_SEQ'], keep='first')
            after_dedup = len(combined_df)
            
            new_count = after_dedup - len(cached_df)
            if new_count > 0:
                print(f"[CACHE] 신규 데이터 {new_count}건 추가 (중복 {before_dedup - after_dedup}건 제거)")
            else:
                print(f"[CACHE] 신규 데이터 없음 (중복 {before_dedup - after_dedup}건 제거)")
            
            df = combined_df
        else:
            # 신규 데이터 없으면 캐시 그대로 사용
            cached_df['SELF_IMPORT_SEQ'] = cached_df['SELF_IMPORT_SEQ'].astype(str)
            df = cached_df
            print("[CACHE] 신규 데이터 없음 - 캐시 데이터 사용")
    else:
        # 캐시가 없는 경우: API에서 가져온 데이터만 사용
        if not new_df.empty:
            new_df['SELF_IMPORT_SEQ'] = new_df['SELF_IMPORT_SEQ'].astype(str)
            df = new_df
        else:
            df = pd.DataFrame()
    
    # 등록일 기준으로 최신순 정렬
    if not df.empty and 'CRET_DTM' in df.columns:
        # CRET_DTM을 정수로 변환 (20251204 > 20251128 비교)
        df['CRET_DTM'] = pd.to_numeric(df['CRET_DTM'], errors='coerce').fillna(0).astype(int)
        df = df.sort_values('CRET_DTM', ascending=False).reset_index(drop=True)
    
    # 캐시 파일 저장
    if not df.empty and use_cache:
        df.to_csv(API_CACHE_CSV, index=False, encoding="utf-8-sig")
        print(f"[CACHE] 캐시 파일 저장: {len(df)}건")
    
    print(f"=== 수집 완료: {len(df)}건 ===\n")
    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. 이미지 다운로드
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def download_image(url: str) -> Optional[Path]:
    """이미지 다운로드 (쉼표로 구분된 여러 URL 처리)"""
    IMG_DIR.mkdir(exist_ok=True)
    
    # IMAGE_URL이 쉼표로 구분된 여러 URL일 경우 첫 번째만 사용
    if ',' in url:
        urls = [u.strip() for u in url.split(',')]
        url = urls[0]
        print(f"  [INFO] 여러 이미지 중 첫 번째 사용")
    
    ext = ".jpg"
    if ".png" in url.lower():
        ext = ".png"
    
    filepath = IMG_DIR / f"{uuid.uuid4().hex}{ext}"
    
    try:
        resp = requests.get(url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.foodsafetykorea.go.kr/'
        })
        resp.raise_for_status()
        
        # 0바이트 체크
        if len(resp.content) == 0:
            print(f"  [ERROR] 이미지 크기 0바이트")
            return None
        
        filepath.write_bytes(resp.content)
        print(f"  [SUCCESS] 이미지 다운로드: {len(resp.content):,} bytes")
        return filepath
        
    except Exception as e:
        print(f"  [ERROR] 이미지 다운로드 실패: {e}")
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. Selenium 드라이버
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
# 4. 구글 이미지 검색 + iHerb URL 추출
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


def find_iherb_url_by_image(driver, image_path: Path) -> tuple:
    """
    Google Images 이미지 업로드 + 스크롤 트릭 + iherb 검색
    
    Returns:
        (iherb_url: str or None, thumbnail_url: str or None)
    """
    wait = WebDriverWait(driver, 25)

    try:
        # 1. 구글 이미지 접속
        driver.get("https://images.google.com/")
        time.sleep(1.5)

        # 2. 카메라 아이콘 클릭
        try:
            camera_btn = wait.until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "div[aria-label*='Search by image' i], div[aria-label*='이미지로 검색' i]"
                ))
            )
            camera_btn.click()
            print("  [Google Images] 카메라 버튼 클릭")
        except Exception as e:
            print(f"  [ERROR] 카메라 버튼 클릭 실패: {e}")
            return None, None

        time.sleep(1)

        # 3. 이미지 업로드
        try:
            file_input = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']"))
            )
            file_input.send_keys(str(image_path.resolve()))
            print("  [Google Images] 이미지 업로드 성공")
        except Exception as e:
            print(f"  [ERROR] 이미지 업로드 실패: {e}")
            return None, None

        # 4. AI 검색 UI 로딩 대기
        time.sleep(1)
        
        # 5. 스크롤 다운/업으로 이미지 자동 제출 (핵심 트릭!)
        print("  [Google Images] 스크롤로 이미지 제출...")
        driver.execute_script("window.scrollTo(0, 800);")
        time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)

        # 6. 검색창에 'iherb' 입력
        print("  [Google Images] 검색창에 'iherb' 입력 중...")
        try:
            # 검색창이 나타날 때까지 대기
            search_box = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='q'], textarea[name='q']"))
            )
            
            # 검색창 클릭 가능할 때까지 대기
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='q'], textarea[name='q']")))
            
            search_box.click()
            search_box.clear()
            search_box.send_keys("iherb")
            search_box.send_keys(Keys.ENTER)
            print("  [Google Images] 'iherb' 검색 실행")
            
        except Exception as e:
            print(f"  [ERROR] 검색창 입력 실패: {e}")
            return None, None

        # 7. 검색 결과 로딩 대기
        try:
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "a")))
            time.sleep(2)  # 모든 링크 로딩 대기
        except:
            time.sleep(3)

        # 8. 검색 결과의 첫 번째 링크만 확인 (가장 관련도 높은 결과)
        try:
            links = driver.find_elements(By.CSS_SELECTOR, "a")
            
            # 첫 번째 유효한 링크 찾기
            first_valid_url = None
            thumbnail_url = None  # ← 추가
            
            for a in links:
                href = a.get_attribute("href")
                if not href:
                    continue
                
                real_url = extract_real_url(href)
                
                # 구글/내부 URL 제외 (더 넓은 범위)
                exclude_domains = [
                    "google.com", "google.co.kr", "gstatic.com", 
                    "youtube.com", "googleusercontent.com",
                    "about/products"  # 구글 제품 소개 페이지
                ]
                if any(x in real_url.lower() for x in exclude_domains):
                    continue
                
                # 너무 짧은 URL이나 상대경로 제외
                if len(real_url) < 10 or not real_url.startswith("http"):
                    continue
                
                # 첫 번째 유효한 링크 발견
                first_valid_url = real_url
                
                # ===== [추가] 썸네일 추출 =====
                try:
                    img = a.find_element(By.TAG_NAME, "img")
                    thumbnail_url = img.get_attribute("src")
                except:
                    pass
                # =============================
                
                break
            
            if not first_valid_url:
                print("  [INFO] 유효한 링크를 찾지 못함")
                return None, None
            
            # 첫 번째 링크가 iHerb인지만 확인 (단순 규칙)
            if "iherb.com" in first_valid_url.lower():
                print(f"  [SUCCESS] 첫 번째 링크가 iHerb: {first_valid_url}")
                return first_valid_url, thumbnail_url
            else:
                print(f"  [INFO] 첫 번째 링크가 iHerb 아님: {first_valid_url[:80]}...")
                return None, None

        except Exception as e:
            print(f"  [ERROR] iHerb 링크 탐색 실패: {e}")
            return None, None
            
    except Exception as e:
        print(f"  [ERROR] 전체 검색 실패: {e}")
        return None, None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 메인 처리 로직
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_hazard_items(
    hazard_df: pd.DataFrame,
    out_csv: str = "hazard_iherb_links.csv",
    max_items: Optional[int] = None,
    headless: bool = False
):
    """위해식품 목록 처리"""
    
    if hazard_df.empty:
        print("[WARN] hazard_df가 비어 있습니다.")
        return

    if max_items is not None:
        hazard_df = hazard_df.head(max_items)

    IMG_DIR.mkdir(exist_ok=True)

    # 기존 결과 파일 로드 (이미 처리된 상품 확인)
    processed_seqs = set()
    results = []
    
    if Path(out_csv).exists():
        try:
            existing_df = pd.read_csv(out_csv, encoding="utf-8-sig")
            processed_seqs = set(existing_df['SELF_IMPORT_SEQ'].astype(str))
            results = existing_df.to_dict('records')
            print(f"[INFO] 기존 결과 파일 로드: {len(processed_seqs)}건 이미 처리됨\n")
        except Exception as e:
            print(f"[WARN] 기존 결과 파일 로드 실패: {e}\n")

    driver = create_driver(headless=headless)

    try:
        total = len(hazard_df)
        processed_count = 0
        skipped_count = 0
        
        for idx, (_, row) in enumerate(hazard_df.iterrows(), start=1):
            self_import_seq = str(row.get("SELF_IMPORT_SEQ", ""))
            prdt_nm = row.get("PRDT_NM", "")
            mufc_nm = row.get("MUFC_NM", "")  # 제조사
            mufc_cntry_nm = row.get("MUFC_CNTRY_NM", "")  # 제조국가
            ingr_nm_lst = row.get("INGR_NM_LST", "")  # 검출성분
            cret_dtm = row.get("CRET_DTM", "")  # 등록일
            image_url = row.get("IMAGE_URL", "")

            print(f"\n{'='*70}")
            print(f"[{idx}/{total}] {prdt_nm}")
            print(f"SEQ: {self_import_seq} | 제조사: {mufc_nm} | 국가: {mufc_cntry_nm}")
            print(f"{'='*70}")

            # 이미 처리된 상품인지 확인
            if self_import_seq in processed_seqs:
                print("  [SKIP] 이미 처리된 상품")
                skipped_count += 1
                continue

            if not image_url or (isinstance(image_url, float) and pd.isna(image_url)):
                print("  [SKIP] 이미지 URL 없음")
                results.append({
                    "SELF_IMPORT_SEQ": self_import_seq,
                    "PRDT_NM": prdt_nm,
                    "MUFC_NM": mufc_nm,
                    "MUFC_CNTRY_NM": mufc_cntry_nm,
                    "INGR_NM_LST": ingr_nm_lst,
                    "CRET_DTM": cret_dtm,
                    "IMAGE_URL_MFDS": image_url,
                    "IHERB_URL": None,
                    "STATUS": "NO_IMAGE",
                    "GEMINI_VERIFIED": None,
                    "GEMINI_REASON": None
                })
                processed_seqs.add(self_import_seq)
                
                # 즉시 저장
                out_df = pd.DataFrame(results)
                out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
                continue

            # 이미지 다운로드
            image_path = download_image(str(image_url))
            if not image_path:
                results.append({
                    "SELF_IMPORT_SEQ": self_import_seq,
                    "PRDT_NM": prdt_nm,
                    "MUFC_NM": mufc_nm,
                    "MUFC_CNTRY_NM": mufc_cntry_nm,
                    "INGR_NM_LST": ingr_nm_lst,
                    "CRET_DTM": cret_dtm,
                    "IMAGE_URL_MFDS": image_url,
                    "IHERB_URL": None,
                    "STATUS": "DOWNLOAD_FAILED",
                    "GEMINI_VERIFIED": None,
                    "GEMINI_REASON": None
                })
                processed_seqs.add(self_import_seq)
                
                # 즉시 저장
                out_df = pd.DataFrame(results)
                out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
                continue

            # 구글 이미지 검색 + iherb URL 찾기
            iherb_url, thumbnail_url = find_iherb_url_by_image(driver, image_path)

            # ===== [추가] Gemini 검증 =====
            gemini_verified = None
            gemini_reason = None
            
            if iherb_url and thumbnail_url and GEMINI_ENABLED:
                print("  [GEMINI] 검증 시작...")
                is_match, reason = verify_with_gemini(
                    hazard_image_path=str(image_path),
                    search_thumbnail_url=thumbnail_url,
                    hazard_name=prdt_nm,
                    hazard_brand=mufc_nm
                )
                gemini_verified = is_match
                gemini_reason = reason
                print(f"  [GEMINI] 결과: {is_match} - {reason[:50]}...")
            # =============================

            # 결과 저장
            results.append({
                "SELF_IMPORT_SEQ": self_import_seq,
                "PRDT_NM": prdt_nm,
                "MUFC_NM": mufc_nm,
                "MUFC_CNTRY_NM": mufc_cntry_nm,
                "INGR_NM_LST": ingr_nm_lst,
                "CRET_DTM": cret_dtm,
                "IMAGE_URL_MFDS": image_url,
                "IHERB_URL": iherb_url,
                "STATUS": "FOUND" if iherb_url else "NOT_FOUND",
                "GEMINI_VERIFIED": gemini_verified,  # ← 추가
                "GEMINI_REASON": gemini_reason       # ← 추가
            })
            processed_seqs.add(self_import_seq)
            processed_count += 1

            # 이미지 파일 삭제
            try:
                os.remove(image_path)
            except Exception:
                pass

            # 즉시 저장 (매 상품 처리마다)
            out_df = pd.DataFrame(results)
            out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
            print(f"  [SAVE] 결과 저장 완료 (총 {len(results)}건)")

            # 다음 상품 처리 전 최소 대기
            time.sleep(1)

        # 최종 저장
        out_df = pd.DataFrame(results)
        out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        
        print(f"\n{'='*70}")
        print(f"[DONE] 처리 완료!")
        print(f"총 처리: {processed_count}건 (스킵: {skipped_count}건)")
        print(f"iHerb 발견: {len(out_df[out_df['STATUS'] == 'FOUND'])}건")
        if GEMINI_ENABLED:
            verified = len(out_df[out_df['GEMINI_VERIFIED'] == True])
            print(f"Gemini 검증 완료: {verified}건")
        print(f"결과 파일: {out_csv}")
        print(f"{'='*70}")

    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] 사용자 중단")
        if results:
            temp_df = pd.DataFrame(results)
            temp_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
            print(f"[SAVE] 중단 시점까지 결과 저장: {out_csv}")
    
    finally:
        try:
            driver.quit()
        except Exception:
            pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. 메인 실행
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    """메인 실행 함수"""
    
    print("\n" + "="*70)
    print("식약처 해외직구 위해식품 × iHerb 자동 매칭 시스템")
    if GEMINI_ENABLED:
        print("(Gemini Vision 검증 활성화)")
    print("="*70 + "\n")
    
    # 1. 해외직구 위해식품 목록 수집 (전체)
    hazard_df = fetch_hazard_data(limit=None)  # None = 전체 데이터

    if hazard_df.empty:
        print("[ERROR] 데이터를 가져오지 못했습니다.")
        return

    # 2. 각 상품에 대해 iHerb URL 수집
    # - 기존 CSV가 있으면 이미 처리된 상품은 스킵
    # - 매 처리마다 자동 저장
    process_hazard_items(
        hazard_df,
        out_csv="hazard_iherb_matched_final.csv",
        max_items=None,  # None = 전체 처리
        headless=False   # False = 브라우저 표시, True = 백그라운드
    )


if __name__ == "__main__":
    main()