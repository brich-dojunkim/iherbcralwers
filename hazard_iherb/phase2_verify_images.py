#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2: Gemini Web 이미지 검증 (디버깅 버전)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HTML 덤프 및 스크린샷을 통한 디버깅 지원
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
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 설정
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

OUTPUT_CSV = "/Users/brich/Desktop/iherb_price/hazard_iherb/csv/hazard_iherb_matched_final.csv"
DEBUG_DIR = Path("/Users/brich/Desktop/iherb_price/hazard_iherb/debug")
GEMINI_WEB_URL = "https://gemini.google.com/app"

# 디버그 디렉토리 생성
DEBUG_DIR.mkdir(exist_ok=True)

class Status:
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    NO_IMAGE = "NO_IMAGE"
    DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
    VERIFIED_MATCH = "VERIFIED_MATCH"
    VERIFIED_MISMATCH = "VERIFIED_MISMATCH"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"

CSV_COLUMNS = [
    'SELF_IMPORT_SEQ', 'PRDT_NM', 'MUFC_NM', 'MUFC_CNTRY_NM', 
    'INGR_NM_LST', 'CRET_DTM', 'IMAGE_URL_MFDS', 'IHERB_URL', 
    'STATUS', 'IHERB_PRODUCT_IMAGES', 'GEMINI_VERIFIED', 
    'GEMINI_REASON', 'VERIFIED_DTM'
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 디버깅 유틸리티
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def save_debug_info(driver, step_name: str, seq: str = "unknown"):
    """디버그 정보 저장 (HTML + 스크린샷)"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = f"{timestamp}_{seq}_{step_name}"
    
    # HTML 저장
    try:
        html_path = DEBUG_DIR / f"{prefix}.html"
        html_content = driver.page_source
        html_path.write_text(html_content, encoding='utf-8')
        print(f"  [DEBUG] HTML 저장: {html_path.name}")
    except Exception as e:
        print(f"  [DEBUG ERROR] HTML 저장 실패: {e}")
    
    # 스크린샷 저장
    try:
        screenshot_path = DEBUG_DIR / f"{prefix}.png"
        driver.save_screenshot(str(screenshot_path))
        print(f"  [DEBUG] 스크린샷 저장: {screenshot_path.name}")
    except Exception as e:
        print(f"  [DEBUG ERROR] 스크린샷 저장 실패: {e}")
    
    # 페이지 정보 저장
    try:
        info_path = DEBUG_DIR / f"{prefix}_info.txt"
        info = f"""
=== 페이지 정보 ===
URL: {driver.current_url}
Title: {driver.title}
Window Size: {driver.get_window_size()}
Cookies: {len(driver.get_cookies())} 개

=== 모든 Input 요소 ===
"""
        # 모든 input 요소 찾기
        inputs = driver.find_elements(By.TAG_NAME, "input")
        for i, inp in enumerate(inputs):
            try:
                info += f"\nInput {i+1}:"
                info += f"\n  - type: {inp.get_attribute('type')}"
                info += f"\n  - id: {inp.get_attribute('id')}"
                info += f"\n  - class: {inp.get_attribute('class')}"
                info += f"\n  - placeholder: {inp.get_attribute('placeholder')}"
                info += f"\n  - name: {inp.get_attribute('name')}"
                info += f"\n  - aria-label: {inp.get_attribute('aria-label')}"
            except:
                pass
        
        info += "\n\n=== 모든 Textarea 요소 ===\n"
        textareas = driver.find_elements(By.TAG_NAME, "textarea")
        for i, ta in enumerate(textareas):
            try:
                info += f"\nTextarea {i+1}:"
                info += f"\n  - id: {ta.get_attribute('id')}"
                info += f"\n  - class: {ta.get_attribute('class')}"
                info += f"\n  - placeholder: {ta.get_attribute('placeholder')}"
                info += f"\n  - aria-label: {ta.get_attribute('aria-label')}"
            except:
                pass
        
        info += "\n\n=== 모든 Rich-Textarea 요소 ===\n"
        rich_textareas = driver.find_elements(By.TAG_NAME, "rich-textarea")
        for i, rt in enumerate(rich_textareas):
            try:
                info += f"\nRich-Textarea {i+1}:"
                info += f"\n  - id: {rt.get_attribute('id')}"
                info += f"\n  - class: {rt.get_attribute('class')}"
                info += f"\n  - placeholder: {rt.get_attribute('placeholder')}"
            except:
                pass
        
        info += "\n\n=== 모든 Button 요소 ===\n"
        buttons = driver.find_elements(By.TAG_NAME, "button")
        for i, btn in enumerate(buttons):
            try:
                info += f"\nButton {i+1}:"
                info += f"\n  - type: {btn.get_attribute('type')}"
                info += f"\n  - class: {btn.get_attribute('class')}"
                info += f"\n  - aria-label: {btn.get_attribute('aria-label')}"
                info += f"\n  - text: {btn.text[:50]}"
            except:
                pass
        
        info_path.write_text(info, encoding='utf-8')
        print(f"  [DEBUG] 페이지 정보 저장: {info_path.name}")
    except Exception as e:
        print(f"  [DEBUG ERROR] 페이지 정보 저장 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Selenium 드라이버
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def create_driver(headless: bool = False):
    """undetected-chromedriver 생성"""
    options = uc.ChromeOptions()
    
    # Chrome 프로필 경로 (로그인 정보 저장)
    profile_dir = Path.home() / "Library/Application Support/Google/Chrome/GeminiBot"
    profile_dir.mkdir(parents=True, exist_ok=True)
    
    options.add_argument(f"--user-data-dir={profile_dir}")
    
    if headless:
        options.add_argument("--headless=new")
    
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1400,1000")
    
    driver = uc.Chrome(options=options, version_main=None)
    driver.set_page_load_timeout(60)
    
    return driver


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# iHerb 스크래퍼
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def scrape_iherb_simple(driver, url: str) -> tuple:
    """
    iHerb에서 메인 이미지와 제품명, 브랜드 추출
    
    Returns:
        (image_url, product_name, brand)
    """
    try:
        print(f"  [SCRAPER] iHerb 방문 중...")
        driver.get(url)
        time.sleep(2)
        
        # 메인 이미지
        image_url = None
        try:
            img = driver.find_element(By.CSS_SELECTOR, "#iherb-product-image")
            src = img.get_attribute("src")
            if src and "cloudinary.images-iherb.com" in src:
                image_url = src.replace("/v/", "/l/")
        except:
            pass
        
        # 제품명
        product_name = "Unknown"
        try:
            element = driver.find_element(By.CSS_SELECTOR, "h1#name")
            product_name = element.text.strip()
        except:
            pass
        
        # 브랜드
        brand = "Unknown"
        try:
            element = driver.find_element(By.CSS_SELECTOR, "#brand a")
            brand = element.text.strip()
        except:
            pass
        
        if image_url:
            print(f"  [SCRAPER] ✓ 추출 완료: {brand} - {product_name}")
        
        return image_url, product_name, brand
        
    except Exception as e:
        print(f"  [SCRAPER ERROR] {e}")
        return None, "Unknown", "Unknown"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Gemini Web 인터페이스
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class GeminiWebInterface:
    """Gemini 웹 인터페이스 자동화"""
    
    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(driver, 15)
    
    def verify_images(
        self,
        hazard_image_url: str,
        iherb_image_url: str,
        hazard_name: str,
        hazard_brand: str,
        iherb_name: str,
        iherb_brand: str,
        seq: str = "unknown"
    ) -> tuple:
        """
        Gemini로 두 이미지 비교
        
        Returns:
            (is_match: bool, reason: str)
        """
        try:
            prompt = f"""Compare these two product images:

IMAGE 1 (Korean Hazard Product):
{hazard_image_url}
- Product: {hazard_name}
- Brand: {hazard_brand}

IMAGE 2 (iHerb Product):
{iherb_image_url}
- Product: {iherb_name}
- Brand: {iherb_brand}

Are they the SAME product? Answer with YES or NO first, then explain briefly.
Example: "YES: Both show NOW Foods Vitamin D3"
Example: "NO: Different brands - Jarrow vs NOW"

Your answer:"""
            
            # 디버그: 입력 전 상태 저장
            print(f"  [DEBUG] 입력 전 페이지 상태 저장 중...")
            save_debug_info(self.driver, "before_input", seq)
            
            # 입력창 찾기
            print(f"  [VERIFY] 입력창 찾는 중...")
            input_box = self._find_input_box()
            
            if not input_box:
                print(f"  [ERROR] 입력창을 찾을 수 없습니다")
                save_debug_info(self.driver, "input_not_found", seq)
                raise Exception("입력창 없음")
            
            print(f"  [GEMINI] ✓ 입력창 발견")
            
            # 디버그: 입력창 정보 출력
            try:
                print(f"  [DEBUG] 입력창 정보:")
                print(f"    - tag: {input_box.tag_name}")
                print(f"    - id: {input_box.get_attribute('id')}")
                print(f"    - class: {input_box.get_attribute('class')}")
                print(f"    - displayed: {input_box.is_displayed()}")
                print(f"    - enabled: {input_box.is_enabled()}")
            except Exception as e:
                print(f"  [DEBUG] 입력창 정보 출력 실패: {e}")
            
            # 텍스트 입력 (여러 방법 시도)
            success = self._input_text(input_box, prompt, seq)
            
            if not success:
                save_debug_info(self.driver, "input_failed", seq)
                raise Exception("텍스트 입력 실패")
            
            print(f"  [GEMINI] ✓ 텍스트 입력 완료")
            
            # 디버그: 입력 후 상태 저장
            save_debug_info(self.driver, "after_input", seq)
            
            # 전송
            print(f"  [GEMINI] 전송 시도 중...")
            success = self._submit_message(input_box, seq)
            
            if not success:
                save_debug_info(self.driver, "submit_failed", seq)
                raise Exception("전송 실패")
            
            print(f"  [GEMINI] ✓ 전송 완료")
            
            # 디버그: 전송 후 상태 저장
            save_debug_info(self.driver, "after_submit", seq)
            
            # 응답 대기
            print(f"  [GEMINI] 응답 대기 중...")
            is_match, reason = self._wait_for_response(seq)
            
            return is_match, reason
            
        except Exception as e:
            error_msg = str(e)
            print(f"  [GEMINI ERROR] {error_msg}")
            return False, error_msg
    
    def _find_input_box(self):
        """입력창 찾기 - Quill 에디터 내부 찾기"""
        selectors = [
            "rich-textarea .ql-editor",
            ".ql-editor[contenteditable='true']",
            "div[contenteditable='true'].ql-editor",
            "rich-textarea div[contenteditable='true']",
            "div[contenteditable='true'][role='textbox']",
            "textarea[placeholder*='Ask']"
        ]
        
        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.is_displayed():
                        print(f"  [DEBUG] 입력창 발견 (selector: {selector})")
                        return element
            except:
                continue
        
        return None
    
    def _input_text(self, element, text: str, seq: str) -> bool:
        """텍스트 입력 - Quill 에디터용"""
        
        # 방법 1: JavaScript로 innerText 설정 (Quill 에디터에 최적)
        try:
            print(f"  [DEBUG] 방법 1: JavaScript innerText 설정")
            self.driver.execute_script("""
                var element = arguments[0];
                var text = arguments[1];
                
                // innerText로 설정 (Quill 에디터 호환)
                element.innerText = text;
                element.textContent = text;
                
                // input 이벤트 발생
                element.dispatchEvent(new Event('input', { bubbles: true }));
                element.dispatchEvent(new Event('change', { bubbles: true }));
                
                // Quill 에디터 업데이트 (있다면)
                if (element.closest('rich-textarea')) {
                    var richTextarea = element.closest('rich-textarea');
                    if (richTextarea.__ngContext__) {
                        // Angular 컴포넌트 업데이트 트리거
                    }
                }
            """, element, text)
            time.sleep(2)
            
            # 값이 입력되었는지 확인
            value = element.get_attribute('innerText') or element.get_attribute('textContent') or element.text
            if value and len(value) > 10:
                print(f"  [DEBUG] 방법 1 성공 (입력된 길이: {len(value)})")
                return True
        except Exception as e:
            print(f"  [DEBUG] 방법 1 실패: {e}")
        
        # 방법 2: 클릭 후 JavaScript 설정
        try:
            print(f"  [DEBUG] 방법 2: 클릭 후 JavaScript 설정")
            element.click()
            time.sleep(0.5)
            
            self.driver.execute_script("""
                var element = arguments[0];
                var text = arguments[1];
                element.innerText = text;
                element.focus();
            """, element, text)
            time.sleep(2)
            
            value = element.get_attribute('innerText') or element.text
            if value and len(value) > 10:
                print(f"  [DEBUG] 방법 2 성공 (입력된 길이: {len(value)})")
                return True
        except Exception as e:
            print(f"  [DEBUG] 방법 2 실패: {e}")
        
        # 방법 3: 일반 send_keys
        try:
            print(f"  [DEBUG] 방법 3: send_keys 시도")
            element.clear()
            element.send_keys(text)
            time.sleep(1)
            
            value = element.text or element.get_attribute('innerText')
            if value and len(value) > 10:
                print(f"  [DEBUG] 방법 3 성공 (입력된 길이: {len(value)})")
                return True
        except Exception as e:
            print(f"  [DEBUG] 방법 3 실패: {e}")
        
        # 방법 4: 클릭 후 send_keys
        try:
            print(f"  [DEBUG] 방법 4: 클릭 후 send_keys")
            element.click()
            time.sleep(0.5)
            element.send_keys(text)
            time.sleep(1)
            
            value = element.text or element.get_attribute('innerText')
            if value and len(value) > 10:
                print(f"  [DEBUG] 방법 4 성공 (입력된 길이: {len(value)})")
                return True
        except Exception as e:
            print(f"  [DEBUG] 방법 4 실패: {e}")
        
        print(f"  [DEBUG] 모든 입력 방법 실패")
        return False
    
    def _submit_message(self, input_element, seq: str) -> bool:
        """메시지 전송 - 전송 버튼 직접 클릭"""
        
        # 방법 1: aria-label로 전송 버튼 찾기 (가장 확실)
        try:
            print(f"  [DEBUG] 전송 방법 1: aria-label='메시지 보내기' 버튼")
            button = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label='메시지 보내기']")
            if button.is_displayed() and button.is_enabled():
                print(f"  [DEBUG] 전송 버튼 발견, 클릭 시도...")
                button.click()
                time.sleep(3)
                print(f"  [DEBUG] 방법 1 성공!")
                return True
        except Exception as e:
            print(f"  [DEBUG] 방법 1 실패: {e}")
        
        # 방법 2: send-button 클래스로 찾기
        try:
            print(f"  [DEBUG] 전송 방법 2: button.send-button")
            buttons = self.driver.find_elements(By.CSS_SELECTOR, "button.send-button")
            for btn in buttons:
                if btn.is_displayed() and btn.is_enabled():
                    aria = btn.get_attribute('aria-label')
                    print(f"  [DEBUG] 버튼 발견: aria-label='{aria}'")
                    btn.click()
                    time.sleep(3)
                    print(f"  [DEBUG] 방법 2 성공!")
                    return True
        except Exception as e:
            print(f"  [DEBUG] 방법 2 실패: {e}")
        
        # 방법 3: JavaScript로 버튼 클릭
        try:
            print(f"  [DEBUG] 전송 방법 3: JavaScript 클릭")
            self.driver.execute_script("""
                var button = document.querySelector("button[aria-label='메시지 보내기']");
                if (!button) {
                    button = document.querySelector("button.send-button");
                }
                if (button) {
                    button.click();
                    return true;
                }
                return false;
            """)
            time.sleep(3)
            print(f"  [DEBUG] 방법 3 시도 완료")
            return True
        except Exception as e:
            print(f"  [DEBUG] 방법 3 실패: {e}")
        
        # 방법 4: 모든 submit 버튼 순회
        try:
            print(f"  [DEBUG] 전송 방법 4: submit 버튼 전체 검색")
            buttons = self.driver.find_elements(By.CSS_SELECTOR, "button[type='submit']")
            for btn in buttons:
                try:
                    aria = btn.get_attribute('aria-label')
                    if '보내기' in str(aria) or 'Send' in str(aria):
                        print(f"  [DEBUG] 전송 버튼 발견: {aria}")
                        btn.click()
                        time.sleep(3)
                        print(f"  [DEBUG] 방법 4 성공!")
                        return True
                except:
                    continue
        except Exception as e:
            print(f"  [DEBUG] 방법 4 실패: {e}")
        
        print(f"  [DEBUG] 모든 전송 방법 실패")
        save_debug_info(self.driver, "all_submit_failed", seq)
        
        # 수동 입력 안내
        print(f"\n" + "="*70)
        print(f"  [MANUAL] 자동 전송 실패 - 수동으로 전송 버튼을 눌러주세요")
        print(f"  [MANUAL] 브라우저에서 전송 버튼을 클릭한 후 10초 대기합니다...")
        print(f"="*70 + "\n")
        time.sleep(10)
        
        return True
    
    def _wait_for_response(self, seq: str) -> tuple:
        """Gemini 응답 대기 및 파싱"""
        print(f"  [GEMINI] 응답 대기 (최대 60초)...")
        
        max_wait = 60
        start_time = time.time()
        last_length = 0
        stable_count = 0
        
        # 먼저 로딩 인디케이터가 사라질 때까지 대기
        loading_wait = 0
        while loading_wait < 10:
            try:
                # 로딩 중인지 확인
                loading_elements = self.driver.find_elements(By.CSS_SELECTOR, 
                    ".loading, .spinner, [aria-busy='true']")
                if not loading_elements:
                    break
            except:
                pass
            time.sleep(1)
            loading_wait += 1
        
        while time.time() - start_time < max_wait:
            try:
                # 응답 메시지 찾기 - 더 구체적인 selector들
                response_selectors = [
                    "message-content",  # Gemini의 메시지 컨텐츠
                    ".model-response-text",
                    ".response-container",
                    "[data-test-id*='conversation-turn']",
                    ".conversation-container message-content",
                    "model-response",
                    ".markdown-content"
                ]
                
                for selector in response_selectors:
                    try:
                        messages = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if messages and len(messages) > 0:
                            # 마지막 메시지 (가장 최근 응답)
                            last_message = messages[-1]
                            response_text = last_message.text.strip()
                            
                            # 응답이 충분히 길고 유의미한지
                            if response_text and len(response_text) > 20:
                                # "YES" 또는 "NO"가 포함되어 있는지 확인
                                first_line = response_text.split('\n')[0].upper()
                                if 'YES' in first_line or 'NO' in first_line:
                                    # 길이 안정화 확인
                                    if len(response_text) == last_length:
                                        stable_count += 1
                                        if stable_count >= 3:
                                            print(f"  [GEMINI] ✓ 응답 완료 ({len(response_text)} chars)")
                                            
                                            # YES/NO 파싱
                                            first_word = response_text.split()[0].upper().rstrip(':,.')
                                            is_match = first_word == 'YES' or response_text.upper().startswith('YES')
                                            
                                            # 디버그: 응답 저장
                                            save_debug_info(self.driver, "response_received", seq)
                                            
                                            return is_match, response_text
                                    else:
                                        stable_count = 0
                                        last_length = len(response_text)
                                        print(f"  [GEMINI] 응답 수신 중... ({len(response_text)} chars)")
                    except Exception as e:
                        continue
            except:
                pass
            
            time.sleep(2)
        
        # 타임아웃
        print(f"  [GEMINI] 응답 타임아웃")
        save_debug_info(self.driver, "response_timeout", seq)
        raise Exception("응답 타임아웃 (60초)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메인 처리
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_verification(
    input_csv: str = OUTPUT_CSV,
    headless: bool = False,
    start_seq: Optional[str] = None,
    limit: Optional[int] = None
):
    """이미지 검증 처리"""
    
    if not Path(input_csv).exists():
        print(f"[ERROR] CSV 파일 없음: {input_csv}")
        return
    
    print(f"\n{'='*70}")
    print(f"Gemini Web 이미지 검증 (디버깅 버전)")
    print(f"{'='*70}\n")
    print(f"[INFO] 디버그 파일 저장 위치: {DEBUG_DIR}\n")
    
    # CSV 로드
    dtype_dict = {col: str for col in CSV_COLUMNS}
    df = pd.read_csv(input_csv, encoding="utf-8-sig", dtype=dtype_dict)
    print(f"[INFO] 전체 데이터: {len(df)}건")
    
    # 미검증 항목만
    df = df[
        (df['STATUS'] == Status.FOUND) &
        ((df['GEMINI_VERIFIED'].isna()) | (df['GEMINI_VERIFIED'] == '') | (df['GEMINI_VERIFIED'] == 'nan'))
    ]
    print(f"[INFO] 미검증 항목: {len(df)}건")
    
    # 특정 SEQ부터 시작
    if start_seq:
        start_idx = df[df['SELF_IMPORT_SEQ'] == str(start_seq)].index
        if len(start_idx) > 0:
            df = df.loc[start_idx[0]:]
            print(f"[INFO] SEQ {start_seq}부터 시작: {len(df)}건")
    
    # 개수 제한
    if limit:
        df = df.head(limit)
        print(f"[INFO] {limit}건으로 제한")
    
    if df.empty:
        print("[INFO] 검증할 항목 없음")
        return
    
    # 드라이버 생성
    driver = create_driver(headless=headless)
    
    try:
        # Gemini 페이지 열기
        print(f"[LOGIN] Gemini 페이지 열기...\n")
        driver.get(GEMINI_WEB_URL)
        time.sleep(3)
        
        # 디버그: 초기 페이지 상태
        save_debug_info(driver, "initial_page", "login")
        
        # 로그인 확인
        print(f"{'='*70}")
        print(f"[LOGIN] 로그인 확인")
        print(f"{'='*70}\n")
        print(f"열린 Chrome 브라우저에서:")
        print(f"1. 이미 로그인되어 있으면 → 바로 Enter")
        print(f"2. 로그인 필요하면 → Google 계정으로 로그인 → Enter\n")
        input(f"준비되면 Enter를 눌러주세요...")
        print(f"{'='*70}\n")
        
        print(f"[LOGIN] ✓ 계속 진행합니다\n")
        
        # Gemini 인터페이스 초기화
        gemini = GeminiWebInterface(driver)
        
        total = len(df)
        processed = 0
        success = 0
        
        for idx, row in df.iterrows():
            seq = str(row['SELF_IMPORT_SEQ'])
            prdt_nm = row['PRDT_NM']
            mufc_nm = row['MUFC_NM']
            iherb_url = row['IHERB_URL']
            hazard_image_url = row['IMAGE_URL_MFDS']
            
            # 쉼표로 구분된 여러 URL 중 첫 번째만
            if pd.notna(hazard_image_url) and ',' in str(hazard_image_url):
                hazard_image_url = str(hazard_image_url).split(',')[0].strip()
            
            print(f"\n{'='*70}")
            print(f"[{processed+1}/{total}] {prdt_nm}")
            print(f"SEQ: {seq}")
            print(f"{'='*70}")
            
            # iHerb 이미지 추출
            iherb_image, iherb_name, iherb_brand = scrape_iherb_simple(driver, iherb_url)
            
            # Gemini로 돌아가기
            print(f"  [SCRAPER] Gemini로 복귀 중...")
            driver.get(GEMINI_WEB_URL)
            time.sleep(2)
            
            if not iherb_image:
                print("  [ERROR] iHerb 이미지 없음")
                df.at[idx, 'STATUS'] = Status.VERIFICATION_FAILED
                df.at[idx, 'GEMINI_REASON'] = "No iHerb image"
                processed += 1
                save_dataframe(df, input_csv)
                continue
            
            # Gemini로 비교
            is_match, reason = gemini.verify_images(
                hazard_image_url=hazard_image_url,
                iherb_image_url=iherb_image,
                hazard_name=prdt_nm,
                hazard_brand=mufc_nm,
                iherb_name=iherb_name,
                iherb_brand=iherb_brand,
                seq=seq
            )
            
            # 결과 저장
            df.at[idx, 'IHERB_PRODUCT_IMAGES'] = str(json.dumps([iherb_image]))
            df.at[idx, 'GEMINI_VERIFIED'] = str(is_match)
            df.at[idx, 'GEMINI_REASON'] = str(reason)
            df.at[idx, 'VERIFIED_DTM'] = datetime.now().strftime("%Y%m%d%H%M%S")
            
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
            
            time.sleep(2)
        
        print(f"\n{'='*70}")
        print(f"[DONE] 검증 완료!")
        print(f"총 처리: {processed}건")
        print(f"매칭 성공: {success}건")
        print(f"매칭 실패: {processed - success}건")
        print(f"{'='*70}")
    
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] 중단")
        save_dataframe(df, input_csv)
    
    finally:
        try:
            driver.quit()
        except:
            pass


def save_dataframe(df: pd.DataFrame, output_path: str):
    """DataFrame 저장"""
    dtype_dict = {col: str for col in CSV_COLUMNS}
    full_df = pd.read_csv(output_path, encoding="utf-8-sig", dtype=dtype_dict)
    
    # 업데이트된 행만 반영
    for idx, row in df.iterrows():
        if idx in full_df.index:
            for col in row.index:
                value = row[col]
                if pd.notna(value) and value != '':
                    full_df.at[idx, col] = str(value)
    
    # 저장
    full_df = full_df[CSV_COLUMNS]
    full_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"  [SAVE] 저장 완료")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    parser = argparse.ArgumentParser(description="Phase 2: Gemini Web 검증 (디버깅)")
    parser.add_argument("--csv", default=OUTPUT_CSV, help="입력 CSV 파일")
    parser.add_argument("--headless", action="store_true", help="헤드리스 모드")
    parser.add_argument("--start-seq", type=str, help="시작 SEQ")
    parser.add_argument("--limit", type=int, help="처리 개수 제한")
    
    args = parser.parse_args()
    
    process_verification(
        input_csv=args.csv,
        headless=args.headless,
        start_seq=args.start_seq,
        limit=args.limit
    )


if __name__ == "__main__":
    main()