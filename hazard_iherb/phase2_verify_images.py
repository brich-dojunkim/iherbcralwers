#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2: Gemini Web 이미지 검증 (듀얼 브라우저 버전)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 브라우저 1: iHerb 전용 (스크래핑)
- 브라우저 2: Gemini 전용 (검증)
- Rate Limit 대응: 5개마다 새 채팅 시작
- 프롬프트 강화: 정확한 제품 매칭
"""

import os
import json
import time
import argparse
import requests
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait

# macOS Finder 다이얼로그 닫기용
try:
    import pyautogui
    PYAUTOGUI_AVAILABLE = True
except ImportError:
    PYAUTOGUI_AVAILABLE = False
    print("[WARNING] PyAutoGUI 미설치: Finder 다이얼로그 자동 닫기 불가")
    print("[WARNING] 설치 방법: pip install pyautogui")
    print("[WARNING] 기능은 정상 작동하나 Finder 창이 남을 수 있습니다.\n")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 설정
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

OUTPUT_CSV = "/Users/brich/Desktop/iherb_price/hazard_iherb/csv/hazard_iherb_matched_final.csv"
DEBUG_DIR = Path("/Users/brich/Desktop/iherb_price/hazard_iherb/debug")
TEMP_DIR = Path("/Users/brich/Desktop/iherb_price/hazard_iherb/temp_images")
GEMINI_WEB_URL = "https://gemini.google.com"

# 채팅방 관리
MAX_MESSAGES_PER_CHAT = 5

# 대기 시간
WAIT_AFTER_UPLOAD = 3
WAIT_AFTER_VERIFY = 15
WAIT_NEW_CHAT = 10
WAIT_RETRY_BASE = 30

# 디렉토리 생성
DEBUG_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)

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
# 이미지 다운로드
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def download_image(url: str, save_path: Path) -> bool:
    """URL에서 이미지 다운로드"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            save_path.write_bytes(response.content)
            print(f"  [DOWNLOAD] ✓ 이미지 저장: {save_path.name}")
            return True
        else:
            print(f"  [DOWNLOAD ERROR] HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"  [DOWNLOAD ERROR] {e}")
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 디버깅 유틸리티
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def save_debug_info(driver, step_name: str, seq: str = "unknown"):
    """디버그 정보 저장"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = f"{timestamp}_{seq}_{step_name}"
    
    try:
        html_path = DEBUG_DIR / f"{prefix}.html"
        html_content = driver.page_source
        html_path.write_text(html_content, encoding='utf-8')
        print(f"  [DEBUG] HTML 저장: {html_path.name}")
    except Exception as e:
        print(f"  [DEBUG ERROR] HTML 저장 실패: {e}")
    
    try:
        screenshot_path = DEBUG_DIR / f"{prefix}.png"
        driver.save_screenshot(str(screenshot_path))
        print(f"  [DEBUG] 스크린샷 저장: {screenshot_path.name}")
    except Exception as e:
        print(f"  [DEBUG ERROR] 스크린샷 저장 실패: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Selenium 드라이버
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def create_driver(headless: bool = False, profile_name: str = "default"):
    """undetected-chromedriver 생성"""
    options = uc.ChromeOptions()
    
    profile_base = Path.home() / ".chrome_automation_profiles"
    profile_base.mkdir(parents=True, exist_ok=True)
    
    profile_dir = profile_base / profile_name
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
    """iHerb에서 메인 이미지와 제품명, 브랜드 추출"""
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
    
    def start_new_chat(self):
        """새 채팅 시작"""
        try:
            new_chat_selectors = [
                "button[aria-label='새 채팅']",
                "button[aria-label*='New chat']",
                ".side-nav-action-collapsed-button"
            ]
            
            for selector in new_chat_selectors:
                try:
                    btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if '채팅' in btn.get_attribute('aria-label') or 'chat' in btn.get_attribute('aria-label').lower():
                        btn.click()
                        time.sleep(WAIT_NEW_CHAT)
                        print(f"  [CHAT] ✓ 새 채팅 시작 ({WAIT_NEW_CHAT}초 대기)")
                        return True
                except:
                    continue
            
            self.driver.execute_script("""
                var btn = document.querySelector("button[aria-label*='새 채팅']");
                if (btn) btn.click();
            """)
            time.sleep(WAIT_NEW_CHAT)
            print(f"  [CHAT] ✓ 새 채팅 시작 (JS, {WAIT_NEW_CHAT}초 대기)")
            return True
            
        except Exception as e:
            print(f"  [CHAT ERROR] 새 채팅 실패: {e}")
            return False
    
    def verify_images_with_retry(
        self,
        hazard_image_url: str,
        iherb_image_url: str,
        hazard_name: str,
        hazard_brand: str,
        iherb_name: str,
        iherb_brand: str,
        seq: str = "unknown"
    ) -> tuple:
        """재시도 로직 포함 이미지 검증"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                is_match, reason = self.verify_images(
                    hazard_image_url=hazard_image_url,
                    iherb_image_url=iherb_image_url,
                    hazard_name=hazard_name,
                    hazard_brand=hazard_brand,
                    iherb_name=iherb_name,
                    iherb_brand=iherb_brand,
                    seq=seq
                )
                return is_match, reason
                
            except Exception as e:
                error_msg = str(e)
                
                is_rate_limit = (
                    "문제가 발생했습니다" in error_msg or
                    "(8)" in error_msg or
                    "rate" in error_msg.lower() or
                    "limit" in error_msg.lower()
                )
                
                if is_rate_limit and attempt < max_retries - 1:
                    wait_time = WAIT_RETRY_BASE * (attempt + 1)
                    print(f"  [RETRY] Rate limit 감지! {wait_time}초 대기 후 재시도 ({attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue
                
                print(f"  [ERROR] {error_msg}")
                return False, error_msg
        
        return False, "최대 재시도 횟수 초과 (Rate Limit)"
    
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
        """Gemini로 두 이미지 비교"""
        try:
            try:
                _ = self.driver.current_url
            except Exception as e:
                raise Exception(f"브라우저가 닫혀 있습니다: {e}")
            
            # ===== 1. 이미지 다운로드 =====
            print(f"  [IMAGE] 이미지 다운로드 중...")
            
            hazard_img_path = TEMP_DIR / f"hazard_{seq}.jpg"
            iherb_img_path = TEMP_DIR / f"iherb_{seq}.jpg"
            
            if not download_image(hazard_image_url, hazard_img_path):
                raise Exception("위해식품 이미지 다운로드 실패")
            
            if not download_image(iherb_image_url, iherb_img_path):
                raise Exception("iHerb 이미지 다운로드 실패")
            
            # ===== 2. 이미지 업로드 =====
            print(f"  [UPLOAD] 이미지 업로드 중...")
            
            self._upload_image(str(hazard_img_path), seq, "image1", close_menu=False)
            print(f"  [UPLOAD] ✓ image1 업로드 완료, 메뉴 유지")
            
            time.sleep(2)
            
            self._upload_image(str(iherb_img_path), seq, "image2", close_menu=True)
            print(f"  [UPLOAD] ✓ image2 업로드 완료, 메뉴 닫음")
            
            print(f"  [UPLOAD] ✓ 2개 이미지 업로드 완료")
            
            # ===== 3. 텍스트 프롬프트 입력 (강화!) =====
            prompt = f"""Compare these two supplement product images carefully:

IMAGE 1 (Korean Import Product):
- Product: {hazard_name}
- Brand: {hazard_brand}

IMAGE 2 (iHerb Product):
- Product: {iherb_name}
- Brand: {iherb_brand}

TASK: Determine if these are the SAME product (same supplement, same brand, same formulation).

ANSWER FORMAT (ONE sentence only):
- "YES: [brief reason why they match]"
- "NO: [brief reason why they differ]"

Your answer:"""
            
            print(f"  [TEXT] 텍스트 입력 중...")
            input_box = self._find_input_box()
            
            if not input_box:
                raise Exception("입력창 없음")
            
            success = self._input_text(input_box, prompt, seq)
            if not success:
                raise Exception("텍스트 입력 실패")
            
            print(f"  [TEXT] ✓ 텍스트 입력 완료")
            
            # ===== 4. 전송 =====
            print(f"  [SUBMIT] 전송 중...")
            success = self._submit_message(input_box, seq)
            if not success:
                raise Exception("전송 실패")
            
            print(f"  [SUBMIT] ✓ 전송 완료")
            
            # ===== 5. 응답 대기 =====
            print(f"  [RESPONSE] 응답 대기 중...")
            is_match, reason = self._wait_for_response(seq)
            
            try:
                hazard_img_path.unlink()
                iherb_img_path.unlink()
            except:
                pass
            
            return is_match, reason
            
        except Exception as e:
            error_msg = str(e)
            
            if "no such window" in error_msg.lower() or "브라우저가 닫혀" in error_msg:
                print(f"  [ERROR] ✗ 브라우저가 닫혀 있습니다!")
                error_msg = "브라우저가 사용자에 의해 닫힘"
            else:
                print(f"  [ERROR] {error_msg}")
            
            raise
        
        finally:
            try:
                print(f"  [CLEANUP] UI 초기화 중...")
                
                self.driver.execute_script("""
                    document.querySelectorAll('.cdk-overlay-backdrop, .cdk-overlay-pane, .cdk-overlay-container, [role="menu"], [role="dialog"]').forEach(el => {
                        el.remove();
                    });
                    
                    document.querySelectorAll('input[type="file"]').forEach(f => {
                        f.value = '';
                        f.style.display = 'none';
                    });
                    
                    var escEvent = new KeyboardEvent('keydown', {
                        key: 'Escape',
                        code: 'Escape',
                        keyCode: 27,
                        which: 27,
                        bubbles: true,
                        cancelable: true
                    });
                    document.body.dispatchEvent(escEvent);
                """)
                
                time.sleep(1)
                
                body = self.driver.find_element(By.TAG_NAME, 'body')
                body.send_keys(Keys.ESCAPE)
                body.send_keys(Keys.ESCAPE)
                body.send_keys(Keys.ESCAPE)
                
                time.sleep(1)
                
                print(f"  [CLEANUP] ✓ 최종 정리 완료")
                
            except Exception as cleanup_error:
                print(f"  [CLEANUP] 정리 시도 중 오류: {cleanup_error}")
    
    def _upload_image(self, image_path: str, seq: str, img_num: str, close_menu: bool = True):
        """이미지 업로드"""
        try:
            try:
                _ = self.driver.current_url
            except:
                raise Exception("브라우저가 닫혀 있습니다. 브라우저를 닫지 마세요!")
            
            # ===== 0단계: 첫 번째 이미지만 사전 정리 =====
            if img_num == "image1":
                print(f"  [PRE-CLEANUP] 업로드 전 사전 정리 중...")
                
                self.driver.execute_script("""
                    document.querySelectorAll('.cdk-overlay-backdrop, .cdk-overlay-pane, .cdk-overlay-container, [role="menu"], [role="dialog"]').forEach(el => {
                        try {
                            el.remove();
                        } catch(e) {}
                    });
                    
                    document.querySelectorAll('input[type="file"]').forEach(f => {
                        try {
                            f.value = '';
                            f.style.display = 'none';
                        } catch(e) {}
                    });
                """)
                
                try:
                    body = self.driver.find_element(By.TAG_NAME, 'body')
                    body.send_keys(Keys.ESCAPE)
                    body.send_keys(Keys.ESCAPE)
                except:
                    pass
                
                time.sleep(1.5)
                print(f"  [PRE-CLEANUP] ✓ 사전 정리 완료")
            
            # ===== 1단계: 파일 업로드 메뉴 열기 =====
            print(f"  [UPLOAD] 1단계: 업로드 메뉴 열기")
            try:
                upload_menu_btn = self.driver.find_element(By.CSS_SELECTOR, 
                    "button[aria-label*='파일 업로드 메뉴']")
                upload_menu_btn.click()
                time.sleep(1.5)
                print(f"  [UPLOAD] ✓ 메뉴 열림")
                
            except Exception as e:
                print(f"  [UPLOAD] 메뉴 열기 실패: {e}")
                self.driver.execute_script("""
                    var btn = document.querySelector("button[aria-label*='파일 업로드']");
                    if (btn) btn.click();
                """)
                time.sleep(1.5)
            
            # ===== 2단계: "파일 업로드" 메뉴 항목 클릭 =====
            print(f"  [UPLOAD] 2단계: '파일 업로드' 메뉴 항목 클릭")
            try:
                menu_selectors = [
                    "button[aria-label*='파일 업로드. 문서']",
                    "button[aria-label*='파일 업로드']",
                    ".mat-mdc-list-item[aria-label*='파일 업로드']"
                ]
                
                menu_clicked = False
                for selector in menu_selectors:
                    try:
                        menu_items = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for item in menu_items:
                            if '문서' in str(item.get_attribute('aria-label')) or '파일 업로드' in item.text:
                                item.click()
                                time.sleep(1.5)
                                menu_clicked = True
                                break
                        if menu_clicked:
                            break
                    except:
                        continue
                
                if not menu_clicked:
                    self.driver.execute_script("""
                        var items = document.querySelectorAll('button[aria-label*="파일 업로드"]');
                        for (var i = 0; i < items.length; i++) {
                            if (items[i].getAttribute('aria-label').includes('문서')) {
                                items[i].click();
                                break;
                            }
                        }
                    """)
                    time.sleep(1.5)
                
                print(f"  [UPLOAD] ✓ 메뉴 항목 클릭 완료")
                
            except Exception as e:
                print(f"  [UPLOAD] 메뉴 항목 클릭 실패: {e}")
            
            # ===== 3단계: file input 찾아서 파일 전달 =====
            print(f"  [UPLOAD] 3단계: file input에 파일 전달")
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    time.sleep(1 + (attempt * 0.5))
                    
                    file_input_selectors = [
                        "input[type='file']",
                        ".hidden-local-upload-button",
                        ".hidden-local-file-upload-button",
                        "input[type='file'][accept*='image']"
                    ]
                    
                    file_input = None
                    for selector in file_input_selectors:
                        try:
                            inputs = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            for inp in inputs:
                                if inp.is_displayed() or inp.size['height'] > 0:
                                    file_input = inp
                                    break
                                file_input = inp
                                break
                            if file_input:
                                break
                        except:
                            continue
                    
                    if not file_input:
                        if attempt < max_retries - 1:
                            print(f"  [UPLOAD] file input 없음, 재시도...")
                            continue
                        raise Exception("file input을 찾을 수 없습니다")
                    
                    # file input 활성화
                    self.driver.execute_script("""
                        var inp = arguments[0];
                        inp.style.display = 'block';
                        inp.style.position = 'fixed';
                        inp.style.top = '0';
                        inp.style.left = '0';
                        inp.style.opacity = '0.01';
                        inp.style.zIndex = '9999';
                        inp.style.width = '100px';
                        inp.style.height = '100px';
                        inp.style.pointerEvents = 'auto';
                    """, file_input)
                    
                    time.sleep(0.5)
                    
                    # 파일 경로 전달
                    file_input.send_keys(image_path)
                    print(f"  [UPLOAD] 파일 경로 전달 완료, Finder 처리 대기 중...")
                    
                    time.sleep(3)
                    
                    # Finder 처리
                    finder_closed = self._close_finder_dialog()
                    
                    if finder_closed:
                        print(f"  [UPLOAD] ✓ {img_num} 파일 전달 완료 (Finder 닫음)")
                    else:
                        print(f"  [UPLOAD] ⚠️  {img_num} 파일 전달 완료 (Finder 수동 닫기 필요)")
                    
                    time.sleep(WAIT_AFTER_UPLOAD)
                    
                    # ===== file input 정리 =====
                    print(f"  [POST-UPLOAD] file input 정리 중...")
                    try:
                        self.driver.execute_script("""
                            document.querySelectorAll('input[type="file"]').forEach(f => {
                                try {
                                    f.value = '';
                                    f.style.display = 'none';
                                    f.style.position = 'absolute';
                                    f.style.zIndex = '-9999';
                                    f.style.pointerEvents = 'none';
                                } catch(e) {
                                    console.log('file input cleanup error:', e);
                                }
                            });
                        """)
                        
                        print(f"  [POST-UPLOAD] ✓ file input 정리 완료")
                    except Exception as e:
                        print(f"  [POST-UPLOAD] file input 정리 실패 (무시): {str(e)[:50]}")
                    
                    time.sleep(0.5)
                    
                    # ===== 4단계: 메뉴 닫기 (옵션) =====
                    if close_menu:
                        print(f"  [CLEANUP] 업로드 후 메뉴 닫는 중...")
                        
                        body = self.driver.find_element(By.TAG_NAME, 'body')
                        body.send_keys(Keys.ESCAPE)
                        time.sleep(0.5)
                        
                        self.driver.execute_script("""
                            var backdrops = document.querySelectorAll('.cdk-overlay-backdrop, [role="menu"]');
                            backdrops.forEach(b => {
                                try {
                                    b.remove();
                                } catch(e) {}
                            });
                        """)
                        time.sleep(0.5)
                        
                        print(f"  [CLEANUP] ✓ 메뉴 닫힘")
                    
                    break
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"  [UPLOAD] 재시도 {attempt + 1}/{max_retries}: {str(e)[:50]}...")
                        try:
                            self.driver.execute_script("""
                                document.querySelectorAll('.cdk-overlay-backdrop, [role="menu"]').forEach(el => el.remove());
                                document.querySelectorAll('input[type="file"]').forEach(f => {
                                    f.value = '';
                                    f.style.display = 'none';
                                    f.style.zIndex = '-9999';
                                });
                            """)
                            self._close_finder_dialog()
                        except:
                            pass
                        continue
                    else:
                        raise
            
        except Exception as e:
            print(f"  [UPLOAD ERROR] {e}")
            save_debug_info(self.driver, f"upload_failed_{img_num}", seq)
            raise
    
    def _close_finder_dialog(self) -> bool:
        """macOS Finder 다이얼로그 닫기"""
        try:
            import subprocess
            
            # 방법 1: AppleScript로 Return 키
            try:
                script = '''
                tell application "System Events"
                    tell process "Google Chrome"
                        if exists window 1 then
                            keystroke return
                            delay 0.5
                        end if
                    end tell
                end tell
                '''
                result = subprocess.run(['osascript', '-e', script], 
                             capture_output=True, timeout=5)
                time.sleep(1.5)
                
                if result.returncode == 0:
                    return True
            except Exception as e:
                print(f"  [FINDER] AppleScript 방법 1 실패: {e}")
            
            # 방법 2: Chrome 활성화 후 Return
            try:
                subprocess.run([
                    'osascript', '-e',
                    'tell application "Google Chrome" to activate'
                ], capture_output=True, timeout=2)
                time.sleep(0.5)
                
                subprocess.run([
                    'osascript', '-e',
                    'tell application "System Events" to keystroke return'
                ], capture_output=True, timeout=2)
                time.sleep(1)
                return True
            except Exception as e:
                print(f"  [FINDER] AppleScript 방법 2 실패: {e}")
            
            # 방법 3: ESC 키
            try:
                subprocess.run([
                    'osascript', '-e',
                    'tell application "System Events" to keystroke (ASCII character 27)'
                ], capture_output=True, timeout=2)
                time.sleep(0.5)
                return True
            except Exception as e:
                print(f"  [FINDER] ESC 방법 실패: {e}")
            
            # 방법 4: PyAutoGUI
            if PYAUTOGUI_AVAILABLE:
                try:
                    subprocess.run([
                        'osascript', '-e',
                        'tell application "Google Chrome" to activate'
                    ], capture_output=True, timeout=2)
                    time.sleep(0.5)
                    
                    pyautogui.press('return')
                    time.sleep(0.5)
                    pyautogui.press('escape')
                    time.sleep(0.3)
                    return True
                except Exception as e:
                    print(f"  [FINDER] PyAutoGUI 방법 실패: {e}")
            
            return False
            
        except Exception as e:
            print(f"  [FINDER] 전체 처리 실패: {e}")
            return False
    
    def _find_input_box(self):
        """입력창 찾기"""
        selectors = [
            "rich-textarea .ql-editor",
            ".ql-editor[contenteditable='true']",
            "div[contenteditable='true'].ql-editor",
            "rich-textarea div[contenteditable='true']",
            "div[contenteditable='true'][role='textbox']"
        ]
        
        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.is_displayed():
                        return element
            except:
                continue
        
        return None
    
    def _input_text(self, element, text: str, seq: str) -> bool:
        """텍스트 입력"""
        try:
            self.driver.execute_script("""
                var element = arguments[0];
                var text = arguments[1];
                element.innerText = text;
                element.textContent = text;
                element.dispatchEvent(new Event('input', { bubbles: true }));
                element.dispatchEvent(new Event('change', { bubbles: true }));
            """, element, text)
            time.sleep(1)
            
            value = element.get_attribute('innerText') or element.text
            if value and len(value) > 10:
                return True
        except Exception as e:
            print(f"  [INPUT] 입력 실패: {e}")
        
        return False
    
    def _submit_message(self, input_element, seq: str) -> bool:
        """메시지 전송"""
        try:
            button = self.driver.find_element(By.CSS_SELECTOR, 
                "button[aria-label='메시지 보내기'], button[aria-label*='Send']")
            if button.is_displayed() and button.is_enabled():
                button.click()
                time.sleep(2)
                return True
        except:
            pass
        
        try:
            self.driver.execute_script("""
                var button = document.querySelector("button[aria-label='메시지 보내기']") ||
                             document.querySelector("button.send-button");
                if (button) {
                    button.click();
                    return true;
                }
                return false;
            """)
            time.sleep(2)
            return True
        except:
            pass
        
        return False
    
    def _wait_for_response(self, seq: str) -> tuple:
        """Gemini 응답 대기"""
        print(f"  [RESPONSE] 응답 대기 (최대 60초)...")
        
        max_wait = 60
        start_time = time.time()
        last_length = 0
        stable_count = 0
        
        while time.time() - start_time < max_wait:
            try:
                response_selectors = [
                    "message-content",
                    ".model-response-text",
                    ".response-container",
                    "model-response",
                    ".markdown-content"
                ]
                
                for selector in response_selectors:
                    try:
                        messages = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if messages and len(messages) > 0:
                            last_message = messages[-1]
                            response_text = last_message.text.strip()
                            
                            if response_text and len(response_text) > 20:
                                first_line = response_text.split('\n')[0].upper()
                                if 'YES' in first_line or 'NO' in first_line:
                                    if len(response_text) == last_length:
                                        stable_count += 1
                                        if stable_count >= 3:
                                            print(f"  [RESPONSE] ✓ 응답 완료 ({len(response_text)} chars)")
                                            
                                            clean_response = self._extract_first_sentence(response_text)
                                            
                                            first_word = clean_response.split()[0].upper().rstrip(':,.')
                                            is_match = first_word == 'YES' or clean_response.upper().startswith('YES')
                                            
                                            return is_match, clean_response
                                    else:
                                        stable_count = 0
                                        last_length = len(response_text)
                    except:
                        continue
            except:
                pass
            
            time.sleep(2)
        
        print(f"  [RESPONSE] 타임아웃")
        raise Exception("응답 타임아웃 (60초)")
    
    def _extract_first_sentence(self, text: str) -> str:
        """응답에서 첫 문장만 추출"""
        lines = text.split('\n')
        first_line = lines[0].strip()
        
        if first_line.upper().startswith('YES') or first_line.upper().startswith('NO'):
            for end_marker in ['. ', '! ', '? ', '\n']:
                if end_marker in first_line:
                    first_sentence = first_line.split(end_marker)[0] + '.'
                    return first_sentence
            return first_line
        
        for end_marker in ['. Would', '. Do you', '? ', '.\n', '. \n']:
            if end_marker in text:
                first_sentence = text.split(end_marker)[0] + '.'
                return first_sentence
        
        return text[:200] if len(text) > 200 else text


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
    print(f"Gemini Web 이미지 검증 (개선 버전)")
    print(f"{'='*70}\n")
    print(f"[INFO] 디버그 파일: {DEBUG_DIR}")
    print(f"[INFO] 임시 이미지: {TEMP_DIR}")
    print(f"[INFO] 채팅당 최대 {MAX_MESSAGES_PER_CHAT}개 처리")
    print(f"[INFO] 검증 후 대기: {WAIT_AFTER_VERIFY}초\n")
    
    dtype_dict = {col: str for col in CSV_COLUMNS}
    df = pd.read_csv(input_csv, encoding="utf-8-sig", dtype=dtype_dict)
    print(f"[INFO] 전체 데이터: {len(df)}건")
    
    df = df[
        (df['STATUS'] == Status.FOUND) &
        ((df['GEMINI_VERIFIED'].isna()) | (df['GEMINI_VERIFIED'] == '') | (df['GEMINI_VERIFIED'] == 'nan'))
    ]
    print(f"[INFO] 미검증 항목: {len(df)}건")
    
    if start_seq:
        start_idx = df[df['SELF_IMPORT_SEQ'] == str(start_seq)].index
        if len(start_idx) > 0:
            df = df.loc[start_idx[0]:]
            print(f"[INFO] SEQ {start_seq}부터 시작: {len(df)}건")
    
    if limit:
        df = df.head(limit)
        print(f"[INFO] {limit}건으로 제한")
    
    if df.empty:
        print("[INFO] 검증할 항목 없음")
        return
    
    print(f"\n{'='*70}")
    print(f"[BROWSER] 브라우저 2개 생성 중...")
    print(f"{'='*70}\n")
    
    print(f"[BROWSER] 1. iHerb 브라우저 생성...")
    iherb_driver = create_driver(headless=headless, profile_name="iHerbBot")
    iherb_driver.get("https://kr.iherb.com")
    time.sleep(2)
    print(f"[BROWSER] ✓ iHerb 브라우저 준비 완료\n")
    
    time.sleep(3)
    
    print(f"[BROWSER] 2. Gemini 브라우저 생성...")
    gemini_driver = create_driver(headless=headless, profile_name="GeminiBot")
    
    print(f"[BROWSER] Gemini 페이지 로드 중...")
    try:
        gemini_driver.get("https://gemini.google.com")
        time.sleep(3)
        print(f"[BROWSER] ✓ Gemini 브라우저 준비 완료\n")
    except Exception as e:
        print(f"[BROWSER ERROR] Gemini 페이지 로드 실패: {e}\n")
    
    save_debug_info(gemini_driver, "initial_page", "login")
    
    print(f"{'='*70}")
    print(f"[LOGIN] Gemini 로그인 확인")
    print(f"{'='*70}\n")
    print(f"Gemini 브라우저에서:")
    print(f"1. 이미 로그인되어 있으면 → 바로 Enter")
    print(f"2. 로그인 필요하면 → Google 계정으로 로그인 → Enter\n")
    print(f"⚠️  중요: Enter 누른 후 브라우저 창을 절대 닫지 마세요!\n")
    input(f"준비되면 Enter를 눌러주세요...")
    print(f"{'='*70}\n")
    
    print(f"[LOGIN] ✓ 계속 진행합니다\n")
    
    print(f"[WAIT] 세션 안정화 중...")
    time.sleep(3)
    print(f"[WAIT] ✓ 안정화 완료\n")
    
    try:
        current_url = gemini_driver.current_url
        print(f"[CHECK] ✓ Gemini 브라우저 정상 작동 중")
        if current_url and current_url != "about:blank":
            print(f"[CHECK] 현재 URL: {current_url[:60]}...\n")
    except Exception as e:
        print(f"[ERROR] ✗ Gemini 브라우저에 문제가 있습니다!")
        print(f"[ERROR] 상세: {e}\n")
        iherb_driver.quit()
        return
    
    gemini = GeminiWebInterface(gemini_driver)
    
    print(f"[READY] 첫 번째 작업 준비 중...")
    time.sleep(2)
    print(f"[READY] ✓ 작업 시작\n")
    
    try:
        total = len(df)
        processed = 0
        success = 0
        chat_count = 0
        
        for idx, row in df.iterrows():
            if chat_count >= MAX_MESSAGES_PER_CHAT and chat_count > 0:
                print(f"\n{'='*70}")
                print(f"[CHAT] {chat_count}개 처리 완료 → 새 채팅 시작")
                print(f"{'='*70}\n")
                
                gemini.start_new_chat()
                chat_count = 0
            
            seq = str(row['SELF_IMPORT_SEQ'])
            prdt_nm = row['PRDT_NM']
            mufc_nm = row['MUFC_NM']
            iherb_url = row['IHERB_URL']
            hazard_image_url = row['IMAGE_URL_MFDS']
            
            if pd.notna(hazard_image_url) and ',' in str(hazard_image_url):
                hazard_image_url = str(hazard_image_url).split(',')[0].strip()
            
            print(f"\n{'='*70}")
            print(f"[{processed+1}/{total}] {prdt_nm}")
            print(f"SEQ: {seq}")
            print(f"{'='*70}")
            
            iherb_image, iherb_name, iherb_brand = scrape_iherb_simple(
                iherb_driver,
                iherb_url
            )
            
            if not iherb_image:
                print("  [ERROR] iHerb 이미지 없음")
                df.at[idx, 'STATUS'] = Status.VERIFICATION_FAILED
                df.at[idx, 'GEMINI_REASON'] = "No iHerb image"
                processed += 1
                save_dataframe(df, input_csv)
                continue
            
            is_match, reason = gemini.verify_images_with_retry(
                hazard_image_url=hazard_image_url,
                iherb_image_url=iherb_image,
                hazard_name=prdt_nm,
                hazard_brand=mufc_nm,
                iherb_name=iherb_name,
                iherb_brand=iherb_brand,
                seq=seq
            )
            
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
            chat_count += 1
            save_dataframe(df, input_csv)
            
            print(f"  [WAIT] Rate Limit 회피: {WAIT_AFTER_VERIFY}초 대기...")
            time.sleep(WAIT_AFTER_VERIFY)
        
        print(f"\n{'='*70}")
        print(f"[DONE] 검증 완료!")
        print(f"총 처리: {processed}건")
        print(f"매칭 성공: {success}건")
        print(f"매칭 실패: {processed - success}건")
        print(f"성공률: {success/processed*100:.1f}%")
        print(f"{'='*70}")
    
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] 중단")
        save_dataframe(df, input_csv)
    
    finally:
        try:
            print(f"\n[CLEANUP] 브라우저 종료 중...")
            iherb_driver.quit()
            gemini_driver.quit()
            print(f"[CLEANUP] ✓ 완료")
        except:
            pass


def save_dataframe(df: pd.DataFrame, output_path: str):
    """DataFrame 저장"""
    dtype_dict = {col: str for col in CSV_COLUMNS}
    full_df = pd.read_csv(output_path, encoding="utf-8-sig", dtype=dtype_dict)
    
    for idx, row in df.iterrows():
        if idx in full_df.index:
            for col in row.index:
                value = row[col]
                if pd.notna(value) and value != '':
                    full_df.at[idx, col] = str(value)
    
    full_df = full_df[CSV_COLUMNS]
    full_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"  [SAVE] 저장 완료")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    parser = argparse.ArgumentParser(description="Phase 2: Gemini Web 검증 (개선 버전)")
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