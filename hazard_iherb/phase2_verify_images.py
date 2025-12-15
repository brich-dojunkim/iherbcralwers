#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2: Gemini Web 이미지 검증 (듀얼 브라우저 버전)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 브라우저 1: iHerb 전용 (스크래핑)
- 브라우저 2: Gemini 전용 (검증)
- 20개마다 새 채팅 시작
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

# macOS Finder 다이얼로그 닫기용 (선택적)
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
MAX_MESSAGES_PER_CHAT = 20  # 채팅당 최대 검증 개수

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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Selenium 드라이버
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def create_driver(headless: bool = False, profile_name: str = "default"):
    """undetected-chromedriver 생성 (안정적인 프로필)"""
    options = uc.ChromeOptions()
    
    # 사용자 홈 디렉토리에 프로필 생성 (macOS 안정성)
    profile_base = Path.home() / ".chrome_automation_profiles"
    profile_base.mkdir(parents=True, exist_ok=True)
    
    profile_dir = profile_base / profile_name
    profile_dir.mkdir(parents=True, exist_ok=True)
    
    options.add_argument(f"--user-data-dir={profile_dir}")
    
    if headless:
        options.add_argument("--headless=new")
    
    # 안정성 및 자동 재시작 방지 옵션
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1400,1000")
    
    # UC 드라이버 생성 (기본 설정)
    driver = uc.Chrome(options=options, version_main=None)
    driver.set_page_load_timeout(60)
    
    return driver


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# iHerb 스크래퍼
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def scrape_iherb_simple(driver, url: str) -> tuple:
    """
    iHerb에서 메인 이미지와 제품명, 브랜드 추출
    
    Args:
        driver: iHerb 전용 브라우저 드라이버
        url: iHerb 제품 URL
        
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
                        time.sleep(2)
                        print(f"  [CHAT] ✓ 새 채팅 시작")
                        return True
                except:
                    continue
            
            # JavaScript로 시도
            self.driver.execute_script("""
                var btn = document.querySelector("button[aria-label*='새 채팅']");
                if (btn) btn.click();
            """)
            time.sleep(2)
            print(f"  [CHAT] ✓ 새 채팅 시작 (JS)")
            return True
            
        except Exception as e:
            print(f"  [CHAT ERROR] 새 채팅 실패: {e}")
            return False
    
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
            # 브라우저 생존 재확인 (중요!)
            try:
                _ = self.driver.current_url
            except Exception as e:
                raise Exception(f"브라우저가 닫혀 있습니다 (verify 시작 시): {e}")
            
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
            
            # 첫 번째 이미지 업로드
            self._upload_image(str(hazard_img_path), seq, "image1")
            
            # 두 번째 이미지 업로드
            self._upload_image(str(iherb_img_path), seq, "image2")
            
            print(f"  [UPLOAD] ✓ 2개 이미지 업로드 완료")
            
            # ===== 3. 텍스트 프롬프트 입력 =====
            prompt = f"""Compare these two product images I just uploaded:

IMAGE 1 (Korean Hazard Product):
- Product: {hazard_name}
- Brand: {hazard_brand}

IMAGE 2 (iHerb Product):
- Product: {iherb_name}
- Brand: {iherb_brand}

IMPORTANT: Answer with EXACTLY ONE sentence in this format:
- "YES: [brief reason]" if they are the same product
- "NO: [brief reason]" if they are different products

Do NOT ask follow-up questions. Do NOT offer to search for more information.

Examples:
✓ "YES: Both show NOW Foods Vitamin D3 5000 IU"
✓ "NO: Different brands (Nature's Craft vs Sunergetic)"

Your answer:"""
            
            # 입력창 찾기
            print(f"  [TEXT] 텍스트 입력 중...")
            input_box = self._find_input_box()
            
            if not input_box:
                raise Exception("입력창 없음")
            
            # 텍스트 입력
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
            
            # 임시 파일 삭제
            try:
                hazard_img_path.unlink()
                iherb_img_path.unlink()
            except:
                pass
            
            return is_match, reason
            
        except Exception as e:
            error_msg = str(e)
            
            # 브라우저 닫힘 에러인지 확인
            if "no such window" in error_msg.lower() or "브라우저가 닫혀" in error_msg:
                print(f"  [ERROR] ✗ 브라우저가 닫혀 있습니다!")
                print(f"  [ERROR] 작업 중에 브라우저 창을 닫지 마세요.")
                error_msg = "브라우저가 사용자에 의해 닫힘"
            else:
                print(f"  [ERROR] {error_msg}")
            
            return False, error_msg
    
    def _upload_image(self, image_path: str, seq: str, img_num: str):
        """이미지 업로드"""
        try:
            # 브라우저 생존 확인
            try:
                _ = self.driver.current_url
            except:
                raise Exception("브라우저가 닫혀 있습니다. 브라우저를 닫지 마세요!")
            
            # 1단계: 파일 업로드 메뉴 열기 버튼 클릭
            print(f"  [UPLOAD] 1단계: 업로드 메뉴 열기")
            try:
                upload_menu_btn = self.driver.find_element(By.CSS_SELECTOR, 
                    "button[aria-label*='파일 업로드 메뉴']")
                upload_menu_btn.click()
                time.sleep(1)
                print(f"  [UPLOAD] ✓ 메뉴 열림")
                
                # 디버그: 메뉴 열린 후 상태 저장
                save_debug_info(self.driver, f"menu_opened_{img_num}", seq)
                
            except Exception as e:
                print(f"  [UPLOAD] 메뉴 열기 실패: {e}")
                self.driver.execute_script("""
                    var btn = document.querySelector("button[aria-label*='파일 업로드']");
                    if (btn) btn.click();
                """)
                time.sleep(1)
            
            # 2단계: "파일 업로드" 메뉴 항목 클릭
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
                                time.sleep(1)
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
                    time.sleep(1)
                
                print(f"  [UPLOAD] ✓ 메뉴 항목 클릭 완료")
                
            except Exception as e:
                print(f"  [UPLOAD] 메뉴 항목 클릭 실패: {e}")
            
            # 3단계: file input 찾아서 파일 전달 (재시도 로직)
            print(f"  [UPLOAD] 3단계: file input에 파일 전달")
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # DOM 안정화 대기
                    if attempt > 0:
                        time.sleep(0.5)
                    
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
                                file_input = inp
                                break
                            if file_input:
                                break
                        except:
                            continue
                    
                    if not file_input:
                        raise Exception("file input을 찾을 수 없습니다")
                    
                    # 파일 경로 전달
                    file_input.send_keys(image_path)
                    time.sleep(1)
                    
                    # macOS Finder 다이얼로그 닫기 (여러 방법 시도)
                    finder_closed = False
                    
                    if PYAUTOGUI_AVAILABLE:
                        try:
                            # 방법 1: Return 키 (가장 안전 - "열기" 버튼 클릭)
                            pyautogui.press('return')
                            time.sleep(0.5)
                            
                            # 방법 2: 여전히 열려있으면 ESC
                            pyautogui.press('escape')
                            time.sleep(0.3)
                            
                            finder_closed = True
                            print(f"  [UPLOAD] ✓ {img_num} 업로드 완료 (Finder 닫음)")
                        except Exception as e:
                            print(f"  [UPLOAD] ✓ {img_num} 업로드 완료 (Finder 닫기 시도: {e})")
                    
                    # PyAutoGUI 없거나 실패 시 AppleScript 시도
                    if not finder_closed:
                        try:
                            import subprocess
                            # ESC 키로 Finder 닫기
                            subprocess.run([
                                'osascript', '-e',
                                'tell application "System Events" to key code 53'  # ESC 키코드
                            ], capture_output=True, timeout=2)
                            time.sleep(0.3)
                            print(f"  [UPLOAD] ✓ {img_num} 업로드 완료 (Finder 닫음 - AppleScript)")
                        except:
                            time.sleep(1)
                            print(f"  [UPLOAD] ✓ {img_num} 업로드 완료")
                    
                    # 디버그: 파일 업로드 직후 상태 저장 (메뉴 상태 확인용)
                    save_debug_info(self.driver, f"after_upload_{img_num}", seq)
                    
                    # 메뉴 상태 확인
                    try:
                        menu_check = self.driver.execute_script("""
                            var menu = document.querySelector('[role="menu"]');
                            var backdrop = document.querySelector('.cdk-overlay-backdrop');
                            return {
                                menuExists: !!menu,
                                menuVisible: menu ? menu.offsetParent !== null : false,
                                backdropExists: !!backdrop,
                                backdropVisible: backdrop ? backdrop.offsetParent !== null : false
                            };
                        """)
                        print(f"  [DEBUG] 메뉴 상태: {menu_check}")
                    except:
                        pass
                    
                    break  # 성공하면 루프 종료
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"  [UPLOAD] 재시도 {attempt + 1}/{max_retries}...")
                        continue
                    else:
                        # 마지막 시도 실패
                        raise
            
        except Exception as e:
            print(f"  [UPLOAD ERROR] {e}")
            save_debug_info(self.driver, f"upload_failed_{img_num}", seq)
            raise
    
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
                                            
                                            # 첫 문장만 추출
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
        """
        응답에서 첫 문장만 추출
        "Would you like me to..." 같은 후속 질문 제거
        """
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
    """이미지 검증 처리 (듀얼 브라우저)"""
    
    if not Path(input_csv).exists():
        print(f"[ERROR] CSV 파일 없음: {input_csv}")
        return
    
    print(f"\n{'='*70}")
    print(f"Gemini Web 이미지 검증 (듀얼 브라우저 버전)")
    print(f"{'='*70}\n")
    print(f"[INFO] 디버그 파일: {DEBUG_DIR}")
    print(f"[INFO] 임시 이미지: {TEMP_DIR}")
    print(f"[INFO] 채팅당 최대 {MAX_MESSAGES_PER_CHAT}개 처리\n")
    
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
    
    # ===== 브라우저 2개 생성 =====
    print(f"\n{'='*70}")
    print(f"[BROWSER] 브라우저 2개 생성 중...")
    print(f"{'='*70}\n")
    
    # 1. iHerb 전용 브라우저
    print(f"[BROWSER] 1. iHerb 브라우저 생성...")
    iherb_driver = create_driver(headless=headless, profile_name="iHerbBot")
    
    # iHerb는 자동 접속 유지 (문제 없음)
    iherb_driver.get("https://kr.iherb.com")
    time.sleep(2)
    
    print(f"[BROWSER] ✓ iHerb 브라우저 준비 완료\n")
    
    # 첫 번째 브라우저 완전 초기화 대기
    time.sleep(3)
    
    # 2. Gemini 전용 브라우저
    print(f"[BROWSER] 2. Gemini 브라우저 생성...")
    gemini_driver = create_driver(headless=headless, profile_name="GeminiBot")
    
    # Gemini 페이지로 직접 이동 (프로필 경로 수정으로 안정화)
    print(f"[BROWSER] Gemini 페이지 로드 중...")
    try:
        gemini_driver.get("https://gemini.google.com")
        time.sleep(3)
        print(f"[BROWSER] ✓ Gemini 브라우저 준비 완료\n")
    except Exception as e:
        print(f"[BROWSER ERROR] Gemini 페이지 로드 실패: {e}")
        print(f"[BROWSER] 브라우저는 열렸으나 수동으로 gemini.google.com으로 이동하세요.\n")
    
    save_debug_info(gemini_driver, "initial_page", "login")
    
    # 로그인 확인
    print(f"{'='*70}")
    print(f"[LOGIN] Gemini 로그인 확인")
    print(f"{'='*70}\n")
    print(f"Gemini 브라우저에서:")
    print(f"1. 이미 로그인되어 있으면 → 바로 Enter")
    print(f"2. 로그인 필요하면 → Google 계정으로 로그인 → Enter\n")
    print(f"⚠️  중요: Enter 누른 후 브라우저 창을 절대 닫지 마세요!")
    print(f"⚠️  자동화가 진행되는 동안 브라우저를 그대로 두세요.\n")
    input(f"준비되면 Enter를 눌러주세요...")
    print(f"{'='*70}\n")
    
    print(f"[LOGIN] ✓ 계속 진행합니다\n")
    
    # 로그인 후 안정화 대기
    print(f"[WAIT] 세션 안정화 중...")
    time.sleep(3)
    print(f"[WAIT] ✓ 안정화 완료\n")
    
    # 브라우저 생존 확인
    try:
        current_url = gemini_driver.current_url
        print(f"[CHECK] ✓ Gemini 브라우저 정상 작동 중")
        if current_url and current_url != "about:blank":
            print(f"[CHECK] 현재 URL: {current_url[:60]}...\n")
        else:
            print(f"[CHECK] 빈 페이지 상태 (정상)\n")
    except Exception as e:
        print(f"[ERROR] ✗ Gemini 브라우저에 문제가 있습니다!")
        print(f"[ERROR] 상세: {e}\n")
        iherb_driver.quit()
        return
    
    # Gemini 인터페이스 초기화
    gemini = GeminiWebInterface(gemini_driver)
    
    # 첫 번째 작업 전 추가 안정화
    print(f"[READY] 첫 번째 작업 준비 중...")
    time.sleep(2)
    print(f"[READY] ✓ 작업 시작\n")
    
    try:
        total = len(df)
        processed = 0
        success = 0
        chat_count = 0  # 현재 채팅에서 처리한 개수
        
        for idx, row in df.iterrows():
            # 20개마다 새 채팅 시작
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
            
            # 쉼표로 구분된 여러 URL 중 첫 번째만
            if pd.notna(hazard_image_url) and ',' in str(hazard_image_url):
                hazard_image_url = str(hazard_image_url).split(',')[0].strip()
            
            print(f"\n{'='*70}")
            print(f"[{processed+1}/{total}] {prdt_nm}")
            print(f"SEQ: {seq}")
            print(f"{'='*70}")
            
            # iHerb 스크래핑 (브라우저 1 - 별도)
            iherb_image, iherb_name, iherb_brand = scrape_iherb_simple(
                iherb_driver,  # ← iHerb 전용 브라우저
                iherb_url
            )
            
            if not iherb_image:
                print("  [ERROR] iHerb 이미지 없음")
                df.at[idx, 'STATUS'] = Status.VERIFICATION_FAILED
                df.at[idx, 'GEMINI_REASON'] = "No iHerb image"
                processed += 1
                save_dataframe(df, input_csv)
                continue
            
            # Gemini 검증 (브라우저 2 - 별도, 복귀 불필요!)
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
            chat_count += 1  # 채팅 카운트 증가
            save_dataframe(df, input_csv)
            
            time.sleep(2)
        
        print(f"\n{'='*70}")
        print(f"[DONE] 검증 완료!")
        print(f"총 처리: {processed}건")
        print(f"매칭 성공: {success}건")
        print(f"매칭 실패: {processed - success}건")
        print(f"사용된 채팅방: {(processed // MAX_MESSAGES_PER_CHAT) + 1}개")
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
    parser = argparse.ArgumentParser(description="Phase 2: Gemini Web 검증 (듀얼 브라우저)")
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