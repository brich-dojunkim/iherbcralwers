#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gemini 이미지 검증
"""

import time
from pathlib import Path
from typing import Tuple

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait

from utils.selenium_utils import save_debug_info
from utils.image_utils import download_image


class GeminiVerifier:
    """Gemini 웹 인터페이스로 이미지 검증"""
    
    def __init__(self, driver, temp_dir: Path, debug_dir: Path):
        self.driver = driver
        self.wait = WebDriverWait(driver, 15)
        self.temp_dir = temp_dir
        self.debug_dir = debug_dir
        
        self.temp_dir.mkdir(exist_ok=True)
        self.debug_dir.mkdir(exist_ok=True)
        
        # 설정
        self.WAIT_AFTER_UPLOAD = 3
        self.WAIT_RETRY_BASE = 30
    
    def _wait_for_dom_stability(self, timeout: int = 5) -> bool:
        """DOM 안정화 대기 (URL 변경 감지)"""
        try:
            initial_url = self.driver.current_url
            stable_count = 0
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                time.sleep(0.5)
                current_url = self.driver.current_url
                
                if current_url == initial_url:
                    stable_count += 1
                    if stable_count >= 3:
                        return True
                else:
                    initial_url = current_url
                    stable_count = 0
            
            return False
            
        except Exception as e:
            return False
    
    def start_new_chat(self):
        """새 채팅 시작"""
        try:
            new_chat_selectors = [
                "button[aria-label='새 채팅']",
                "button[aria-label*='New chat']",
            ]
            
            for selector in new_chat_selectors:
                try:
                    btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                    btn.click()
                    time.sleep(10)
                    print(f"  [CHAT] ✓ 새 채팅 시작")
                    return True
                except:
                    continue
            
            return False
            
        except Exception as e:
            print(f"  [CHAT ERROR] {e}")
            return False
    
    def verify_images_with_retry(
        self,
        hazard_image_url: str,
        iherb_image_url: str,
        hazard_name: str,
        hazard_brand: str,
        iherb_name: str,
        iherb_brand: str,
        seq: str = "unknown",
        max_retries: int = 3
    ) -> Tuple[bool, str]:
        """재시도 로직 포함 검증"""
        
        for attempt in range(max_retries):
            try:
                return self.verify_images(
                    hazard_image_url, iherb_image_url,
                    hazard_name, hazard_brand,
                    iherb_name, iherb_brand, seq
                )
            except Exception as e:
                error_msg = str(e)
                
                is_rate_limit = any(x in error_msg.lower() for x in ["rate", "limit", "문제가 발생"])
                
                if is_rate_limit and attempt < max_retries - 1:
                    wait_time = self.WAIT_RETRY_BASE * (attempt + 1)
                    print(f"  [RETRY] {wait_time}초 대기 후 재시도 ({attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue
                
                return False, error_msg
        
        return False, "최대 재시도 횟수 초과"
    
    def verify_images(
        self,
        hazard_image_url: str,
        iherb_image_url: str,
        hazard_name: str,
        hazard_brand: str,
        iherb_name: str,
        iherb_brand: str,
        seq: str
    ) -> Tuple[bool, str]:
        """이미지 검증"""
        
        try:
            # 브라우저 체크
            try:
                _ = self.driver.current_url
            except Exception as e:
                raise Exception(f"브라우저가 닫혀 있습니다: {e}")
            
            # 1. 이미지 다운로드
            print(f"  [IMAGE] 이미지 다운로드 중...")
            
            hazard_img_path = self.temp_dir / f"hazard_{seq}.jpg"
            iherb_img_path = self.temp_dir / f"iherb_{seq}.jpg"
            
            if not download_image(hazard_image_url, hazard_img_path):
                raise Exception("위해식품 이미지 다운로드 실패")
            
            if not download_image(iherb_image_url, iherb_img_path):
                raise Exception("iHerb 이미지 다운로드 실패")
            
            # 2. 이미지 업로드
            print(f"  [UPLOAD] 이미지 업로드 중...")
            
            self._upload_image(str(hazard_img_path), seq, "image1")
            time.sleep(2)
            self._upload_image(str(iherb_img_path), seq, "image2")
            
            print(f"  [UPLOAD] ✓ 업로드 완료")
            
            # 3. 프롬프트 입력 및 전송
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
            
            self._input_text(input_box, prompt)
            
            print(f"  [SUBMIT] 전송 중...")
            self._submit_message()
            print(f"  [SUBMIT] ✓ 전송 완료")
            
            # 4. 응답 대기
            print(f"  [RESPONSE] 응답 대기 중...")
            is_match, reason = self._wait_for_response()
            
            # 임시 파일 삭제
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
                # JavaScript로 오버레이 제거
                self.driver.execute_script("""
                    document.querySelectorAll('.cdk-overlay-backdrop, .cdk-overlay-pane, .cdk-overlay-container, [role="menu"], [role="dialog"]').forEach(el => {
                        try { el.remove(); } catch(e) {}
                    });
                    
                    document.querySelectorAll('input[type="file"]').forEach(f => {
                        try {
                            f.value = '';
                            f.style.display = 'none';
                        } catch(e) {}
                    });
                """)
                
                time.sleep(0.5)
                
                # ESC 키 한 번만 전송
                try:
                    body = self.driver.find_element(By.TAG_NAME, 'body')
                    body.send_keys(Keys.ESCAPE)
                except:
                    pass
                
                time.sleep(0.5)
                
            except Exception as cleanup_error:
                pass
    
    def _upload_image(self, image_path: str, seq: str, img_num: str):
        """이미지 업로드"""
        try:
            file_input = None
            
            # 기존 file input 찾기
            inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")
            for inp in inputs:
                accept = inp.get_attribute('accept')
                if not accept or 'image' in accept:
                    file_input = inp
                    break
            
            # 없으면 생성
            if not file_input:
                self.driver.execute_script("""
                    var input = document.createElement('input');
                    input.type = 'file';
                    input.accept = 'image/*';
                    input.style.position = 'fixed';
                    input.style.top = '-1000px';
                    input.id = 'upload-' + Date.now();
                    document.body.appendChild(input);
                """)
                inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[id^='upload-']")
                if inputs:
                    file_input = inputs[-1]
            
            if not file_input:
                raise Exception("file input을 찾거나 생성할 수 없습니다")
            
            # 파일 경로 전달
            file_input.send_keys(image_path)
            
            # DOM 안정화 대기
            time.sleep(2)
            self._wait_for_dom_stability(timeout=5)
            time.sleep(self.WAIT_AFTER_UPLOAD)
            
        except Exception as e:
            print(f"  [UPLOAD ERROR] {img_num}: {e}")
            save_debug_info(self.driver, f"upload_failed_{img_num}", seq, self.debug_dir)
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
    
    def _input_text(self, element, text: str):
        """텍스트 입력"""
        self.driver.execute_script("""
            var element = arguments[0];
            var text = arguments[1];
            element.innerText = text;
            element.textContent = text;
            element.dispatchEvent(new Event('input', { bubbles: true }));
            element.dispatchEvent(new Event('change', { bubbles: true }));
        """, element, text)
        time.sleep(1)
    
    def _submit_message(self):
        """전송"""
        try:
            button = self.driver.find_element(By.CSS_SELECTOR, 
                "button[aria-label='메시지 보내기'], button[aria-label*='Send']")
            if button.is_displayed() and button.is_enabled():
                button.click()
                time.sleep(2)
                return
        except:
            pass
        
        try:
            self.driver.execute_script("""
                var button = document.querySelector("button[aria-label='메시지 보내기']") ||
                             document.querySelector("button.send-button");
                if (button) {
                    button.click();
                }
            """)
            time.sleep(2)
        except:
            pass
    
    def _wait_for_response(self) -> Tuple[bool, str]:
        """응답 대기"""
        max_wait = 60
        start_time = time.time()
        
        # 새 메시지 텍스트 감지
        new_message_timeout = 40
        
        while time.time() - start_time < new_message_timeout:
            try:
                messages = self.driver.find_elements(By.TAG_NAME, "message-content")
                if not messages:
                    time.sleep(1)
                    continue
                
                last_message = messages[-1]
                markdown = last_message.find_element(By.CSS_SELECTOR, ".markdown")
                
                text = markdown.get_attribute("textContent") or ""
                text = text.strip()
                
                if len(text) > 10:
                    break
            except:
                pass
            
            time.sleep(1)
        else:
            raise Exception("새 응답 감지 타임아웃 (40초)")
        
        # 응답 완료 대기
        while time.time() - start_time < max_wait:
            try:
                messages = self.driver.find_elements(By.TAG_NAME, "message-content")
                last_message = messages[-1]
                markdown = last_message.find_element(By.CSS_SELECTOR, ".markdown")
                
                aria_live = markdown.get_attribute("aria-live")
                
                if aria_live == "polite":
                    text = markdown.get_attribute("textContent").strip()
                    
                    print(f"  [RESPONSE] ✓ 응답 완료 (길이: {len(text)} chars)")
                    
                    # YES/NO 판정
                    first_word = text.split()[0].upper().rstrip(':,.')
                    is_match = first_word == 'YES' or text.upper().startswith('YES')
                    
                    print(f"  [RESPONSE] 판정: {'YES' if is_match else 'NO'}")
                    
                    return is_match, text
                
                time.sleep(2)
            except:
                time.sleep(1)
        
        raise Exception("응답 완료 타임아웃 (60초)")