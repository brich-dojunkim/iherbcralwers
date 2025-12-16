#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google 이미지 검색
"""

import time
from pathlib import Path
from typing import Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils.selenium_utils import extract_real_url


class GoogleImageSearch:
    """Google 이미지 역검색"""
    
    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(driver, 25)
    
    def find_iherb_url(self, image_path: Path) -> Optional[str]:
        """Google Images 역검색으로 iHerb URL 찾기"""
        try:
            self.driver.get("https://images.google.com/")
            time.sleep(1.5)

            # 카메라 버튼
            try:
                camera_btn = self.wait.until(
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
                file_input = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']"))
                )
                file_input.send_keys(str(image_path.resolve()))
            except:
                return None

            time.sleep(1)
            
            # 스크롤 트릭
            self.driver.execute_script("window.scrollTo(0, 800);")
            time.sleep(0.5)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            # 검색창에 'iherb' 입력
            try:
                search_box = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='q'], textarea[name='q']"))
                )
                self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='q'], textarea[name='q']")))
                search_box.click()
                search_box.clear()
                search_box.send_keys("iherb")
                search_box.send_keys(Keys.ENTER)
            except:
                return None

            # 결과 대기
            try:
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "a")))
                time.sleep(2)
            except:
                time.sleep(3)

            # 첫 번째 유효한 iHerb 링크 찾기
            links = self.driver.find_elements(By.CSS_SELECTOR, "a")
            
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