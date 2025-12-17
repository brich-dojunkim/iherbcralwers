#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Images 역검색
"""

import time
from pathlib import Path
from typing import Optional, Dict
from collections import Counter
from urllib.parse import urlparse, parse_qs, unquote

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class GoogleImageSearch:
    """Google 이미지 역검색"""
    
    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(driver, 25)
    
    def find_iherb_url(self, image_path: Path) -> Optional[str]:
        """
        Google Images 역검색으로 iHerb URL 찾기
        
        Args:
            image_path: 검색할 이미지 파일 경로
            
        Returns:
            iHerb URL or None
        """
        try:
            print(f"  [GOOGLE] 이미지 업로드 중...")
            self.driver.get("https://images.google.com/")
            time.sleep(2)
            
            # 카메라 버튼 클릭
            camera_btn = self.wait.until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "div[aria-label*='Search by image' i], div[aria-label*='이미지로 검색' i]"
                ))
            )
            camera_btn.click()
            time.sleep(1)
            
            # 이미지 업로드
            file_input = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']"))
            )
            file_input.send_keys(str(image_path.resolve()))
            print(f"  [UPLOAD] ✓ 업로드 완료")
            
            time.sleep(3)  # 업로드 후 대기
            
            # 스크롤 트릭 (크롭 UI 방지)
            self.driver.execute_script("window.scrollTo(0, 800);")
            time.sleep(0.5)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            print(f"  [REVERSE] ✓ 역검색 완료")
            
            # 검색창에 'iherb' 입력
            search_box = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='q'], textarea[name='q']"))
            )
            self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='q'], textarea[name='q']")))
            search_box.click()
            search_box.clear()
            search_box.send_keys("iherb")
            search_box.send_keys(Keys.ENTER)
            
            # 결과 대기
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "a")))
            time.sleep(5)  # 검색 결과 로드 대기
            
            print(f"  [SEARCH] ✓ 검색 완료")
            
            # URL 선택
            best_url = self._select_best_url()
            
            if best_url:
                print(f"  [SUCCESS] ✓ URL 발견")
                return best_url
            
            return None
            
        except Exception as e:
            print(f"  [ERROR] 검색 실패: {e}")
            return None
    
    def _select_best_url(self) -> Optional[str]:
        """
        최고 iHerb URL 선택 (투표 방식)
        
        Returns:
            선택된 URL or None
        """
        # 모든 링크 수집
        links = self.driver.find_elements(By.CSS_SELECTOR, "a")
        
        print(f"  [LINKS] 발견된 링크 수: {len(links)}")
        
        # /pr/ 경로만 수집
        pr_urls = []
        
        for link in links:
            href = link.get_attribute("href")
            if not href:
                continue
            
            # 실제 URL 추출
            real_url = self._extract_real_url(href)
            
            # iHerb 도메인 체크
            if "iherb.com" not in real_url.lower():
                continue
            
            # /pr/ 경로만 (제품 페이지)
            if "/pr/" in real_url:
                pr_urls.append(real_url)
        
        if not pr_urls:
            print(f"  [CANDIDATES] /pr/ 제품 페이지 없음")
            return None
        
        print(f"  [CANDIDATES] /pr/ 제품 페이지 {len(pr_urls)}개 수집")
        
        # Product Code 투표
        code_votes = Counter()
        url_by_code = {}
        
        for url in pr_urls:
            code = self._extract_product_code(url)
            if code:
                code_votes[code] += 1
                # kr.iherb.com 우선
                if code not in url_by_code or "kr.iherb.com" in url:
                    url_by_code[code] = url
        
        if not code_votes:
            return None
        
        # 최다 득표
        winner_code, votes = code_votes.most_common(1)[0]
        winner_url = url_by_code[winner_code]
        
        print(f"  [VOTE] Product Code: {winner_code} ({votes}표)")
        
        # 도메인 표시
        if "kr.iherb.com" in winner_url:
            print(f"  [SELECT] kr.iherb.com 선택")
        else:
            print(f"  [SELECT] www.iherb.com 선택")
        
        return winner_url
    
    def _extract_real_url(self, google_url: str) -> str:
        """
        구글 redirect URL에서 실제 URL 추출
        
        Args:
            google_url: Google redirect URL
            
        Returns:
            실제 URL
        """
        try:
            if "/url?q=" in google_url:
                parsed = urlparse(google_url)
                params = parse_qs(parsed.query)
                if 'q' in params:
                    return unquote(params['q'][0])
            return google_url
        except:
            return google_url
    
    def _extract_product_code(self, url: str) -> Optional[str]:
        """
        URL에서 Product Code 추출
        
        Args:
            url: iHerb URL
            
        Returns:
            Product code or None
        """
        try:
            # URL 패턴: https://kr.iherb.com/pr/product-name/12345
            parts = url.rstrip('/').split('/')
            if len(parts) > 0:
                last_part = parts[-1]
                code = ''.join(filter(str.isdigit, last_part))
                if code:
                    return code
            return None
        except:
            return None
    
    def extract_ai_overview(self) -> Optional[Dict[str, str]]:
        """
        Google AI Overview 추출
        
        현재 검색 결과 페이지에서 AI Overview 정보를 추출합니다.
        find_iherb_url() 실행 직후 호출해야 합니다.
        
        Returns:
            {'product_name': str, 'full_text': str} or None
        """
        try:
            # AI Overview 컨테이너 찾기
            # Google은 여러 CSS 클래스를 사용할 수 있음
            ai_section = None
            
            # 시도 1: data-subtree 속성
            try:
                ai_section = self.driver.find_element(
                    By.CSS_SELECTOR,
                    "div[data-subtree='mfc']"
                )
            except:
                pass
            
            # 시도 2: 특정 클래스
            if not ai_section:
                try:
                    ai_section = self.driver.find_element(
                        By.CSS_SELECTOR,
                        "div.s7d4ef, div.kno-rdesc, div[data-attrid='kc:/medicine/drug:overview']"
                    )
                except:
                    pass
            
            if not ai_section:
                # AI Overview 없음
                return None
            
            # 제품명 추출
            product_name = "Unknown"
            try:
                # 헤더 텍스트 찾기
                header = ai_section.find_element(
                    By.CSS_SELECTOR,
                    "span[aria-level='2'], h2, h3"
                )
                product_name = header.text.strip()
            except:
                pass
            
            # 전체 텍스트 추출
            full_text = ai_section.text.strip()
            
            if not full_text:
                return None
            
            print(f"  [AI OVERVIEW] ✓ 추출 완료 ({len(full_text)} chars)")
            
            return {
                'product_name': product_name,
                'full_text': full_text
            }
            
        except Exception as e:
            # AI Overview가 없는 경우는 정상적인 상황
            # 에러 메시지 출력하지 않음
            return None