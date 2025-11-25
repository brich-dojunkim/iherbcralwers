"""
GNC 상품 검색 - 검색 결과 페이지에서 직접 추출
"""

import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
from typing import Optional
from dataclasses import dataclass


@dataclass
class GNCProduct:
    """GNC 상품 정보"""
    product_code: str
    product_name: str
    brand: str
    gnc_url: str
    thumbnail_url: Optional[str] = None
    count: Optional[int] = None


class GNCCrawler:
    """GNC 크롤러"""
    
    def __init__(self, browser_manager=None, debug: bool = False):
        if not browser_manager:
            raise ValueError("browser_manager 필요")
        
        self.browser = browser_manager
        self.driver = browser_manager.driver
        self.debug = debug
    
    def search_product(self, product_code: str) -> Optional[GNCProduct]:
        """GNC 상품 검색"""
        try:
            print(f"  GNC 검색 URL 접속...")
            
            search_url = f"https://www.gnc.com/search/?q={product_code}"
            self.driver.get(search_url)
            time.sleep(8)
            
            if self.debug:
                print(f"  [DEBUG] 현재 URL: {self.driver.current_url}")
            
            # 첫 번째 상품 찾기
            product_elem = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".product-tile"))
            )
            
            if self.debug:
                print(f"  [DEBUG] 상품 요소 발견")
            
            # 상품명 - 정확한 선택자
            product_name = ""
            try:
                name_elem = product_elem.find_element(By.CSS_SELECTOR, ".tile-product-name")
                product_name = name_elem.text.strip()
            except:
                pass
            
            if not product_name:
                print(f"  ✗ 상품명을 찾을 수 없음")
                return None
            
            print(f"  ✓ GNC 상품: {product_name[:50]}...")
            
            # URL
            product_url = ""
            try:
                link_elem = product_elem.find_element(By.CSS_SELECTOR, "a.thumb-link, a.name-link")
                product_url = link_elem.get_attribute('href')
            except:
                product_url = search_url
            
            # 썸네일
            thumbnail_url = None
            try:
                img_elem = product_elem.find_element(By.CSS_SELECTOR, "img.product-tile-img")
                thumbnail_url = img_elem.get_attribute('src')
            except:
                pass
            
            # 브랜드
            brand = "GNC"
            try:
                brand_elem = product_elem.find_element(By.CSS_SELECTOR, ".tile-brand-name")
                brand_text = brand_elem.text.strip()
                if brand_text:
                    brand = brand_text
            except:
                pass
            
            # 정수
            count = self._extract_count(product_name)
            
            return GNCProduct(
                product_code=str(product_code),
                product_name=product_name,
                brand=brand,
                gnc_url=product_url,
                thumbnail_url=thumbnail_url,
                count=count
            )
            
        except Exception as e:
            print(f"  ✗ GNC 검색 오류: {e}")
            return None
    
    def _extract_count(self, text: str) -> Optional[int]:
        """정수 추출"""
        if not text:
            return None
        
        patterns = [
            r'(\d+)\s*(?:tablets|capsules|caplets|softgels)',
            r'(\d+)\s*ct',
        ]
        
        text_lower = text.lower()
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                count = int(match.group(1))
                if 10 <= count <= 1000:
                    return count
        
        return None