#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
iHerb 상세페이지 스크래퍼
"""

import time
from typing import Optional, Dict

from selenium.webdriver.common.by import By


class IHerbScraper:
    """iHerb 상세페이지에서 제품 정보 추출"""
    
    def __init__(self, driver):
        self.driver = driver
    
    def scrape_product(self, url: str) -> Dict:
        """
        iHerb 상세페이지 스크래핑
        
        Returns:
            {
                'image_url': str,
                'product_name': str,
                'brand': str
            }
        """
        try:
            print(f"  [SCRAPER] iHerb 방문 중...")
            self.driver.get(url)
            time.sleep(2)
            
            # 메인 이미지
            image_url = self._extract_main_image()
            
            # 제품명
            product_name = self._extract_product_name()
            
            # 브랜드
            brand = self._extract_brand()
            
            if image_url and product_name and brand:
                print(f"  [SCRAPER] ✓ 스크래핑 완료: {brand} - {product_name}")
            
            return {
                'image_url': image_url,
                'product_name': product_name,
                'brand': brand
            }
            
        except Exception as e:
            print(f"  [SCRAPER ERROR] {e}")
            return {
                'image_url': None,
                'product_name': None,
                'brand': None
            }
    
    def _extract_main_image(self) -> Optional[str]:
        """메인 이미지 URL 추출"""
        # 방법 1: src 속성
        try:
            img = self.driver.find_element(By.CSS_SELECTOR, "#iherb-product-image")
            src = img.get_attribute("src")
            if src and "cloudinary.images-iherb.com" in src and len(src) > 100:
                return src
        except:
            pass
        
        # 방법 2: srcset 속성에서 1.5x 버전 추출
        try:
            img = self.driver.find_element(By.CSS_SELECTOR, "#iherb-product-image")
            srcset = img.get_attribute("srcset")
            if srcset:
                parts = srcset.split(',')
                for part in parts:
                    part = part.strip()
                    if '1.5x' in part or '2x' in part:
                        url = part.split()[0]
                        if "cloudinary.images-iherb.com" in url:
                            return url
        except:
            pass
        
        # 방법 3: JavaScript로 currentSrc 가져오기
        try:
            img = self.driver.find_element(By.CSS_SELECTOR, "#iherb-product-image")
            current_src = self.driver.execute_script("return arguments[0].currentSrc;", img)
            if current_src and "cloudinary.images-iherb.com" in current_src:
                return current_src
        except:
            pass
        
        return None
    
    def _extract_product_name(self) -> Optional[str]:
        """제품명 추출"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, "h1#name")
            return element.text.strip()
        except:
            pass
        return None
    
    def _extract_brand(self) -> Optional[str]:
        """브랜드명 추출"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, "#brand a")
            return element.text.strip()
        except:
            pass
        return None