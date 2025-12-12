#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
iHerb 상세페이지 스크래퍼
"""

import time
import json
from typing import Optional, List, Dict
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class IHerbScraper:
    """iHerb 상세페이지에서 제품 정보 추출"""
    
    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(driver, 15)
    
    def scrape_product_page(self, url: str) -> Optional[Dict]:
        """
        iHerb 상세페이지 크롤링
        
        Args:
            url: iHerb 상품 URL
            
        Returns:
            {
                'product_name': str,
                'brand': str,
                'product_code': str,
                'main_image': str,
                'thumbnail_images': List[str]
            }
        """
        try:
            print(f"  [SCRAPER] 페이지 로딩: {url}")
            self.driver.get(url)
            time.sleep(2)  # 페이지 로딩 대기
            
            # 제품명 추출
            product_name = self._extract_product_name()
            
            # 브랜드 추출
            brand = self._extract_brand()
            
            # 상품코드 추출
            product_code = self._extract_product_code(url)
            
            # 이미지 추출
            main_image = self._extract_main_image()
            thumbnail_images = self._extract_thumbnail_images()
            
            result = {
                'product_name': product_name,
                'brand': brand,
                'product_code': product_code,
                'main_image': main_image,
                'thumbnail_images': thumbnail_images,
                'all_images': [main_image] + thumbnail_images if main_image else thumbnail_images
            }
            
            print(f"  [SCRAPER] 성공: {product_name}")
            print(f"  [SCRAPER] 이미지 {len(result['all_images'])}개 추출")
            
            return result
            
        except Exception as e:
            print(f"  [SCRAPER ERROR] {e}")
            return None
    
    def _extract_product_name(self) -> Optional[str]:
        """제품명 추출"""
        try:
            element = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1#name"))
            )
            return element.text.strip()
        except:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, ".product-summary-title")
                return element.text.strip()
            except:
                return None
    
    def _extract_brand(self) -> Optional[str]:
        """브랜드명 추출"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, "#brand a span")
            return element.text.strip()
        except:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, "#brand a")
                return element.text.strip()
            except:
                return None
    
    def _extract_product_code(self, url: str) -> Optional[str]:
        """URL에서 상품코드 추출"""
        try:
            # URL 패턴: https://kr.iherb.com/pr/product-name/12345
            parts = url.rstrip('/').split('/')
            if len(parts) > 0:
                # 마지막 부분에서 숫자 추출
                last_part = parts[-1]
                code = ''.join(filter(str.isdigit, last_part))
                if code:
                    return code
            return None
        except:
            return None
    
    def _extract_main_image(self) -> Optional[str]:
        """메인 이미지 URL 추출"""
        try:
            # 방법 1: #iherb-product-image
            img = self.driver.find_element(By.CSS_SELECTOR, "#iherb-product-image")
            src = img.get_attribute("src")
            if src and "cloudinary.images-iherb.com" in src:
                # 고해상도 버전으로 변경 (v → l)
                return src.replace("/v/", "/l/")
        except:
            pass
        
        try:
            # 방법 2: .product-summary-image img
            img = self.driver.find_element(By.CSS_SELECTOR, ".product-summary-image img")
            src = img.get_attribute("src")
            if src and "cloudinary.images-iherb.com" in src:
                return src.replace("/v/", "/l/")
        except:
            pass
        
        return None
    
    def _extract_thumbnail_images(self) -> List[str]:
        """썸네일 이미지들 추출"""
        images = []
        
        try:
            # .thumbnail-item img들 추출
            thumbnails = self.driver.find_elements(By.CSS_SELECTOR, ".thumbnail-item img")
            
            for thumb in thumbnails:
                # data-large-img 속성 우선
                large_img = thumb.get_attribute("data-large-img")
                if large_img and "cloudinary.images-iherb.com" in large_img:
                    # 배너 이미지 제외
                    if not any(x in large_img for x in ["dPDP_Authenticity", "dPDP_Fresh", "banner"]):
                        images.append(large_img)
                        continue
                
                # src 속성
                src = thumb.get_attribute("src")
                if src and "cloudinary.images-iherb.com" in src:
                    if not any(x in src for x in ["dPDP_Authenticity", "dPDP_Fresh", "banner"]):
                        # 고해상도로 변경
                        high_res = src.replace("/r/", "/l/").replace("/s/", "/l/")
                        images.append(high_res)
        except Exception as e:
            print(f"  [SCRAPER WARN] 썸네일 추출 실패: {e}")
        
        # 중복 제거
        return list(dict.fromkeys(images))
    
    def get_best_comparison_image(self, scraped_data: Dict) -> Optional[str]:
        """
        비교에 가장 적합한 이미지 선택
        
        우선순위:
        1. 메인 이미지
        2. 첫 번째 썸네일
        """
        if scraped_data.get('main_image'):
            return scraped_data['main_image']
        
        thumbnails = scraped_data.get('thumbnail_images', [])
        if thumbnails:
            return thumbnails[0]
        
        return None
