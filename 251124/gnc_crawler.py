"""
GNC 상품 검색 및 정보 수집 모듈
기존 프로젝트의 BrowserManager 사용
"""

import sys
import os

# 프로젝트 루트 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
from typing import Optional, Dict
from dataclasses import dataclass


@dataclass
class GNCProduct:
    """GNC 상품 정보"""
    product_code: str
    product_name: str
    brand: str
    gnc_url: str
    thumbnail_url: Optional[str] = None
    count: Optional[int] = None  # 정수 (캡슐/정 개수)
    description: Optional[str] = None
    category: Optional[str] = None


class GNCCrawler:
    """GNC 웹사이트 크롤러 - BrowserManager 사용"""
    
    def __init__(self, browser_manager=None):
        """
        Args:
            browser_manager: 기존 BrowserManager 인스턴스 (필수)
        """
        if not browser_manager:
            raise ValueError("browser_manager가 필요합니다")
        
        self.browser = browser_manager
        self.driver = browser_manager.driver
    
    def search_product(self, product_code: str) -> Optional[GNCProduct]:
        """
        GNC에서 상품 코드로 검색 (검색창 입력 방식)
        
        Args:
            product_code: GNC 상품 코드
            
        Returns:
            GNCProduct 또는 None
        """
        try:
            # GNC 메인 페이지 접속
            print(f"  GNC 메인 페이지 접속...")
            self.driver.get("https://www.gnc.com")
            time.sleep(3)
            
            # 검색창 찾기 및 입력
            try:
                print(f"  검색창에 상품코드 입력: {product_code}")
                
                # 검색창 선택자들 시도
                search_selectors = [
                    "input[name='q']",
                    "input[type='search']",
                    "input.search-field",
                    "#q",
                    ".search-input"
                ]
                
                search_input = None
                for selector in search_selectors:
                    try:
                        search_input = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        break
                    except:
                        continue
                
                if not search_input:
                    print(f"  ✗ 검색창을 찾을 수 없음")
                    return None
                
                # 검색어 입력
                search_input.clear()
                search_input.send_keys(str(product_code))
                time.sleep(1)
                
                # 엔터 또는 검색 버튼 클릭
                from selenium.webdriver.common.keys import Keys
                search_input.send_keys(Keys.RETURN)
                
                print(f"  검색 실행 중...")
                time.sleep(4)  # 검색 결과 로딩 대기
                
            except Exception as e:
                print(f"  ✗ 검색 입력 실패: {e}")
                return None
            
            # 검색 결과에서 첫 번째 상품 선택
            try:
                # 상품 링크 찾기
                product_link = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a.thumb-link, .product-tile a, .product-link"))
                )
                
                product_url = product_link.get_attribute('href')
                print(f"  ✓ 상품 발견: {product_url[:50]}...")
                
                # 상품 상세 페이지로 이동
                self.driver.get(product_url)
                time.sleep(3)
                
                # 상품 정보 추출
                product_info = self._extract_product_info(product_code, product_url)
                
                return product_info
                
            except Exception as e:
                print(f"  ✗ 검색 결과 없음: {e}")
                return None
                
        except Exception as e:
            print(f"  ✗ GNC 검색 오류: {e}")
            return None
    
    def _extract_product_info(self, product_code: str, product_url: str) -> Optional[GNCProduct]:
        """상품 상세 페이지에서 정보 추출"""
        try:
            # 상품명
            product_name = ""
            try:
                name_elem = self.driver.find_element(By.CSS_SELECTOR, "h1.product-name")
                product_name = name_elem.text.strip()
            except:
                try:
                    name_elem = self.driver.find_element(By.CSS_SELECTOR, ".product-name")
                    product_name = name_elem.text.strip()
                except:
                    pass
            
            # 브랜드
            brand = "GNC"  # 기본값
            try:
                brand_elem = self.driver.find_element(By.CSS_SELECTOR, ".product-brand")
                brand = brand_elem.text.strip()
            except:
                pass
            
            # 썸네일 이미지
            thumbnail_url = None
            try:
                img_elem = self.driver.find_element(By.CSS_SELECTOR, "img.product-tile-img, img.primary-image")
                thumbnail_url = img_elem.get_attribute('src')
                
                # data-src 확인
                if not thumbnail_url or thumbnail_url.startswith('data:'):
                    thumbnail_url = img_elem.get_attribute('data-src')
            except:
                pass
            
            # 정수 추출 (상품명에서)
            count = self._extract_count(product_name)
            
            # 카테고리
            category = None
            try:
                breadcrumb = self.driver.find_element(By.CSS_SELECTOR, ".breadcrumb")
                category = breadcrumb.text.strip()
            except:
                pass
            
            # 설명
            description = None
            try:
                desc_elem = self.driver.find_element(By.CSS_SELECTOR, ".product-description, .short-description")
                description = desc_elem.text.strip()
            except:
                pass
            
            print(f"  ✓ GNC 상품: {product_name[:50]}...")
            
            return GNCProduct(
                product_code=str(product_code),
                product_name=product_name,
                brand=brand,
                gnc_url=product_url,
                thumbnail_url=thumbnail_url,
                count=count,
                description=description,
                category=category
            )
            
        except Exception as e:
            print(f"  ✗ 정보 추출 실패: {e}")
            return None
    
    def _extract_count(self, text: str) -> Optional[int]:
        """텍스트에서 정수 추출 (캡슐/정 개수)"""
        if not text:
            return None
        
        # 패턴: 숫자 + (Tablets, Capsules, Caplets, Softgels 등)
        patterns = [
            r'(\d+)\s*(?:tablets|capsules|caplets|softgels|veggie caps|vcaps)',
            r'(\d+)\s*ct',
            r'(\d+)\s*count',
        ]
        
        text_lower = text.lower()
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                count = int(match.group(1))
                if 10 <= count <= 1000:  # 유효 범위
                    return count
        
        return None