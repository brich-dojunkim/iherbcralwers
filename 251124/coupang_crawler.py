"""
쿠팡 상품 검색 및 정보 수집 모듈
기존 프로젝트의 coupang_manager.BrowserManager 사용
"""

import sys
import os

# 프로젝트 루트 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import re
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass


@dataclass
class CoupangProduct:
    """쿠팡 상품 정보"""
    rank: int
    name: str
    price: int
    shipping_fee: int
    final_price: int
    url: str
    thumbnail_url: Optional[str] = None
    count: Optional[int] = None
    brand: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    seller_name: Optional[str] = None


class CoupangCrawler:
    """쿠팡 크롤러 - 기존 BrowserManager 사용"""
    
    def __init__(self, browser_manager):
        """
        Args:
            browser_manager: coupang.coupang_manager.BrowserManager 인스턴스
        """
        if not browser_manager:
            raise ValueError("browser_manager가 필요합니다")
        
        self.browser = browser_manager
        self.driver = browser_manager.driver
    
    def search_products(self, query: str, top_n: int = 5) -> List[CoupangProduct]:
        """
        쿠팡 검색 및 상위 N개 제품 반환
        
        Args:
            query: 검색 쿼리
            top_n: 반환할 상위 제품 수
            
        Returns:
            CoupangProduct 리스트
        """
        products = []
        
        try:
            search_url = f"https://www.coupang.com/np/search?q={query}"
            print(f"  쿠팡 검색: {search_url}")
            
            self.browser.get_with_coupang_referrer(search_url)
            time.sleep(3)
            
            # 낱개상품 필터 적용
            self._apply_single_item_filter()
            time.sleep(2)
            
            # 검색 결과 파싱
            products = self._parse_search_results(top_n)
            
        except Exception as e:
            print(f"  ✗ 검색 오류: {e}")
        
        return products
    
    def _apply_single_item_filter(self):
        """낱개상품 필터 적용 (첨부 코드 참조)"""
        try:
            filter_script = """
            const filterLabels = document.querySelectorAll('label');
            for (let label of filterLabels) {
                const text = label.textContent.trim();
                if (text === '낱개상품') {
                    label.click();
                    return true;
                }
            }
            return false;
            """
            
            result = self.driver.execute_script(filter_script)
            if result:
                print("  ✓ 낱개상품 필터 적용")
            else:
                print("  ⚠ 낱개상품 필터 없음")
                
        except Exception as e:
            print(f"  ⚠ 필터 적용 실패: {e}")
    
    def _parse_search_results(self, top_n: int) -> List[CoupangProduct]:
        """검색 결과 파싱"""
        products = []
        
        try:
            # 상품 리스트 대기
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "li[class*='search-product']"))
            )
            
            # 상품 요소 찾기
            product_elements = self.driver.find_elements(By.CSS_SELECTOR, "li[class*='search-product']")
            
            for idx, elem in enumerate(product_elements[:top_n], 1):
                try:
                    product = self._parse_product_element(elem, idx)
                    if product:
                        products.append(product)
                except Exception as e:
                    print(f"  ⚠ 상품 {idx} 파싱 실패: {e}")
                    continue
            
            print(f"  ✓ {len(products)}개 상품 수집")
            
        except Exception as e:
            print(f"  ✗ 검색 결과 파싱 실패: {e}")
        
        return products
    
    def _parse_product_element(self, elem, rank: int) -> Optional[CoupangProduct]:
        """개별 상품 요소 파싱"""
        try:
            # 상품명
            name_elem = elem.find_element(By.CSS_SELECTOR, "div.name")
            name = name_elem.text.strip()
            
            # URL
            link_elem = elem.find_element(By.CSS_SELECTOR, "a")
            url = link_elem.get_attribute('href')
            
            # 가격
            price = 0
            try:
                price_elem = elem.find_element(By.CSS_SELECTOR, "strong.price-value")
                price_text = price_elem.text.strip().replace(',', '').replace('원', '')
                price = int(price_text)
            except:
                pass
            
            # 배송비
            shipping_fee = 0
            try:
                shipping_elem = elem.find_element(By.CSS_SELECTOR, ".shipping-fee")
                shipping_text = shipping_elem.text
                if '무료' not in shipping_text:
                    match = re.search(r'([\d,]+)원', shipping_text)
                    if match:
                        shipping_fee = int(match.group(1).replace(',', ''))
            except:
                pass
            
            final_price = price + shipping_fee
            
            # 썸네일
            thumbnail_url = None
            try:
                img_elem = elem.find_element(By.CSS_SELECTOR, "img")
                thumbnail_url = img_elem.get_attribute('src')
                if not thumbnail_url or thumbnail_url.startswith('data:'):
                    thumbnail_url = img_elem.get_attribute('data-src')
            except:
                pass
            
            # 정수 추출
            count = self._extract_count(name)
            
            # 브랜드 추출
            brand = self._extract_brand(name)
            
            # 리뷰 정보
            rating = None
            review_count = None
            try:
                rating_elem = elem.find_element(By.CSS_SELECTOR, ".rating")
                rating_text = rating_elem.text
                match = re.search(r'(\d+\.?\d*)', rating_text)
                if match:
                    rating = float(match.group(1))
                
                review_elem = elem.find_element(By.CSS_SELECTOR, ".rating-total-count")
                review_text = review_elem.text.strip('()')
                review_count = int(review_text.replace(',', ''))
            except:
                pass
            
            return CoupangProduct(
                rank=rank,
                name=name,
                price=price,
                shipping_fee=shipping_fee,
                final_price=final_price,
                url=url,
                thumbnail_url=thumbnail_url,
                count=count,
                brand=brand,
                rating=rating,
                review_count=review_count
            )
            
        except Exception as e:
            return None
    
    def get_product_detail(self, product_url: str) -> Optional[Dict]:
        """
        상품 상세 페이지 정보 수집
        
        Args:
            product_url: 쿠팡 상품 URL
            
        Returns:
            상세 정보 딕셔너리
        """
        try:
            print(f"  상세 페이지 접속...")
            self.browser.get_with_coupang_referrer(product_url)
            time.sleep(3)
            
            detail_info = {}
            
            # 상품명
            try:
                name_elem = self.driver.find_element(By.CSS_SELECTOR, "h1.prod-buy-header__title")
                detail_info['name'] = name_elem.text.strip()
            except:
                pass
            
            # 가격
            try:
                price_elem = self.driver.find_element(By.CSS_SELECTOR, ".total-price strong")
                price_text = price_elem.text.strip().replace(',', '').replace('원', '')
                detail_info['price'] = int(price_text)
            except:
                pass
            
            # 배송비
            try:
                shipping_elem = self.driver.find_element(By.CSS_SELECTOR, ".shipping-fee-txt")
                shipping_text = shipping_elem.text
                if '무료' in shipping_text:
                    detail_info['shipping_fee'] = 0
                else:
                    match = re.search(r'([\d,]+)원', shipping_text)
                    if match:
                        detail_info['shipping_fee'] = int(match.group(1).replace(',', ''))
            except:
                detail_info['shipping_fee'] = 0
            
            # 리뷰 정보
            try:
                rating_elem = self.driver.find_element(By.CSS_SELECTOR, ".rating-star-container")
                detail_info['rating'] = rating_elem.get_attribute('data-rating')
                
                review_elem = self.driver.find_element(By.CSS_SELECTOR, ".rating-count-txt")
                review_text = review_elem.text.strip()
                match = re.search(r'(\d+)', review_text.replace(',', ''))
                if match:
                    detail_info['review_count'] = int(match.group(1))
            except:
                pass
            
            # 판매자
            try:
                seller_elem = self.driver.find_element(By.CSS_SELECTOR, ".seller-name")
                detail_info['seller_name'] = seller_elem.text.strip()
            except:
                pass
            
            # 썸네일 이미지
            try:
                img_elem = self.driver.find_element(By.CSS_SELECTOR, "img.prod-image__detail")
                detail_info['thumbnail_url'] = img_elem.get_attribute('src')
            except:
                pass
            
            return detail_info
            
        except Exception as e:
            print(f"  ✗ 상세 정보 수집 실패: {e}")
            return None
    
    def _extract_count(self, text: str) -> Optional[int]:
        """텍스트에서 정수 추출"""
        if not text:
            return None
        
        patterns = [
            r'(\d+)정',
            r'(\d+)캡슐',
            r'(\d+)알',
            r'(\d+)개입',
            r'(\d+)tablets',
            r'(\d+)caps',
        ]
        
        text_lower = text.lower()
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                count = int(match.group(1))
                if 10 <= count <= 1000:
                    return count
        
        return None
    
    def _extract_brand(self, text: str) -> Optional[str]:
        """텍스트에서 브랜드 추출"""
        if not text:
            return None
        
        # 알려진 브랜드 리스트
        brands = [
            'GNC', '지앤씨',
            '나우푸드', 'NOW', 'Now Foods',
            '닥터스베스트', "Doctor's Best",
            '쏜리서치', 'Thorne',
            '솔가', 'Solgar',
            '라이프익스텐션', 'Life Extension',
        ]
        
        text_upper = text.upper()
        for brand in brands:
            if brand.upper() in text_upper:
                return brand
        
        return None