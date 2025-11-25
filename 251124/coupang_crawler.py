"""
쿠팡 상품 검색 및 정보 수집 모듈
직접 URL 방식으로 봇 탐지 우회
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
from typing import List, Optional
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
    """쿠팡 크롤러 - 직접 URL 방식"""
    
    def __init__(self, browser_manager):
        """
        Args:
            browser_manager: coupang_manager.BrowserManager 인스턴스
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
            print(f"  쿠팡 검색: {search_url[:80]}...")
            
            # ✅ 직접 open_with_referrer 호출 (봇 탐지 우회)
            self.browser.open_with_referrer(search_url)
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
        """낱개상품 필터 적용"""
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
        """검색 결과 파싱 - 실제 HTML 구조 반영"""
        products = []
        
        try:
            # 상품 리스트 대기 (최신 쿠팡 선택자)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "li.ProductUnit_productUnit__Qd6sv"))
            )
            
            # 상품 요소 찾기
            product_elements = self.driver.find_elements(By.CSS_SELECTOR, "li.ProductUnit_productUnit__Qd6sv")
            print(f"  ✓ {len(product_elements)}개 상품 발견")
            
            for idx, elem in enumerate(product_elements[:top_n], 1):
                try:
                    product = self._parse_product_element(elem, idx)
                    if product:
                        products.append(product)
                except Exception as e:
                    print(f"  ⚠ 상품 {idx} 파싱 실패: {e}")
                    continue
            
            print(f"  ✓ {len(products)}개 상품 수집 완료")
            
        except Exception as e:
            print(f"  ✗ 검색 결과 파싱 실패: {e}")
        
        return products
    
    def _parse_product_element(self, elem, rank: int) -> Optional[CoupangProduct]:
        """개별 상품 요소 파싱 - 최신 쿠팡 HTML 구조"""
        try:
            # 상품명 (최신 선택자)
            name_elem = elem.find_element(By.CSS_SELECTOR, "div.ProductUnit_productNameV2__cV9cw")
            name = name_elem.text.strip()
            
            # URL
            link_elem = elem.find_element(By.CSS_SELECTOR, "a")
            url = link_elem.get_attribute('href')
            
            # 가격 파싱 (복잡한 구조)
            price = 0
            shipping_fee = 0
            
            try:
                price_area = elem.find_element(By.CSS_SELECTOR, "div.PriceArea_priceArea__NntJz")
                price_texts = price_area.find_elements(By.CSS_SELECTOR, "span")
                
                for text_elem in price_texts:
                    text = text_elem.text.strip()
                    # "33,000원" 형태 찾기 (1정당 제외)
                    if '원' in text and '정당' not in text:
                        clean = text.replace(',', '').replace('원', '').strip()
                        if clean.isdigit():
                            price = int(clean)
                            break
            except:
                pass
            
            # 배송비
            try:
                shipping_texts = elem.find_elements(By.CSS_SELECTOR, "span, div")
                for text_elem in shipping_texts:
                    text = text_elem.text.strip()
                    if '무료배송' in text or '무료' in text:
                        shipping_fee = 0
                        break
                    elif '배송비' in text:
                        match = re.search(r'([\d,]+)원', text)
                        if match:
                            shipping_fee = int(match.group(1).replace(',', ''))
                            break
            except:
                pass
            
            final_price = price + shipping_fee
            
            # 썸네일
            thumbnail_url = None
            try:
                img_elem = elem.find_element(By.CSS_SELECTOR, "figure.ProductUnit_productImage__Mqcg1 img")
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
                rating_div = elem.find_element(By.CSS_SELECTOR, "div.ProductRating_productRating__jjf7W")
                rating_text = rating_div.text
                
                # 평점 추출
                match = re.search(r'(\d+\.?\d*)', rating_text)
                if match:
                    rating = float(match.group(1))
                
                # 리뷰 개수 추출 (123)
                match_review = re.search(r'\((\d+,?\d*)\)', rating_text)
                if match_review:
                    review_count = int(match_review.group(1).replace(',', ''))
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
    
    def get_product_detail(self, product_url: str) -> Optional[dict]:
        """
        상품 상세 페이지 정보 수집
        
        Args:
            product_url: 쿠팡 상품 URL
            
        Returns:
            상세 정보 딕셔너리
        """
        try:
            print(f"  상세 페이지 접속...")
            # ✅ 직접 open_with_referrer 사용
            self.browser.open_with_referrer(product_url)
            time.sleep(3)
            
            detail_info = {}
            
            # 판매자 정보 추출 (여러 방법 시도)
            seller_name = None
            
            # 방법 1: JavaScript로 정확히 추출
            try:
                script = """
                // Tailwind CSS 구조
                const sellerLink = document.querySelector('a[href*="shop.coupang.com"]');
                if (sellerLink) {
                    // childNodes에서 텍스트만 추출 (div 제외)
                    let text = '';
                    for (let node of sellerLink.childNodes) {
                        if (node.nodeType === Node.TEXT_NODE) {
                            text += node.textContent.trim();
                        }
                    }
                    if (text) return text;
                    
                    // 또는 firstChild가 텍스트인 경우
                    if (sellerLink.firstChild && sellerLink.firstChild.nodeType === Node.TEXT_NODE) {
                        return sellerLink.firstChild.textContent.trim();
                    }
                }
                
                // 기존 구조
                const oldSelectors = ['.seller-name', '.prod-sale-vendor-name', '.vendor-name'];
                for (let sel of oldSelectors) {
                    const elem = document.querySelector(sel);
                    if (elem) return elem.textContent.trim();
                }
                
                return null;
                """
                
                seller_name = self.driver.execute_script(script)
                if seller_name:
                    print(f"  ✓ 판매자: {seller_name}")
                
            except Exception as e:
                print(f"  ⚠ JS 판매자 추출 실패: {e}")
            
            # 방법 2: Selenium으로 추출 (백업)
            if not seller_name:
                seller_selectors = [
                    "a[href*='shop.coupang.com/vid']",
                    "a[href*='shop.coupang.com']",
                    ".seller-name",
                    ".prod-sale-vendor-name",
                    ".vendor-name",
                ]
                
                for selector in seller_selectors:
                    try:
                        seller_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                        seller_text = seller_elem.text.strip()
                        
                        # 줄바꿈으로 분리하여 첫 번째 유효한 텍스트 추출
                        if seller_text:
                            lines = seller_text.split('\n')
                            for line in lines:
                                clean = line.strip()
                                # "판매자:", "보러가기" 등 제외
                                if clean and len(clean) > 1 and \
                                   '보러가기' not in clean and \
                                   '판매자:' not in clean and \
                                   '판매자' != clean:
                                    seller_name = clean
                                    print(f"  ✓ 판매자: {seller_name}")
                                    break
                            if seller_name:
                                break
                    except:
                        continue
            
            if not seller_name:
                print("  ⚠ 판매자 정보 없음")
            
            if seller_name:
                detail_info['seller_name'] = seller_name
            
            # 상세 썸네일 (더 고화질)
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
                if 10 <= count <= 1000:  # 유효 범위
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
            '칼라일', 'Carlyle',
            '네이처스웨이', "Nature's Way",
            '블루보넷', 'Bluebonnet',
        ]
        
        text_upper = text.upper()
        for brand in brands:
            if brand.upper() in text_upper:
                return brand
        
        return None