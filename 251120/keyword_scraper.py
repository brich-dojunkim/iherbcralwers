"""
쿠팡 검색 결과 페이지 스크래퍼 - 실제 HTML 구조 반영
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
import re
import time
from typing import List, Dict, Optional
from bs4 import BeautifulSoup


class CoupangScraper:
    """쿠팡 검색 결과 스크래퍼 - 실제 HTML 구조 반영"""
    
    def scrape_search_results(self, driver, keyword: str, page: int) -> List[Dict]:
        """검색 결과 페이지에서 상품 정보 추출"""
        products = []
        
        try:
            # 페이지 완전히 로드될 때까지 대기
            time.sleep(3)
            
            # 스크롤 다운하여 동적 콘텐츠 로딩
            self._scroll_page(driver)
            
            # BeautifulSoup으로 HTML 파싱
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # 여러 선택자로 상품 요소 찾기
            product_elements = self._find_product_elements(driver, soup)
            
            if not product_elements:
                print(f"상품 요소를 찾을 수 없습니다.")
                return products
            
            print(f"찾은 상품 요소: {len(product_elements)}개")
            
            # 각 상품 정보 추출
            for idx, element in enumerate(product_elements, 1):
                try:
                    product = self._extract_product_info(element, keyword, page, idx, soup)
                    if product and product.get('product_id'):
                        products.append(product)
                except Exception as e:
                    continue
            
        except Exception as e:
            print(f"페이지 스크래핑 오류: {e}")
        
        return products
    
    def _scroll_page(self, driver):
        """페이지 스크롤하여 동적 콘텐츠 로딩"""
        try:
            # 천천히 스크롤
            for i in range(5):
                driver.execute_script(f"window.scrollTo(0, {800 * (i + 1)});")
                time.sleep(0.8)
            
            # 맨 위로
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
        except:
            pass
    
    def _find_product_elements(self, driver, soup):
        """실제 쿠팡 HTML 구조에 맞춘 상품 요소 찾기"""
        
        # 디버깅: HTML 저장
        debug_html_path = '/tmp/coupang_debug.html'
        try:
            with open(debug_html_path, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print(f"[DEBUG] HTML 저장됨: {debug_html_path}")
        except:
            pass
        
        # ========== 실제 쿠팡 HTML 구조에 맞춘 선택자 ==========
        
        # 방법 1: ul#product-list > li (가장 확실)
        try:
            elements = soup.select('ul#product-list > li')
            print(f"[DEBUG] 'ul#product-list > li': {len(elements)}개")
            if elements and len(elements) > 5:
                print(f"✓ ul#product-list로 {len(elements)}개 찾음")
                return elements
        except Exception as e:
            print(f"[DEBUG] ul#product-list 실패: {e}")
        
        # 방법 2: li[class*="ProductUnit_productUnit"]
        try:
            elements = soup.select('li[class*="ProductUnit_productUnit"]')
            print(f"[DEBUG] 'ProductUnit_productUnit': {len(elements)}개")
            if elements and len(elements) > 5:
                print(f"✓ ProductUnit 클래스로 {len(elements)}개 찾음")
                return elements
        except Exception as e:
            print(f"[DEBUG] ProductUnit 클래스 실패: {e}")
        
        # 방법 3: li[data-id] (data-id 속성 있는 li)
        try:
            elements = soup.select('li[data-id]')
            print(f"[DEBUG] 'li[data-id]': {len(elements)}개")
            if elements and len(elements) > 5:
                print(f"✓ li[data-id]로 {len(elements)}개 찾음")
                return elements
        except Exception as e:
            print(f"[DEBUG] li[data-id] 실패: {e}")
        
        # 방법 4: Selenium으로 시도
        print(f"[DEBUG] Selenium 선택자 시도 중...")
        selectors_selenium = [
            'ul#product-list > li',
            'li[class*="ProductUnit"]',
            'li[data-id]',
        ]
        
        for selector in selectors_selenium:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"[DEBUG]   Selenium '{selector}': {len(elements)}개")
                if elements and len(elements) > 5:
                    print(f"✓ Selenium '{selector}'로 {len(elements)}개 찾음")
                    return elements
            except Exception as e:
                print(f"[DEBUG]   Selenium '{selector}': 오류 - {e}")
                continue
        
        # 방법 5: 모든 li 요소 중 상품 URL 패턴만 필터링
        print(f"[DEBUG] 모든 li 요소 찾기 시도...")
        all_li = soup.find_all('li')
        print(f"[DEBUG] 전체 li 요소: {len(all_li)}개")
        
        if len(all_li) > 0:
            product_li = []
            for li in all_li:
                link = li.find('a', href=True)
                if link and ('/products/' in link['href'] or '/vp/products/' in link['href']):
                    product_li.append(li)
            
            print(f"[DEBUG] 상품 URL 패턴 li: {len(product_li)}개")
            if len(product_li) > 5:
                print(f"✓ URL 패턴으로 {len(product_li)}개 찾음")
                return product_li
        
        # 디버깅: 클래스명 분석
        print(f"\n[DEBUG] 페이지의 상품 관련 클래스명:")
        all_elements = soup.find_all(class_=True)
        class_names = set()
        for elem in all_elements[:200]:
            if elem.get('class'):
                for cls in elem['class']:
                    if 'product' in cls.lower() or 'item' in cls.lower():
                        class_names.add(cls)
        
        for cls in sorted(class_names)[:30]:
            print(f"  - {cls}")
        
        return []
    
    def _extract_product_info(self, element, keyword: str, page: int, rank: int, soup) -> Optional[Dict]:
        """단일 상품 정보 추출 - 실제 HTML 구조 반영"""
        try:
            product = {
                'keyword': keyword,
                'page': page,
                'rank': rank
            }
            
            # BeautifulSoup 요소인지 Selenium 요소인지 확인
            is_bs = hasattr(element, 'find')
            
            # 1. 상품 URL & ID 추출
            if is_bs:
                link = element.find('a', href=True)
                url = link['href'] if link else None
            else:
                try:
                    link = element.find_element(By.CSS_SELECTOR, 'a')
                    url = link.get_attribute('href')
                except:
                    url = None
            
            if not url:
                return None
            
            # URL에서 product_id 추출
            product_id = self._extract_product_id(url)
            if not product_id:
                return None
            
            product['product_id'] = product_id
            product['url'] = url if url.startswith('http') else f"https://www.coupang.com{url}"
            
            # 2. 상품명 추출 - 실제 클래스명 반영
            product['name'] = self._extract_product_name_bs(element) if is_bs else self._extract_product_name_selenium(element)
            product['brand'] = self._extract_brand(product['name'])
            
            # 3. 가격 정보 - 실제 클래스명 반영
            product['price'] = self._extract_price_bs(element) if is_bs else self._extract_price_selenium(element)
            
            # 4. 원가
            product['original_price'] = self._extract_original_price_bs(element) if is_bs else self._extract_price_selenium(element, original=True)
            
            # 5. 할인율
            product['discount_rate'] = self._extract_discount_bs(element) if is_bs else self._extract_discount_selenium(element)
            
            # 6. 평점
            product['rating'] = self._extract_rating_bs(element) if is_bs else self._extract_rating_selenium(element)
            
            # 7. 리뷰 수
            product['review_count'] = self._extract_review_count_bs(element) if is_bs else self._extract_review_count_selenium(element)
            
            # 8. 로켓배송 여부
            product['is_rocket'] = self._check_rocket_bs(element) if is_bs else self._check_rocket_selenium(element)
            
            # 9. 이미지 URL
            product['image_url'] = self._extract_image_bs(element) if is_bs else self._extract_image_selenium(element)
            
            # 10. 판매자
            product['seller_name'] = ''
            
            return product
            
        except Exception as e:
            return None
    
    # ========== BeautifulSoup 기반 추출 메서드 - 실제 HTML 구조 반영 ==========
    
    def _extract_product_name_bs(self, element) -> str:
        """BeautifulSoup로 상품명 추출 - 실제 클래스명"""
        # 실제 클래스명: ProductUnit_productNameV2__cV9cw
        selectors = [
            'div[class*="ProductUnit_productName"]',  # 부분 매칭
            'div[class*="productName"]',
            'div.name',
            'span.name'
        ]
        
        for selector in selectors:
            elem = element.select_one(selector)
            if elem and elem.get_text(strip=True):
                return elem.get_text(strip=True)
        
        # title 속성
        link = element.find('a', title=True)
        if link and link['title']:
            return link['title'].strip()
        
        # alt 속성 (이미지)
        img = element.find('img', alt=True)
        if img and img['alt']:
            return img['alt'].strip()
        
        return ''
    
    def _extract_price_bs(self, element) -> Optional[int]:
        """BeautifulSoup로 가격 추출 - 실제 클래스명"""
        # 실제 클래스명: Price_priceValue__A4KOr
        selectors = [
            'strong[class*="Price_priceValue"]',  # 부분 매칭
            'strong[class*="priceValue"]',
            'div[class*="Price_salePrice"] strong',
            'strong.price-value',
            'span.price-value',
            'em.sale'
        ]
        
        for selector in selectors:
            elem = element.select_one(selector)
            if elem:
                price = self._parse_price(elem.get_text(strip=True))
                if price and price > 0:
                    return price
        
        return None
    
    def _extract_original_price_bs(self, element) -> Optional[int]:
        """BeautifulSoup로 원가 추출"""
        # 실제: PriceInfo_basePrice__8BQ32 또는 del 태그
        selectors = [
            'del[class*="PriceInfo_basePrice"]',
            'del[class*="basePrice"]',
            'del',
            'span.base-price'
        ]
        
        for selector in selectors:
            elem = element.select_one(selector)
            if elem:
                price = self._parse_price(elem.get_text(strip=True))
                if price:
                    return price
        
        return None
    
    def _extract_discount_bs(self, element) -> Optional[int]:
        """BeautifulSoup로 할인율 추출"""
        # 실제: PriceInfo_discountRate__EsQ8I
        selectors = [
            'span[class*="PriceInfo_discountRate"]',
            'span[class*="discountRate"]',
            'div[class*="discount"]',
            'span.discount-percentage'
        ]
        
        for selector in selectors:
            elem = element.select_one(selector)
            if elem:
                return self._parse_discount(elem.get_text(strip=True))
        
        return None
    
    def _extract_rating_bs(self, element) -> Optional[float]:
        """BeautifulSoup로 평점 추출"""
        # 실제: ProductRating_star__RGSlV의 width 스타일 또는 텍스트
        selectors = [
            'div[class*="ProductRating_star"]',
            'span[class*="rating"]',
            'em.rating'
        ]
        
        for selector in selectors:
            elem = element.select_one(selector)
            if elem:
                # width 스타일에서 추출 (예: width: 100% → 5.0)
                style = elem.get('style', '')
                if 'width' in style:
                    match = re.search(r'width:\s*(\d+)%', style)
                    if match:
                        width_percent = int(match.group(1))
                        return round(width_percent / 20, 1)  # 100% = 5.0
                
                # 텍스트에서 추출
                try:
                    return float(elem.get_text(strip=True))
                except:
                    pass
        
        return None
    
    def _extract_review_count_bs(self, element) -> int:
        """BeautifulSoup로 리뷰수 추출"""
        # 실제: ProductRating_ratingCount__R0Vhz
        selectors = [
            'span[class*="ProductRating_ratingCount"]',
            'span[class*="ratingCount"]',
            'span.rating-total-count'
        ]
        
        for selector in selectors:
            elem = element.select_one(selector)
            if elem:
                return self._parse_review_count(elem.get_text(strip=True))
        
        return 0
    
    def _check_rocket_bs(self, element) -> bool:
        """BeautifulSoup로 로켓배송 확인"""
        # 실제: ImageBadge 내 rocket 이미지
        selectors = [
            'img[alt*="로켓"]',
            'img[src*="rocket"]',
            'div[class*="rocket"]',
            'span.rocket-badge'
        ]
        
        for selector in selectors:
            elem = element.select_one(selector)
            if elem:
                return True
        
        return False
    
    def _extract_image_bs(self, element) -> str:
        """BeautifulSoup로 이미지 URL 추출"""
        # 실제: ProductUnit_productImage 내 img
        img = element.find('img')
        if img:
            # src 또는 data-src
            return img.get('src', '') or img.get('data-src', '')
        return ''
    
    # ========== Selenium 기반 추출 메서드 ==========
    
    def _extract_product_name_selenium(self, element) -> str:
        """Selenium으로 상품명 추출 - 실제 클래스명"""
        selectors = [
            'div[class*="ProductUnit_productName"]',
            'div[class*="productName"]',
            'div.name'
        ]
        
        for selector in selectors:
            try:
                elem = element.find_element(By.CSS_SELECTOR, selector)
                text = elem.text.strip()
                if text and len(text) > 5:
                    return text
            except:
                continue
        
        # title 속성
        try:
            link = element.find_element(By.CSS_SELECTOR, 'a')
            title = link.get_attribute('title')
            if title:
                return title.strip()
        except:
            pass
        
        # alt 속성
        try:
            img = element.find_element(By.CSS_SELECTOR, 'img')
            alt = img.get_attribute('alt')
            if alt:
                return alt.strip()
        except:
            pass
        
        return ''
    
    def _extract_price_selenium(self, element, original=False) -> Optional[int]:
        """Selenium으로 가격 추출 - 실제 클래스명"""
        if original:
            selectors = [
                'del[class*="basePrice"]',
                'del'
            ]
        else:
            selectors = [
                'strong[class*="priceValue"]',
                'strong[class*="Price_priceValue"]',
                'strong.price-value',
                'span.price-value'
            ]
        
        for selector in selectors:
            try:
                elem = element.find_element(By.CSS_SELECTOR, selector)
                price = self._parse_price(elem.text.strip())
                if price and price > 0:
                    return price
            except:
                continue
        
        return None
    
    def _extract_discount_selenium(self, element) -> Optional[int]:
        """Selenium으로 할인율 추출"""
        selectors = [
            'span[class*="discountRate"]',
            'span.discount-percentage'
        ]
        
        for selector in selectors:
            try:
                elem = element.find_element(By.CSS_SELECTOR, selector)
                return self._parse_discount(elem.text.strip())
            except:
                continue
        
        return None
    
    def _extract_rating_selenium(self, element) -> Optional[float]:
        """Selenium으로 평점 추출"""
        selectors = [
            'div[class*="ProductRating_star"]',
            'em.rating'
        ]
        
        for selector in selectors:
            try:
                elem = element.find_element(By.CSS_SELECTOR, selector)
                
                # style에서 width 추출
                style = elem.get_attribute('style')
                if style and 'width' in style:
                    match = re.search(r'width:\s*(\d+)%', style)
                    if match:
                        width_percent = int(match.group(1))
                        return round(width_percent / 20, 1)
                
                # 텍스트 추출
                text = elem.text.strip()
                if text:
                    return float(text)
            except:
                continue
        
        return None
    
    def _extract_review_count_selenium(self, element) -> int:
        """Selenium으로 리뷰수 추출"""
        selectors = [
            'span[class*="ratingCount"]',
            'span.rating-total-count'
        ]
        
        for selector in selectors:
            try:
                elem = element.find_element(By.CSS_SELECTOR, selector)
                return self._parse_review_count(elem.text.strip())
            except:
                continue
        
        return 0
    
    def _check_rocket_selenium(self, element) -> bool:
        """Selenium으로 로켓배송 확인"""
        selectors = [
            'img[alt*="로켓"]',
            'img[src*="rocket"]',
            'span.rocket-badge'
        ]
        
        for selector in selectors:
            try:
                element.find_element(By.CSS_SELECTOR, selector)
                return True
            except:
                continue
        
        return False
    
    def _extract_image_selenium(self, element) -> str:
        """Selenium으로 이미지 URL 추출"""
        try:
            img = element.find_element(By.CSS_SELECTOR, 'img')
            return img.get_attribute('src') or ''
        except:
            return ''
    
    # ========== 파싱 유틸리티 ==========
    
    def _extract_product_id(self, url: str) -> str:
        """URL에서 상품 ID 추출"""
        if not url:
            return ''
        
        # /products/ 패턴
        match = re.search(r'/products/(\d+)', url)
        if match:
            return match.group(1)
        
        # /vp/products/ 패턴
        match = re.search(r'/vp/products/(\d+)', url)
        if match:
            return match.group(1)
        
        return ''
    
    def _extract_brand(self, product_name: str) -> str:
        """상품명에서 브랜드 추출 (첫 단어)"""
        if not product_name:
            return ''
        
        # 공백, 대괄호, 쉼표 등으로 분리된 첫 단어
        parts = re.split(r'[\s\[\],]+', product_name.strip())
        if parts:
            return parts[0]
        return ''
    
    def _parse_price(self, price_text: str) -> Optional[int]:
        """가격 텍스트 파싱"""
        if not price_text:
            return None
        
        # 숫자만 추출
        numbers = re.sub(r'[^\d]', '', price_text)
        if numbers:
            return int(numbers)
        return None
    
    def _parse_discount(self, discount_text: str) -> Optional[int]:
        """할인율 텍스트 파싱"""
        if not discount_text:
            return None
        
        # 숫자만 추출
        match = re.search(r'(\d+)', discount_text)
        if match:
            return int(match.group(1))
        return None
    
    def _parse_review_count(self, review_text: str) -> int:
        """리뷰 수 텍스트 파싱"""
        if not review_text:
            return 0
        
        # 괄호 안의 숫자 추출 (1,234)
        # 또는 "9,999+"를 9999로 변환
        text = review_text.replace('+', '').replace(',', '')
        numbers = re.sub(r'[^\d]', '', text)
        if numbers:
            return int(numbers)
        return 0