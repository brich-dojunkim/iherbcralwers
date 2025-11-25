"""
쿠팡 크롤러
"""

import time
import random
from typing import List, Optional

from .selectors import CoupangSelectors, CoupangHTMLHelper
from .models import CoupangProduct


class CoupangCrawler:
    """쿠팡 크롤러"""
    
    def __init__(self, browser_manager):
        """
        Args:
            browser_manager: BrowserManager 인스턴스
        """
        if not browser_manager:
            raise ValueError("browser_manager가 필요합니다")
        
        self.browser = browser_manager
        self.driver = browser_manager.driver
        
        # 선택자 & 헬퍼 로드
        self.selectors = CoupangSelectors()
        self.helper = CoupangHTMLHelper()
    
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
            # 1. 쿠팡 메인
            if "coupang.com" not in self.driver.current_url:
                self.driver.get("https://www.coupang.com")
                time.sleep(3)
            
            # 2. 검색창
            search_input = self._find_search_input()
            if not search_input:
                print("  ✗ 검색창을 찾을 수 없음")
                return []
            
            # 3. 검색어 입력
            search_input.clear()
            time.sleep(0.5)
            
            for char in query:
                search_input.send_keys(char)
                time.sleep(random.uniform(0.03, 0.1))
            
            # 4. 검색 실행
            search_input.send_keys("\n")
            time.sleep(5)
            
            # 5. Access Denied 체크
            if 'access denied' in self.driver.page_source.lower():
                print("  ✗ Access Denied 발생")
                return []
            
            # 6. 낱개상품 필터
            self._apply_single_item_filter()
            time.sleep(2)
            
            # 7. 파싱
            products = self._parse_search_results(top_n)
            
        except Exception as e:
            print(f"  ✗ 검색 오류: {e}")
        
        return products
    
    def get_product_detail(self, product_url: str) -> Optional[dict]:
        """
        상품 상세 정보 수집
        
        Args:
            product_url: 쿠팡 상품 URL
            
        Returns:
            상세 정보 딕셔너리
        """
        try:
            self.browser.get_with_coupang_referrer(product_url)
            time.sleep(3)
            
            detail_info = {}
            
            # 상품명
            try:
                elem = self.driver.find_element("css selector", self.selectors.DETAIL_PRODUCT_NAME)
                detail_info['name'] = elem.text.strip()
            except:
                pass
            
            # 가격
            try:
                elem = self.driver.find_element("css selector", self.selectors.DETAIL_PRICE)
                detail_info['price'] = self.helper.extract_price(elem.text)
            except:
                pass
            
            # 배송비
            try:
                elem = self.driver.find_element("css selector", self.selectors.DETAIL_SHIPPING)
                detail_info['shipping_fee'] = self.helper.extract_shipping_fee(elem.text)
            except:
                detail_info['shipping_fee'] = 0
            
            # 판매자 (JavaScript)
            seller_name = self._extract_seller_with_js()
            if seller_name:
                detail_info['seller_name'] = seller_name
            
            # 썸네일
            try:
                elem = self.driver.find_element("css selector", self.selectors.DETAIL_IMAGE)
                detail_info['thumbnail_url'] = elem.get_attribute('src')
            except:
                pass
            
            return detail_info
            
        except Exception as e:
            print(f"  ✗ 상세 정보 수집 실패: {e}")
            return None
    
    def _find_search_input(self):
        """검색창 찾기"""
        for selector in self.selectors.SEARCH_INPUT_ALTERNATIVES:
            try:
                element = self.driver.find_element("css selector", selector)
                if element.is_displayed():
                    return element
            except:
                continue
        return None
    
    def _apply_single_item_filter(self):
        """낱개상품 필터 적용"""
        try:
            script = """
            const labels = document.querySelectorAll('label');
            for (let label of labels) {
                if (label.textContent.trim() === '낱개상품') {
                    label.click();
                    return true;
                }
            }
            return false;
            """
            self.driver.execute_script(script)
        except:
            pass
    
    def _parse_search_results(self, top_n: int) -> List[CoupangProduct]:
        """검색 결과 파싱"""
        products = []
        
        try:
            # 상품 리스트 대기 (간단한 방법)
            time.sleep(2)
            
            elements = self.driver.find_elements("css selector", self.selectors.PRODUCT_LIST_ITEM)
            
            for idx, elem in enumerate(elements[:top_n], 1):
                try:
                    product = self._parse_product_element(elem, idx)
                    if product:
                        products.append(product)
                except:
                    continue
            
            print(f"  ✓ {len(products)}개 상품 수집")
            
        except Exception as e:
            print(f"  ✗ 파싱 실패: {e}")
        
        return products
    
    def _parse_product_element(self, elem, rank: int) -> Optional[CoupangProduct]:
        """개별 상품 파싱"""
        try:
            # 상품명
            name_elem = elem.find_element("css selector", self.selectors.PRODUCT_NAME)
            name = name_elem.text.strip()
            
            # URL
            link = elem.find_element("css selector", self.selectors.PRODUCT_LINK)
            url = link.get_attribute('href')
            
            # 가격
            price_area = elem.find_element("css selector", self.selectors.PRICE_AREA)
            price = self.helper.extract_price(price_area.text) or 0
            
            # 배송비
            shipping_fee = 0
            try:
                shipping_elem = elem.find_element("css selector", self.selectors.SHIPPING_INFO)
                shipping_fee = self.helper.extract_shipping_fee(shipping_elem.text)
            except:
                pass
            
            # 썸네일
            thumbnail_url = None
            try:
                img = elem.find_element("css selector", self.selectors.PRODUCT_IMAGE)
                thumbnail_url = img.get_attribute('src')
            except:
                pass
            
            # 정수
            count = self.helper.extract_count(name)
            
            # 리뷰
            rating = None
            review_count = None
            try:
                rating_elem = elem.find_element("css selector", self.selectors.RATING_CONTAINER)
                review_count = self.helper.extract_review_count(rating_elem.text)
            except:
                pass
            
            return CoupangProduct(
                rank=rank,
                name=name,
                price=price,
                shipping_fee=shipping_fee,
                final_price=price + shipping_fee,
                url=url,
                thumbnail_url=thumbnail_url,
                count=count,
                brand=None,
                rating=rating,
                review_count=review_count
            )
            
        except:
            return None
    
    def _extract_seller_with_js(self) -> Optional[str]:
        """판매자명 추출 (JavaScript)"""
        script = f"""
        const link = document.querySelector('{self.selectors.DETAIL_SELLER}');
        if (link) {{
            let text = '';
            for (let node of link.childNodes) {{
                if (node.nodeType === Node.TEXT_NODE) {{
                    text += node.textContent.trim();
                }}
            }}
            return text;
        }}
        return null;
        """
        
        try:
            seller = self.driver.execute_script(script)
            if seller:
                return self.helper.clean_seller_name(seller)
        except:
            pass
        
        return None
