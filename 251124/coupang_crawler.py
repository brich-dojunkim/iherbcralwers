"""
쿠팡 상품 검색 및 정보 수집 모듈
🔄 coupang_manager 모듈 완전 활용 버전
✅ selectors.py의 CoupangSelectors 사용
✅ CoupangHTMLHelper 헬퍼 함수 활용
"""

import sys
import os
import time
import re
from typing import List, Optional
from dataclasses import dataclass

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from coupang_manager import CoupangBrowser
from coupang_manager.selectors import CoupangSelectors, CoupangHTMLHelper


@dataclass
class CoupangProduct:
    """GNC 매칭용 쿠팡 상품 정보 (brand/count 제거 버전)"""
    rank: int
    name: str
    price: int
    shipping_fee: int
    final_price: int
    url: str
    thumbnail_url: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    seller_name: Optional[str] = None


class CoupangCrawler:
    """쿠팡 크롤러 - coupang_manager 활용"""
    
    def __init__(self, browser_manager: CoupangBrowser):
        if not browser_manager:
            raise ValueError("browser_manager 필요")
        
        self.browser = browser_manager
        self.driver = browser_manager.driver
        self.selectors = CoupangSelectors()
    
    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------
    
    def search_products(self, query: str, top_n: int = 5) -> List[CoupangProduct]:
        """쿠팡 검색 및 상위 N개 제품 반환"""
        products: List[CoupangProduct] = []
        
        try:
            search_url = f"https://www.coupang.com/np/search?q={query}"
            print(f"  쿠팡 검색: {search_url[:80]}...")
            
            # CoupangBrowser의 get_with_coupang_referrer 사용
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
    
    def get_product_detail(self, product_url: str) -> Optional[dict]:
        """상품 상세 페이지 정보 수집"""
        try:
            print(f"  상세 페이지 접속...")
            self.browser.get_with_coupang_referrer(product_url)
            time.sleep(3)
            
            detail_info: dict = {}
            
            # 상품명
            try:
                name_elem = self.driver.find_element("css selector", self.selectors.DETAIL_PRODUCT_NAME)
                detail_info['name'] = name_elem.text.strip()
            except:
                pass
            
            # 가격
            try:
                price_elem = self.driver.find_element("css selector", self.selectors.DETAIL_PRICE)
                price_text = price_elem.text.strip()
                detail_info['price'] = CoupangHTMLHelper.extract_price(price_text) or 0
            except:
                pass
            
            # 배송비
            try:
                shipping_elem = self.driver.find_element("css selector", self.selectors.DETAIL_SHIPPING)
                shipping_text = shipping_elem.text
                detail_info['shipping_fee'] = CoupangHTMLHelper.extract_shipping_fee(shipping_text)
            except:
                detail_info['shipping_fee'] = 0
            
            # 리뷰 정보
            try:
                rating_value: Optional[float] = None
                try:
                    rating_elem = self.driver.find_element("css selector", self.selectors.DETAIL_RATING)
                    style = ""
                    try:
                        inner = rating_elem.find_element("css selector", ".rating-star-num")
                        style = inner.get_attribute("style") or ""
                    except:
                        style = rating_elem.get_attribute("style") or ""
                    
                    m = re.search(r'width:\s*([\d\.]+)%', style)
                    if m:
                        width = float(m.group(1))
                        rating_value = round(width / 20.0, 1)
                except:
                    pass
                
                if rating_value is not None:
                    detail_info['rating'] = rating_value
                
                try:
                    review_elem = self.driver.find_element("css selector", self.selectors.DETAIL_REVIEW_COUNT)
                    review_text = review_elem.text.strip()
                    rc = CoupangHTMLHelper.extract_review_count(review_text)
                    if rc is not None:
                        detail_info['review_count'] = rc
                except:
                    pass
            except:
                pass
            
            # 판매자
            seller_name: Optional[str] = None
            seller_selectors = [self.selectors.DETAIL_SELLER] + self.selectors.DETAIL_SELLER_ALTERNATIVES
            for selector in seller_selectors:
                try:
                    seller_elem = self.driver.find_element("css selector", selector)
                    seller_text = seller_elem.text.strip()
                    cleaned = CoupangHTMLHelper.clean_seller_name(seller_text)
                    if cleaned:
                        seller_name = cleaned
                        break
                except:
                    continue
            
            if seller_name:
                detail_info['seller_name'] = seller_name
            
            # 썸네일
            try:
                img_elem = self.driver.find_element("css selector", self.selectors.DETAIL_IMAGE)
                thumb = img_elem.get_attribute('src') or img_elem.get_attribute('data-src')
                if thumb and thumb.startswith("//"):
                    thumb = "https:" + thumb
                detail_info['thumbnail_url'] = thumb
            except:
                pass
            
            return detail_info
        
        except Exception as e:
            print(f"  ✗ 상세 정보 수집 실패: {e}")
            return None
    
    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    
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
        """검색 결과 파싱"""
        from selenium.webdriver.support.ui import WebDriverWait
        
        products: List[CoupangProduct] = []
        
        try:
            WebDriverWait(self.driver, 15).until(
                lambda d: d.find_elements("css selector", self.selectors.PRODUCT_LIST_ITEM)
            )
            
            product_elements = self.driver.find_elements("css selector", self.selectors.PRODUCT_LIST_ITEM)
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
        """개별 상품 요소 파싱"""
        try:
            # 상품명
            try:
                name_elem = elem.find_element("css selector", self.selectors.PRODUCT_NAME)
                name = name_elem.text.strip()
            except:
                return None
            
            # URL
            url = ""
            try:
                link_elem = elem.find_element("css selector", self.selectors.PRODUCT_LINK)
                url = link_elem.get_attribute('href')
                if not url.startswith('http'):
                    url = f"https://www.coupang.com{url}"
            except:
                pass
            
            # 가격
            price = 0
            try:
                price_elem = elem.find_element("css selector", self.selectors.PRICE_FINAL)
                price_text = price_elem.text.strip()
                price = CoupangHTMLHelper.extract_price(price_text) or 0
            except:
                pass
            
            # 배송비
            shipping_fee = 0
            try:
                shipping_elem = elem.find_element("css selector", self.selectors.SHIPPING_INFO)
                shipping_text = shipping_elem.text
                shipping_fee = CoupangHTMLHelper.extract_shipping_fee(shipping_text)
            except:
                pass
            
            final_price = price + shipping_fee
            
            # 썸네일
            thumbnail_url: Optional[str] = None
            try:
                img_elem = elem.find_element("css selector", self.selectors.PRODUCT_IMAGE)
                thumbnail_url = img_elem.get_attribute('src')
                if not thumbnail_url or thumbnail_url.startswith('data:'):
                    thumbnail_url = img_elem.get_attribute('data-src')
                if thumbnail_url and thumbnail_url.startswith("//"):
                    thumbnail_url = "https:" + thumbnail_url
            except:
                pass
            
            # 리뷰 정보 (검색 결과 기준)
            rating: Optional[float] = None
            review_count: Optional[int] = None
            try:
                rating_elem = elem.find_element("css selector", self.selectors.RATING_STAR)
                rating_text = rating_elem.text.strip()
                
                # 텍스트에 평점 숫자가 있는 경우
                if rating_text:
                    m = re.search(r'(\d+\.?\d*)', rating_text)
                    if m:
                        rating = float(m.group(1))
                
                # style width% 로만 표현된 경우
                if rating is None:
                    style = rating_elem.get_attribute("style") or ""
                    m = re.search(r'width:\s*([\d\.]+)%', style)
                    if m:
                        width = float(m.group(1))
                        rating = round(width / 20.0, 1)
                
                # 리뷰 수
                review_elem = elem.find_element("css selector", self.selectors.RATING_COUNT)
                review_text = review_elem.text
                review_count = CoupangHTMLHelper.extract_review_count(review_text)
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
                rating=rating,
                review_count=review_count,
            )
        
        except Exception:
            return None
