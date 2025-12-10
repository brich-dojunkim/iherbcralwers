"""
쿠팡 상품 검색 및 정보 수집 모듈
✨ v3.0 - 검색 결과 페이지에서 모든 정보 수집 (상세 페이지 불필요)
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
    """쿠팡 상품 정보 - 검색 결과 기반"""
    rank: int
    name: str
    url: str
    thumbnail_url: Optional[str] = None
    
    # 가격 정보
    original_price: int = 0          # 정가
    sale_price: int = 0              # 판매가
    discount_rate: int = 0           # 할인율 (%)
    unit_price: Optional[int] = None  # 1정당 가격
    shipping_fee: int = 0            # 배송비
    final_price: int = 0             # 최종가 (판매가 + 배송비)
    
    # 리뷰 정보
    rating: Optional[float] = None
    review_count: Optional[int] = None
    
    # 배송 정보
    delivery_type: Optional[str] = None    # "로켓배송", "직구", etc.
    delivery_date: Optional[str] = None    # "내일(수) 도착"
    is_rocket: bool = False                # 로켓배송 여부
    is_free_shipping: bool = False         # 무료배송 여부
    
    # 배지
    badges: List[str] = None              # ["쿠팡PICK", "로켓배송"]
    
    def __post_init__(self):
        if self.badges is None:
            self.badges = []


class CoupangCrawler:
    """쿠팡 크롤러 v3.0 - 검색 결과 전용"""
    
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
        """쿠팡 검색 및 상위 N개 제품 반환 (검색 결과에서 모든 정보 수집)"""
        products: List[CoupangProduct] = []
        
        try:
            search_url = f"https://www.coupang.com/np/search?q={query}"
            print(f"  쿠팡 검색: {search_url[:80]}...")
            
            self.browser.get_with_coupang_referrer(search_url)
            time.sleep(3)
            
            # 낱개상품 필터 적용
            self._apply_single_item_filter()
            time.sleep(2)
            
            # 검색 결과 파싱 (모든 정보 수집)
            products = self._parse_search_results(top_n)
            
        except Exception as e:
            print(f"  ✗ 검색 오류: {e}")
        
        return products
    
    # ---------------------------------------------------------------------
    # Private Methods
    # ---------------------------------------------------------------------
    
    def _apply_single_item_filter(self):
        """낱개상품 필터 적용"""
        try:
            labels = self.driver.find_elements("css selector", self.selectors.FILTER_LABEL)
            for label in labels:
                if "낱개상품" in label.text:
                    label.click()
                    print(f"  ✓ 낱개상품 필터 적용")
                    return
            print(f"  ⚠ 낱개상품 필터 없음")
        except:
            print(f"  ⚠ 낱개상품 필터 없음")
    
    def _parse_search_results(self, top_n: int) -> List[CoupangProduct]:
        """검색 결과에서 모든 정보 추출"""
        products: List[CoupangProduct] = []
        
        try:
            elements = self.driver.find_elements("css selector", self.selectors.PRODUCT_LIST_ITEM)
            print(f"  ✓ {len(elements)}개 상품 발견")
            
            for rank, elem in enumerate(elements[:top_n], 1):
                product = self._parse_product_element(elem, rank)
                if product:
                    products.append(product)
            
            print(f"  ✓ {len(products)}개 상품 수집 완료")
        
        except Exception as e:
            print(f"  ✗ 파싱 오류: {e}")
        
        return products
    
    def _parse_product_element(self, elem, rank: int) -> Optional[CoupangProduct]:
        """상품 카드에서 모든 정보 추출"""
        try:
            # 상품명
            name = ""
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
                if url and not url.startswith('http'):
                    url = "https://www.coupang.com" + url
            except:
                return None
            
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
            
            # ============================================================
            # 가격 정보
            # ============================================================
            
            # 할인율
            discount_rate = 0
            try:
                discount_elems = elem.find_elements("css selector", self.selectors.PRICE_DISCOUNT_RATE)
                for d_elem in discount_elems:
                    text = d_elem.text.strip()
                    rate = CoupangHTMLHelper.extract_discount_rate(text)
                    if rate:
                        discount_rate = rate
                        break
            except:
                pass
            
            # 정가 (할인 전)
            original_price = 0
            try:
                original_elem = elem.find_element("css selector", self.selectors.PRICE_ORIGINAL)
                original_text = original_elem.text.strip()
                original_price = CoupangHTMLHelper.extract_price(original_text) or 0
            except:
                pass
            
            # 판매가 (할인 후)
            sale_price = 0
            try:
                price_elems = elem.find_elements("css selector", self.selectors.PRICE_SALE)
                for p_elem in price_elems:
                    # fw-text-[20px] 클래스를 가진 요소 찾기
                    classes = p_elem.get_attribute('class') or ''
                    if 'fw-text-[20px]' in classes or 'fw-font-bold' in classes:
                        price_text = p_elem.text.strip()
                        sale_price = CoupangHTMLHelper.extract_price(price_text) or 0
                        if sale_price > 0:
                            break
            except:
                pass
            
            # 1정당 가격
            unit_price: Optional[int] = None
            try:
                unit_elems = elem.find_elements("css selector", self.selectors.PRICE_UNIT)
                for u_elem in unit_elems:
                    unit_text = u_elem.text.strip()
                    unit_price = CoupangHTMLHelper.extract_unit_price(unit_text)
                    if unit_price:
                        break
            except:
                pass
            
            # ============================================================
            # 배송 정보
            # ============================================================
            
            # 배송비 추출
            is_free_shipping = False
            shipping_fee = 0
            
            try:
                # 1. 유료 배송비 배지 확인 (우선순위)
                fee_badge = elem.find_elements("css selector", self.selectors.SHIPPING_FEE_BADGE)
                if fee_badge:
                    badge_text = fee_badge[0].text.strip()
                    # "배송비 2,500원 조건부 무료배송" → 2500 추출
                    shipping_fee = CoupangHTMLHelper.extract_shipping_fee(badge_text)
                    if shipping_fee == 0 and "무료" in badge_text:
                        is_free_shipping = True
                else:
                    # 2. 무료배송 span 확인
                    free_span = elem.find_elements("css selector", self.selectors.FREE_SHIPPING_SPAN)
                    if free_span:
                        for span in free_span:
                            if CoupangHTMLHelper.is_free_shipping(span.text):
                                is_free_shipping = True
                                shipping_fee = 0
                                break
            except Exception as e:
                pass
            
            # 도착 예정일
            delivery_date: Optional[str] = None
            try:
                date_spans = elem.find_elements("css selector", self.selectors.DELIVERY_DATE_SPAN)
                if date_spans and len(date_spans) >= 2:
                    # 보통 2개: "내일(수)" + "도착 보장"
                    delivery_date = date_spans[0].text.strip() + " " + date_spans[1].text.strip()
            except:
                pass
            
            # 배지 (쿠팡PICK, 로켓배송, 직구 등)
            badges: List[str] = []
            delivery_type: Optional[str] = None
            
            try:
                # 이미지 배지
                badge_imgs = elem.find_elements("css selector", self.selectors.IMAGE_BADGE)
                for img in badge_imgs:
                    src = img.get_attribute('src') or ''
                    d_type = CoupangHTMLHelper.parse_delivery_type(src)
                    if d_type:
                        badges.append(d_type)
                        if not delivery_type:
                            delivery_type = d_type
                
                # 쿠팡PICK 배지
                coupick = elem.find_elements("css selector", self.selectors.COUPICK_BADGE)
                if coupick:
                    badges.append("쿠팡PICK")
            except:
                pass
            
            # 로켓배송 여부
            is_rocket = CoupangHTMLHelper.is_rocket_delivery(badges)
            
            # ============================================================
            # 리뷰 정보
            # ============================================================
            
            rating: Optional[float] = None
            review_count: Optional[int] = None
            
            try:
                rating_container = elem.find_element("css selector", self.selectors.RATING_CONTAINER)
                
                # 별점
                try:
                    star_elem = rating_container.find_element("css selector", self.selectors.RATING_STAR)
                    style = star_elem.get_attribute("style") or ""
                    rating = CoupangHTMLHelper.extract_rating_from_style(style)
                except:
                    pass
                
                # 리뷰 수
                try:
                    count_elem = rating_container.find_element("css selector", self.selectors.RATING_COUNT)
                    count_text = count_elem.text.strip()
                    review_count = CoupangHTMLHelper.extract_review_count(count_text)
                except:
                    pass
            except:
                pass
            
            # 최종가
            final_price = sale_price + shipping_fee
            
            return CoupangProduct(
                rank=rank,
                name=name,
                url=url,
                thumbnail_url=thumbnail_url,
                original_price=original_price,
                sale_price=sale_price,
                discount_rate=discount_rate,
                unit_price=unit_price,
                shipping_fee=shipping_fee,
                final_price=final_price,
                rating=rating,
                review_count=review_count,
                delivery_type=delivery_type,
                delivery_date=delivery_date,
                is_rocket=is_rocket,
                is_free_shipping=is_free_shipping,
                badges=badges,
            )
        
        except Exception as e:
            print(f"  [DEBUG] 상품 파싱 오류: {e}")
            return None