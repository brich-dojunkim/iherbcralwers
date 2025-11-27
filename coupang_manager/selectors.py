"""
쿠팡 HTML 선택자 라이브러리
HTML 구조 변경 시 이 파일만 수정하면 됨

✨ v3.0 - 검색 결과 페이지 기반 (상세 페이지 불필요)
"""

from dataclasses import dataclass
from typing import List, Optional
import re


@dataclass
class CoupangSelectors:
    """쿠팡 CSS 선택자 모음"""
    
    # ========================================
    # 검색 페이지
    # ========================================
    
    # 검색창
    SEARCH_INPUT = "input.headerSearchKeyword"
    SEARCH_INPUT_ALTERNATIVES = [
        "input.headerSearchKeyword",
        "input#headerSearchKeyword", 
        "input[name='q']",
        "input[placeholder*='검색']"
    ]
    
    SEARCH_BUTTON = "button.headerSearchBtn"
    
    # ========================================
    # 상품 리스트 (검색 결과) - 모든 정보 수집 가능
    # ========================================
    
    # 상품 카드
    PRODUCT_LIST_ITEM = "li.ProductUnit_productUnit__Qd6sv"
    
    # 상품명
    PRODUCT_NAME = "div.ProductUnit_productNameV2__cV9cw"
    
    # 가격 영역
    PRICE_AREA = "div.PriceArea_priceArea__NntJz"
    
    # 가격 요소들
    PRICE_DISCOUNT_RATE = "div.custom-oos"  # 할인율 (여러 개 중 필터 필요)
    PRICE_ORIGINAL = "del.custom-oos"        # 정가
    PRICE_SALE = "div.custom-oos"            # 판매가 (fw-text-[20px] 포함)
    PRICE_UNIT = "span.custom-oos"           # 1정당 가격
    
    # 배송 정보
    DELIVERY_INFO_CONTAINER = "div.fw-text-[14px]"
    DELIVERY_DATE_SPAN = "span[style*='color']"  # 도착 예정일
    FREE_SHIPPING_SPAN = "span[style*='454F5B']"  # 무료배송
    SHIPPING_FEE_BADGE = "div.TextBadge_feePrice__n_gta"  # 배송비 배지 (NEW)
    
    # 배지
    BADGE_CONTAINER = "div.fw-flex.fw-flex-wrap.fw-gap-[4px]"
    IMAGE_BADGE = "div.ImageBadge_default__JWaYp img"
    COUPICK_BADGE = "div.ImageBadge_coupick"
    ROCKET_BADGE = "img[src*='rocket']"
    JIKGU_BADGE = "img[src*='jikgu']"
    
    # 리뷰
    RATING_CONTAINER = "div.ProductRating_productRating__jjf7W"
    RATING_STAR = "div.ProductRating_star__RGSlV"
    RATING_COUNT = "span.ProductRating_ratingCount__R0Vhz"
    
    # 상품 링크
    PRODUCT_LINK = "a"
    
    # 상품 이미지
    PRODUCT_IMAGE = (
        "img[alt='Product image'], "
        "img.twc-w-full, "
        "figure.ProductUnit_productImage__Mqcg1 img"
    )
    
    # 필터
    FILTER_LABEL = "label"


@dataclass
class CoupangHTMLPatterns:
    """쿠팡 HTML 패턴 및 정규식"""
    
    # 가격 패턴
    PRICE_PATTERN = r'([\d,]+)원'
    DISCOUNT_PATTERN = r'(\d+)%'
    UNIT_PRICE_PATTERN = r'1정당\s*([\d,]+)원'
    
    # 정수 패턴
    COUNT_PATTERNS = [
        r'(\d+)정',
        r'(\d+)캡슐',
        r'(\d+)알',
        r'(\d+)개입',
        r'(\d+)tablets',
        r'(\d+)caps',
    ]
    
    # 배송비 패턴
    SHIPPING_FREE_KEYWORDS = ['무료', '무료배송', 'free']
    SHIPPING_FEE_PATTERN = r'([\d,]+)원'
    
    # 리뷰 수 패턴
    REVIEW_COUNT_PATTERN = r'\((\d+)\)'


class CoupangHTMLHelper:
    """쿠팡 HTML 파싱 헬퍼 함수"""
    
    @staticmethod
    def extract_price(text: str) -> Optional[int]:
        """가격 추출: "104,700원" → 104700"""
        match = re.search(CoupangHTMLPatterns.PRICE_PATTERN, text)
        if match:
            return int(match.group(1).replace(',', ''))
        return None
    
    @staticmethod
    def extract_discount_rate(text: str) -> Optional[int]:
        """할인율 추출: "46%" → 46"""
        match = re.search(CoupangHTMLPatterns.DISCOUNT_PATTERN, text)
        if match:
            return int(match.group(1))
        return None
    
    @staticmethod
    def extract_unit_price(text: str) -> Optional[int]:
        """1정당 가격 추출: "(1정당 340원)" → 340"""
        match = re.search(CoupangHTMLPatterns.UNIT_PRICE_PATTERN, text)
        if match:
            return int(match.group(1).replace(',', ''))
        return None
    
    @staticmethod
    def extract_count(text: str, min_count: int = 10, max_count: int = 1000) -> Optional[int]:
        """정수(캡슐/정) 추출"""
        text_lower = text.lower()
        for pattern in CoupangHTMLPatterns.COUNT_PATTERNS:
            match = re.search(pattern, text_lower)
            if match:
                count = int(match.group(1))
                if min_count <= count <= max_count:
                    return count
        return None
    
    @staticmethod
    def is_free_shipping(text: str) -> bool:
        """무료배송 여부"""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in CoupangHTMLPatterns.SHIPPING_FREE_KEYWORDS)
    
    @staticmethod
    def extract_shipping_fee(text: str) -> int:
        """
        배송비 추출
        
        Args:
            text: "배송비 2,500원" 또는 "배송비 11,900원 조건부 무료배송" 또는 "무료배송"
        
        Returns:
            배송비 (int)
        """
        if CoupangHTMLHelper.is_free_shipping(text):
            # "무료" 키워드가 있어도 금액이 명시되어 있으면 그 금액 추출
            # 예: "배송비 2,500원 조건부 무료배송" → 2500
            pass
        
        # "배송비 11,900원" → 11900 추출
        match = re.search(r'배송비\s*([\d,]+)원', text)
        if match:
            return int(match.group(1).replace(',', ''))
        
        # 무료배송
        return 0
    
    @staticmethod
    def extract_review_count(text: str) -> Optional[int]:
        """리뷰 수 추출: "(5135)" → 5135"""
        match = re.search(CoupangHTMLPatterns.REVIEW_COUNT_PATTERN, text)
        if match:
            return int(match.group(1))
        return None
    
    @staticmethod
    def parse_delivery_type(img_src: str) -> Optional[str]:
        """배지 이미지에서 배송 타입: "logo_jikgu.png" → "직구" """
        if not img_src:
            return None
        
        img_src_lower = img_src.lower()
        
        if 'jikgu' in img_src_lower:
            return "직구"
        elif 'rocket' in img_src_lower or 'badge_' in img_src_lower:
            return "로켓배송"
        elif 'wow' in img_src_lower:
            return "와우배송"
        
        return None
    
    @staticmethod
    def is_rocket_delivery(badges: List[str]) -> bool:
        """배지 목록에서 로켓배송 여부"""
        return any('로켓' in badge for badge in badges)
    
    @staticmethod
    def extract_rating_from_style(style: str) -> Optional[float]:
        """style width%에서 별점: "width:100%" → 5.0"""
        match = re.search(r'width:\s*([\d\.]+)%', style)
        if match:
            width = float(match.group(1))
            return round(width / 20.0, 1)
        return None