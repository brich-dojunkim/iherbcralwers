"""
쿠팡 HTML 선택자 라이브러리
HTML 구조 변경 시 이 파일만 수정하면 됨
"""

from dataclasses import dataclass
from typing import List, Optional


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
    # 상품 리스트 (검색 결과)
    # ========================================
    
    # 상품 카드
    PRODUCT_LIST_ITEM = "li.ProductUnit_productUnit__Qd6sv"
    
    # 상품명
    PRODUCT_NAME = "div.ProductUnit_productNameV2__cV9cw"
    
    # 가격 영역
    PRICE_AREA = "div.PriceArea_priceArea__NntJz"
    
    # 개별 가격 요소들
    PRICE_DISCOUNT_RATE = "div.custom-oos.fw-mr-[2px]"  # 할인율
    PRICE_ORIGINAL = "del.custom-oos"  # 원가
    PRICE_FINAL = "div.custom-oos.fw-text-[20px]"  # 최종가
    
    # 배송 정보
    SHIPPING_FREE = "span[style*='color:#454F5B']"  # "무료배송" 텍스트
    SHIPPING_INFO = "div.fw-text-[14px]"
    
    # 리뷰
    RATING_CONTAINER = "div.ProductRating_productRating__jjf7W"
    RATING_STAR = "div.ProductRating_star__RGSlV"
    RATING_COUNT = "span.ProductRating_ratingCount__R0Vhz"
    
    # 상품 링크
    PRODUCT_LINK = "a"
    
    # 상품 이미지
    PRODUCT_IMAGE = "figure.ProductUnit_productImage__Mqcg1 img"
    
    # ========================================
    # 상품 상세 페이지
    # ========================================
    
    # 상품명
    DETAIL_PRODUCT_NAME = "h1.prod-buy-header__title"
    
    # 가격
    DETAIL_PRICE = ".total-price strong"
    
    # 배송비
    DETAIL_SHIPPING = ".shipping-fee-txt"
    
    # 리뷰
    DETAIL_RATING = ".rating-star-container"
    DETAIL_REVIEW_COUNT = ".rating-count-txt"
    
    # 판매자 (중요!)
    DETAIL_SELLER = "a[href*='shop.coupang.com']"
    DETAIL_SELLER_ALTERNATIVES = [
        "a[href*='shop.coupang.com/vid']",
        ".seller-name",
        ".vendor-name"
    ]
    
    # 상품 이미지
    DETAIL_IMAGE = "img.prod-image__detail"
    
    # ========================================
    # 필터
    # ========================================
    
    # 낱개상품 필터 레이블
    FILTER_LABEL = "label"  # JavaScript로 텍스트 매칭 필요


@dataclass
class CoupangHTMLPatterns:
    """쿠팡 HTML 패턴 및 정규식"""
    
    # 가격 패턴
    PRICE_PATTERN = r'([\d,]+)원'
    DISCOUNT_PATTERN = r'(\d+)%'
    
    # 정수 패턴 (캡슐/정 개수)
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
    SHIPPING_FEE_PATTERN = r'배송비\s*([\d,]+)원'
    
    # 리뷰 수 패턴
    REVIEW_COUNT_PATTERN = r'\((\d+)\)'
    REVIEW_COUNT_PATTERN_ALT = r'(\d+)개'


class CoupangHTMLStructure:
    """쿠팡 HTML 구조 참조 (주석용)"""
    
    SEARCH_RESULT_EXAMPLE = """
    <li class="ProductUnit_productUnit__Qd6sv" data-id="...">
        <a href="/vp/products/...">
            <figure class="ProductUnit_productImage__Mqcg1">
                <img alt="..." src="...">
            </figure>
            <div class="ProductUnit_productInfo__1l0il">
                <div class="ProductUnit_productNameV2__cV9cw">
                    상품명
                </div>
                <div class="PriceArea_priceArea__NntJz">
                    <div class="custom-oos">
                        <div class="custom-oos fw-mr-[2px]">할인율</div>
                        <div class="custom-oos fw-text-[20px]">가격</div>
                    </div>
                </div>
                <div class="ProductRating_productRating__jjf7W">
                    <span class="ProductRating_rating__lMxS9">
                        <div class="ProductRating_star__RGSlV">별점</div>
                    </span>
                    <span class="ProductRating_ratingCount__R0Vhz">(리뷰수)</span>
                </div>
            </div>
        </a>
    </li>
    """
    
    DETAIL_PAGE_EXAMPLE = """
    <div class="twc-flex">
        판매자:
        <a href="https://shop.coupang.com/vid/A00506659">
            판매자명
            <div>판매자 상품 보러가기</div>
        </a>
    </div>
    """
    
    SEARCH_INPUT_EXAMPLE = """
    <input type="text" 
           maxlength="49" 
           placeholder="찾고 싶은 상품을 검색해보세요!" 
           class="headerSearchKeyword coupang-search" 
           name="q" 
           value="">
    """


class CoupangHTMLHelper:
    """쿠팡 HTML 파싱 헬퍼 함수"""
    
    @staticmethod
    def extract_price(text: str) -> Optional[int]:
        """
        텍스트에서 가격 추출
        
        Args:
            text: "104,700원" 또는 "104700원"
            
        Returns:
            104700 (int) 또는 None
        """
        import re
        match = re.search(CoupangHTMLPatterns.PRICE_PATTERN, text)
        if match:
            return int(match.group(1).replace(',', ''))
        return None
    
    @staticmethod
    def extract_discount_rate(text: str) -> Optional[int]:
        """
        텍스트에서 할인율 추출
        
        Args:
            text: "46%" 또는 "46 %"
            
        Returns:
            46 (int) 또는 None
        """
        import re
        match = re.search(CoupangHTMLPatterns.DISCOUNT_PATTERN, text)
        if match:
            return int(match.group(1))
        return None
    
    @staticmethod
    def extract_count(text: str, min_count: int = 10, max_count: int = 1000) -> Optional[int]:
        """
        텍스트에서 정수(캡슐/정 개수) 추출
        
        Args:
            text: 상품명
            min_count: 최소 유효 개수
            max_count: 최대 유효 개수
            
        Returns:
            정수 또는 None
        """
        import re
        
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
        """
        무료배송 여부 확인
        
        Args:
            text: 배송 정보 텍스트
            
        Returns:
            True if 무료배송
        """
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in CoupangHTMLPatterns.SHIPPING_FREE_KEYWORDS)
    
    @staticmethod
    def extract_shipping_fee(text: str) -> int:
        """
        배송비 추출
        
        Args:
            text: "배송비 2,500원" 또는 "무료배송"
            
        Returns:
            배송비 (int), 무료면 0
        """
        if CoupangHTMLHelper.is_free_shipping(text):
            return 0
        
        import re
        match = re.search(CoupangHTMLPatterns.SHIPPING_FEE_PATTERN, text)
        if match:
            return int(match.group(1).replace(',', ''))
        
        # 패턴 매칭 실패 시 일반 숫자 찾기
        match = re.search(r'([\d,]+)원', text)
        if match:
            return int(match.group(1).replace(',', ''))
        
        return 0
    
    @staticmethod
    def extract_review_count(text: str) -> Optional[int]:
        """
        리뷰 수 추출
        
        Args:
            text: "(5135)" 또는 "5135개"
            
        Returns:
            리뷰 수 (int) 또는 None
        """
        import re
        
        # (5135) 형태
        match = re.search(CoupangHTMLPatterns.REVIEW_COUNT_PATTERN, text)
        if match:
            return int(match.group(1).replace(',', ''))
        
        # 5135개 형태
        match = re.search(r'(\d+)개', text)
        if match:
            return int(match.group(1).replace(',', ''))
        
        return None
    
    @staticmethod
    def clean_seller_name(text: str) -> str:
        """
        판매자명 정리
        
        Args:
            text: "판매자명\n판매자 상품 보러가기"
            
        Returns:
            "판매자명"
        """
        # 줄바꿈 기준 분리
        lines = text.split('\n')
        
        # 제외할 키워드
        exclude_keywords = ['보러가기', '판매자:', 'seller']
        
        for line in lines:
            line = line.strip()
            if line and not any(keyword in line.lower() for keyword in exclude_keywords):
                return line
        
        return text.strip()


# ========================================
# 버전 정보
# ========================================

COUPANG_HTML_VERSION = "2024-11-25"
LAST_VERIFIED = "2024-11-25"

# 변경 이력
CHANGELOG = """
2024-11-25:
- 초기 버전 생성
- 검색 결과 페이지 선택자 추가
- 상품 상세 페이지 선택자 추가
- 헬퍼 함수 추가
"""
