"""
coupang_manager 모듈 사용 예시
"""

# ========================================
# 예시 1: 기본 사용
# ========================================

from coupang_manager import CoupangBrowser, CoupangCrawler

# 브라우저 초기화
browser = CoupangBrowser(headless=False)

# 크롤러 생성
crawler = CoupangCrawler(browser)

# 검색
products = crawler.search_products("비타민C", top_n=5)

# 결과 출력
for product in products:
    print(f"[{product.rank}] {product.name}")
    print(f"  가격: {product.final_price:,}원")
    print(f"  리뷰: {product.review_count}개")
    print()

# 상세 정보
if products:
    detail = crawler.get_product_detail(products[0].url)
    if detail:
        print(f"판매자: {detail.get('seller_name', 'N/A')}")

# 브라우저 종료
browser.close()


# ========================================
# 예시 2: 선택자만 사용
# ========================================

from coupang_manager import CoupangSelectors, CoupangHTMLHelper

selectors = CoupangSelectors()
helper = CoupangHTMLHelper()

# 가격 파싱
price = helper.extract_price("104,700원")  # 104700
print(f"가격: {price}")

# 배송비 파싱
shipping = helper.extract_shipping_fee("무료배송")  # 0
print(f"배송비: {shipping}")


# ========================================
# 예시 3: 251124 폴더에서 사용
# ========================================

# 251124/main.py
import sys
import os

# coupang_manager 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coupang_manager import CoupangBrowser, CoupangCrawler

class ProductMatchingSystem:
    def __init__(self):
        self.browser = CoupangBrowser(headless=False)
        self.crawler = CoupangCrawler(self.browser)
    
    def search_coupang(self, query):
        return self.crawler.search_products(query, top_n=5)
    
    def cleanup(self):
        self.browser.close()


# ========================================
# 예시 4: coupang2에서 사용
# ========================================

# coupang2/src/my_script.py
import sys
sys.path.insert(0, '/Users/brich/Desktop/iherb_price')

from coupang_manager import CoupangBrowser, CoupangCrawler

browser = CoupangBrowser()
crawler = CoupangCrawler(browser)

products = crawler.search_products("GNC", top_n=10)
for p in products:
    print(p)

browser.close()


# ========================================
# 예시 5: 모델 변환
# ========================================

from coupang_manager import CoupangProduct

# CoupangProduct를 딕셔너리로
product_dict = product.to_dict()

# DataFrame으로 변환
import pandas as pd
products_list = [p.to_dict() for p in products]
df = pd.DataFrame(products_list)
