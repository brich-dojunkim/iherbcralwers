# Coupang Manager

쿠팡 크롤링 통합 모듈 (undetected-chromedriver)

---

## 설치

```bash
pip install undetected-chromedriver
```

**주의**: selenium을 별도로 설치하지 마세요. undetected-chromedriver가 내부적으로 처리합니다.

---

## 프로젝트 구조

```
iherb_price/
├── coupang_manager/        ← 이 모듈
│   ├── __init__.py
│   ├── browser.py          # 브라우저 (undetected-chromedriver)
│   ├── crawler.py          # 크롤러
│   ├── selectors.py        # HTML 선택자 & 헬퍼
│   └── models.py           # CoupangProduct 모델
│
├── 251124/                 ← 프로젝트 1
├── coupang2/               ← 프로젝트 2
│   └── src/
└── coupang/                ← 프로젝트 3
```

---

## 사용법

### 251124에서 사용

```python
# 251124/main.py
import sys
import os

# coupang_manager 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coupang_manager import CoupangBrowser, CoupangCrawler

# 초기화
browser = CoupangBrowser(headless=False)
crawler = CoupangCrawler(browser)

# 검색
products = crawler.search_products("비타민C", top_n=5)

# 상세
detail = crawler.get_product_detail(products[0].url)

# 종료
browser.close()
```

### coupang2에서 사용

```python
# coupang2/src/my_script.py
import sys
sys.path.insert(0, '/Users/brich/Desktop/iherb_price')

from coupang_manager import CoupangBrowser, CoupangCrawler

browser = CoupangBrowser()
crawler = CoupangCrawler(browser)
products = crawler.search_products("GNC", top_n=10)
browser.close()
```

---

## 주요 클래스

### CoupangBrowser

```python
browser = CoupangBrowser(headless=False)
browser.get_with_coupang_referrer(url)
browser.close()
```

### CoupangCrawler

```python
crawler = CoupangCrawler(browser)
products = crawler.search_products(query, top_n=5)
detail = crawler.get_product_detail(url)
```

### CoupangProduct

```python
product.rank            # 순위
product.name            # 상품명
product.price           # 가격
product.final_price     # 최종가 (가격 + 배송비)
product.url             # URL
product.to_dict()       # 딕셔너리 변환
```

### CoupangSelectors

```python
from coupang_manager import CoupangSelectors

selectors = CoupangSelectors()
selectors.PRODUCT_NAME      # "div.ProductUnit_productNameV2__cV9cw"
selectors.SEARCH_INPUT      # "input.headerSearchKeyword"
```

### CoupangHTMLHelper

```python
from coupang_manager import CoupangHTMLHelper

helper = CoupangHTMLHelper()
helper.extract_price("104,700원")              # 104700
helper.extract_shipping_fee("무료배송")        # 0
helper.extract_count("GNC 비타민 120정")       # 120
```

---

## HTML 변경 대응

HTML 구조 변경 시 `selectors.py`만 수정:

```python
# iherb_price/coupang_manager/selectors.py
class CoupangSelectors:
    PRODUCT_NAME = "div.NEW_CLASS"  # 여기만 수정
```

모든 프로젝트에 자동 반영됨.

---

## 예시

`EXAMPLES.py` 참조
