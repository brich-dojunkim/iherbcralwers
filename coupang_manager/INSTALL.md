# Coupang Manager 모듈 설치 가이드

## 📦 1단계: 모듈 설치

### 방법 1: 직접 복사 (권장)

```bash
cd /Users/brich/Desktop/iherb_price

# coupang_manager 폴더 복사
cp -r /mnt/user-data/outputs/coupang_manager .
```

### 방법 2: 다운로드 후 복사

1. 다운로드:
   - [coupang_manager_final.tar.gz](computer:///mnt/user-data/outputs/coupang_manager_final.tar.gz)

2. 압축 해제 후:
   ```bash
   cd /Users/brich/Desktop/iherb_price
   # 다운로드 폴더에서 복사
   cp -r ~/Downloads/coupang_manager .
   ```

---

## ✅ 2단계: 의존성 설치

```bash
pip install undetected-chromedriver
```

**주의**: selenium을 별도로 설치하지 마세요. undetected-chromedriver만 설치하면 됩니다.

---

## 🧪 3단계: 테스트

### 간단 테스트

```bash
cd /Users/brich/Desktop/iherb_price
python -c "from coupang_manager import CoupangBrowser; print('✓ 모듈 로드 성공')"
```

### 실제 크롤링 테스트

```python
# test_module.py
from coupang_manager import CoupangBrowser, CoupangCrawler

browser = CoupangBrowser()
crawler = CoupangCrawler(browser)

products = crawler.search_products("비타민C", top_n=3)

for p in products:
    print(p)

browser.close()
```

실행:
```bash
python test_module.py
```

---

## 🔧 4단계: 기존 프로젝트 적용

### 251124 프로젝트에 적용

```python
# 251124/main.py

import sys
import os

# coupang_manager 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 이제 사용 가능!
from coupang_manager import CoupangBrowser, CoupangCrawler

class ProductMatchingSystem:
    def __init__(self, excel_path, gemini_api_key=None, headless=False):
        self.excel_path = excel_path
        
        # 기존 코드 교체
        # self.browser = BrowserManager(headless=headless)
        
        # 새로운 코드
        self.browser = CoupangBrowser(headless=headless)
        self.coupang_crawler = CoupangCrawler(self.browser)
```

---

## 📁 최종 디렉토리 구조

```
iherb_price/
├── coupang_manager/          # ← 모듈 위치
│   ├── __init__.py
│   ├── browser.py
│   ├── crawler.py
│   ├── selectors.py
│   ├── models.py
│   ├── EXAMPLES.py
│   └── README.md
│
├── 251124/
│   └── main.py              # coupang_manager 사용
│
├── coupang2/
│   └── src/                 # coupang_manager 사용 가능
│
└── coupang/
    └── ...                  # coupang_manager 사용 가능
```

---

## 🎯 사용 예시

### 예시 1: 251124 프로젝트

```python
# 251124/main.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coupang_manager import CoupangBrowser, CoupangCrawler

browser = CoupangBrowser()
crawler = CoupangCrawler(browser)

# 쿠팡 검색
products = crawler.search_products("GNC 비타민", top_n=5)

# 상세 정보
detail = crawler.get_product_detail(products[0].url)

browser.close()
```

### 예시 2: 새 프로젝트

```python
# new_project/scraper.py
import sys
sys.path.insert(0, '/Users/brich/Desktop/iherb_price')

from coupang_manager import CoupangBrowser, CoupangCrawler

def main():
    browser = CoupangBrowser(headless=False)
    crawler = CoupangCrawler(browser)
    
    queries = ["비타민C", "오메가3", "프로바이오틱스"]
    
    for query in queries:
        print(f"\n검색: {query}")
        products = crawler.search_products(query, top_n=5)
        
        for p in products:
            print(f"  {p.rank}. {p.name[:40]}")
            print(f"     {p.final_price:,}원")
    
    browser.close()

if __name__ == '__main__':
    main()
```

---

## 🔄 업데이트 방법

### HTML 선택자 변경 시

```bash
# selectors.py만 수정
vim /Users/brich/Desktop/iherb_price/coupang_manager/selectors.py
```

```python
# 예: 상품명 선택자 변경
PRODUCT_NAME = "div.NEW_SELECTOR"  # 여기만 수정
```

모든 프로젝트가 자동으로 새 선택자 사용!

---

## 🐛 문제 해결

### ImportError: No module named 'coupang_manager'

```python
# 경로 확인
import sys
print(sys.path)

# iherb_price 경로 추가
sys.path.insert(0, '/Users/brich/Desktop/iherb_price')
```

### ModuleNotFoundError: No module named 'undetected_chromedriver'

```bash
pip install undetected-chromedriver
```

**주의**: `selenium`은 설치하지 마세요.

### Access Denied 발생

→ 정상 동작 (undetected-chromedriver가 이미 적용됨)
→ VPN 사용 권장

---

## 📊 성능 비교

### 기존 방식
```
- 각 프로젝트마다 별도 크롤러 코드
- HTML 변경 시 모든 파일 수정
- 선택자 하드코딩
```

### 모듈 방식
```
✅ 하나의 공통 모듈
✅ HTML 변경 시 한 파일만 수정
✅ 선택자 중앙 관리
✅ 재사용성 극대화
```

---

## 🎉 완료!

이제 `coupang_manager`를 모든 프로젝트에서 사용할 수 있습니다!

```python
from coupang_manager import CoupangBrowser, CoupangCrawler
```

---

## 📚 참고 문서

- [README.md](computer:///mnt/user-data/outputs/coupang_manager/README.md) - 모듈 설명
- [EXAMPLES.py](computer:///mnt/user-data/outputs/coupang_manager/EXAMPLES.py) - 사용 예시
