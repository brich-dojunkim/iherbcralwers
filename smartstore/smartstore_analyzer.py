#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
네이버 스마트스토어 특성 분석 모듈
- 스토어의 인기 상품 40개 수집
- Gemini API를 활용한 스토어 특성 분석
- 상품 카테고리, 가격대, 타겟층 등 분석

사용:
  pip install google-generativeai
"""

import time
import json
import re
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

import google.generativeai as genai
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# ===== 설정 =====
PRODUCTS_PAGE_PATH = "category/ALL?st=POPULAR&dt=BIG_IMAGE&page=1&size=40"
PRODUCT_ITEM_SELECTOR = 'li.Hz4XxKbt9h'  # 상품 아이템 컨테이너
PAGE_WAIT = 15

# Gemini API 설정
GEMINI_API_KEY = "AIzaSyD-GIKFbdwJO2BW5l-w3nCv_DQGFHA4VOU"  # 여기에 실제 API 키 입력
GEMINI_MODEL = "gemini-2.5-flash"

class ProductInfo:
    """상품 정보 데이터 클래스"""
    def __init__(self):
        self.name = ""
        self.price = ""
        self.original_price = ""
        self.discount_rate = ""
        self.rating = ""
        self.review_count = ""
        self.description = ""
        self.shipping_info = ""
        self.badges = []  # BEST, NEW 등
        self.category_hints = ""

    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'price': self.price,
            'original_price': self.original_price,
            'discount_rate': self.discount_rate,
            'rating': self.rating,
            'review_count': self.review_count,
            'description': self.description[:200],  # 설명은 200자로 제한
            'shipping_info': self.shipping_info,
            'badges': ', '.join(self.badges),
            'category_hints': self.category_hints
        }

class StoreAnalyzer:
    """스토어 특성 분석기"""
    
    def __init__(self):
        self.gemini_api_key = GEMINI_API_KEY
        if self.gemini_api_key:
            genai.configure(api_key=self.gemini_api_key)
            self.model = genai.GenerativeModel(GEMINI_MODEL)
        else:
            self.model = None
            print("⚠️  Gemini API 키가 설정되지 않았습니다. 분석 기능이 비활성화됩니다.")

    def extract_product_info(self, product_element) -> ProductInfo:
        """단일 상품 정보 추출"""
        product = ProductInfo()
        
        try:
            # 상품명 추출
            try:
                name_elem = product_element.find_element(By.CSS_SELECTOR, 'strong.xSW7C99vO3')
                product.name = name_elem.text.strip()
            except NoSuchElementException:
                pass

            # 가격 정보 추출
            try:
                # 현재 가격
                price_elem = product_element.find_element(By.CSS_SELECTOR, 'span.zIK_uvWc6D')
                product.price = price_elem.text.strip()
                
                # 원가 (할인 전 가격) - 있는 경우만
                try:
                    original_price_elem = product_element.find_element(By.CSS_SELECTOR, '.original_price, .discount_price')
                    product.original_price = original_price_elem.text.strip()
                except NoSuchElementException:
                    pass
                
                # 할인율 - 있는 경우만
                try:
                    discount_elem = product_element.find_element(By.CSS_SELECTOR, '.discount_rate')
                    product.discount_rate = discount_elem.text.strip()
                except NoSuchElementException:
                    pass
                    
            except NoSuchElementException:
                pass

            # 평점 및 리뷰 수
            try:
                rating_elem = product_element.find_element(By.CSS_SELECTOR, 'span.QvZNCo_N1O')
                product.rating = rating_elem.text.strip()
            except NoSuchElementException:
                pass
                
            try:
                review_elem = product_element.find_element(By.CSS_SELECTOR, 'span.GF9kbo_Z2x')
                product.review_count = review_elem.text.strip()
            except NoSuchElementException:
                pass

            # 상품 설명 (상품 상세 설명 영역)
            try:
                desc_elem = product_element.find_element(By.CSS_SELECTOR, 'p.mPC1nrqpeJ')
                product.description = desc_elem.text.strip()
            except NoSuchElementException:
                pass

            # 배송 정보
            try:
                shipping_elem = product_element.find_element(By.CSS_SELECTOR, '.WO8xT6tEmv .text')
                product.shipping_info = shipping_elem.text.strip()
            except NoSuchElementException:
                pass

            # 뱃지 정보 (BEST, NEW 등)
            try:
                badge_elems = product_element.find_elements(By.CSS_SELECTOR, 'em.w3CcguDho8')
                product.badges = [badge.text.strip() for badge in badge_elems if badge.text.strip()]
            except NoSuchElementException:
                pass

            # 카테고리 힌트 (data 속성에서)
            try:
                category_data = product_element.get_attribute('data-shp-contents-dtl')
                if category_data:
                    # JSON 파싱해서 카테고리 정보 추출
                    import json
                    data = json.loads(category_data)
                    for item in data:
                        if item.get('key') == 'exhibition_category':
                            product.category_hints = item.get('value', '')
                            break
            except:
                pass

        except Exception as e:
            print(f"  상품 정보 추출 오류: {e}")

        return product

    def collect_store_products(self, driver, store_url: str) -> List[ProductInfo]:
        """스토어의 인기 상품 40개 수집"""
        products = []
        
        try:
            # 상품 목록 페이지 URL 생성
            parsed_url = urlparse(store_url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
            products_url = urljoin(base_url.rstrip('/') + '/', PRODUCTS_PAGE_PATH)
            
            print(f"  상품 목록 페이지 접속: {products_url}")
            driver.get(products_url)
            
            # 페이지 로드 대기
            WebDriverWait(driver, PAGE_WAIT).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # 상품 목록이 로드될 때까지 대기
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, PRODUCT_ITEM_SELECTOR))
                )
            except TimeoutException:
                print("  상품 목록을 찾을 수 없습니다.")
                return products

            # 스크롤해서 모든 상품 로드
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # 상품 요소들 찾기
            product_elements = driver.find_elements(By.CSS_SELECTOR, PRODUCT_ITEM_SELECTOR)
            print(f"  발견된 상품 수: {len(product_elements)}개")

            # 각 상품 정보 추출
            for i, element in enumerate(product_elements[:40]):  # 최대 40개
                try:
                    product = self.extract_product_info(element)
                    if product.name:  # 상품명이 있는 경우만 추가
                        products.append(product)
                except Exception as e:
                    print(f"  상품 {i+1} 추출 실패: {e}")
                    continue

            print(f"  성공적으로 추출된 상품 수: {len(products)}개")

        except Exception as e:
            print(f"  상품 수집 오류: {e}")

        return products

    def analyze_store_characteristics(self, products: List[ProductInfo], store_name: str = "") -> str:
        """Gemini API를 사용하여 스토어 특성 분석"""
        if not self.model or not products:
            return "분석 불가"

        try:
            # 분석용 데이터 준비
            products_data = [product.to_dict() for product in products]
            
            # 프롬프트 생성
            prompt = f"""
다음은 네이버 스마트스토어의 상품 {len(products)}개 데이터입니다.

상품 데이터:
{json.dumps(products_data, ensure_ascii=False, indent=2)[:4000]}

다음 상품 데이터를 분석하여 이 쇼핑몰의 주요 특성 3가지를 간결하게 요약해 주세요. 
스토어의 핵심 판매 상품, 가격 경쟁력, 마케팅 포인트를 중심으로 답변해 주세요.

답변은 다음 형식에 맞춰 한국어로 작성해 주세요. 특수문자(**나 ##)는 사용하지 말고 일반 텍스트로만 작성해주세요.

[특성 1] 제목
(근거) 구체적인 근거

[특성 2] 제목  
(근거) 구체적인 근거

[특성 3] 제목
(근거) 구체적인 근거
"""

            # Gemini API 호출
            response = self.model.generate_content(prompt)
            analysis = response.text.strip()
            
            print(f"  → 스토어 분석 완료!")
            return analysis

        except Exception as e:
            print(f"  스토어 분석 오류: {e}")
            return f"분석 오류: {str(e)[:50]}"

    def get_store_analysis(self, driver, store_url: str, store_name: str = "") -> Dict[str, str]:
        """스토어 분석 통합 실행"""
        print("  📊 스토어 특성 분석 시작...")
        
        result = {
            'products_count': '0',
            'analysis': '분석 실패',
            'error': ''
        }
        
        try:
            # 1. 상품 정보 수집
            products = self.collect_store_products(driver, store_url)
            result['products_count'] = str(len(products))
            
            if not products:
                result['analysis'] = '상품 정보 없음'
                result['error'] = '상품 목록을 찾을 수 없음'
                return result
            
            # 2. 스토어 특성 분석
            if self.model:
                analysis = self.analyze_store_characteristics(products, store_name)
                result['analysis'] = analysis
            else:
                result['analysis'] = f"{len(products)}개 상품, 분석 API 비활성화"
            
            print(f"  ✓ 스토어 분석 완료!")
            
        except Exception as e:
            result['error'] = str(e)
            result['analysis'] = '분석 실패'
            print(f"  ✗ 스토어 분석 실패: {e}")
        
        return result

def create_analyzer() -> StoreAnalyzer:
    """분석기 인스턴스 생성 (API 키는 모듈에서 자체 관리)"""
    return StoreAnalyzer()

# 테스트용 메인 함수
if __name__ == "__main__":
    import os
    import argparse
    import undetected_chromedriver as uc
    
    # 명령행 인수 파싱
    parser = argparse.ArgumentParser(description='스토어 분석기 테스트')
    parser.add_argument('--url', default="https://smartstore.naver.com/37avenue", help='테스트할 스토어 URL')
    parser.add_argument('--headless', action='store_true', help='헤드리스 모드')
    
    args = parser.parse_args()
    
    # API 키 설정 (store_analyzer.py에서 자체 관리)
    analyzer = create_analyzer()
    
    # 테스트 드라이버 설정
    def make_test_driver(headless=False):
        opt = uc.ChromeOptions()
        opt.add_argument("--disable-popup-blocking")
        opt.add_argument("--no-sandbox")
        opt.add_argument("--disable-blink-features=AutomationControlled")
        if headless:
            opt.add_argument("--headless=new")
        driver = uc.Chrome(options=opt)
        driver.set_page_load_timeout(45)
        return driver
    
    print(f"테스트 URL: {args.url}")
    print(f"Gemini API: {'활성화' if GEMINI_API_KEY else '비활성화'}")
    print("="*50)
    
    # 드라이버 생성 및 테스트 실행
    driver = make_test_driver(headless=args.headless)
    
    try:
        # 분석 실행
        result = analyzer.get_store_analysis(driver, args.url, "테스트스토어")
        
        print("\n=== 테스트 결과 ===")
        print(f"수집된 상품 수: {result.get('products_count', '0')}개")
        print(f"\n=== 스토어 분석 결과 ===")
        print(result.get('analysis', '분석 실패'))
        print("=" * 50)
        if result.get('error'):
            print(f"오류: {result.get('error')}")
        
        print("\n테스트 완료!")
        
    except Exception as e:
        print(f"테스트 중 오류 발생: {e}")
    finally:
        driver.quit()
        
    print("스토어 분석 모듈 테스트 완료")