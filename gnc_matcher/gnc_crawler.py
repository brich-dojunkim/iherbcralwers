"""
GNC 상품 검색
🔄 리팩토링: coupang_manager 모듈 사용 (selenium import 제거)
"""

import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import time
import re
from typing import Optional
from dataclasses import dataclass


@dataclass
class GNCProduct:
    """GNC 상품 정보"""
    product_code: str
    product_name: str
    brand: str
    gnc_url: str
    thumbnail_url: Optional[str] = None
    count: Optional[int] = None


class GNCCrawler:
    """GNC 크롤러"""
    
    def __init__(self, browser_manager=None, debug: bool = False):
        if not browser_manager:
            raise ValueError("browser_manager 필요")
        
        self.browser = browser_manager
        self.driver = browser_manager.driver
        self.debug = debug
    
    def _check_perimeterx(self) -> bool:
        """PerimeterX 감지 - 정확한 키워드만 사용"""
        try:
            # 페이지 소스와 URL 확인
            page_source = self.driver.page_source.lower()
            current_url = self.driver.current_url.lower()
            page_title = self.driver.title.lower()
            
            # ⭐ PerimeterX 명확한 키워드만 (false positive 방지)
            px_exact_keywords = [
                '_pxhd=',  # PerimeterX 헤더
                'px-captcha',  # CAPTCHA 페이지
                'perimeterx.net',  # PerimeterX 도메인
                'px-block-page',  # 차단 페이지
                'press & hold',  # CAPTCHA 지시사항
                'human verification',  # 인증 필요
            ]
            
            # 명확한 차단 시그널
            for keyword in px_exact_keywords:
                if keyword in page_source or keyword in current_url:
                    return True
            
            # 제목에 "access denied" 또는 "blocked" 있으면서 상품 없으면
            if ('access denied' in page_title or 'blocked' in page_title):
                if 'product-tile' not in page_source:  # 상품이 없으면
                    return True
            
            return False
            
        except Exception as e:
            if self.debug:
                print(f"  [DEBUG] PerimeterX 감지 오류: {e}")
            return False
    
    def _wait_for_user_resolution(self):
        """사용자가 PerimeterX 해결할 때까지 대기"""
        print("\n" + "="*60)
        print("⚠️  PerimeterX 감지!")
        print("="*60)
        print("\n다음 단계를 수행하세요:")
        print("1. 브라우저 창에서 보안 인증(CAPTCHA)을 완료하세요")
        print("2. GNC 사이트가 정상적으로 표시되는지 확인하세요")
        print("3. 완료되면 아무 키나 누르세요 (q: 종료)")
        print("\n브라우저를 닫지 마세요!")
        print("="*60)
        
        # 사용자 입력 대기
        while True:
            response = input("\n✓ PerimeterX 해결 완료? (Enter: 계속, q: 종료): ").strip().lower()
            
            if response == 'q':
                print("종료합니다...")
                raise KeyboardInterrupt("사용자가 종료를 선택했습니다")
            
            # PerimeterX 재확인
            if not self._check_perimeterx():
                print("\n✅ PerimeterX 해결 완료! 크롤링을 재개합니다...\n")
                time.sleep(2)
                break
            else:
                print("\n⚠️  아직 PerimeterX가 감지됩니다. 다시 확인하세요.")
                print("   브라우저에서 CAPTCHA를 완료했는지 확인하세요.")
    
    def search_product(self, product_code: str) -> Optional[GNCProduct]:
        """GNC 상품 검색"""
        try:
            print(f"  GNC 검색 URL 접속...")
            
            search_url = f"https://www.gnc.com/search/?q={product_code}"
            self.driver.get(search_url)
            time.sleep(8)
            
            if self.debug:
                print(f"  [DEBUG] 현재 URL: {self.driver.current_url}")
            
            # ⭐ PerimeterX 감지
            if self._check_perimeterx():
                self._wait_for_user_resolution()
                # 해결 후 페이지 새로고침
                self.driver.get(search_url)
                time.sleep(5)
            
            # 첫 번째 상품 찾기 (문자열 방식)
            max_wait = 10
            for _ in range(max_wait):
                product_elem = self.driver.find_elements("css selector", ".product-tile")
                if product_elem:
                    product_elem = product_elem[0]
                    break
                time.sleep(1)
            else:
                print(f"  ✗ 상품을 찾을 수 없음")
                return None
            
            if self.debug:
                print(f"  [DEBUG] 상품 요소 발견")
            
            # 상품명
            product_name = ""
            try:
                name_elem = product_elem.find_element("css selector", ".tile-product-name")
                product_name = name_elem.text.strip()
            except:
                pass
            
            if not product_name:
                print(f"  ✗ 상품명을 찾을 수 없음")
                return None
            
            print(f"  ✓ GNC 상품: {product_name[:50]}...")
            
            # URL
            product_url = ""
            try:
                link_elem = product_elem.find_element("css selector", "a.thumb-link, a.name-link")
                product_url = link_elem.get_attribute('href')
            except:
                product_url = search_url
            
            # 썸네일
            thumbnail_url = None
            try:
                img_elem = product_elem.find_element("css selector", "img.product-tile-img")
                thumbnail_url = img_elem.get_attribute('src')
            except:
                pass
            
            # 브랜드
            brand = "GNC"
            try:
                brand_elem = product_elem.find_element("css selector", ".tile-brand-name")
                brand_text = brand_elem.text.strip()
                if brand_text:
                    brand = brand_text
            except:
                pass
            
            # 정수
            count = self._extract_count(product_name)
            
            return GNCProduct(
                product_code=str(product_code),
                product_name=product_name,
                brand=brand,
                gnc_url=product_url,
                thumbnail_url=thumbnail_url,
                count=count
            )
            
        except Exception as e:
            print(f"  ✗ GNC 검색 오류: {e}")
            return None
    
    def _extract_count(self, text: str) -> Optional[int]:
        """정수 추출"""
        if not text:
            return None
        
        patterns = [
            r'(\d+)\s*(?:tablets|capsules|caplets|softgels)',
            r'(\d+)\s*ct',
        ]
        
        text_lower = text.lower()
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                count = int(match.group(1))
                if 10 <= count <= 1000:
                    return count
        
        return None