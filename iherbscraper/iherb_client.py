"""
아이허브 사이트 상호작용 모듈 - 다양한 HTML 패턴 강화 대응
"""

import time
import re
import os
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from config import Config
from PIL import Image


class IHerbClient:
    """아이허브 사이트와의 모든 상호작용 담당 - 다양한 HTML 패턴 대응"""
    
    def __init__(self, browser_manager):
        self.browser = browser_manager
        self.driver = browser_manager.driver
    
    # ========== 기존 언어 설정 관련 메서드 ==========
    
    def set_language_to_english(self):
        """아이허브 언어를 영어로 자동 설정 - 빠른 버전"""
        try:
            print("  아이허브 언어 설정 (자동)...")
            
            # 1. 한국 아이허브 페이지 접속
            if not self.browser.safe_get(Config.KOREA_URL):
                print("  아이허브 접속 실패 - 기본 설정으로 진행")
                return False
            
            # 2. 설정 버튼 클릭 (타임아웃 단축: 10초 → 3초)
            try:
                setting_button = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '.selected-country-wrapper'))
                )
                setting_button.click()
                print("    설정 버튼 클릭 완료")
            except TimeoutException:
                print("  설정 버튼을 찾을 수 없습니다 - 수동 설정으로 진행")
                return self._manual_language_setting()
            
            # 짧은 대기 (기존: 2-4초 → 0.5초)
            time.sleep(0.5)
            
            # 3. 모달 열림 확인 (타임아웃 단축: 10초 → 2초)
            try:
                WebDriverWait(self.driver, 2).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.selection-list-wrapper'))
                )
                print("    설정 모달 열림 확인")
            except TimeoutException:
                print("  설정 모달이 열리지 않음 - 수동 설정으로 진행")
                return self._manual_language_setting()
            
            # 4. 언어 드롭다운 클릭 (타임아웃 단축: 5초 → 2초)
            try:
                language_dropdown = WebDriverWait(self.driver, 2).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '.select-language'))
                )
                language_dropdown.click()
                print("    언어 드롭다운 클릭 완료")
            except TimeoutException:
                print("  언어 드롭다운을 찾을 수 없음 - 수동 설정으로 진행")
                return self._manual_language_setting()
            
            # 짧은 대기
            time.sleep(0.3)
            
            # 5. English 옵션 선택 (타임아웃 단축: 5초 → 2초)
            try:
                english_option = WebDriverWait(self.driver, 2).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-val="en-US"]'))
                )
                english_option.click()
                print("    English 옵션 선택 완료")
            except TimeoutException:
                print("  English 옵션을 찾을 수 없음 - 수동 설정으로 진행")
                return self._manual_language_setting()
            
            # 짧은 대기
            time.sleep(0.3)
            
            # 6. 저장 버튼 클릭 (타임아웃 단축: 5초 → 2초)
            try:
                save_button = WebDriverWait(self.driver, 2).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '.save-selection'))
                )
                save_button.click()
                print("    저장 버튼 클릭 완료")
            except TimeoutException:
                print("  저장 버튼을 찾을 수 없음 - 수동 설정으로 진행")
                return self._manual_language_setting()
            
            # 페이지 리로드 대기 (기존: 10초 → 5초)
            print("  페이지 변경 대기 중...")
            time.sleep(3)
            
            # 7. 언어 설정 완료 확인 (URL 변경 또는 텍스트 변경 확인)
            current_url = self.driver.current_url
            if "kr.iherb.com" in current_url:
                print("  언어 설정 완료 - 영어 페이지로 이동됨")
                return True
            else:
                print("  URL 변경 감지되지 않음 - 수동 확인 필요")
                return True
            
        except Exception as e:
            print(f"  언어 설정 중 오류: {e}")
            return self._manual_language_setting()
    
    def _manual_language_setting(self):
        """수동 언어 설정 가이드"""
        print("  수동 언어 설정이 필요합니다.")
        print("  브라우저에서 직접 언어를 영어로 변경해주세요.")
        return True
    
    # ========== 검색 관련 메서드 ==========
    
    def search_product(self, search_term):
        """상품 검색 수행"""
        try:
            print(f"  '{search_term}' 검색 중...")
            
            # 검색 URL 구성
            search_url = f"{Config.BASE_URL}/search?kw={search_term.replace(' ', '+')}"
            
            if not self.browser.safe_get(search_url):
                return False
            
            # 페이지 로딩 완료 대기
            time.sleep(2)
            
            # 검색 결과 확인
            try:
                product_containers = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, Config.SELECTORS['product_containers']))
                )
                
                if len(product_containers) > 0:
                    print(f"    검색 결과: {len(product_containers)}개 상품 발견")
                    return True
                else:
                    print("    검색 결과가 없습니다.")
                    return False
                    
            except TimeoutException:
                print("    검색 결과 로딩 실패")
                return False
                
        except Exception as e:
            print(f"  검색 중 오류: {e}")
            return False
    
    def get_search_results(self, max_products=None):
        """검색 결과에서 상품 정보 수집"""
        products = []
        
        try:
            product_containers = self.driver.find_elements(By.CSS_SELECTOR, Config.SELECTORS['product_containers'])
            
            if max_products:
                product_containers = product_containers[:max_products]
            
            for idx, container in enumerate(product_containers):
                try:
                    # 상품 링크 추출
                    link_element = container.find_element(By.CSS_SELECTOR, Config.SELECTORS['product_link'])
                    product_url = link_element.get_attribute('href')
                    
                    # 상품명 추출
                    title_element = container.find_element(By.CSS_SELECTOR, Config.SELECTORS['product_title'])
                    product_name = title_element.text.strip()
                    
                    products.append({
                        'index': idx + 1,
                        'name': product_name,
                        'url': product_url
                    })
                    
                except Exception as e:
                    print(f"    상품 {idx+1} 정보 추출 실패: {e}")
                    continue
            
            print(f"    {len(products)}개 상품 정보 수집 완료")
            return products
            
        except Exception as e:
            print(f"  검색 결과 수집 중 오류: {e}")
            return []
    
    # ========== 상품 페이지 정보 추출 (강화 버전) ==========
    
    def extract_product_info(self, product_url):
        """상품 페이지에서 상세 정보 추출 - 다양한 HTML 패턴 대응"""
        try:
            if not self.browser.safe_get(product_url):
                return {}
            
            # 페이지 로딩 대기
            time.sleep(2)
            
            # HTML 소스 가져오기
            html_content = self.driver.page_source
            
            # 정보 추출
            product_info = {
                'url': product_url,
                'name': self._extract_product_name(),
                'product_code': self._extract_product_code(html_content),
                'price_info': self._extract_price_info_enhanced(html_content),
                'stock_info': self._extract_stock_info_enhanced(html_content),
                'specs': self._extract_product_specs()
            }
            
            return product_info
            
        except Exception as e:
            print(f"  상품 정보 추출 중 오류: {e}")
            return {}
    
    def _extract_product_name(self):
        """상품명 추출"""
        for selector in Config.SELECTORS['product_name']:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, selector)
                return element.text.strip()
            except:
                continue
        return ""
    
    def _extract_product_code(self, html_content):
        """상품 코드 추출"""
        # URL에서 추출
        url_match = re.search(Config.PATTERNS['product_code_url'], html_content)
        if url_match:
            return url_match.group(1)
        
        # item code에서 추출
        item_code_match = re.search(Config.PATTERNS['item_code'], html_content, re.IGNORECASE)
        if item_code_match:
            return item_code_match.group(1)
        
        return ""
    
    def _extract_price_info_enhanced(self, html_content):
        """가격 정보 추출 - 강화된 패턴 대응"""
        price_info = {
            'current_price_krw': 0,
            'list_price_krw': 0,
            'discount_percent': 0,
            'subscription_discount': 0,
            'price_per_unit': ""
        }
        
        try:
            # 현재 가격 추출 (우선순위 순서)
            price_patterns = [
                'krw_discount_price_red',      # 빨간색 할인 가격
                'krw_discount_price_inline',   # 신규: 인라인 스타일 할인 가격
                'krw_discount_price_simple',   # 단순 할인 가격
                'krw_out_of_stock_price',      # 품절 상품 가격
                'krw_list_price_span',         # 정가 span
                'krw_list_price_general',      # 일반 정가
            ]
            
            for pattern_name in price_patterns:
                pattern = Config.PATTERNS[pattern_name]
                match = re.search(pattern, html_content)
                if match:
                    price_info['current_price_krw'] = int(match.group(1).replace(',', ''))
                    break
            
            # 정가 추출
            list_price_patterns = ['krw_list_price_span', 'krw_list_price_general']
            for pattern_name in list_price_patterns:
                pattern = Config.PATTERNS[pattern_name]
                match = re.search(pattern, html_content)
                if match:
                    list_price = int(match.group(1).replace(',', ''))
                    if list_price > price_info['current_price_krw']:
                        price_info['list_price_krw'] = list_price
                        break
            
            # 할인율 추출 (강화된 패턴)
            discount_patterns = [
                'percent_off_bracket',
                'percent_off_parentheses',  # 신규: 한국어 패턴
                'percent_off_simple'
            ]
            
            for pattern_name in discount_patterns:
                pattern = Config.PATTERNS[pattern_name]
                match = re.search(pattern, html_content)
                if match:
                    price_info['discount_percent'] = int(match.group(1))
                    break
            
            # 정기배송 할인 추출
            sub_patterns = ['subscription_discount_future', 'subscription_discount_autoship']
            for pattern_name in sub_patterns:
                pattern = Config.PATTERNS[pattern_name]
                match = re.search(pattern, html_content)
                if match:
                    price_info['subscription_discount'] = int(match.group(1))
                    break
            
            # 단위당 가격 추출 (다양한 패턴 대응)
            unit_price_patterns = [
                'price_per_unit_small',        # 신규: 작은 텍스트
                'discount_price_per_unit',     # 신규: 할인 단위가격
                'list_price_per_unit',         # 신규: 정가 단위가격
                'price_per_unit_korean',       # 신규: 한국어 패턴
                'price_per_unit_span',
                'price_per_serving_direct',
                'price_per_unit_text'
            ]
            
            for pattern_name in unit_price_patterns:
                pattern = Config.PATTERNS[pattern_name]
                match = re.search(pattern, html_content)
                if match:
                    if len(match.groups()) >= 2:
                        price_info['price_per_unit'] = f"₩{match.group(1)}/{match.group(2)}"
                    else:
                        price_info['price_per_unit'] = match.group(0)
                    break
            
            return price_info
            
        except Exception as e:
            print(f"    가격 정보 추출 오류: {e}")
            return price_info
    
    def _extract_stock_info_enhanced(self, html_content):
        """재고 정보 추출 - 강화된 패턴 대응"""
        stock_info = {
            'is_in_stock': True,
            'stock_message': "",
            'back_in_stock_date': ""
        }
        
        try:
            # 품절 상태 확인 (강화된 패턴)
            out_of_stock_patterns = [
                'out_of_stock_testid_korean',  # 신규: 한국어 data-testid
                'out_of_stock_testid',
                'out_of_stock_korean',         # 신규: 한국어 품절
                'out_of_stock_text'
            ]
            
            for pattern_name in out_of_stock_patterns:
                pattern = Config.PATTERNS[pattern_name]
                if re.search(pattern, html_content, re.IGNORECASE):
                    stock_info['is_in_stock'] = False
                    stock_info['stock_message'] = "품절"
                    break
            
            # 알림 받기 버튼 확인 (추가 검증)
            notify_patterns = [
                'notify_me_button_korean',     # 신규: 한국어 알림받기
                'notify_me_button'
            ]
            
            for pattern_name in notify_patterns:
                pattern = Config.PATTERNS[pattern_name]
                if re.search(pattern, html_content, re.IGNORECASE):
                    stock_info['is_in_stock'] = False
                    if not stock_info['stock_message']:
                        stock_info['stock_message'] = "재고 없음"
                    break
            
            # 재입고 날짜 추출 (강화된 패턴)
            if not stock_info['is_in_stock']:
                restock_patterns = [
                    'back_in_stock_date_testid',
                    'back_in_stock_korean',        # 신규: 한국어 패턴
                    'back_in_stock_general'
                ]
                
                for pattern_name in restock_patterns:
                    pattern = Config.PATTERNS[pattern_name]
                    match = re.search(pattern, html_content, re.IGNORECASE)
                    if match:
                        stock_info['back_in_stock_date'] = match.group(1).strip()
                        break
            
            return stock_info
            
        except Exception as e:
            print(f"    재고 정보 추출 오류: {e}")
            return stock_info
    
    def _extract_product_specs(self):
        """상품 스펙 정보 추출"""
        try:
            specs_element = self.driver.find_element(By.CSS_SELECTOR, Config.SELECTORS['product_specs'])
            return specs_element.text.strip()
        except:
            return ""
    
    # ========== 이미지 다운로드 ==========
    
    def download_product_image(self, product_url, save_path):
        """상품 이미지 다운로드"""
        try:
            if not self.browser.safe_get(product_url):
                return False
            
            # 메인 이미지 찾기
            try:
                img_element = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.product-image img, .main-product-image img, img[data-testid="product-image"]'))
                )
                
                img_url = img_element.get_attribute('src')
                if not img_url:
                    return False
                
                # 이미지 다운로드
                response = requests.get(img_url, timeout=Config.IMAGE_DOWNLOAD_TIMEOUT)
                if response.status_code == 200:
                    with open(save_path, 'wb') as f:
                        f.write(response.content)
                    
                    # 이미지 크기 확인
                    if os.path.getsize(save_path) > Config.MAX_IMAGE_SIZE_MB * 1024 * 1024:
                        # 이미지 압축
                        self._compress_image(save_path)
                    
                    return True
                
            except TimeoutException:
                print(f"    이미지 요소를 찾을 수 없음")
                return False
                
        except Exception as e:
            print(f"    이미지 다운로드 실패: {e}")
            return False
    
    def _compress_image(self, image_path):
        """이미지 압축"""
        try:
            with Image.open(image_path) as img:
                # RGB로 변환 (JPEG 저장을 위해)
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # 품질을 낮춰서 압축
                img.save(image_path, 'JPEG', quality=85, optimize=True)
                
        except Exception as e:
            print(f"    이미지 압축 실패: {e}")