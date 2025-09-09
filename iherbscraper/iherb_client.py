"""
아이허브 사이트 상호작용 모듈 - 이미지 크롤링 기능 추가
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
    """아이허브 사이트와의 모든 상호작용 담당 - 이미지 크롤링 포함"""
    
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
            time.sleep(2)  # 기존: 5초 → 2초
            
            # 7. 영어 사이트 변경 확인 (타임아웃 단축: 10초 → 3초)
            try:
                WebDriverWait(self.driver, 3).until(
                    lambda driver: "kr.iherb.com" not in driver.current_url
                )
                print("  언어 변경 완료 ✓")
                return True
            except TimeoutException:
                print("  URL 변경이 감지되지 않음")
                # URL이 변경되지 않아도 성공으로 간주 (언어만 변경된 경우)
                return True
                
        except Exception as e:
            print(f"  자동 언어 설정 실패: {e}")
            return self._manual_language_setting()

    def _manual_language_setting(self):
        """수동 언어 설정 안내 (폴백)"""
        print("\n" + "="*50)
        print("  수동으로 언어를 영어로 변경해주세요:")
        print("  1. 우상단 설정 버튼 클릭")
        print("  2. 언어를 'English'로 선택")
        print("  3. '저장하기' 버튼 클릭")
        print("="*50)
        
        input("  언어 변경 완료 후 Enter 키를 눌러주세요...")
        
        # 영어 사이트로 직접 이동
        if self.browser.safe_get("https://www.iherb.com"):
            print("  영어 사이트 직접 접속 완료 ✓")
            return True
        else:
            print("  영어 사이트 접속 실패")
            return False
    
    # ========== 기존 검색 관련 메서드 ==========
    
    def get_multiple_products(self, search_url):
        """검색 결과에서 여러 상품 정보 추출"""
        try:
            if not self.browser.safe_get(search_url):
                return []
            
            products = []
            
            # 검색 결과 로딩 대기
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, Config.SELECTORS['product_containers']))
                )
            except TimeoutException:
                print("    검색 결과 로딩 실패")
                return []
            
            product_elements = self.driver.find_elements(
                By.CSS_SELECTOR, 
                Config.SELECTORS['product_containers']
            )[:Config.MAX_PRODUCTS_TO_COMPARE]
            
            for i, element in enumerate(product_elements):
                try:
                    link_elem = element.find_element(By.CSS_SELECTOR, Config.SELECTORS['product_link'])
                    product_url = link_elem.get_attribute("href")
                    
                    title_elem = element.find_element(By.CSS_SELECTOR, Config.SELECTORS['product_title'])
                    product_title = title_elem.text.strip()
                    
                    if product_url and "/pr/" in product_url and product_title:
                        products.append({
                            'url': product_url,
                            'title': product_title,
                            'rank': i + 1
                        })
                
                except Exception:
                    continue
            
            return products
            
        except Exception as e:
            print(f"    검색 결과 추출 실패: {e}")
            return []
    
    # ========== 기존 상품 정보 추출 메서드 ==========
    
    def extract_product_name(self):
        """상품명 추출 - 타임아웃 단축"""
        for selector in Config.SELECTORS['product_name']:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, selector)
                name = element.text.strip()
                if name and len(name) > 5:
                    return name
            except:
                continue
        return None
    
    def extract_product_code(self, product_url):
        """상품코드 추출 - 빠른 우선순위"""
        # 방법 1: URL에서 추출 (가장 빠름)
        try:
            url_match = re.search(Config.PATTERNS['product_code_url'], product_url)
            if url_match:
                return url_match.group(1)
        except:
            pass
        
        # 방법 2: data 속성에서 찾기
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, Config.SELECTORS['part_number'])
            for element in elements:
                value = element.get_attribute("data-part-number")
                if value and re.match(r'^[A-Z0-9-]+$', value):
                    return value
        except:
            pass
        
        # 방법 3: product-specs-list에서 찾기 (가장 느림)
        try:
            specs_element = self.driver.find_element(By.CSS_SELECTOR, Config.SELECTORS['product_specs'])
            text = specs_element.text
            match = re.search(Config.PATTERNS['item_code'], text, re.IGNORECASE)
            if match:
                return match.group(1)
        except:
            pass
        
        return None
    
    # ========== 기존 정규식 기반 가격 정보 추출 메서드 ==========
    
    def extract_price_info(self):
        """정규식 기반 가격 정보 추출 - 최대 속도 최적화"""
        try:
            extraction_start = time.time()
            
            # 1. 페이지 소스 한 번만 가져오기
            page_source = self.driver.page_source
            
            # 2. 정규식으로 모든 정보 일괄 추출
            price_info = self._extract_with_regex_optimized(page_source)
            
            print(f"    가격 정보: ✓ 정규식 일괄처리 ({time.time() - extraction_start:.2f}초)")
            
            return price_info
            
        except Exception as e:
            print(f"    가격 정보 추출 중 오류: {e}")
            return {}
    
    def _extract_with_regex_optimized(self, page_source):
        """최적화된 정규식 가격 정보 추출"""
        price_info = {}
        
        try:
            # 1. 할인가 추출 (우선순위별)
            discount_patterns = [
                Config.PATTERNS['krw_discount_price_red'],      # 빨간색 할인가 (가장 정확)
                Config.PATTERNS['krw_discount_price_simple'],   # 일반 할인가
                Config.PATTERNS['krw_out_of_stock_price'],      # 품절 상품 가격
            ]
            
            for pattern in discount_patterns:
                match = re.search(pattern, page_source, re.IGNORECASE)
                if match:
                    price_info['discount_price'] = match.group(1).replace(',', '')
                    break
            
            # 2. 정가 추출 (모든 매치에서 최고가 선택)
            list_patterns = [
                Config.PATTERNS['krw_list_price_span'],      # span 태그 내 정가
                Config.PATTERNS['krw_list_price_general'],   # 일반 정가
            ]
            
            all_list_prices = []
            for pattern in list_patterns:
                matches = re.findall(pattern, page_source, re.IGNORECASE)
                if matches:
                    for match in matches:
                        try:
                            clean_price = int(match.replace(',', ''))
                            if clean_price >= 1000:  # 최소 1000원 이상만
                                all_list_prices.append(clean_price)
                        except:
                            continue
            
            if all_list_prices:
                price_info['list_price'] = str(max(all_list_prices))
            
            # 3. 할인율 추출
            percent_patterns = [
                Config.PATTERNS['percent_off_bracket'],   # (29% off) 형태
                Config.PATTERNS['percent_off_simple'],    # 29% off 형태
            ]
            
            for pattern in percent_patterns:
                match = re.search(pattern, page_source, re.IGNORECASE)
                if match:
                    price_info['discount_percent'] = match.group(1)
                    break
            
            # 4. 단위당 가격 추출
            unit_patterns = [
                Config.PATTERNS['price_per_unit_span'],      # span 태그 내
                Config.PATTERNS['price_per_serving_direct'], # ₩xxx/serving 직접
                Config.PATTERNS['price_per_unit_text'],      # 일반 텍스트
            ]
            
            for pattern in unit_patterns:
                match = re.search(pattern, page_source, re.IGNORECASE)
                if match:
                    if len(match.groups()) >= 2:
                        price_info['price_per_unit'] = f"₩{match.group(1)}/{match.group(2)}"
                    else:
                        price_info['price_per_unit'] = match.group(0)
                    break
            
            # 5. 정기배송 할인 추출
            subscription_patterns = [
                Config.PATTERNS['subscription_discount_future'],   # future orders
                Config.PATTERNS['subscription_discount_autoship'], # autoship
            ]
            
            for pattern in subscription_patterns:
                match = re.search(pattern, page_source, re.IGNORECASE)
                if match:
                    price_info['subscription_discount'] = match.group(1)
                    break
            
            # 6. 품절 상태 확인
            stock_patterns = [
                Config.PATTERNS['out_of_stock_testid'],   # data-testid 기반
                Config.PATTERNS['out_of_stock_text'],     # 일반 텍스트
            ]
            
            is_out_of_stock = False
            for pattern in stock_patterns:
                if re.search(pattern, page_source, re.IGNORECASE):
                    is_out_of_stock = True
                    break
            
            price_info['is_in_stock'] = not is_out_of_stock
            price_info['stock_message'] = 'Out of stock' if is_out_of_stock else ''
            
            # 7. 재입고 날짜 추출 (품절인 경우만)
            if is_out_of_stock:
                back_stock_patterns = [
                    Config.PATTERNS['back_in_stock_date_testid'],  # data-testid 기반
                    Config.PATTERNS['back_in_stock_general'],      # 일반 패턴
                ]
                
                for pattern in back_stock_patterns:
                    match = re.search(pattern, page_source, re.IGNORECASE)
                    if match:
                        back_date = match.group(1).strip()
                        if back_date and len(back_date) > 3:
                            price_info['back_in_stock_date'] = back_date
                        break
            
            # 8. 데이터 검증 및 정리
            self._validate_and_clean_price_info(price_info)
            
            return price_info
            
        except Exception as e:
            print(f"    정규식 추출 오류: {e}")
            return {}
    
    def _validate_and_clean_price_info(self, price_info):
        """가격 정보 검증 및 정리"""
        try:
            # 할인가와 정가 논리 검증
            if price_info.get('discount_price') and price_info.get('list_price'):
                discount = int(price_info['discount_price'])
                list_price = int(price_info['list_price'])
                
                # 할인가가 정가보다 높으면 교환
                if discount > list_price:
                    price_info['discount_price'], price_info['list_price'] = \
                        price_info['list_price'], price_info['discount_price']
                
                # 할인율이 없으면 계산
                if not price_info.get('discount_percent'):
                    discount_rate = round((list_price - discount) / list_price * 100)
                    if discount_rate > 0:
                        price_info['discount_percent'] = str(discount_rate)
            
            # 가격 필드가 없으면 기본값 설정
            if not price_info.get('is_in_stock'):
                price_info['is_in_stock'] = True
            if not price_info.get('stock_message'):
                price_info['stock_message'] = ''
            if not price_info.get('back_in_stock_date'):
                price_info['back_in_stock_date'] = ''
            
        except Exception as e:
            print(f"    가격 정보 검증 오류: {e}")
    
    # ========== 새로 추가: 이미지 크롤링 메서드 ==========
    
    def extract_product_image_url(self, product_url=None):
        """아이허브 상품 이미지 URL 추출"""
        try:
            if product_url:
                if not self.browser.safe_get(product_url):
                    return None
                time.sleep(2)
            
            # 실제 HTML 구조 기반 선택자 (제공된 HTML 구조 반영)
            image_selectors = [
                "#iherb-product-image",                                    # 메인 상품 이미지 ID
                ".product-summary-image img",                              # 상품 요약 이미지
                "img[src*='cloudinary.images-iherb.com']",                # 클라우드너리 이미지
                ".product-easyzoom img",                                   # 줌 가능한 이미지
                "img[alt*='NOW Foods']",                                   # 브랜드명 기반
                "img[width='400'][height='400']"                          # 400x400 상품 이미지
            ]
            
            for selector in image_selectors:
                try:
                    img_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    img_url = img_element.get_attribute("src")
                    if img_url and img_url.startswith("http") and "iherb.com" in img_url:
                        print(f"    이미지 URL 추출 성공: {selector}")
                        return img_url
                except:
                    continue
            
            print("    이미지 URL 추출 실패")
            return None
            
        except Exception as e:
            print(f"    이미지 URL 추출 오류: {e}")
            return None
    
    def download_product_image(self, image_url, output_path):
        """상품 이미지 다운로드"""
        try:
            if not image_url:
                return False
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Referer': 'https://www.iherb.com/'
            }
            
            response = requests.get(image_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # 디렉토리 생성
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            # 이미지 유효성 검사
            try:
                with Image.open(output_path) as img:
                    img.verify()
                print(f"    이미지 다운로드 성공: {os.path.basename(output_path)}")
                return True
            except:
                # 손상된 파일 삭제
                if os.path.exists(output_path):
                    os.remove(output_path)
                print(f"    이미지 파일 손상: {os.path.basename(output_path)}")
                return False
                
        except Exception as e:
            print(f"    이미지 다운로드 실패: {e}")
            return False
    
    def extract_and_download_image(self, product_url, output_path):
        """이미지 URL 추출 + 다운로드 통합 메서드"""
        try:
            # 이미지 URL 추출
            image_url = self.extract_product_image_url(product_url)
            if not image_url:
                return False
            
            # 이미지 다운로드
            return self.download_product_image(image_url, output_path)
            
        except Exception as e:
            print(f"    이미지 추출/다운로드 실패: {e}")
            return False
    
    # ========== 기존 통합 메서드 ==========
    
    def extract_product_info_with_price(self, product_url):
        """상품 정보와 가격 정보를 함께 추출 - 정규식 기반 최적화"""
        try:
            print("  상품 정보 추출 중...")
            
            if not self.browser.safe_get(product_url):
                print("    상품 페이지 로딩 실패")
                return None, None, {}
            
            # 페이지 로딩 대기 (타임아웃 단축)
            try:
                WebDriverWait(self.driver, 3).until(  # 5초 -> 3초
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
                )
            except TimeoutException:
                print("    페이지 로딩 타임아웃")
            
            # 상품명 추출
            iherb_product_name = self.extract_product_name()
            if iherb_product_name:
                print(f"    상품명: ✓")
            else:
                print(f"    상품명: ✗ 추출 실패")
            
            # 상품코드 추출 (URL 우선)
            product_code = self.extract_product_code(product_url)
            if product_code:
                print(f"    상품코드: ✓ {product_code}")
            else:
                print(f"    상품코드: ✗ 추출 실패")
            
            # 가격 정보 추출 (정규식 기반)
            price_info = self.extract_price_info()
            
            return product_code, iherb_product_name, price_info
            
        except Exception as e:
            print(f"    상품 정보 추출 중 오류: {e}")
            return None, None, {}