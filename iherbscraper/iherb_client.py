"""
아이허브 사이트 상호작용 모듈
"""

import time
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from config import Config


class IHerbClient:
    """아이허브 사이트와의 모든 상호작용 담당"""
    
    def __init__(self, browser_manager):
        self.browser = browser_manager
        self.driver = browser_manager.driver
    
    def set_language_to_english(self):
        """아이허브 언어를 영어로 설정"""
        try:
            print("  아이허브 언어 설정 (수동)...")
            
            # 1. 한국 아이허브 페이지 접속
            if not self.browser.safe_get(Config.KOREA_URL):
                print("  아이허브 접속 실패 - 기본 설정으로 진행")
                return
            
            # 2. 현재 URL 확인 - 이미 영어 사이트인지 체크
            current_url = self.browser.current_url
            if "iherb.com" in current_url and "kr.iherb.com" not in current_url:
                print("  이미 영어 사이트입니다 ✓")
                return
            
            # 3. 수동 설정 안내
            print("\n" + "="*60)
            print("  수동으로 언어를 영어로 변경해주세요:")
            print("  1. 브라우저에서 우상단 국가/언어 설정 버튼 클릭")
            print("  2. 언어를 'English'로 선택")
            print("  3. '저장하기' 버튼 클릭")
            print("  4. 페이지가 영어로 변경되면 아래 Enter 키 입력")
            print("="*60)
            
            input("  언어 변경 완료 후 Enter 키를 눌러주세요...")
            
            # 4. 변경 확인
            current_url = self.browser.current_url
            if "kr.iherb.com" in current_url:
                print("  아직 한국 사이트입니다. 영어 사이트로 직접 이동합니다.")
                if self.browser.safe_get("https://www.iherb.com"):
                    print("  영어 사이트 직접 접속 완료 ✓")
                else:
                    print("  영어 사이트 접속 실패")
            else:
                print("  언어 변경 완료 ✓")
                
        except Exception as e:
            print(f"  언어 설정 실패: {e}")
            print("  기본 설정으로 진행...")
    
    def get_multiple_products(self, search_url):
        """검색 결과에서 여러 상품 정보 추출"""
        try:
            if not self.browser.safe_get(search_url):
                return []
            
            products = []
            
            # 검색 결과 로딩 대기
            try:
                WebDriverWait(self.driver, 10).until(
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
    
    def extract_product_name(self):
        """상품명 추출"""
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
        """상품코드 추출"""
        product_code = None
        
        # 방법 1: product-specs-list에서 찾기
        try:
            specs_element = self.driver.find_element(By.CSS_SELECTOR, Config.SELECTORS['product_specs'])
            text = specs_element.text
            match = re.search(Config.PATTERNS['item_code'], text, re.IGNORECASE)
            if match:
                product_code = match.group(1)
                return product_code
        except:
            pass
        
        # 방법 2: data 속성에서 찾기
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, Config.SELECTORS['part_number'])
            for element in elements:
                value = element.get_attribute("data-part-number")
                if value and re.match(r'^[A-Z0-9-]+$', value):
                    product_code = value
                    return product_code
        except:
            pass
        
        # 방법 3: URL에서 추출
        try:
            url_match = re.search(Config.PATTERNS['product_code_url'], product_url)
            if url_match:
                product_code = url_match.group(1)
                return product_code
        except:
            pass
        
        return None
    
    def extract_price_info(self):
        """아이허브 가격 정보 추출 (한국 사이트 기준, 원화 표시)"""
        try:
            price_info = {}
            extracted_items = []
            
            # 1. 구독 옵션의 할인가 추출 (우선순위 높음)
            subscription_selectors = [
                '.strike-through-price-wrapper.show .discount-price',
                '.discount-price-wrapper .discount-price',
                'b.discount-price[style*="color: rgb(211, 47, 47)"]',
                '.auto-ship-first .discount-price'
            ]
            
            for selector in subscription_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    price_text = element.text.strip()
                    if price_text and '₩' in price_text:
                        # ₩25,048 -> 25048
                        price_clean = re.sub(r'[^\d]', '', price_text)
                        if price_clean and len(price_clean) >= 4:
                            price_info['discount_price'] = price_clean
                            extracted_items.append('할인가(구독)')
                            break
                except:
                    continue
            
            # 2. 일회성 구매 정가 추출
            onetime_selectors = [
                '.original-price-config.show .list-price',
                '.one-time-second .list-price',
                'span.list-price'
            ]
            
            for selector in onetime_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    price_text = element.text.strip()
                    if price_text and '₩' in price_text:
                        # ₩35,278 -> 35278
                        price_clean = re.sub(r'[^\d]', '', price_text)
                        if price_clean and len(price_clean) >= 4:
                            price_info['list_price'] = price_clean
                            extracted_items.append('정가(일회성)')
                            break
                except:
                    continue
            
            # 3. 할인율 추출
            discount_percent_selectors = [
                '.percent-off',
                'span.percent-off',
                '.strike-through-price-wrapper .percent-off'
            ]
            
            for selector in discount_percent_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    percent_text = element.text.strip()
                    if percent_text and '%' in percent_text:
                        # (29% off) -> 29
                        percent_match = re.search(r'(\d+)%', percent_text)
                        if percent_match:
                            price_info['discount_percent'] = percent_match.group(1)
                            extracted_items.append('할인율')
                            break
                except:
                    continue
            
            # 4. 정기배송 추가 할인 정보 추출
            subscription_message_selectors = [
                '.auto-ship-message-item',
                '.subscription-off-message',
                '.auto-ship-message'
            ]
            
            for selector in subscription_message_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text.lower()
                        # "29% off your first order, and 10% off on future orders" 패턴
                        subscription_matches = re.findall(r'(\d+)%\s*off', text)
                        if subscription_matches:
                            # 첫 번째 할인율이 구독 할인
                            price_info['subscription_discount'] = subscription_matches[0]
                            extracted_items.append('정기배송할인')
                            break
                except:
                    continue
            
            # 5. 단위당 가격 (serving 당 가격)
            unit_price_selectors = [
                '.discount-price-per-unit',
                '.list-price-per-unit',
                'span[class*="per-unit"]'
            ]
            
            for selector in unit_price_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    unit_price_text = element.text.strip()
                    if unit_price_text and ('₩' in unit_price_text or '/serving' in unit_price_text):
                        price_info['price_per_unit'] = unit_price_text
                        extracted_items.append('단위당가격')
                        break
                except:
                    continue
            
            # 6. 백업: 페이지 소스에서 패턴 검색
            if not price_info.get('discount_price') and not price_info.get('list_price'):
                backup_prices = self._extract_krw_prices_from_source()
                price_info.update(backup_prices)
                if backup_prices:
                    extracted_items.append('패턴추출')
            
            if extracted_items:
                print(f"    가격 정보: ✓ {len(extracted_items)}개 항목 ({', '.join(extracted_items)})")
            else:
                print(f"    가격 정보: ✗ 추출 실패")
            
            return price_info
            
        except Exception as e:
            print(f"    가격 정보 추출 중 오류: {e}")
            return {}
    
    def _extract_krw_prices_from_source(self):
        """페이지 소스에서 원화 가격 패턴 검색"""
        price_info = {}
        
        try:
            page_source = self.browser.page_source
            
            # 원화 패턴들
            krw_patterns = [
                r'₩([\d,]+)',  # ₩25,048
                r'"₩([\d,]+)"',  # "₩25,048"
                r'₩\s*([\d,]+)',  # ₩ 25,048
            ]
            
            all_prices = []
            for pattern in krw_patterns:
                matches = re.findall(pattern, page_source)
                for match in matches:
                    clean_price = re.sub(r'[^\d]', '', match)
                    if len(clean_price) >= 4:  # 최소 4자리
                        all_prices.append(int(clean_price))
            
            if all_prices:
                # 중복 제거 후 정렬
                unique_prices = sorted(list(set(all_prices)), reverse=True)
                
                if len(unique_prices) >= 2:
                    # 가장 높은 가격을 정가, 두 번째를 할인가로
                    price_info['list_price'] = str(unique_prices[0])
                    price_info['discount_price'] = str(unique_prices[1])
                elif len(unique_prices) == 1:
                    price_info['list_price'] = str(unique_prices[0])
        except:
            pass
        
        return price_info
    
    def extract_product_info_with_price(self, product_url):
        """상품 정보와 가격 정보를 함께 추출"""
        try:
            print("  상품 정보 추출 중...")
            
            if not self.browser.safe_get(product_url):
                print("    상품 페이지 로딩 실패")
                return None, None, {}
            
            # 페이지 로딩 대기
            try:
                WebDriverWait(self.driver, 10).until(
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
            
            # 상품코드 추출
            product_code = self.extract_product_code(product_url)
            if product_code:
                print(f"    상품코드: ✓ {product_code}")
            else:
                print(f"    상품코드: ✗ 추출 실패")
            
            # 가격 정보 추출
            price_info = self.extract_price_info()
            
            return product_code, iherb_product_name, price_info
            
        except Exception as e:
            print(f"    상품 정보 추출 중 오류: {e}")
            return None, None, {}