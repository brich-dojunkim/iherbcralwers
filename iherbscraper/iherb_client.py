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
            """아이허브 언어를 수동으로 영어로 설정"""
            try:
                print("  아이허브 언어 설정 (수동)...")
                
                # 1. 한국 아이허브 페이지 접속
                if not self.browser.safe_get(Config.KOREA_URL):
                    print("  한국 사이트 접속 실패 - 영어 사이트로 이동")
                    if self.browser.safe_get(Config.BASE_URL):
                        print("  영어 사이트 접속 완료 ✓")
                        return
                    else:
                        print("  영어 사이트 접속도 실패")
                        return
                
                # 2. 현재 URL 확인
                current_url = self.browser.current_url
                if "kr.iherb.com" not in current_url:
                    print("  이미 영어 사이트에 있음 ✓")
                    return
                
                # 3. 수동 설정 안내
                print("\n" + "="*60)
                print("  수동으로 언어를 영어로 변경해주세요:")
                print("  1. 브라우저에서 우상단 국가/언어 설정 버튼 클릭")
                print("  2. 언어를 'English'로 선택")
                print("  3. '저장하기' 버튼 클릭")
                print("  4. 페이지가 영어로 변경되면 아래 Enter 키 입력")
                print("="*60)
                
                # 4. 사용자 입력 대기
                input("  언어 변경 완료 후 Enter 키를 눌러주세요...")
                
                # 5. 변경 확인
                time.sleep(2)
                final_url = self.browser.current_url
                
                if "kr.iherb.com" not in final_url:
                    print("  아이허브 언어 설정: 영어로 변경 확인 ✓")
                else:
                    print("  아직 한국 사이트입니다. 영어 사이트로 직접 이동합니다.")
                    if self.browser.safe_get(Config.BASE_URL):
                        print("  영어 사이트 직접 접속 완료 ✓")
                    else:
                        print("  영어 사이트 접속 실패")
                    
            except Exception as e:
                print(f"  언어 설정 중 오류: {e}")
                print("  영어 사이트로 직접 이동합니다.")
                if self.browser.safe_get(Config.BASE_URL):
                    print("  영어 사이트 직접 접속 완료 ✓")
                else:
                    print("  영어 사이트 접속 실패")
    
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
        """아이허브 가격 정보 추출 (영어 사이트 기준)"""
        try:
            price_info = {}
            extracted_items = []
            
            # 정가 추출
            for selector in Config.SELECTORS['list_price']:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    price_text = element.text.strip()
                    if price_text and '$' in price_text:
                        price_clean = re.sub(r'[^\d.]', '', price_text)
                        if price_clean:
                            price_info['list_price'] = price_clean
                            extracted_items.append('정가')
                            break
                except:
                    continue
            
            # 할인가 추출
            for selector in Config.SELECTORS['discount_price']:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    price_text = element.text.strip()
                    if price_text and '$' in price_text:
                        price_clean = re.sub(r'[^\d.]', '', price_text)
                        if price_clean:
                            price_info['discount_price'] = price_clean
                            extracted_items.append('할인가')
                            break
                except:
                    continue
            
            # 할인율 추출
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, Config.SELECTORS['discount_percent'])
                percent_text = element.text.strip()
                if percent_text and '%' in percent_text:
                    percent_match = re.search(Config.PATTERNS['discount_percent'], percent_text)
                    if percent_match:
                        price_info['discount_percent'] = percent_match.group(1)
                        extracted_items.append('할인율')
            except:
                pass
            
            # 정기배송 할인 정보 추출
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, Config.SELECTORS['subscription_discount'])
                for element in elements:
                    text = element.text.lower()
                    subscription_match = re.search(Config.PATTERNS['subscription_discount'], text)
                    if subscription_match:
                        price_info['subscription_discount'] = subscription_match.group(1)
                        extracted_items.append('정기배송할인')
                        break
            except:
                pass
            
            # 단위당 가격
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, Config.SELECTORS['price_per_unit'])
                price_per_unit = element.text.strip()
                if price_per_unit:
                    price_info['price_per_unit'] = price_per_unit
                    extracted_items.append('단위당가격')
            except:
                pass
            
            # 추가 시도: 페이지 소스에서 가격 패턴 검색
            if not price_info.get('discount_price') and not price_info.get('list_price'):
                price_info.update(self._extract_prices_from_source())
                if price_info.get('list_price'):
                    extracted_items.append('정가(패턴)')
                if price_info.get('discount_price'):
                    extracted_items.append('할인가(패턴)')
            
            if extracted_items:
                print(f"    가격 정보: ✓ {len(extracted_items)}개 항목 ({', '.join(extracted_items)})")
            else:
                print(f"    가격 정보: ✗ 추출 실패")
            
            return price_info
            
        except Exception as e:
            print(f"    가격 정보 추출 중 오류: {e}")
            return {}
    
    def _extract_prices_from_source(self):
        """페이지 소스에서 가격 패턴 검색"""
        price_info = {}
        
        try:
            page_source = self.browser.page_source
            price_patterns = [
                Config.PATTERNS['price_usd'], 
                Config.PATTERNS['price_pattern']
            ]
            
            for pattern in price_patterns:
                matches = re.findall(pattern, page_source)
                if matches:
                    prices = [re.sub(r'[^\d.]', '', match) for match in matches[:3]]
                    prices = [p for p in prices if len(p) >= 3 and '.' in p]
                    
                    if prices:
                        if not price_info.get('list_price'):
                            price_info['list_price'] = max(prices, key=float)
                        if len(prices) > 1 and not price_info.get('discount_price'):
                            sorted_prices = sorted(prices, key=float, reverse=True)
                            price_info['discount_price'] = sorted_prices[1] if len(sorted_prices) > 1 else sorted_prices[0]
                        break
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