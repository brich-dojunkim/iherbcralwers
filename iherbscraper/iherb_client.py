"""
아이허브 사이트 상호작용 모듈 - 최적화된 가격 추출 포함
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
        """최적화된 아이허브 가격 정보 추출"""
        try:
            # 먼저 빠른 추출 방법 시도
            fast_result = self._extract_price_info_fast()
            if fast_result:
                return fast_result
            
            # 빠른 방법이 실패하면 기존 방법으로 폴백
            print("    빠른 추출 실패 - 기존 방식으로 진행")
            return self._extract_price_info_original()
            
        except Exception as e:
            print(f"    가격 정보 추출 중 오류: {e}")
            return {}
    
    def _extract_price_info_fast(self):
        """빠른 가격 정보 추출 - 우선순위 기반 접근"""
        price_info = {}
        extraction_start = time.time()
        
        try:
            # 1단계: 가장 가능성 높은 선택자부터 시도 (병렬적 접근)
            price_data = self._extract_all_prices_at_once()
            
            if price_data:
                price_info.update(price_data)
                print(f"    가격 정보: ✓ CSS 선택자 방식 ({time.time() - extraction_start:.2f}초)")
                return price_info
            
            # 2단계: JavaScript 실행으로 즉시 가격 데이터 추출
            js_price_data = self._extract_prices_with_javascript()
            
            if js_price_data:
                price_info.update(js_price_data)
                print(f"    가격 정보: ✓ JavaScript 방식 ({time.time() - extraction_start:.2f}초)")
                return price_info
            
            # 3단계: 페이지 소스 정규식 검색 (최후 수단)
            regex_price_data = self._extract_prices_from_source_optimized()
            
            if regex_price_data:
                price_info.update(regex_price_data)
                print(f"    가격 정보: ✓ 정규식 방식 ({time.time() - extraction_start:.2f}초)")
                return price_info
            
            return None
            
        except Exception as e:
            print(f"    빠른 가격 추출 중 오류: {e}")
            return None
    
    def _extract_all_prices_at_once(self):
        """한 번에 모든 가격 정보 추출 시도"""
        try:
            # 모든 가격 관련 요소를 한 번에 찾기
            price_elements = self.driver.find_elements(
                By.CSS_SELECTOR, 
                '.price-inner-text, .discount-price, .list-price, .percent-off, .price-per-unit, [data-testid="product-price"]'
            )
            
            price_info = {}
            
            for element in price_elements:
                try:
                    text = element.text.strip()
                    class_names = element.get_attribute('class') or ''
                    testid = element.get_attribute('data-testid') or ''
                    
                    # 원화 가격 패턴 체크
                    if '₩' in text:
                        # 숫자만 추출
                        price_clean = re.sub(r'[^\d]', '', text)
                        if len(price_clean) >= 4:
                            
                            # data-testid가 있는 경우 우선 처리
                            if 'product-price' in testid:
                                price_info['discount_price'] = price_clean
                            # 클래스명으로 가격 유형 판단
                            elif 'discount' in class_names.lower():
                                price_info['discount_price'] = price_clean
                            elif 'list' in class_names.lower():
                                price_info['list_price'] = price_clean
                            elif 'price-inner-text' in class_names:
                                # 기본 가격으로 사용
                                if not price_info.get('discount_price'):
                                    price_info['discount_price'] = price_clean
                    
                    # 할인율 체크
                    elif '%' in text and 'off' in text.lower():
                        percent_match = re.search(r'(\d+)%', text)
                        if percent_match:
                            price_info['discount_percent'] = percent_match.group(1)
                    
                    # 단위당 가격 체크
                    elif '/serving' in text.lower() or ('₩' in text and '/unit' in text.lower()):
                        price_info['price_per_unit'] = text
                        
                except Exception:
                    continue
            
            return price_info if price_info else None
            
        except Exception:
            return None
    
    def _extract_prices_with_javascript(self):
        """JavaScript로 가격 정보 즉시 추출"""
        try:
            # JavaScript로 가격 정보를 한 번에 수집
            js_script = """
            var priceData = {};
            
            // 모든 가격 텍스트 수집
            var allElements = document.querySelectorAll('*');
            var krwPrices = [];
            
            for (var i = 0; i < allElements.length; i++) {
                var text = allElements[i].textContent || '';
                if (text.includes('₩') && text.match(/₩[\\d,]+/)) {
                    var matches = text.match(/₩([\\d,]+)/g);
                    if (matches) {
                        matches.forEach(function(match) {
                            var cleanPrice = match.replace(/[^\\d]/g, '');
                            if (cleanPrice.length >= 4) {
                                krwPrices.push(parseInt(cleanPrice));
                            }
                        });
                    }
                }
            }
            
            // 중복 제거 후 정렬
            krwPrices = [...new Set(krwPrices)].sort(function(a, b) { return b - a; });
            
            if (krwPrices.length >= 2) {
                priceData.list_price = krwPrices[0].toString();
                priceData.discount_price = krwPrices[1].toString();
            } else if (krwPrices.length === 1) {
                priceData.discount_price = krwPrices[0].toString();
            }
            
            // 할인율 찾기
            var percentElements = document.querySelectorAll('*');
            for (var j = 0; j < percentElements.length; j++) {
                var percentText = percentElements[j].textContent || '';
                if (percentText.includes('%') && percentText.includes('off')) {
                    var percentMatch = percentText.match(/(\\d+)%/);
                    if (percentMatch) {
                        priceData.discount_percent = percentMatch[1];
                        break;
                    }
                }
            }
            
            return priceData;
            """
            
            result = self.driver.execute_script(js_script)
            return result if result and len(result) > 0 else None
            
        except Exception:
            return None
    
    def _extract_prices_from_source_optimized(self):
        """최적화된 페이지 소스 정규식 검색"""
        try:
            page_source = self.driver.page_source
            price_info = {}
            
            # 더 정확한 정규식 패턴들
            patterns = {
                'krw_in_data_testid': r'data-testid="product-price"[^>]*>\s*<p>\s*₩([\d,]+)',
                'krw_general': r'₩([\d,]+)',
                'krw_quoted': r'"₩([\d,]+)"',
                'discount_percent': r'(\d+)%\s*off',
                'price_per_serving': r'₩(\d+)/serving'
            }
            
            # 첨부된 HTML 구조에 맞는 특별 패턴
            testid_match = re.search(patterns['krw_in_data_testid'], page_source)
            if testid_match:
                price_info['discount_price'] = testid_match.group(1).replace(',', '')
            
            # 일반 원화 패턴으로 모든 가격 수집
            krw_matches = re.findall(patterns['krw_general'], page_source)
            if krw_matches:
                clean_prices = []
                for match in krw_matches:
                    clean_price = match.replace(',', '')
                    if len(clean_price) >= 4:
                        clean_prices.append(int(clean_price))
                
                if clean_prices:
                    unique_prices = sorted(list(set(clean_prices)), reverse=True)
                    
                    if not price_info.get('discount_price') and len(unique_prices) >= 1:
                        price_info['discount_price'] = str(unique_prices[0])
                    
                    if len(unique_prices) >= 2:
                        price_info['list_price'] = str(unique_prices[0])
                        if not price_info.get('discount_price'):
                            price_info['discount_price'] = str(unique_prices[1])
            
            # 할인율 패턴
            discount_match = re.search(patterns['discount_percent'], page_source, re.IGNORECASE)
            if discount_match:
                price_info['discount_percent'] = discount_match.group(1)
            
            # 단위당 가격 패턴
            per_serving_match = re.search(patterns['price_per_serving'], page_source)
            if per_serving_match:
                price_info['price_per_unit'] = f"₩{per_serving_match.group(1)}/serving"
            
            return price_info if price_info else None
            
        except Exception:
            return None
    
    def _extract_price_info_original(self):
        """아이허브 가격 정보 추출 (기존 방식 - 한국 사이트 기준, 원화 표시)"""
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
                WebDriverWait(self.driver, 5).until(
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
            
            # 가격 정보 추출 (최적화된 방식)
            price_info = self.extract_price_info()
            
            return product_code, iherb_product_name, price_info
            
        except Exception as e:
            print(f"    상품 정보 추출 중 오류: {e}")
            return None, None, {}