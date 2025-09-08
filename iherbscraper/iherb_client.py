"""
아이허브 사이트 상호작용 모듈 - 최대 속도 최적화 버전
"""

import time
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from config import Config


class IHerbClient:
    """아이허브 사이트와의 모든 상호작용 담당 - 최대 속도 최적화"""
    
    def __init__(self, browser_manager):
        self.browser = browser_manager
        self.driver = browser_manager.driver
    
    # ========== 언어 설정 관련 메서드 ==========
    
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
    
    # ========== 검색 관련 메서드 ==========
    
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
    
    # ========== 상품 정보 추출 메서드 ==========
    
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
    
    # ========== 가격 정보 추출 메서드 (최대 속도 최적화) ==========
    
    def extract_price_info(self):
        """최대 속도 최적화 가격 정보 추출"""
        try:
            # JavaScript 한 번 실행으로 모든 정보 수집
            result = self._extract_all_info_with_javascript()
            if result:
                return result
            
            # JavaScript 실패 시에만 정규식 백업 (CSS 단계 완전 생략)
            print("    JavaScript 실패 - 정규식 백업")
            return self._extract_with_regex_fast()
            
        except Exception as e:
            print(f"    가격 정보 추출 중 오류: {e}")
            return {}
    
    def _extract_all_info_with_javascript(self):
        """JavaScript 한 번 실행으로 모든 정보 수집 (최고 속도)"""
        try:
            extraction_start = time.time()
            
            # 모든 정보를 한 번에 수집하는 최적화된 JavaScript
            js_script = """
            var result = {
                prices: [],
                discount_percent: '',
                is_in_stock: true,
                stock_message: '',
                back_in_stock_date: '',
                price_per_unit: '',
                subscription_discount: ''
            };
            
            // 한 번의 DOM 스캔으로 모든 정보 수집
            var allElements = document.querySelectorAll('*');
            
            for (var i = 0; i < allElements.length; i++) {
                var el = allElements[i];
                var text = el.textContent || '';
                var testId = el.getAttribute('data-testid') || '';
                var className = el.className || '';
                
                // 1. 가격 수집
                if (text.includes('₩') && text.match(/₩[\\d,]+/)) {
                    var matches = text.match(/₩([\\d,]+)/g);
                    if (matches) {
                        matches.forEach(function(match) {
                            var cleanPrice = match.replace(/[^\\d]/g, '');
                            if (cleanPrice.length >= 4) {
                                result.prices.push(parseInt(cleanPrice));
                            }
                        });
                    }
                }
                
                // 2. 품절 상태 확인
                if (testId === 'product-stock-status' && text.toLowerCase().includes('out of stock')) {
                    result.is_in_stock = false;
                    result.stock_message = 'Out of stock';
                }
                
                // 3. 재입고 날짜
                if (testId === 'product-stock-status-text' && text.trim()) {
                    result.back_in_stock_date = text.trim();
                }
                
                // 4. 단위당 가격
                if ((className.includes('price-per-unit') || className.includes('per-unit')) && 
                    text.includes('₩') && (text.includes('/serving') || text.includes('/unit'))) {
                    result.price_per_unit = text.trim();
                }
                
                // 5. 할인율
                if (text.includes('%') && text.includes('off') && !result.discount_percent) {
                    var percentMatch = text.match(/(\\d+)%/);
                    if (percentMatch) {
                        result.discount_percent = percentMatch[1];
                    }
                }
                
                // 6. 정기배송 할인 (미래 주문)
                if (text.toLowerCase().includes('future') && text.toLowerCase().includes('orders') && 
                    text.includes('%') && text.includes('off')) {
                    var futureMatch = text.match(/(\\d+)%\\s*off\\s+on.*?future/);
                    if (futureMatch && !result.subscription_discount) {
                        result.subscription_discount = futureMatch[1];
                    }
                }
            }
            
            // 가격 정리
            var uniquePrices = [...new Set(result.prices)].sort(function(a, b) { return b - a; });
            var finalResult = {};
            
            if (uniquePrices.length >= 2) {
                finalResult.list_price = uniquePrices[0].toString();
                finalResult.discount_price = uniquePrices[1].toString();
            } else if (uniquePrices.length === 1) {
                finalResult.discount_price = uniquePrices[0].toString();
            }
            
            // 기타 정보 추가
            if (result.discount_percent) finalResult.discount_percent = result.discount_percent;
            if (result.subscription_discount) finalResult.subscription_discount = result.subscription_discount;
            if (result.price_per_unit) finalResult.price_per_unit = result.price_per_unit;
            
            finalResult.is_in_stock = result.is_in_stock;
            finalResult.stock_message = result.stock_message;
            finalResult.back_in_stock_date = result.back_in_stock_date;
            
            return finalResult;
            """
            
            result = self.driver.execute_script(js_script)
            
            if result and (result.get('discount_price') or result.get('list_price')):
                print(f"    가격 정보: ✓ JavaScript 올인원 ({time.time() - extraction_start:.2f}초)")
                return result
            
            return None
            
        except Exception as e:
            print(f"    JavaScript 올인원 추출 오류: {e}")
            return None
    
    def _extract_with_regex_fast(self):
        """빠른 정규식 백업 추출"""
        price_info = {}
        page_source = self.driver.page_source
        
        # 필수 가격 정보만 빠르게 추출
        krw_matches = re.findall(r'₩([\d,]+)', page_source)
        if krw_matches:
            clean_prices = []
            for match in krw_matches:
                clean_price = match.replace(',', '')
                if len(clean_price) >= 4:
                    clean_prices.append(int(clean_price))
            
            unique_prices = sorted(list(set(clean_prices)), reverse=True)
            if len(unique_prices) >= 2:
                price_info['list_price'] = str(unique_prices[0])
                price_info['discount_price'] = str(unique_prices[1])
            elif len(unique_prices) == 1:
                price_info['discount_price'] = str(unique_prices[0])
        
        # 품절 상태 빠른 확인
        if re.search(r'out\s+of\s+stock', page_source, re.IGNORECASE):
            price_info['is_in_stock'] = False
            price_info['stock_message'] = 'Out of stock'
        else:
            price_info['is_in_stock'] = True
            price_info['stock_message'] = ''
        
        # 할인율 빠른 확인
        discount_match = re.search(r'(\d+)%.*?off', page_source, re.IGNORECASE)
        if discount_match:
            price_info['discount_percent'] = discount_match.group(1)
        
        # 단위당 가격 빠른 확인
        unit_match = re.search(r'₩(\d+)/(serving|unit)', page_source)
        if unit_match:
            price_info['price_per_unit'] = f"₩{unit_match.group(1)}/{unit_match.group(2)}"
        
        if price_info:
            print(f"    가격 정보: ✓ 정규식 백업 (기본 정보)")
        else:
            print(f"    가격 정보: ✗ 모든 방식 실패")
        
        return price_info
    
    # ========== 통합 메서드 ==========
    
    def extract_product_info_with_price(self, product_url):
        """상품 정보와 가격 정보를 함께 추출 - 최대 속도 최적화"""
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
            
            # 가격 정보 추출 (최대 속도 최적화)
            price_info = self.extract_price_info()
            
            return product_code, iherb_product_name, price_info
            
        except Exception as e:
            print(f"    상품 정보 추출 중 오류: {e}")
            return None, None, {}