import pandas as pd
import time
import re
import random
import os
from datetime import datetime
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import urllib.parse
from difflib import SequenceMatcher

class EnglishIHerbScraper:
    def __init__(self, headless=False, delay_range=(3, 6), max_products_to_compare=5):
        self.delay_range = delay_range
        self.max_products_to_compare = max_products_to_compare
        self.base_url = "https://www.iherb.com"  # 영어 사이트로 변경
        self.success_count = 0
        self.retry_count = 0
        self.max_retries = 3
        self.browser_restart_interval = 30  # 30개 처리마다 브라우저 재시작
        
        self._initialize_browser(headless)
        # 아이허브 언어를 영어로 설정
        self._set_iherb_to_english()
    
    def _initialize_browser(self, headless):
        """브라우저 초기화"""
        try:
            print("  브라우저 초기화 중...")
            options = uc.ChromeOptions()
            
            if headless:
                options.add_argument("--headless=new")
            
            # 안정성 향상을 위한 추가 옵션
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-images")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            options.add_argument("--disable-web-security")
            options.add_argument("--allow-running-insecure-content")
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-backgrounding-occluded-windows")
            options.add_argument("--disable-renderer-backgrounding")
            options.add_argument("--disable-features=TranslateUI")
            options.add_argument("--disable-ipc-flooding-protection")
            options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            options.add_argument("--window-size=1920,1080")
            
            # 메모리 관리
            options.add_argument("--memory-pressure-off")
            options.add_argument("--max_old_space_size=4096")
            
            # 페이지 로딩 타임아웃 설정
            options.add_argument("--page-load-strategy=eager")
            
            self.driver = uc.Chrome(options=options, version_main=None)
            
            # 타임아웃 설정
            self.driver.set_page_load_timeout(30)  # 페이지 로딩 타임아웃
            self.driver.implicitly_wait(10)  # 암시적 대기
            
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            print("  브라우저 초기화 완료 ✓")
            
        except Exception as e:
            print(f"  브라우저 초기화 실패: {e}")
            raise
    
    def _set_iherb_to_english(self):
        """아이허브 언어를 영어로 설정"""
        try:
            print("  아이허브 언어 설정을 영어로 변경 중...")
            
            # 1. 한국 아이허브 페이지 접속
            if not self._safe_get("https://kr.iherb.com"):
                print("  아이허브 접속 실패 - 기본 설정으로 진행")
                return
            
            # 2. 설정 버튼 클릭 시도
            try:
                # 설정 버튼 대기 및 클릭
                settings_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".selected-country-wrapper"))
                )
                settings_button.click()
                print("    설정 버튼 클릭 완료")
                
                # 모달 로딩 대기
                time.sleep(2)
                
                # 3. 언어 드롭다운에서 English 선택
                english_option = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-val="en-US"]'))
                )
                english_option.click()
                print("    영어 선택 완료")
                
                time.sleep(1)
                
                # 4. 저장하기 버튼 클릭
                save_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".save-selection"))
                )
                save_button.click()
                print("    설정 저장 완료")
                
                # 5. 설정 적용 대기
                time.sleep(5)
                
                # 6. 영어 사이트로 리다이렉트 확인
                current_url = self.driver.current_url
                if "iherb.com" in current_url:
                    print("  아이허브 언어 설정: 영어로 변경 완료 ✓")
                else:
                    print(f"  예상과 다른 URL: {current_url}")
                
            except TimeoutException:
                print("  설정 버튼을 찾을 수 없음 - 이미 영어일 수 있음")
            except Exception as e:
                print(f"  설정 변경 중 오류: {e}")
                
        except Exception as e:
            print(f"  언어 설정 실패: {e}")
            print("  기본 설정으로 진행...")
    
    def _safe_get(self, url, max_retries=3):
        """안전한 페이지 로딩"""
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"    페이지 로딩 재시도 {attempt + 1}/{max_retries}")
                
                # 페이지 로딩
                self.driver.get(url)
                
                # JavaScript 실행이 완료될 때까지 대기
                WebDriverWait(self.driver, 15).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
                
                # 추가 대기 시간
                time.sleep(random.uniform(2, 4))
                
                # 페이지가 제대로 로딩되었는지 확인
                if "iherb.com" in self.driver.current_url:
                    if attempt > 0:
                        print(f"    페이지 로딩 성공 ✓")
                    return True
                else:
                    print(f"    페이지 URL 이상: {self.driver.current_url}")
                    continue
                    
            except TimeoutException:
                print(f"    타임아웃 발생 (시도 {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                else:
                    return False
                    
            except WebDriverException as e:
                print(f"    WebDriver 오류: {str(e)[:100]}...")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    # 심각한 오류의 경우 브라우저 재시작
                    if "chrome not reachable" in str(e).lower() or "session deleted" in str(e).lower():
                        try:
                            self._restart_browser_with_cleanup()
                        except:
                            return False
                    continue
                else:
                    return False
                    
            except Exception as e:
                print(f"    기타 오류: {str(e)[:100]}...")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                else:
                    return False
        
        return False
    
    def random_delay(self):
        """랜덤 딜레이"""
        delay = random.uniform(self.delay_range[0], self.delay_range[1])
        time.sleep(delay)
    
    def calculate_enhanced_similarity(self, korean_name, english_name, iherb_name):
        """한글명과 영어명 모두 고려한 유사도 계산"""
        
        # 영어명 기반 유사도 (주요)
        english_similarity = SequenceMatcher(
            None, 
            english_name.lower(), 
            iherb_name.lower()
        ).ratio()
        
        # 한글명 기반 유사도 (보조)
        korean_similarity = SequenceMatcher(
            None, 
            korean_name.lower(), 
            iherb_name.lower()
        ).ratio()
        
        # 브랜드 매칭 확인
        brand_similarity = 0.0
        common_brands = ['now foods', 'nature\'s way', 'solgar', 'life extension', 'jarrow formulas']
        for brand in common_brands:
            if brand in english_name.lower() and brand in iherb_name.lower():
                brand_similarity = 1.0
                break
            if brand in korean_name.lower() and brand in iherb_name.lower():
                brand_similarity = 0.8
                break
        
        # 제형 매핑 (영어 기준)
        form_mapping = {
            'tablet': ['tablet', 'tablets', 'tab'],
            'capsule': ['capsule', 'capsules', 'caps', 'vcaps'],
            'softgel': ['softgel', 'softgels', 'soft gel'],
            'gummy': ['gummy', 'gummies'],
            'powder': ['powder', 'pwd'],
            'liquid': ['liquid', 'drops']
        }
        
        # 개수 정확 매칭
        exact_count_match = False
        
        # 영어명에서 개수 추출
        english_count = re.search(r'(\d+)\s*(?:count|ct|tablets?|capsules?|softgels?)', english_name.lower())
        iherb_count = re.search(r'(\d+)\s*(?:count|ct|tablets?|capsules?|softgels?)', iherb_name.lower())
        
        if english_count and iherb_count:
            e_count = int(english_count.group(1))
            i_count = int(iherb_count.group(1))
            
            if e_count == i_count:
                exact_count_match = True
            else:
                return 0.0, {
                    'reason': 'count_mismatch', 
                    'english_count': e_count, 
                    'iherb_count': i_count
                }
        
        # mg 단위 매칭
        english_mg = re.search(r'(\d+(?:,\d+)*)\s*mg', english_name.lower())
        iherb_mg = re.search(r'(\d+(?:,\d+)*)\s*mg', iherb_name.lower())
        
        if english_mg and iherb_mg:
            e_mg = int(english_mg.group(1).replace(',', ''))
            i_mg = int(iherb_mg.group(1).replace(',', ''))
            
            if e_mg != i_mg:
                return 0.0, {
                    'reason': 'dosage_mismatch', 
                    'english_mg': e_mg, 
                    'iherb_mg': i_mg
                }
        
        # 제형 체크
        form_penalty = 0
        english_forms = []
        iherb_forms = []
        
        for form_type, variations in form_mapping.items():
            for variation in variations:
                if variation in english_name.lower():
                    english_forms.append(form_type)
                if variation in iherb_name.lower():
                    iherb_forms.append(form_type)
        
        if english_forms and iherb_forms:
            if not any(form in iherb_forms for form in english_forms):
                form_penalty = 0.2
        
        # 가중 평균 계산 (영어명 중심)
        final_score = (
            english_similarity * 0.7 + 
            korean_similarity * 0.2 + 
            brand_similarity * 0.1
        ) - form_penalty
        
        # 최소 임계값 설정
        if final_score < 0.7:
            return final_score * 0.5, {
                'reason': 'low_similarity', 
                'score': final_score,
                'english_similarity': english_similarity,
                'korean_similarity': korean_similarity
            }
        
        return final_score, {
            'english_similarity': english_similarity,
            'korean_similarity': korean_similarity,
            'brand_similarity': brand_similarity,
            'exact_count_match': exact_count_match,
            'form_penalty': form_penalty
        }
    
    def get_multiple_products(self, search_url):
        """검색 결과에서 여러 상품 정보 추출"""
        try:
            if not self._safe_get(search_url):
                return []
            
            products = []
            
            # 검색 결과 로딩 대기
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".product-cell-container"))
                )
            except TimeoutException:
                print("    검색 결과 로딩 실패")
                return []
            
            product_elements = self.driver.find_elements(
                By.CSS_SELECTOR, 
                ".product-cell-container"
            )[:self.max_products_to_compare]
            
            for i, element in enumerate(product_elements):
                try:
                    link_elem = element.find_element(By.CSS_SELECTOR, ".absolute-link.product-link")
                    product_url = link_elem.get_attribute("href")
                    
                    title_elem = element.find_element(By.CSS_SELECTOR, ".product-title")
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
    
    def find_best_matching_product(self, korean_name, english_name, iherb_products):
        """최적 매칭 상품 찾기 - 영어명 기준"""
        print("  매칭 분석 중...")
        
        best_match = None
        best_score = 0
        best_details = None
        
        for idx, product in enumerate(iherb_products):
            score, details = self.calculate_enhanced_similarity(
                korean_name, english_name, product['title']
            )
            
            status = "매칭 가능" if score >= 0.6 else "매칭 불가"
            reason = ""
            
            if score < 0.6:
                if 'reason' in details:
                    if details['reason'] == 'count_mismatch':
                        reason = f"개수 불일치: 영어명 {details['english_count']}개 ≠ 아이허브 {details['iherb_count']}개"
                    elif details['reason'] == 'dosage_mismatch':
                        reason = f"용량 불일치: 영어명 {details['english_mg']}mg ≠ 아이허브 {details['iherb_mg']}mg"
                    elif details['reason'] == 'low_similarity':
                        reason = f"낮은 유사도 (영어:{details.get('english_similarity', 0):.2f}, 한글:{details.get('korean_similarity', 0):.2f})"
                else:
                    reason = "제형 불일치" if details.get('form_penalty', 0) > 0 else ""
            else:
                if details.get('exact_count_match'):
                    reason = "개수/용량 정확 매칭"
                else:
                    reason = f"높은 유사도 (영어:{details.get('english_similarity', 0):.2f})"
            
            print(f"    후보 {idx+1}: {status} ({score:.3f}) - {product['title'][:60]}...")
            if reason:
                print(f"      └ {reason}")
            
            if score > best_score:
                best_score = score
                best_match = product
                best_details = details
        
        print()
        if best_match and best_score >= 0.6:
            print(f"  최종 매칭: 성공 ({best_score:.3f})")
            print(f"    선택된 상품: {best_match['title'][:60]}...")
        else:
            print(f"  최종 매칭: 실패 (최고점수: {best_score:.3f})")
            if best_match:
                print(f"    가장 유사한 상품: {best_match['title'][:60]}...")
        
        return best_match, best_score, best_details
    
    def search_product_enhanced(self, korean_name, english_name):
        """영어 상품명으로 검색 (한글명은 로깅용)"""
        try:
            # 영어 이름 정리
            cleaned_english_name = re.sub(r'[^\w\s]', ' ', english_name)
            cleaned_english_name = re.sub(r'\s+', ' ', cleaned_english_name).strip()
            
            print(f"  검색어: {english_name[:50]}...")
            print(f"  원본 한글: {korean_name[:40]}...")
            
            search_url = f"{self.base_url}/search?kw={urllib.parse.quote(cleaned_english_name)}"
            
            products = self.get_multiple_products(search_url)
            
            if not products:
                return None, 0, None
            
            best_product, similarity_score, match_details = self.find_best_matching_product(
                korean_name, english_name, products
            )
            
            if best_product and similarity_score >= 0.6:
                return best_product['url'], similarity_score, match_details
            else:
                if products:
                    return products[0]['url'], similarity_score, match_details
                return None, 0, None
            
        except Exception as e:
            print(f"    검색 중 오류: {e}")
            return None, 0, None
    
    def extract_coupang_price_info(self, row):
        """쿠팡 가격 정보 추출"""
        try:
            price_info = {}
            
            # 현재 가격
            if 'current_price' in row:
                current_price = str(row['current_price']).replace('원', '').replace(',', '').strip()
                if current_price and current_price != 'nan':
                    price_info['current_price'] = current_price
            
            # 원가
            if 'original_price' in row:
                original_price = str(row['original_price']).replace('원', '').replace(',', '').strip()
                if original_price and original_price != 'nan':
                    price_info['original_price'] = original_price
            
            # 할인율
            if 'discount_rate' in row:
                discount_rate = str(row['discount_rate']).replace('%', '').strip()
                if discount_rate and discount_rate != 'nan':
                    price_info['discount_rate'] = discount_rate
            
            # 쿠팡 URL
            if 'product_url' in row:
                price_info['url'] = str(row['product_url'])
            
            return price_info
            
        except Exception as e:
            return {}
    
    def extract_iherb_price_info(self):
        """아이허브 가격 정보 추출 (영어 사이트 기준)"""
        try:
            price_info = {}
            extracted_items = []
            
            # 정가 추출 - 영어 사이트 선택자
            list_price_selectors = [
                '.list-price',
                '.original-price-wrapper .list-price',
                '.strike-through-price-wrapper .list-price',
                'span[class*="list-price"]',
                '.price-original'
            ]
            
            for selector in list_price_selectors:
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
            discount_price_selectors = [
                '.discount-price',
                '.strike-through-price-wrapper .discount-price',
                'b[class*="discount-price"]',
                '.price-inner b',
                'b[style*="color: rgb(211, 47, 47)"]'
            ]
            
            for selector in discount_price_selectors:
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
                element = self.driver.find_element(By.CSS_SELECTOR, '.percent-off')
                percent_text = element.text.strip()
                if percent_text and '%' in percent_text:
                    percent_match = re.search(r'(\d+)%', percent_text)
                    if percent_match:
                        price_info['discount_percent'] = percent_match.group(1)
                        extracted_items.append('할인율')
            except:
                pass
            
            # 정기배송 할인 정보 추출
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, '.auto-ship-message-item')
                for element in elements:
                    text = element.text.lower()
                    subscription_match = re.search(r'(\d+)%.*?off', text)
                    if subscription_match:
                        price_info['subscription_discount'] = subscription_match.group(1)
                        extracted_items.append('정기배송할인')
                        break
            except:
                pass
            
            # 단위당 가격
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, '.discount-price-per-unit')
                price_per_unit = element.text.strip()
                if price_per_unit:
                    price_info['price_per_unit'] = price_per_unit
                    extracted_items.append('단위당가격')
            except:
                pass
            
            # 추가 시도: 페이지 소스에서 가격 패턴 검색
            if not price_info.get('discount_price') and not price_info.get('list_price'):
                try:
                    page_source = self.driver.page_source
                    price_patterns = [r'\$([\d,]+\.?\d*)', r'"price"[:\s]*"?\$?([\d,]+\.?\d*)"?']
                    
                    for pattern in price_patterns:
                        matches = re.findall(pattern, page_source)
                        if matches:
                            prices = [re.sub(r'[^\d.]', '', match) for match in matches[:3]]
                            prices = [p for p in prices if len(p) >= 3 and '.' in p]
                            
                            if prices:
                                if not price_info.get('list_price'):
                                    price_info['list_price'] = max(prices, key=float)
                                    extracted_items.append('정가(패턴)')
                                if len(prices) > 1 and not price_info.get('discount_price'):
                                    sorted_prices = sorted(prices, key=float, reverse=True)
                                    price_info['discount_price'] = sorted_prices[1] if len(sorted_prices) > 1 else sorted_prices[0]
                                    extracted_items.append('할인가(패턴)')
                                break
                except:
                    pass
            
            if extracted_items:
                print(f"    가격 정보: ✓ {len(extracted_items)}개 항목 ({', '.join(extracted_items)})")
            else:
                print(f"    가격 정보: ✗ 추출 실패")
            
            return price_info
            
        except Exception as e:
            print(f"    가격 정보 추출 중 오류: {e}")
            return {}
    
    def extract_product_info_with_price(self, product_url):
        """상품 정보와 가격 정보를 함께 추출"""
        try:
            print("  상품 정보 추출 중...")
            
            if not self._safe_get(product_url):
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
            name_selectors = [
                'h1#name[data-testid="product-name"]',
                'h1#name',
                '.product-title h1',
                'h1'
            ]
            
            iherb_product_name = None
            for selector in name_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    name = element.text.strip()
                    if name and len(name) > 5:
                        iherb_product_name = name
                        print(f"    상품명: ✓")
                        break
                except:
                    continue
            
            if not iherb_product_name:
                print(f"    상품명: ✗ 추출 실패")
            
            # 상품코드 추출
            product_code = None
            
            # 방법 1: product-specs-list에서 찾기
            try:
                specs_element = self.driver.find_element(By.CSS_SELECTOR, "#product-specs-list")
                text = specs_element.text
                match = re.search(r'item\s*code:\s*([A-Z0-9-]+)', text, re.IGNORECASE)
                if match:
                    product_code = match.group(1)
                    print(f"    상품코드: ✓ {product_code}")
            except:
                pass
            
            # 방법 2: data 속성에서 찾기
            if not product_code:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-part-number]")
                    for element in elements:
                        value = element.get_attribute("data-part-number")
                        if value and re.match(r'^[A-Z0-9-]+$', value):
                            product_code = value
                            print(f"    상품코드: ✓ {product_code}")
                            break
                except:
                    pass
            
            # 방법 3: URL에서 추출
            if not product_code:
                try:
                    url_match = re.search(r'/pr/([A-Z0-9-]+)', product_url)
                    if url_match:
                        product_code = url_match.group(1)
                        print(f"    상품코드: ✓ {product_code} (URL에서 추출)")
                except:
                    pass
            
            if not product_code:
                print(f"    상품코드: ✗ 추출 실패")
            
            # 가격 정보 추출
            price_info = self.extract_iherb_price_info()
            
            if product_code:
                self.success_count += 1
            
            return product_code, iherb_product_name, price_info
            
        except Exception as e:
            print(f"    상품 정보 추출 중 오류: {e}")
            return None, None, {}
    
    def _auto_detect_start_point(self, input_csv_path, output_csv_path):
        """기존 결과를 분석해서 시작점 자동 감지"""
        try:
            if not os.path.exists(output_csv_path):
                print("  결과 파일 없음 - 처음부터 시작")
                return 0
            
            # 기존 결과 파일 읽기
            existing_df = pd.read_csv(output_csv_path)
            
            if len(existing_df) == 0:
                print("  빈 결과 파일 - 처음부터 시작")
                return 0
            
            # 원본 CSV와 비교해서 시작점 찾기
            input_df = pd.read_csv(input_csv_path)
            
            # actual_index 컬럼이 있으면 그것을 사용
            if 'actual_index' in existing_df.columns:
                last_processed_index = existing_df['actual_index'].max()
                start_from = last_processed_index + 1
                
                print(f"  마지막 처리: {last_processed_index}번째 상품")
                print(f"  자동 시작점: {start_from}번째 상품부터")
                
                # 전체 대비 진행률 표시
                total_count = len(input_df)
                progress = (last_processed_index + 1) / total_count * 100
                print(f"  진행률: {last_processed_index + 1}/{total_count} ({progress:.1f}%)")
                
                return start_from
            
            # actual_index가 없으면 상품명으로 매칭해서 찾기
            else:
                processed_products = set(existing_df['coupang_product_name'].tolist())
                
                for idx, row in input_df.iterrows():
                    if row['product_name'] not in processed_products:
                        print(f"  마지막 처리: {idx-1}번째 상품")
                        print(f"  자동 시작점: {idx}번째 상품부터")
                        return idx
                
                # 모든 상품이 처리되었다면
                print("  모든 상품 처리 완료")
                return len(input_df)
        
        except Exception as e:
            print(f"  시작점 자동 감지 실패: {e}")
            print("  안전을 위해 처음부터 시작")
            return 0
    
    def process_products_complete(self, csv_file_path, output_file_path, limit=None, start_from=None):
        """완전한 상품 처리 - 영어 번역명 기반"""
        try:
            df = pd.read_csv(csv_file_path)
            
            # 영어 번역 컬럼 확인
            if 'product_name_english' not in df.columns:
                raise ValueError("CSV에 'product_name_english' 컬럼이 없습니다.")
            
            # 영어 번역이 없는 행 필터링
            original_count = len(df)
            df = df.dropna(subset=['product_name_english'])
            df = df[df['product_name_english'].str.strip() != '']
            filtered_count = len(df)
            
            print(f"  원본 상품: {original_count}개")
            print(f"  영어 번역된 상품: {filtered_count}개")
            if original_count != filtered_count:
                print(f"  번역 없는 상품: {original_count - filtered_count}개 (제외됨)")
            
            if limit:
                df = df.head(limit)
            
            # 시작점 자동 감지 (start_from이 None인 경우)
            if start_from is None:
                start_from = self._auto_detect_start_point(csv_file_path, output_file_path)
            
            # 시작점 설정
            original_df_length = len(df)
            if start_from > 0:
                df = df.iloc[start_from:].reset_index(drop=True)
                if start_from >= original_df_length:
                    print("  모든 상품이 이미 처리되었습니다!")
                    return output_file_path
                print(f"  {start_from+1}번째 상품부터 재시작")
            
            print("영어 번역 기반 iHerb 가격 비교 스크래퍼 시작")
            print(f"  총 처리 상품: {len(df)}개 (전체: {original_df_length}개)")
            
            # CSV 헤더 초기화 (새로 시작하는 경우만)
            if start_from == 0:
                self._initialize_output_csv(output_file_path)
            
            for idx, (index, row) in enumerate(df.iterrows()):
                actual_idx = idx + start_from  # 실제 인덱스
                korean_name = row['product_name']
                english_name = row['product_name_english']
                
                print(f"\n[{actual_idx+1}/{original_df_length}] {korean_name[:40]}...")
                print(f"  영어명: {english_name[:50]}...")
                
                # 브라우저 재시작 체크 (30개마다)
                if actual_idx > 0 and actual_idx % self.browser_restart_interval == 0:
                    print(f"\n  === 브라우저 완전 재시작 (매 {self.browser_restart_interval}개마다) ===")
                    try:
                        self._restart_browser_with_cleanup()
                        # 재시작 후 언어 설정 다시 적용
                        self._set_iherb_to_english()
                    except Exception as e:
                        print(f"  브라우저 재시작 실패: {e}")
                        break
                
                # 쿠팡 가격 정보 간단 표시
                coupang_price_info = self.extract_coupang_price_info(row)
                coupang_summary = []
                if coupang_price_info.get('current_price'):
                    coupang_summary.append(f"{int(coupang_price_info['current_price']):,}원")
                if coupang_price_info.get('discount_rate'):
                    coupang_summary.append(f"{coupang_price_info['discount_rate']}% 할인")
                
                print(f"  쿠팡 가격: {' '.join(coupang_summary) if coupang_summary else '정보 없음'}")
                
                # 아이허브 검색 및 정보 추출 (재시도 로직 포함)
                product_url = None
                similarity_score = 0
                match_details = None
                product_code = None
                iherb_product_name = None
                iherb_price_info = {}
                
                for retry in range(self.max_retries):
                    try:
                        if retry > 0:
                            print(f"  재시도 {retry + 1}/{self.max_retries}")
                            time.sleep(5)
                        
                        product_url, similarity_score, match_details = self.search_product_enhanced(
                            korean_name, english_name
                        )
                        
                        if product_url:
                            product_code, iherb_product_name, iherb_price_info = self.extract_product_info_with_price(product_url)
                            break  # 성공하면 재시도 루프 종료
                        else:
                            print("  매칭된 상품 없음")
                            break  # 검색 결과가 없으면 재시도할 필요 없음
                            
                    except Exception as e:
                        print(f"  처리 중 오류 (시도 {retry + 1}): {str(e)[:100]}...")
                        if retry == self.max_retries - 1:
                            print("  최대 재시도 횟수 초과 - 건너뜀")
                        else:
                            # 심각한 오류의 경우 브라우저 재시작
                            if any(keyword in str(e).lower() for keyword in ['chrome not reachable', 'session deleted', 'connection refused']):
                                try:
                                    print("  심각한 오류 감지 - 브라우저 완전 재시작")
                                    self._restart_browser_with_cleanup()
                                    self._set_iherb_to_english()  # 언어 설정 재적용
                                except:
                                    print("  브라우저 재시작 실패")
                                    break
                
                # 결과 저장
                result = {
                    # 1. 기본 식별 정보
                    'coupang_product_id': row['product_id'],
                    'coupang_product_name': korean_name,
                    'coupang_product_name_english': english_name,  # 영어명 추가
                    'iherb_product_name': iherb_product_name,
                    'iherb_product_code': product_code,
                    'status': 'success' if product_code else ('not_found' if not product_url else 'code_not_found'),
                    'similarity_score': round(similarity_score, 3) if similarity_score else 0,
                    
                    # 2. URL 정보  
                    'coupang_url': coupang_price_info.get('url', ''),
                    'iherb_product_url': product_url,
                    
                    # 3. 쿠팡 가격 정보 (KRW)
                    'coupang_current_price_krw': coupang_price_info.get('current_price', ''),
                    'coupang_original_price_krw': coupang_price_info.get('original_price', ''),
                    'coupang_discount_rate': coupang_price_info.get('discount_rate', ''),
                    
                    # 4. 아이허브 가격 정보 (USD)
                    'iherb_list_price_usd': iherb_price_info.get('list_price', ''),
                    'iherb_discount_price_usd': iherb_price_info.get('discount_price', ''),
                    'iherb_discount_percent': iherb_price_info.get('discount_percent', ''),
                    'iherb_subscription_discount': iherb_price_info.get('subscription_discount', ''),
                    'iherb_price_per_unit': iherb_price_info.get('price_per_unit', ''),
                    
                    # 5. 가격 비교 (환율 적용 필요)
                    'price_difference_note': 'USD-KRW 환율 적용 필요',
                    
                    # 6. 메타데이터
                    'processed_at': datetime.now().isoformat(),
                    'actual_index': actual_idx,
                    'search_language': 'english'
                }
                
                # 결과를 즉시 CSV에 추가 저장 (누적 방식)
                self._append_result_to_csv(result, output_file_path)
                
                # 결과 출력
                print()
                if product_code:
                    print(f"  결과: 매칭 성공 ✓")
                    print(f"    상품코드: {product_code}")
                    print(f"    유사도: {similarity_score:.3f}")
                    
                    # 가격 비교 (USD vs KRW)
                    coupang_price_str = ""
                    if coupang_price_info.get('current_price'):
                        coupang_price_str = f"{int(coupang_price_info['current_price']):,}원"
                        if coupang_price_info.get('discount_rate'):
                            coupang_price_str += f" ({coupang_price_info['discount_rate']}% 할인)"
                    
                    iherb_price_str = ""
                    if iherb_price_info.get('discount_price'):
                        iherb_price_str = f"${iherb_price_info['discount_price']}"
                        if iherb_price_info.get('discount_percent'):
                            iherb_price_str += f" ({iherb_price_info['discount_percent']}% 할인)"
                        if iherb_price_info.get('subscription_discount'):
                            iherb_price_str += f" + 정기배송 {iherb_price_info['subscription_discount']}% 추가할인"
                    elif iherb_price_info.get('list_price'):
                        iherb_price_str = f"${iherb_price_info['list_price']} (정가)"
                    
                    if coupang_price_str and iherb_price_str:
                        print(f"    쿠팡   : {coupang_price_str}")
                        print(f"    아이허브: {iherb_price_str}")
                        print(f"    주의   : 환율 적용하여 비교 필요")
                    
                elif product_url:
                    print(f"  결과: 상품은 찾았으나 상품코드 추출 실패 ✗")
                    print(f"    유사도: {similarity_score:.3f}")
                else:
                    print(f"  결과: 매칭된 상품 없음 ✗")
                
                print(f"  진행률: {actual_idx+1}/{original_df_length} ({(actual_idx+1)/original_df_length*100:.1f}%)")
                print(f"  성공률: {self.success_count}/{actual_idx+1} ({self.success_count/(actual_idx+1)*100:.1f}%)")
                print(f"  결과 저장: {output_file_path} (실시간 누적)")
                
                self.random_delay()
            
            # 최종 요약 (CSV에서 읽어서 표시)
            try:
                final_df = pd.read_csv(output_file_path)
                self._print_summary(final_df)
            except:
                print("최종 요약 생성 실패")
            
            return output_file_path  # 파일 경로 반환
            
        except KeyboardInterrupt:
            print("\n사용자에 의해 중단됨")
            print(f"현재까지 결과는 {output_file_path}에 저장되어 있습니다.")
            return output_file_path
            
        except Exception as e:
            print(f"처리 중 오류: {e}")
            print(f"현재까지 결과는 {output_file_path}에 저장되어 있습니다.")
            return output_file_path
    
    def _initialize_output_csv(self, output_file_path):
        """CSV 파일 헤더 초기화 - 영어 번역 기반"""
        try:
            # 영어 번역 기반 컬럼 구조
            columns = [
                'coupang_product_id', 'coupang_product_name', 'coupang_product_name_english',
                'iherb_product_name', 'iherb_product_code', 'status', 'similarity_score',
                'coupang_url', 'iherb_product_url',
                'coupang_current_price_krw', 'coupang_original_price_krw', 'coupang_discount_rate',
                'iherb_list_price_usd', 'iherb_discount_price_usd', 'iherb_discount_percent',
                'iherb_subscription_discount', 'iherb_price_per_unit',
                'price_difference_note', 'processed_at', 'actual_index', 'search_language'
            ]
            
            empty_df = pd.DataFrame(columns=columns)
            empty_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
            print(f"  결과 파일 초기화: {output_file_path}")
            
        except Exception as e:
            print(f"  CSV 초기화 실패: {e}")
    
    def _append_result_to_csv(self, result, output_file_path):
        """결과를 CSV에 즉시 추가 (누적 방식)"""
        try:
            # 결과를 DataFrame으로 변환
            result_df = pd.DataFrame([result])
            
            # 기존 파일에 추가 (헤더 제외)
            result_df.to_csv(
                output_file_path, 
                mode='a',  # append 모드
                header=False,  # 헤더 제외
                index=False, 
                encoding='utf-8-sig'
            )
            
        except Exception as e:
            print(f"    CSV 저장 실패: {e}")
    
    def _restart_browser_with_cleanup(self):
        """브라우저 재시작 + 완전한 정리"""
        try:
            print("  브라우저 완전 재시작 중...")
            
            # 1. 기존 브라우저 종료
            if hasattr(self, 'driver'):
                try:
                    # 모든 쿠키 및 캐시 삭제
                    self.driver.delete_all_cookies()
                    self.driver.execute_script("window.localStorage.clear();")
                    self.driver.execute_script("window.sessionStorage.clear();")
                    
                    # 브라우저 종료
                    self.driver.quit()
                except:
                    pass
            
            # 2. 메모리 정리를 위한 대기
            time.sleep(5)
            
            # 3. 새 브라우저 시작
            self._initialize_browser(headless=False)
            
            # 4. 안정화 대기
            time.sleep(3)
            
            print("  브라우저 완전 재시작 완료 ✓")
            print("    - 쿠키/캐시 정리 완료")
            print("    - 메모리 정리 완료") 
            print("    - 새 세션 시작")
            
        except Exception as e:
            print(f"  브라우저 재시작 실패: {e}")
            raise
    
    def _print_summary(self, results_df):
        """결과 요약 - 영어 번역 기반"""
        total = len(results_df)
        successful = len(results_df[results_df['status'] == 'success'])
        price_extracted = len(results_df[
            (results_df['iherb_discount_price_usd'] != '') | 
            (results_df['iherb_list_price_usd'] != '')
        ])
        
        print(f"\n처리 완료")
        print(f"  총 처리: {total}개 상품")
        print(f"  매칭 성공: {successful}개 ({successful/total*100:.1f}%)")
        print(f"  가격 추출: {price_extracted}개 ({price_extracted/total*100:.1f}%)")
        
        if successful > 0:
            print(f"\n주요 결과:")
            successful_df = results_df[results_df['status'] == 'success']
            
            for idx, (_, row) in enumerate(successful_df.head(5).iterrows()):
                korean_name = row['coupang_product_name'][:30] + "..."
                english_name = row.get('coupang_product_name_english', '')[:30] + "..."
                
                coupang_price = row.get('coupang_current_price_krw', '')
                iherb_price = row.get('iherb_discount_price_usd', '')
                
                print(f"  {idx+1}. {korean_name}")
                print(f"     영어: {english_name}")
                
                if coupang_price and iherb_price:
                    try:
                        print(f"     가격: {int(coupang_price):,}원 vs ${float(iherb_price):.2f}")
                    except:
                        print(f"     가격: {coupang_price}원 vs ${iherb_price}")
                
                print()
    
    def close(self):
        """브라우저 종료"""
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
                print("  브라우저 종료 ✓")
        except:
            pass

# 실행
if __name__ == "__main__":
    scraper = None
    try:
        print("영어 번역 기반 iHerb 가격 비교 스크래퍼")
        print("개선사항:")
        print("- 자동 언어 설정 변경 (한글→영어)")
        print("- 영어 번역된 상품명으로 검색")
        print("- 자동 시작점 감지")
        print("- 실시간 CSV 누적 저장")
        print("- 브라우저 완전 재시작 (캐시/메모리 정리)")
        print("- 중단 시 진행상황 자동 보존")
        print("- USD-KRW 가격 분리 저장")
        print()
        
        scraper = EnglishIHerbScraper(
            headless=False,
            delay_range=(2, 4),
            max_products_to_compare=4
        )
        
        input_csv = "coupang_products_translated.csv"
        output_csv = "iherb_english_results.csv"
        
        # start_from을 None으로 설정하면 자동으로 감지
        results = scraper.process_products_complete(
            csv_file_path=input_csv,
            output_file_path=output_csv,
            limit=100,
            start_from=None  # None이면 자동 감지, 숫자 입력시 해당 지점부터 시작
        )
        
        if results is not None:
            print(f"\n최종 결과: {results} ✓")
            print("\n영어 번역 기반 검색 기능:")
            print("- start_from=None: 자동으로 중단 지점 감지")
            print("- start_from=숫자: 특정 지점부터 강제 시작")
            print("- 매 상품마다 즉시 CSV에 저장")
            print("- 영어 사이트 기반 정확한 매칭")
            print("- USD-KRW 분리로 환율 계산 용이")
            print("- 브라우저 자동 관리 및 오류 복구")
    
    except KeyboardInterrupt:
        print("\n중단됨")
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper:
            scraper.close()