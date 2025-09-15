"""
네이버 가격비교 최저가 크롤링 도구 (속도 최적화 버전)
- 기존 모든 기능 보존
- 대기시간 최적화
- 불필요한 페이지 리셋 제거
- 요소 탐색 효율화
"""

import time
import sys
import argparse
import pathlib
import pandas as pd
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
try:
    import undetected_chromedriver as uc
    USE_UNDETECTED = True
except ImportError:
    print("undetected_chromedriver를 찾을 수 없습니다. 기본 selenium을 사용합니다.")
    USE_UNDETECTED = False

# 셀렉터 상수
OPTION_AREA_SEL = 'div.stdOpt_standard_option_area__9Us8c'
OPTION_TITLE_SEL = 'div.stdOpt_option_title__AIqpq'
OPTION_BTN_SEL = 'a.stdOpt_btn_option__whans, button.stdOpt_btn_option__whans'
LOWEST_PRICE_SEL = 'em.lowestPrice_num__adgCI'
PRICE_AREA_SEL = 'div.lowestPrice_price_area__pCFkJ'
PRODUCT_NAME_SEL = 'h1.h1_title__bByTS, h1._22kNQuEXmb, h1.top_summary_title__ViyrM'

class SpeedOptimizedNaverPriceCrawler:
    def __init__(self, headless=False):
        self.driver = None
        self.wait = None
        self.headless = headless
        self.results = []
        self.original_url = None
        self.cached_elements = {}  # 요소 캐싱
        
    def setup_driver(self):
        """Chrome 드라이버 설정 - 기존 기능 유지하면서 속도 최적화"""
        if USE_UNDETECTED:
            try:
                options = uc.ChromeOptions()
                if self.headless:
                    options.add_argument('--headless')
                
                # 필요한 기능은 유지하면서 성능만 향상
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-blink-features=AutomationControlled')
                
                self.driver = uc.Chrome(options=options)
                self.wait = WebDriverWait(self.driver, 15)  # 기존보다 단축
                print("undetected_chromedriver 사용")
                return
            except Exception as e:
                print(f"undetected_chromedriver 실패: {e}")
        
        options = Options()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 15)  # 기존보다 단축
        print("기본 selenium 사용")

    def check_page_not_found_error(self):
        """페이지 오류 감지 - 캐싱으로 속도 향상"""
        try:
            current_url = self.driver.current_url
            
            if 'error' in current_url or 'notfound' in current_url:
                return True
            
            # 페이지 소스는 필요할 때만 가져오기
            if not hasattr(self, '_last_page_check') or time.time() - self._last_page_check > 5:
                page_source = self.driver.page_source.lower()
                self._cached_page_source = page_source
                self._last_page_check = time.time()
            else:
                page_source = self._cached_page_source
            
            error_indicators = [
                'style_content_error__tqybf',
                '페이지를 찾을 수 없습니다',
                'page not found',
                'style_head__rkilf'
            ]
            
            for indicator in error_indicators:
                if indicator in page_source:
                    return True
            
            return False
            
        except Exception as e:
            print(f"페이지 오류 감지 중 예외: {e}")
            return False

    def recover_from_page_error(self, max_retries=2):  # 재시도 횟수 감소
        """페이지 오류 복구 - 속도 향상"""
        print("페이지 오류 복구 시도...")
        
        for attempt in range(max_retries):
            try:
                if self.original_url:
                    self.driver.get(self.original_url)
                else:
                    self.driver.back()
                
                time.sleep(1.5)  # 3초 → 1.5초로 단축
                
                if not self.check_page_not_found_error():
                    print("페이지 오류 복구 성공!")
                    return True
                
            except Exception as e:
                print(f"복구 시도 {attempt + 1} 실패: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)  # 5초 → 2초로 단축
        
        return False

    def safe_click_option(self, area_index, option_index, max_retries=2):  # 재시도 감소
        """안전한 옵션 클릭 - 속도 최적화"""
        for retry in range(max_retries):
            try:
                # 캐시된 요소가 있으면 재사용
                cache_key = f"option_areas_{area_index}"
                if cache_key not in self.cached_elements or retry > 0:
                    option_areas = self.driver.find_elements(By.CSS_SELECTOR, OPTION_AREA_SEL)
                    self.cached_elements[cache_key] = option_areas
                else:
                    option_areas = self.cached_elements[cache_key]
                
                if area_index >= len(option_areas):
                    return self.extract_price_info()
                
                area = option_areas[area_index]
                option_buttons = area.find_elements(By.CSS_SELECTOR, OPTION_BTN_SEL)
                
                if option_index >= len(option_buttons):
                    return self.extract_price_info()
                
                btn = option_buttons[option_index]
                
                if 'stdOpt_active__BJ_n3' in btn.get_attribute('class'):
                    return self.extract_price_info()
                
                self.driver.execute_script("arguments[0].click();", btn)
                time.sleep(1.5)  # 3초 → 1.5초로 단축
                
                # 페이지 오류 체크 간소화
                if self.check_page_not_found_error():
                    if self.recover_from_page_error():
                        continue
                    else:
                        return {
                            'lowest_price': 0,
                            'delivery_fee': 0,
                            'delivery_info': '페이지 오류 복구 실패',
                            'shopping_mall': '페이지 오류'
                        }
                
                return self.extract_price_info()
                
            except StaleElementReferenceException:
                # 캐시 무효화
                self.cached_elements.clear()
                if retry < max_retries - 1:
                    time.sleep(1)  # 3초 → 1초로 단축
                    continue
            except Exception as e:
                print(f"옵션 클릭 중 오류: {e}")
                if retry < max_retries - 1:
                    time.sleep(1)
                    continue
        
        return {
            'lowest_price': 0,
            'delivery_fee': 0,
            'delivery_info': '옵션 클릭 실패',
            'shopping_mall': '옵션 클릭 실패'
        }

    def extract_product_name(self, max_wait_time=3):  # 대기시간 단축
        """상품명 추출 - 기존 로직 유지, 속도만 향상"""
        priority_selectors = [
            '.top_summary_title__t1xLu h2',
            'h1.h1_title__bByTS',
            'h1._22kNQuEXmb', 
            'h1.top_summary_title__ViyrM',
            '.productSet_title__adIWm',
            '.basicList_title__VfX3c'
        ]
        
        if hasattr(self, '_option_changed') and self._option_changed:
            time.sleep(1)  # 2초 → 1초로 단축
            self._option_changed = False
        
        for selector in priority_selectors:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, selector)
                if element and element.text.strip():
                    text = element.text.strip()
                    
                    if selector == '.top_summary_title__t1xLu h2':
                        try:
                            label_element = element.find_element(By.CSS_SELECTOR, 'span.top_label__eIkFB')
                            if label_element:
                                label_text = label_element.text.strip()
                                text = text.replace(label_text, '', 1).strip()
                        except NoSuchElementException:
                            pass
                    
                    text = re.sub(r'^해외\s*', '', text)
                    
                    if text and len(text) > 3:
                        return text
                        
            except NoSuchElementException:
                continue
            except Exception as e:
                continue
        
        # 페이지 타이틀에서 추출 시도
        try:
            title = self.driver.title
            if '네이버 쇼핑' in title:
                product_name = title.split(':')[0].strip()
                if product_name and product_name != '네이버 쇼핑':
                    return product_name
        except:
            pass
            
        return "상품명 없음"
            
    def extract_price_info(self):
        """가격 정보 추출 - 기존 로직 완전 보존"""
        if self.check_page_not_found_error():
            if self.recover_from_page_error():
                pass
            else:
                return {
                    'lowest_price': 0,
                    'delivery_fee': 0,
                    'delivery_info': '페이지 오류로 인한 추출 실패',
                    'shopping_mall': '페이지 오류'
                }
        
        try:
            price_elem = self.driver.find_element(By.CSS_SELECTOR, LOWEST_PRICE_SEL)
            price_text = price_elem.text.strip().replace(',', '')
            lowest_price = int(price_text) if price_text.isdigit() else 0
            
            delivery_fee = 0
            delivery_info = "배송비 정보 없음"
            shopping_mall = "쇼핑몰 정보 없음"
            
            try:
                price_area = self.driver.find_element(By.CSS_SELECTOR, PRICE_AREA_SEL)
                delivery_elems = price_area.find_elements(By.CSS_SELECTOR, 'div.lowestPrice_cell__1_Cz0')
                
                for elem in delivery_elems:
                    text = elem.text.strip()
                    if '배송비' in text or '원 포함' in text:
                        delivery_match = re.search(r'(\d{1,3}(?:,\d{3})*)', text)
                        if delivery_match:
                            delivery_fee = int(delivery_match.group(1).replace(',', ''))
                        delivery_info = text
                    elif '배송비' not in text and '상품구성' not in text and '개' not in text and len(text) > 0:
                        shopping_mall = text
            except NoSuchElementException:
                pass
                
            return {
                'lowest_price': lowest_price,
                'delivery_fee': delivery_fee,
                'delivery_info': delivery_info,
                'shopping_mall': shopping_mall
            }
            
        except NoSuchElementException:
            return {
                'lowest_price': 0,
                'delivery_fee': 0,
                'delivery_info': '가격 정보 없음',
                'shopping_mall': '쇼핑몰 정보 없음'
            }

    def parse_product_options(self):
        """상품 옵션 파싱 - 기존 로직 완전 보존"""
        options_data = {}
        processed_titles = set()
        
        try:
            option_areas = self.driver.find_elements(By.CSS_SELECTOR, OPTION_AREA_SEL)
            
            for area_idx, area in enumerate(option_areas):
                try:
                    title_elem = area.find_element(By.CSS_SELECTOR, OPTION_TITLE_SEL)
                    option_title = title_elem.text.strip()
                    normalized_title = self.normalize_option_title(option_title)
                    
                    if normalized_title in processed_titles:
                        continue
                    
                    processed_titles.add(normalized_title)
                    
                    option_buttons = area.find_elements(By.CSS_SELECTOR, OPTION_BTN_SEL)
                    
                    option_values = []
                    for btn_idx, btn in enumerate(option_buttons):
                        try:
                            title_elem = btn.find_element(By.CSS_SELECTOR, 'span.stdOpt_title__RoGG6')
                            option_text = title_elem.text.strip()
                            
                            option_values.append({
                                'text': option_text,
                                'index': btn_idx
                            })
                        except NoSuchElementException:
                            continue
                            
                    if option_values:
                        options_data[normalized_title] = {
                            'area_index': area_idx,
                            'values': option_values
                        }
                        
                except NoSuchElementException:
                    continue
                    
        except NoSuchElementException:
            pass
            
        return options_data

    def normalize_option_title(self, title):
        """옵션 제목 정규화"""
        normalized = re.sub(r'\s*:\s*.*$', '', title.replace('\n', ' ').strip())
        return normalized

    def detect_dynamic_options(self):
        """동적 옵션 구조 감지 - 테스트 시간 단축"""
        print("동적 옵션 구조 감지 중...")
        
        try:
            initial_options = self.parse_product_options()
            if len(initial_options) < 2:
                return False
            
            option_keys = list(initial_options.keys())
            
            # 키워드 기반 빠른 감지 먼저 수행
            dynamic_keywords = [
                ('개당', '수량'), ('개당수량', '수량'), ('개당 수량', '수량'),
                ('사이즈', '수량'), ('용량', '수량'), ('타입', '수량')
            ]
            
            for first_keyword, second_keyword in dynamic_keywords:
                first_match = any(first_keyword in key.lower() for key in option_keys)
                second_match = any(second_keyword in key.lower() for key in option_keys)
                
                if first_match and second_match:
                    print(f"동적 옵션 키워드 매치: {first_keyword} + {second_keyword}")
                    return True
            
            # 실제 클릭 테스트 (최소한으로)
            first_key = option_keys[0]
            first_option_info = initial_options[first_key]
            
            if len(first_option_info['values']) < 2:
                return False
            
            # 두 번째 옵션 값으로 테스트
            second_value = first_option_info['values'][1]
            self.safe_click_option(
                first_option_info['area_index'], 
                second_value['index']
            )
            
            time.sleep(2)  # 4초 → 2초로 단축
            
            updated_options = self.parse_product_options()
            is_dynamic = self.options_structure_changed(initial_options, updated_options)
            
            if is_dynamic:
                print("동적 옵션 구조 확인됨")
            
            # 빠른 리셋 (전체 페이지 새로고침 대신 첫 옵션 클릭)
            if len(first_option_info['values']) > 0:
                self.safe_click_option(
                    first_option_info['area_index'], 
                    first_option_info['values'][0]['index']
                )
                time.sleep(1)
            
            return is_dynamic
            
        except Exception as e:
            print(f"동적 옵션 감지 오류: {e}")
            return False

    def options_structure_changed(self, initial_options, updated_options):
        """옵션 구조 변화 감지"""
        if len(initial_options) != len(updated_options):
            return True
        
        option_keys = list(initial_options.keys())
        if len(option_keys) < 2:
            return False
        
        for i in range(1, len(option_keys)):
            key = option_keys[i]
            if key not in updated_options:
                return True
            
            initial_values = [v['text'] for v in initial_options[key]['values']]
            updated_values = [v['text'] for v in updated_options[key]['values']]
            
            if len(initial_values) != len(updated_values) or initial_values != updated_values:
                return True
        
        return False

    def reset_to_initial_state(self):
        """페이지 초기 상태로 리셋 - 최소한으로 수행"""
        try:
            # 캐시 초기화
            self.cached_elements.clear()
            
            # 가능하면 첫 번째 옵션 클릭으로 리셋 시도
            try:
                option_areas = self.driver.find_elements(By.CSS_SELECTOR, OPTION_AREA_SEL)
                if option_areas:
                    first_area = option_areas[0]
                    option_buttons = first_area.find_elements(By.CSS_SELECTOR, OPTION_BTN_SEL)
                    if option_buttons:
                        first_btn = option_buttons[0]
                        if 'stdOpt_active__BJ_n3' not in first_btn.get_attribute('class'):
                            self.driver.execute_script("arguments[0].click();", first_btn)
                            time.sleep(1)
                            return
            except:
                pass
            
            # 첫 번째 옵션 클릭이 안되면 페이지 새로고침
            if self.original_url:
                self.driver.get(self.original_url)
            else:
                self.driver.refresh()
            
            time.sleep(1.5)  # 3초 → 1.5초
            
        except Exception as e:
            print(f"페이지 리셋 오류: {e}")

    def crawl_dynamic_options(self, url, model_name, original_product_name):
        """동적 옵션 크롤링 - 리셋 최소화"""
        print("동적 옵션 재귀적 크롤링 시작...")
        
        try:
            base_product_name = self.extract_product_name()
            
            initial_options = self.parse_product_options()
            if not initial_options:
                price_info = self.extract_price_info()
                self.save_single_result(url, model_name, original_product_name, 
                                      base_product_name, "기본 구성", price_info)
                return
            
            option_keys = list(initial_options.keys())
            
            self.process_options_recursively(
                initial_options, option_keys, 0, [],
                url, model_name, original_product_name, base_product_name
            )
                
        except Exception as e:
            print(f"동적 옵션 크롤링 오류: {e}")

    def process_options_recursively(self, current_options, option_keys, level, selected_options,
                                   url, model_name, original_product_name, base_product_name):
        """재귀적 옵션 처리 - 스마트 리셋"""
        if level >= len(option_keys):
            option_desc = " | ".join(selected_options)
            current_product_name = self.extract_product_name()
            price_info = self.extract_price_info()
            self.save_single_result(url, model_name, original_product_name, 
                                  current_product_name, option_desc, price_info)
            return
        
        current_key = option_keys[level]
        
        if current_key not in current_options:
            return
        
        current_option = current_options[current_key]
        
        for idx, value_info in enumerate(current_option['values']):
            try:
                # 레벨 0이 아닌 경우에만 선택적 리셋
                if level > 0 and idx == 0:
                    # 첫 번째 값일 때만 리셋
                    self.reset_to_initial_state()
                    time.sleep(1)  # 2초 → 1초
                    
                    # 이전 옵션들 재선택
                    for prev_level in range(level):
                        prev_key = option_keys[prev_level]
                        prev_selection = selected_options[prev_level].split(': ')[1]
                        
                        temp_options = self.parse_product_options()
                        if prev_key in temp_options:
                            for temp_value in temp_options[prev_key]['values']:
                                if temp_value['text'] == prev_selection:
                                    self.safe_click_option(
                                        temp_options[prev_key]['area_index'],
                                        temp_value['index']
                                    )
                                    time.sleep(1)  # 2초 → 1초
                                    break
                
                # 현재 옵션 선택
                self.safe_click_option(current_option['area_index'], value_info['index'])
                time.sleep(1)  # 2초 → 1초
                
                current_selection = f"{current_key}: {value_info['text']}"
                new_selected_options = selected_options + [current_selection]
                
                if level == len(option_keys) - 1:
                    # 마지막 레벨
                    option_desc = " | ".join(new_selected_options)
                    current_product_name = self.extract_product_name()
                    price_info = self.extract_price_info()
                    self.save_single_result(url, model_name, original_product_name, 
                                          current_product_name, option_desc, price_info)
                else:
                    # 다음 레벨로
                    updated_options = self.parse_product_options()
                    self.process_options_recursively(
                        updated_options, option_keys, level + 1, new_selected_options,
                        url, model_name, original_product_name, base_product_name
                    )
                
            except Exception as e:
                print(f"옵션 '{value_info['text']}' 처리 오류: {e}")
                continue

    def crawl_static_options(self, url, model_name, original_product_name):
        """정적 옵션 크롤링 - 기존 로직 보존"""
        print("정적 옵션 크롤링 시작...")
        
        try:
            base_product_name = self.extract_product_name()
            
            options_data = self.parse_product_options()
            
            combinations = self.generate_all_option_combinations(options_data)
            
            for i, combination in enumerate(combinations):
                try:
                    current_product_name = base_product_name
                    price_info = None
                    
                    if combination['selections']:
                        for selection in combination['selections']:
                            price_info = self.safe_click_option(
                                selection['area_index'], 
                                selection['option_index']
                            )
                            if selection == combination['selections'][-1]:
                                time.sleep(1)  # 2초 → 1초
                                current_product_name = self.extract_product_name()
                    else:
                        price_info = self.extract_price_info()
                    
                    if price_info and price_info.get('lowest_price', 0) == 0:
                        if self.check_page_not_found_error():
                            if self.recover_from_page_error():
                                price_info = self.extract_price_info()
                                current_product_name = self.extract_product_name()
                        elif self.check_captcha_or_block():
                            input("캡차 해결 후 엔터: ")
                            self.driver.refresh()
                            time.sleep(1)
                            price_info = self.extract_price_info()
                            current_product_name = self.extract_product_name()
                    
                    if price_info is None:
                        price_info = {
                            'lowest_price': 0,
                            'delivery_fee': 0,
                            'delivery_info': '가격 정보 추출 실패',
                            'shopping_mall': '가격 정보 추출 실패'
                        }
                    
                    self.save_single_result(url, model_name, original_product_name, 
                                          current_product_name, combination['description'], price_info)
                    
                except Exception as e:
                    print(f"조합 처리 오류: {e}")
                    continue
                    
        except Exception as e:
            print(f"정적 옵션 크롤링 오류: {e}")

    def generate_all_option_combinations(self, options_data):
        """모든 옵션 조합 생성 - 기존 로직 완전 보존"""
        if not options_data:
            return [{'description': '기본 구성', 'selections': []}]
        
        combinations = []
        option_keys = list(options_data.keys())
        
        def generate_combinations(key_index, current_combination, current_selections):
            if key_index >= len(option_keys):
                if current_combination:
                    combinations.append({
                        'description': ' | '.join(current_combination),
                        'selections': current_selections.copy()
                    })
                return
            
            key = option_keys[key_index]
            option_info = options_data[key]
            
            for value_info in option_info['values']:
                new_combination = current_combination + [f"{key}: {value_info['text']}"]
                new_selections = current_selections + [{
                    'option_key': key,
                    'area_index': option_info['area_index'],
                    'option_index': value_info['index'],
                    'text': value_info['text']
                }]
                
                generate_combinations(key_index + 1, new_combination, new_selections)
        
        generate_combinations(0, [], [])
        
        return combinations if combinations else [{'description': '기본 구성', 'selections': []}]

    def check_captcha_or_block(self):
        """캡차/봇 탐지 확인 - 기존 로직 보존"""
        try:
            current_url = self.driver.current_url
            page_source = self.driver.page_source.lower()
            
            if 'captcha' in current_url or 'blocked' in current_url:
                return True
                
            block_indicators = [
                '접속이 일시적으로 제한',
                '비정상적인 접근이 감지',
                'captcha',
                '자동입력 방지',
                '보안문자',
                '네이버는 안정적인'
            ]
            
            for indicator in block_indicators:
                if indicator in page_source:
                    return True
                    
            captcha_selectors = [
                'img[src*="captcha"]',
                'img#captchaImg',
                'input[name="captcha"]',
                '.captcha',
                '#captcha'
            ]
            
            for selector in captcha_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element.is_displayed():
                        return True
                except:
                    continue
                    
            return False
            
        except Exception as e:
            print(f"캡차 감지 중 오류: {e}")
            return False

    def save_single_result(self, url, model_name, original_product_name, 
                          current_product_name, option_desc, price_info):
        """단일 결과 저장 - 기존 로직 완전 보존"""
        if not current_product_name or current_product_name.strip() == "":
            current_product_name = self.extract_product_name()
        
        result_row = {
            '상품링크': url,
            '모델명': model_name,
            '원본 상품명': original_product_name,
            '크롤링된 상품명': current_product_name,
            '최저가': price_info.get('lowest_price', 0),
            '배송비': price_info.get('delivery_fee', 0),
            '배송비포함가': price_info.get('lowest_price', 0) + price_info.get('delivery_fee', 0),
            '배송비정보': price_info.get('delivery_info', ''),
            '쇼핑몰': price_info.get('shopping_mall', ''),
            '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 옵션 파싱 및 개별 칼럼 추가
        if option_desc and option_desc != "기본 구성":
            options = self.parse_option_description(option_desc)
            for option_key, option_value in options.items():
                safe_column_name = self.safe_column_name(option_key)
                result_row[safe_column_name] = option_value
        
        self.results.append(result_row)
        print(f"완료: {option_desc} - {current_product_name[:50]}{'...' if len(current_product_name) > 50 else ''} - {price_info.get('lowest_price', 0)}원")

    def parse_option_description(self, option_desc):
        """옵션 설명을 파싱하여 개별 옵션으로 분리"""
        options = {}
        
        if not option_desc or option_desc == "기본 구성":
            return options
        
        parts = re.split(r'\s*[|,]\s*', option_desc)
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
                
            if ':' in part:
                key, value = part.split(':', 1)
                key = key.strip()
                value = value.strip()
                
                if key and value:
                    options[key] = value
            else:
                options['기타옵션'] = part
                
        return options
    
    def safe_column_name(self, column_name):
        """칼럼명을 CSV 저장에 안전한 형태로 변환"""
        safe_name = column_name.replace(' ', '_')
        safe_name = safe_name.replace('\t', '_')
        safe_name = safe_name.replace('\n', '_')
        
        safe_name = re.sub(r'[^\w가-힣]', '_', safe_name)
        safe_name = re.sub(r'_+', '_', safe_name)
        safe_name = safe_name.strip('_')
        
        if not safe_name:
            safe_name = '옵션'
            
        return safe_name

    def crawl_product_info(self, url, model_name, original_product_name):
        """메인 상품 정보 크롤링 - 기본 대기시간 단축"""
        max_retries = 2  # 3 → 2로 감소
        self.original_url = url
        
        for attempt in range(max_retries):
            try:
                print(f"크롤링 시작 (시도 {attempt + 1}): {url}")
                self.driver.get(url)
                time.sleep(2)  # 3초 → 2초로 단축
                
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                
                if self.check_page_not_found_error():
                    if not self.recover_from_page_error():
                        if attempt < max_retries - 1:
                            time.sleep(3)  # 5초 → 3초로 단축
                            continue
                        else:
                            break
                
                if self.check_captcha_or_block():
                    print("캡차 또는 봇 탐지 페이지가 감지되었습니다!")
                    print("브라우저에서 캡차를 해결하거나 네이버에 로그인한 후 엔터를 누르세요.")
                    input("처리 완료 후 엔터를 누르세요: ")
                    
                    self.driver.get(url)
                    time.sleep(2)  # 3초 → 2초로 단축
                    
                    if self.check_captcha_or_block() or self.check_page_not_found_error():
                        if attempt < max_retries - 1:
                            continue
                        else:
                            break
                
                # 동적 옵션 여부 감지
                has_dynamic_options = self.detect_dynamic_options()
                
                if has_dynamic_options:
                    print("동적 옵션 구조 감지 - 재귀적 크롤링 수행")
                    self.crawl_dynamic_options(url, model_name, original_product_name)
                else:
                    print("정적 옵션 구조 감지 - 조합 방식 크롤링 수행")
                    self.crawl_static_options(url, model_name, original_product_name)
                
                return
                        
            except Exception as e:
                print(f"상품 크롤링 실패 (시도 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(3)  # 5초 → 3초로 단축
                    continue
        
        # 모든 시도가 실패한 경우
        print("모든 재시도가 실패했습니다. 오류 정보를 저장합니다.")
        error_row = {
            '상품링크': url,
            '모델명': model_name,
            '원본 상품명': original_product_name,
            '크롤링된 상품명': '크롤링 실패',
            '최저가': 0,
            '배송비': 0,
            '배송비포함가': 0,
            '배송비정보': '모든 재시도 실패',
            '쇼핑몰': '오류 발생',
            '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        self.results.append(error_row)
            
    def save_results_to_csv(self, output_file):
        """결과를 CSV 파일로 저장 - 기존 로직 완전 보존"""
        if not self.results:
            print("저장할 결과가 없습니다.")
            return
        
        all_columns = set()
        for result in self.results:
            all_columns.update(result.keys())
        
        base_columns = [
            '상품링크', '모델명', '원본 상품명', '크롤링된 상품명',
            '최저가', '배송비', '배송비포함가', '배송비정보', '쇼핑몰', '크롤링시간'
        ]
        
        option_columns = sorted([col for col in all_columns if col not in base_columns])
        final_columns = base_columns + option_columns
        
        df_data = []
        for result in self.results:
            row_data = {}
            for col in final_columns:
                row_data[col] = result.get(col, '')
            df_data.append(row_data)
        
        df = pd.DataFrame(df_data, columns=final_columns)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"결과 저장 완료: {output_file}")
        print(f"총 {len(self.results)}개 항목, {len(final_columns)}개 칼럼")
        print(f"기본 칼럼: {len(base_columns)}개")
        print(f"옵션 칼럼: {len(option_columns)}개 - {option_columns}")
        
        if option_columns:
            print("\n=== 옵션 칼럼별 값 분포 ===")
            for col in option_columns:
                unique_values = df[col].value_counts().head(5)
                print(f"{col}: {len(unique_values)} 종류 (상위 5개: {list(unique_values.index)})")
        
    def run(self, input_csv_path, output_csv_path=None):
        """메인 실행 함수 - 상품 간 대기시간 단축"""
        try:
            df = pd.read_csv(input_csv_path, encoding='utf-8-sig')
            print(f"입력 파일 읽기 완료: {len(df)}개 상품")
            
            required_columns = ['상품링크', '모델명', '상품명']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"필수 컬럼이 없습니다: {missing_columns}")
                return
                
            self.setup_driver()
            
            start_time = time.time()
            
            for idx, row in df.iterrows():
                url = row['상품링크']
                model_name = row['모델명']
                product_name = row['상품명']
                
                print(f"\n[{idx+1}/{len(df)}] {model_name} 처리 중...")
                self.crawl_product_info(url, model_name, product_name)
                
                if idx < len(df) - 1:
                    time.sleep(2)  # 5초 → 2초로 단축
                
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            if output_csv_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_csv_path = f'naver_price_result_speed_optimized_{timestamp}.csv'
                
            self.save_results_to_csv(output_csv_path)
            
            print(f"\n=== 성능 통계 ===")
            print(f"총 소요시간: {elapsed_time:.1f}초 ({elapsed_time/60:.1f}분)")
            print(f"상품당 평균 시간: {elapsed_time/len(df):.1f}초")
            print(f"수집된 총 결과: {len(self.results)}개")
            
        except Exception as e:
            print(f"실행 오류: {e}")
        finally:
            if self.driver:
                self.driver.quit()

def main():
    parser = argparse.ArgumentParser(description='속도 최적화된 네이버 가격비교 크롤러')
    parser.add_argument('input_csv', help='입력 CSV 파일 경로')
    parser.add_argument('-o', '--output', help='출력 CSV 파일 경로')
    parser.add_argument('--headless', action='store_true', help='헤드리스 모드')
    
    args = parser.parse_args()
    
    if not pathlib.Path(args.input_csv).exists():
        print(f"입력 파일이 존재하지 않습니다: {args.input_csv}")
        sys.exit(1)
        
    crawler = SpeedOptimizedNaverPriceCrawler(headless=args.headless)
    crawler.run(args.input_csv, args.output)

if __name__ == '__main__':
    main()