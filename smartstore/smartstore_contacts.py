#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
네이버 스마트스토어 종합 정보 자동 추출 시스템

추출 정보:
- 스토어명 (og:title에서)
- 관심고객수
- 스토어등급 (파워, 일반 등)
- 서비스만족도 (굿서비스, 구매만족, 빠른배송, 문의응답)
- 전체방문자수
- 판매자 연락처 (업체명, 대표자명, 전화번호, 이메일, 사업자등록번호, 주소)
- 전체상품수 (정확한 수치)
- 스토어 특성 분석 (AI 분석, 표본상품수)

특징:
- 실시간 저장 및 중단점 재시작 지원
- 한글 컬럼명으로 결과 저장
- 요청 간격 조절로 안정적 수집

사용법:
  pip install undetected-chromedriver pandas google-generativeai
  python smartstore_extractor.py stores.txt result.csv
  python smartstore_extractor.py stores.txt result.csv --no-analysis --headless
"""

import os, re, time, argparse, json, sys, pickle
import pandas as pd
from typing import List, Dict, Optional
from urllib.parse import urlparse, quote

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, UnexpectedAlertPresentException,
    WebDriverException, JavascriptException, InvalidCookieDomainException
)

# 스토어 분석 모듈 임포트 (선택적)
try:
    from smartstore_analyzer import create_analyzer
    STORE_ANALYZER_AVAILABLE = True
    print("스토어 분석 모듈 로드됨")
except ImportError:
    STORE_ANALYZER_AVAILABLE = False
    print("스토어 분석 모듈 없음 - 기본 기능만 사용")

# ===== 시스템 설정 =====
PAGE_WAIT = 15
USER_DATA_DIR = os.path.expanduser("~/.cache/smartstore_chrome")
COOKIE_PATH = os.path.join(USER_DATA_DIR, "naver_cookies.pkl")
os.makedirs(USER_DATA_DIR, exist_ok=True)

ENABLE_STORE_ANALYSIS = True  # 스토어 분석 기능 활성화 여부

# 로그인 URL
LOGIN_URL = "https://nid.naver.com/nidlogin.login?mode=form&url=https://www.naver.com/"

# CSS 셀렉터
SEL_INTEREST_CUSTOMERS = '.hvkOQZh35E'  # 관심고객수 영역
SEL_SELLER_INFO_BTN = 'button[data-shp-area-id="sellerinfo"]'  # 판매자 상세정보 버튼
SEL_SELLER_PANEL = 'dl._3BlyWp6LJv, dl.UIUfwcFMsm'  # 판매자 정보 패널

# 정규식 패턴
CUSTOMER_COUNT_RE = re.compile(r'관심고객수\s*[^\d]*(\d{1,3}(?:,\d{3})*)')
PHONE_RE = re.compile(r'(?:\d{2,4}-\d{3,4}-\d{4})|(?:\d{8,11})')
EMAIL_RE = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
TOTAL_PRODUCTS_RE = re.compile(r'총\s*<strong>(\d{1,3}(?:,\d{3})*)</strong>\s*개')
TOTAL_PRODUCTS_ALT_RE = re.compile(r'총\s*(\d{1,3}(?:,\d{3})*)\s*개')

# ===== 드라이버 및 로그인 관리 =====
def make_driver(headless=False):
    """Chrome 드라이버 생성"""
    opt = uc.ChromeOptions()
    opt.add_argument("--disable-popup-blocking")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    opt.add_argument("--profile-directory=Default")
    opt.add_argument("--disable-features=SameSiteByDefaultCookies,CookiesWithoutSameSiteMustBeSecure")
    if headless:
        opt.add_argument("--headless=new")
    d = uc.Chrome(options=opt, version_main=140)
    d.set_page_load_timeout(45)
    return d

def load_cookies(driver):
    """저장된 쿠키 로드"""
    if not os.path.exists(COOKIE_PATH):
        return
    
    try:
        driver.get("https://www.naver.com")
        with open(COOKIE_PATH, "rb") as f:
            cookies = pickle.load(f)
        
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except InvalidCookieDomainException:
                pass
        
        driver.refresh()
        time.sleep(1)
        print("쿠키 로드 완료")
    except Exception as e:
        print(f"쿠키 로드 실패: {e}")

def save_cookies(driver):
    """현재 쿠키 저장"""
    try:
        with open(COOKIE_PATH, "wb") as f:
            pickle.dump(driver.get_cookies(), f)
        print("쿠키 저장 완료")
    except Exception as e:
        print(f"쿠키 저장 실패: {e}")

def check_login_status(driver) -> bool:
    """로그인 상태 확인"""
    try:
        driver.get("https://www.naver.com")
        time.sleep(2)
        
        # 로그인 페이지로 리다이렉트 여부 확인
        if "nid.naver.com" in driver.current_url:
            return False
        
        # 네이버 메인페이지에서 로그인 상태 확인
        try:
            # 로그인 버튼 존재 여부
            login_btn = driver.find_elements(By.CSS_SELECTOR, 'a[href*="nid.naver.com"]')
            if login_btn:
                return False
            
            # 로그아웃 버튼이나 사용자 메뉴 존재 여부
            logout_elements = driver.find_elements(By.CSS_SELECTOR, 'a[href*="logout"], .MyView-module__my_area')
            if logout_elements:
                return True
                
        except Exception:
            pass
        
        # 스마트스토어 직접 접근 테스트
        test_url = "https://smartstore.naver.com"
        driver.get(test_url)
        time.sleep(2)
        
        return "nid.naver.com" not in driver.current_url
        
    except Exception as e:
        print(f"로그인 상태 확인 실패: {e}")
        return False

def ensure_login(driver):
    """로그인 확인 및 처리"""
    print("로그인 상태 확인 중...")
    
    # 저장된 쿠키 로드
    load_cookies(driver)
    
    # 로그인 상태 확인
    if check_login_status(driver):
        print("이미 로그인되어 있습니다.")
        return
    
    print("로그인이 필요합니다.")
    driver.get(LOGIN_URL)
    
    print("\n" + "="*50)
    print("네이버 로그인 페이지에서 로그인을 완료해주세요.")
    print("로그인 완료 후 Enter 키를 눌러주세요...")
    print("="*50)
    
    try:
        input()
    except EOFError:
        time.sleep(30)  # 자동화 환경에서는 30초 대기
    
    # 로그인 완료 확인
    if check_login_status(driver):
        print("로그인 성공!")
        save_cookies(driver)
    else:
        print("로그인 확인에 실패했습니다. 수동으로 로그인을 완료해주세요.")
        print("계속하려면 Enter를 눌러주세요...")
        try:
            input()
        except EOFError:
            pass

# ===== 정보 추출 함수들 =====
def extract_store_name(driver) -> Optional[str]:
    """스토어명 추출 (og:title 메타태그에서)"""
    try:
        # 방법 1: og:title 메타 태그에서 추출
        try:
            og_title_element = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
            og_title = og_title_element.get_attribute('content')
            
            if og_title:
                # "헬로 유로 : 네이버 스마트스토어" → "헬로 유로"
                if ':' in og_title and '네이버 스마트스토어' in og_title:
                    store_name = og_title.split(':')[0].strip()
                    return store_name
                else:
                    return og_title
        except NoSuchElementException:
            pass
        
        # 방법 2: 페이지 title에서 추출
        try:
            page_title = driver.title
            if page_title and ':' in page_title and '네이버 스마트스토어' in page_title:
                store_name = page_title.split(':')[0].strip()
                return store_name
        except Exception:
            pass
        
        # 방법 3: 페이지 소스에서 직접 검색
        try:
            page_source = driver.page_source
            og_title_pattern = r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']'
            match = re.search(og_title_pattern, page_source)
            
            if match:
                og_title = match.group(1)
                if ':' in og_title and '네이버 스마트스토어' in og_title:
                    store_name = og_title.split(':')[0].strip()
                    return store_name
                else:
                    return og_title
        except Exception:
            pass
        
        return None
        
    except Exception as e:
        print(f"  스토어명 추출 오류: {e}")
        return None

def extract_interest_customers(driver) -> Optional[int]:
    """관심고객수 추출"""
    try:
        # 관심고객수 영역 찾기
        element = driver.find_element(By.CSS_SELECTOR, SEL_INTEREST_CUSTOMERS)
        text = element.text
        
        # 정규식으로 숫자 추출
        match = CUSTOMER_COUNT_RE.search(text)
        if match:
            count_str = match.group(1).replace(',', '')
            return int(count_str)
        
        # 대안: 숫자만 추출
        numbers = re.findall(r'\d{1,3}(?:,\d{3})*', text)
        if numbers:
            return int(numbers[0].replace(',', ''))
            
    except NoSuchElementException:
        print("  관심고객수 영역을 찾을 수 없음")
    except Exception as e:
        print(f"  관심고객수 추출 오류: {e}")
    
    return None

def extract_store_grade_info(driver, store_url: str) -> Dict[str, str]:
    """스토어 등급 및 서비스 만족도 정보 추출"""
    result = {
        'store_grade': '',
        'service_satisfaction': '',
        'total_visitors': ''
    }
    
    try:
        # 1. 스토어 등급 추출
        try:
            store_grade_elements = driver.find_elements(By.XPATH, "//span[contains(text(), '스토어등급')]/following-sibling::span[@class='s3DfSDQGcc']")
            if store_grade_elements:
                result['store_grade'] = store_grade_elements[0].text.strip()
        except Exception:
            pass
        
        # 2. 서비스 만족도 추출
        try:
            # 방법 1: "서비스만족" 라벨 다음의 값 추출
            service_elements = driver.find_elements(By.XPATH, "//span[contains(text(), '서비스만족')]/following-sibling::span[@class='s3DfSDQGcc']")
            if service_elements:
                service_value = service_elements[0].text.strip()
                if service_value == '굿서비스':
                    # "굿서비스"를 세 항목으로 변환
                    result['service_satisfaction'] = '구매만족/빠른배송/문의응답'
                elif service_value:
                    result['service_satisfaction'] = service_value
            
            # 방법 2: CSS 셀렉터로 직접 찾기 (백업)
            if not result['service_satisfaction']:
                page_source = driver.page_source
                if '서비스만족' in page_source:
                    service_value_elements = driver.find_elements(By.CSS_SELECTOR, '.s3DfSDQGcc')
                    for element in service_value_elements:
                        text = element.text.strip()
                        if text == '굿서비스':
                            result['service_satisfaction'] = '구매만족/빠른배송/문의응답'
                            break
                        elif text and ('서비스' in text or '만족' in text):
                            result['service_satisfaction'] = text
                            break
            
        except Exception:
            pass
        
        # 3. 전체 방문자수 추출
        try:
            visitor_elements = driver.find_elements(By.XPATH, "//span[contains(text(), '전체')]/em[@class='nPVtFhyWhf']")
            if visitor_elements:
                visitor_text = visitor_elements[0].text.strip().replace(',', '')
                if visitor_text.isdigit():
                    result['total_visitors'] = int(visitor_text)
        except Exception:
            pass
        
        return result
        
    except Exception as e:
        print(f"    스토어 등급/서비스 정보 추출 오류: {e}")
        return result

def extract_total_products_count(driver, store_url: str) -> Optional[int]:
    """전체 상품수 추출 (category 페이지에서)"""
    try:
        # 스토어 ID 추출
        if '/store/' in store_url:
            store_id = store_url.split('/store/')[1].split('/')[0]
        elif '/' in store_url.split('smartstore.naver.com/')[-1]:
            store_id = store_url.split('smartstore.naver.com/')[-1].split('/')[0]
        else:
            store_id = store_url.split('smartstore.naver.com/')[-1]
        
        # 카테고리 페이지 URL
        category_url = f"https://smartstore.naver.com/{store_id}/category/ALL?st=POPULAR&dt=BIG_IMAGE&page=1&size=40"
        
        print(f"  전체 상품수 확인: {category_url}")
        driver.get(category_url)
        
        # 페이지 로드 대기
        WebDriverWait(driver, PAGE_WAIT).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        
        time.sleep(2)
        
        # 방법 1: CSS 셀렉터로 찾기
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, ".eJTgO8xT6T, .total-count, .product-count")
            for element in elements:
                text = element.text
                if '총' in text and '개' in text:
                    match = TOTAL_PRODUCTS_RE.search(element.get_attribute('innerHTML'))
                    if not match:
                        match = TOTAL_PRODUCTS_ALT_RE.search(text)
                    
                    if match:
                        count_str = match.group(1).replace(',', '')
                        total_count = int(count_str)
                        print(f"  → 전체 상품수: {total_count:,}개")
                        return total_count
        except Exception as e:
            print(f"  CSS 셀렉터 방법 실패: {e}")
        
        # 방법 2: 페이지 소스에서 검색
        try:
            page_source = driver.page_source
            match = TOTAL_PRODUCTS_RE.search(page_source)
            if not match:
                match = TOTAL_PRODUCTS_ALT_RE.search(page_source)
            
            if match:
                count_str = match.group(1).replace(',', '')
                total_count = int(count_str)
                print(f"  → 전체 상품수: {total_count:,}개")
                return total_count
        except Exception as e:
            print(f"  페이지 소스 검색 실패: {e}")
        
        # 방법 3: 다양한 패턴으로 검색
        try:
            patterns = [
                r'총\s*(\d{1,3}(?:,\d{3})*)\s*개',
                r'전체\s*(\d{1,3}(?:,\d{3})*)\s*개',
                r'(\d{1,3}(?:,\d{3})*)\s*개\s*상품',
                r'상품\s*(\d{1,3}(?:,\d{3})*)\s*개'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, page_source)
                if matches:
                    counts = [int(match.replace(',', '')) for match in matches]
                    max_count = max(counts)
                    if max_count > 0:
                        print(f"  → 전체 상품수 (추정): {max_count:,}개")
                        return max_count
        except Exception as e:
            print(f"  다양한 패턴 검색 실패: {e}")
        
        print("  → 전체 상품수를 찾을 수 없음")
        return None
        
    except Exception as e:
        print(f"  전체 상품수 추출 오류: {e}")
        return None

def scroll_to_bottom(driver):
    """페이지 하단으로 스크롤"""
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
    except Exception:
        pass

def extract_seller_info(driver) -> Dict[str, str]:
    """판매자 정보 추출 (연락처 등)"""
    info = {
        '업체명': '',
        '대표자명': '',
        '전화번호': '',
        '이메일': '',
        '사업자등록번호': '',
        '주소': ''
    }
    
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            if retry_count > 0:
                print(f"  재시도 {retry_count}회차...")
            
            # 페이지 하단으로 스크롤
            scroll_to_bottom(driver)
            
            # 판매자 상세정보 버튼 찾기
            try:
                seller_btn = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, SEL_SELLER_INFO_BTN))
                )
            except TimeoutException:
                print("  판매자 상세정보 버튼을 찾을 수 없음")
                return info
            
            # 현재 창 핸들 저장
            main_window = driver.current_window_handle
            initial_windows = driver.window_handles[:]
            
            # 버튼 클릭
            try:
                seller_btn.click()
            except WebDriverException:
                driver.execute_script("arguments[0].click();", seller_btn)
            
            time.sleep(1)
            
            # 새 창 확인
            try:
                WebDriverWait(driver, 5).until(lambda d: len(d.window_handles) > len(initial_windows))
                new_windows = [w for w in driver.window_handles if w not in initial_windows]
                if new_windows:
                    driver.switch_to.window(new_windows[0])
                    print("  새 창에서 판매자 정보 추출 중...")
            except TimeoutException:
                print("  레이어 팝업에서 판매자 정보 추출 중...")
            
            # 판매자 정보 패널 대기
            try:
                panel = WebDriverWait(driver, 300).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, SEL_SELLER_PANEL))
                )
                
                panel_text = panel.text
                print(f"  판매자 정보 패널 텍스트:\n{panel_text[:200]}...")
                
                # 정보 추출
                lines = [line.strip() for line in panel_text.split('\n') if line.strip()]
                
                i = 0
                while i < len(lines):
                    line = lines[i]
                    
                    if '상호명' in line:
                        if i + 1 < len(lines) and '대표자' not in lines[i + 1]:
                            info['업체명'] = lines[i + 1]
                        elif len(line.split()) > 1:
                            info['업체명'] = line.replace('상호명', '').strip()
                    
                    elif '대표자' in line:
                        if i + 1 < len(lines) and '고객센터' not in lines[i + 1]:
                            info['대표자명'] = lines[i + 1]
                        elif len(line.split()) > 1:
                            info['대표자명'] = line.replace('대표자', '').strip()
                    
                    elif '고객센터' in line:
                        phone_line = line.replace('고객센터', '').replace('인증', '').replace('잘못된 번호 신고', '').strip()
                        phone_match = PHONE_RE.search(phone_line)
                        if phone_match:
                            info['전화번호'] = phone_match.group()
                    
                    elif '사업자등록번호' in line:
                        if i + 1 < len(lines) and not any(keyword in lines[i + 1] for keyword in ['사업장', '통신판매업', 'e-mail']):
                            potential_number = lines[i + 1].strip()
                            if re.match(r'^[\d-]+$', potential_number) and len(potential_number.replace('-', '')) >= 10:
                                info['사업자등록번호'] = potential_number
                        else:
                            number = line.replace('사업자등록번호', '').strip()
                            if number and re.match(r'^[\d-]+$', number) and len(number.replace('-', '')) >= 10:
                                info['사업자등록번호'] = number
                    
                    elif '사업장 소재지' in line:
                        if i + 1 < len(lines) and not any(keyword in lines[i + 1] for keyword in ['통신판매업번호', 'e-mail', '상호명', '대표자']):
                            address_parts = [lines[i + 1]]
                            j = i + 2
                            while j < len(lines) and not any(keyword in lines[j] for keyword in ['통신판매업번호', 'e-mail', '상호명', '대표자']):
                                address_parts.append(lines[j])
                                j += 1
                            info['주소'] = ' '.join(address_parts).strip()
                        else:
                            address = line.replace('사업장 소재지', '').strip()
                            if address:
                                info['주소'] = address
                    
                    elif 'e-mail' in line.lower():
                        email_match = EMAIL_RE.search(line)
                        if email_match:
                            info['이메일'] = email_match.group()
                    
                    i += 1
                
                # 추가 정리
                if not info['전화번호']:
                    phone_match = PHONE_RE.search(panel_text)
                    if phone_match:
                        info['전화번호'] = phone_match.group()
                
                if not info['이메일']:
                    email_match = EMAIL_RE.search(panel_text)
                    if email_match:
                        info['이메일'] = email_match.group()
                
                print("  → 판매자 정보 추출 성공!")
                break
                        
            except TimeoutException:
                print("  판매자 정보 패널을 찾을 수 없음")
                current_windows = driver.window_handles
                if len(current_windows) > len(initial_windows):
                    driver.close()
                    driver.switch_to.window(main_window)
                
                retry_count += 1
                print(f"  정보 패널 로드 실패, {max_retries - retry_count}회 재시도 남음...")
                time.sleep(1)
                continue
            
            # 새 창 닫기
            current_windows = driver.window_handles
            if len(current_windows) > len(initial_windows):
                driver.close()
                driver.switch_to.window(main_window)
            
            break
            
        except Exception as e:
            print(f"  판매자 정보 추출 오류: {e}")
            try:
                driver.switch_to.window(main_window)
            except:
                pass
            
            retry_count += 1
            if retry_count < max_retries:
                print(f"  예외 발생으로 재시도, {max_retries - retry_count}회 재시도 남음...")
                time.sleep(1)
            else:
                print("  최대 재시도 횟수 초과")
                break
    
    if retry_count >= max_retries:
        print(f"  판매자 정보 추출 실패: 최대 재시도 횟수({max_retries}) 초과")
    
    return info

# ===== 통합 데이터 추출 함수 =====
def extract_store_data(driver, store_url: str, analyzer=None) -> Dict:
    """스토어 종합 데이터 추출"""
    result = {
        '스토어URL': store_url,
        '처리상태': '',
        '스토어명': '',
        '관심고객수': None,
        '스토어등급': '',
        '서비스만족': '',
        '전체방문자수': '',
        '업체명': '',
        '대표자명': '',
        '전화번호': '',
        '이메일': '',
        '사업자등록번호': '',
        '주소': '',
        '전체상품수': '',
        '표본상품수': '',
        '스토어분석': '',
        '분석오류': '',
        '오류메시지': ''
    }
    
    try:
        print(f"접속 중: {store_url}")
        driver.get(store_url)
        
        # 페이지 로드 대기
        WebDriverWait(driver, PAGE_WAIT).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        
        # 스마트스토어 확인
        if 'smartstore.naver.com' not in store_url:
            result['처리상태'] = '스마트스토어 아님'
            result['오류메시지'] = 'URL이 스마트스토어가 아닙니다'
            return result
        
        # 1. 스토어명 추출
        print("  스토어명 추출 중...")
        store_name = extract_store_name(driver)
        if store_name:
            result['스토어명'] = store_name
            print(f"  → 스토어명: {store_name}")
        else:
            print("  → 스토어명: 추출 실패")
        
        # 2. 관심고객수 추출
        print("  관심고객수 추출 중...")
        interest_count = extract_interest_customers(driver)
        if interest_count is not None:
            result['관심고객수'] = interest_count
            print(f"  → 관심고객수: {interest_count:,}명")
        else:
            print("  → 관심고객수: 추출 실패")
        
        # 3. 스토어 등급/서비스 정보 추출
        print("  스토어 등급/서비스 정보 추출 중...")
        grade_info = extract_store_grade_info(driver, store_url)
        
        if grade_info['store_grade']:
            result['스토어등급'] = grade_info['store_grade']
            print(f"  → 스토어등급: {grade_info['store_grade']}")
        
        if grade_info['service_satisfaction']:
            result['서비스만족'] = grade_info['service_satisfaction']
            print(f"  → 서비스만족: {grade_info['service_satisfaction']}")
        
        if grade_info['total_visitors']:
            result['전체방문자수'] = grade_info['total_visitors']
            print(f"  → 전체방문자수: {grade_info['total_visitors']:,}")
        
        # 4. 판매자 정보 추출
        print("  판매자 정보 추출 중...")
        seller_info = extract_seller_info(driver)
        
        # 결과 업데이트
        result.update(seller_info)
        
        print(f"  → 업체명: {seller_info.get('업체명', 'N/A')}")
        print(f"  → 전화번호: {seller_info.get('전화번호', 'N/A')}")
        print(f"  → 이메일: {seller_info.get('이메일', 'N/A')}")
        
        # 5. 전체 상품수 추출 및 스토어 분석 (상품 페이지에서 한 번에)
        print("  상품 관련 정보 추출 중...")
        total_products = extract_total_products_count(driver, store_url)
        if total_products is not None:
            result['전체상품수'] = total_products
            print(f"  → 전체 상품수: {total_products:,}개")
        else:
            print("  → 전체 상품수: 추출 실패")
        
        # 6. 스토어 특성 분석 (이미 상품 페이지에 있으므로 복귀 없이 진행)
        if analyzer and ENABLE_STORE_ANALYSIS:
            try:
                print("  스토어 분석 중...")
                store_name_for_analysis = seller_info.get('업체명', '') or result.get('스토어명', '')
                analysis_result = analyzer.get_store_analysis(driver, store_url, store_name_for_analysis)
                
                result['표본상품수'] = analysis_result.get('products_count', '')
                result['스토어분석'] = analysis_result.get('analysis', '')
                result['분석오류'] = analysis_result.get('error', '')
                
                if analysis_result.get('analysis'):
                    print(f"  → 표본상품수: {analysis_result.get('products_count', 'N/A')}")
                    print(f"  → 분석 완료")
                else:
                    print(f"  → 분석 실패: {analysis_result.get('error', 'Unknown')}")
                
            except Exception as e:
                print(f"  스토어 분석 중 오류: {e}")
                result['분석오류'] = str(e)
        
        # 처리 상태 설정
        if interest_count is not None or total_products is not None or store_name or grade_info['store_grade'] or any(seller_info.values()):
            result['처리상태'] = '정상'
        else:
            result['처리상태'] = '정보 없음'
            
    except WebDriverException as e:
        result['처리상태'] = '접속 오류'
        result['오류메시지'] = str(e)
        print(f"  접속 오류: {e}")
    except Exception as e:
        result['처리상태'] = '처리 오류'
        result['오류메시지'] = str(e)
        print(f"  처리 오류: {e}")
    
    return result

# ===== 파일 및 데이터 관리 =====
def load_store_urls(file_path: str) -> List[str]:
    """스토어 URL 목록 로드"""
    urls = []
    
    if file_path.endswith('.txt'):
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url and url.startswith('http'):
                    urls.append(url)
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
        url_columns = ['url', 'URL', 'store_url', '스토어URL', 'smartstore_url']
        url_col = None
        for col in url_columns:
            if col in df.columns:
                url_col = col
                break
        
        if url_col:
            urls = df[url_col].dropna().astype(str).tolist()
        else:
            print(f"CSV 파일에서 URL 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {list(df.columns)}")
            return []
    
    # 중복 제거 및 스마트스토어만 필터링
    unique_urls = []
    for url in urls:
        if url not in unique_urls and 'smartstore.naver.com' in url:
            unique_urls.append(url)
    
    print(f"로드된 스마트스토어 URL 수: {len(unique_urls)}")
    return unique_urls

def load_existing_results(output_file: str) -> Dict[str, Dict]:
    """기존 결과 파일 로드"""
    existing_results = {}
    
    if os.path.exists(output_file):
        try:
            df = pd.read_csv(output_file)
            for _, row in df.iterrows():
                url = row.get('스토어URL', '') or row.get('url', '')
                if url:
                    existing_results[url] = row.to_dict()
            print(f"기존 결과 로드: {len(existing_results)}개")
        except Exception as e:
            print(f"기존 결과 파일 로드 실패: {e}")
    
    return existing_results

def filter_unprocessed_urls(urls: List[str], existing_results: Dict[str, Dict], enable_analysis: bool = False) -> List[str]:
    """미처리 URL 필터링"""
    unprocessed = []
    
    for url in urls:
        if url in existing_results:
            status = existing_results[url].get('처리상태', '') or existing_results[url].get('status', '')
            basic_completed = (status == '정상')
            
            if enable_analysis:
                analysis_completed = bool(existing_results[url].get('스토어분석', '') or existing_results[url].get('store_analysis', ''))
                if basic_completed and analysis_completed:
                    continue
            else:
                if basic_completed:
                    continue
        
        unprocessed.append(url)
    
    return unprocessed

def save_single_result(result: Dict, output_file: str, existing_results: Dict[str, Dict]):
    """단일 결과를 CSV에 저장"""
    url = result['스토어URL']
    existing_results[url] = result
    
    # 데이터프레임 생성
    df = pd.DataFrame(list(existing_results.values()))
    
    # 컬럼 순서 (최종 요구사항 순서)
    final_columns = [
        '스토어URL', '처리상태', '스토어명', '관심고객수', '전체방문자수', '스토어등급', '서비스만족',
        '업체명', '대표자명', '전화번호', '이메일', '사업자등록번호', '주소',
        '전체상품수', '표본상품수', '스토어분석', '분석오류', '오류메시지'
    ]
    
    # 존재하는 컬럼만 선택
    available_columns = [col for col in final_columns if col in df.columns]
    df = df[available_columns]
    
    # CSV 저장
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  저장 완료: {output_file}")

# ===== 메인 실행 함수 =====
def main():
    parser = argparse.ArgumentParser(description='네이버 스마트스토어 종합 정보 추출 시스템')
    parser.add_argument('input_file', help='스토어 URL 목록 파일 (.txt 또는 .csv)')
    parser.add_argument('output_file', help='결과 저장 파일 (.csv)')
    parser.add_argument('--headless', action='store_true', help='헤드리스 모드 (브라우저 창 숨김)')
    parser.add_argument('--start', type=int, default=0, help='시작 인덱스')
    parser.add_argument('--limit', type=int, help='처리할 최대 개수')
    parser.add_argument('--force', action='store_true', help='기존 결과 무시하고 전체 재처리')
    parser.add_argument('--no-analysis', action='store_true', help='스토어 분석 기능 비활성화')
    
    args = parser.parse_args()
    
    # 스토어 분석 기능 설정
    enable_analysis = STORE_ANALYZER_AVAILABLE and not args.no_analysis
    if enable_analysis:
        analyzer = create_analyzer()
        print("✓ 스토어 분석 기능 활성화")
    else:
        analyzer = None
        print("- 스토어 분석 기능 비활성화")
    
    if not os.path.exists(args.input_file):
        print(f"입력 파일을 찾을 수 없습니다: {args.input_file}")
        return
    
    # URL 로드
    all_urls = load_store_urls(args.input_file)
    if not all_urls:
        print("처리할 URL이 없습니다.")
        return
    
    # 기존 결과 로드 (force 옵션이 아닌 경우)
    existing_results = {}
    if not args.force:
        existing_results = load_existing_results(args.output_file)
        urls = filter_unprocessed_urls(all_urls, existing_results, enable_analysis)
        skipped_count = len(all_urls) - len(urls)
        if skipped_count > 0:
            print(f"이미 처리된 URL {skipped_count}개 건너뜀")
    else:
        urls = all_urls
        print("강제 모드: 모든 URL 재처리")
    
    # 처리 범위 설정
    if args.start > 0:
        urls = urls[args.start:]
    if args.limit:
        urls = urls[:args.limit]
    
    if not urls:
        print("처리할 새로운 URL이 없습니다.")
        return
    
    print(f"처리할 URL 수: {len(urls)}")
    
    # Chrome 드라이버 생성
    driver = make_driver(headless=args.headless)
    
    try:
        # 로그인 처리
        ensure_login(driver)
        
        processed_count = 0
        
        # URL 처리 루프
        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] 처리 중...")
            
            # 스토어 데이터 추출
            result = extract_store_data(driver, url, analyzer)
            
            # 즉시 CSV 저장
            save_single_result(result, args.output_file, existing_results)
            
            processed_count += 1
            
            # 진행 상황 출력
            status = result['처리상태']
            if status == '정상':
                info_parts = []
                if result.get('스토어명'):
                    info_parts.append(f"스토어:{result['스토어명']}")
                if result.get('스토어등급'):
                    info_parts.append(f"{result['스토어등급']}")
                if result.get('전체상품수'):
                    info_parts.append(f"상품{result['전체상품수']:,}개")
                if result.get('스토어분석'):
                    info_parts.append("분석완료")
                
                info_str = " | ".join(info_parts)
                print(f"  ✓ 성공: {result.get('업체명', 'N/A')} | {info_str}")
            else:
                print(f"  ✗ 실패: {status}")
            
            # 요청 간격 (서버 부하 방지)
            time.sleep(2)
        
        # 최종 통계 출력
        print(f"\n=== 처리 완료 ===")
        print(f"이번 세션 처리: {processed_count}개")
        
        # 전체 통계
        total_results = len(existing_results)
        success_results = len([r for r in existing_results.values() 
                             if r.get('처리상태') == '정상' or r.get('status') == '정상'])
        
        print(f"전체 결과: {total_results}개")
        print(f"성공: {success_results}개")
        print(f"실패/미처리: {total_results - success_results}개")
        print(f"결과 파일: {args.output_file}")
        
        # 각 항목별 수집 통계
        stat_items = [
            ('스토어명', 'store_name'),
            ('전체상품수', 'total_products'),
            ('스토어등급', 'store_grade'),
            ('서비스만족', 'service'),
            ('전체방문자수', 'visitors')
        ]
        
        for korean_name, eng_name in stat_items:
            count = len([r for r in existing_results.values() 
                        if r.get(korean_name) and r.get(korean_name) != '' and r.get(korean_name) != 0])
            print(f"{korean_name} 수집: {count}개")
        
        # 분석 통계
        if enable_analysis:
            analyzed_count = len([r for r in existing_results.values() 
                                if (r.get('스토어분석') and r.get('스토어분석') != '분석 실패') or 
                                   (r.get('store_analysis') and r.get('store_analysis') != '분석 실패')])
            print(f"스토어 분석 완료: {analyzed_count}개")
        
    except KeyboardInterrupt:
        print(f"\n사용자 중단. 현재까지 처리된 결과가 저장되었습니다.")
        print(f"결과 파일: {args.output_file}")
    except Exception as e:
        print(f"\n오류 발생: {e}")
        print(f"현재까지 처리된 결과가 저장되었습니다.")
        print(f"결과 파일: {args.output_file}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()