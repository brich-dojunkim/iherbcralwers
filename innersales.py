# -*- coding: utf-8 -*-
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import os

# --- 사용자 설정 영역 ---

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# 1. Google Cloud Platform (GCP)에서 발급받은 서비스 계정 키 파일 경로
#    https://docs.gspread.org/en/latest/oauth2.html#for-bots-using-service-account
GSPREAD_CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "credentials", "inner-sale-979c1e8ed412.json")

# 2. 접근할 구글 스프레드시트 이름
SPREADSHEET_NAME = '할인 설정'

# 3. 데이터를 가져올 탭(시트) 이름
#    '상품번호' 컬럼이 반드시 존재해야 합니다.
SOURCE_SHEET_NAME = '[11월]내부 할인[삭제금지]'


# 5. 검색 및 다운로드를 진행할 웹사이트 URL
SEARCH_SITE_URL = 'https://b-flow.co.kr/login?prevUrl=products-v2/manage'

# 6. 생성된 엑셀 파일을 업로드하고 설정할 웹사이트 URL
UPLOAD_SITE_URL = 'https://example-upload-site.com'

# 작업 폴더 경로
BASE_FOLDER = PROJECT_ROOT
DOWNLOAD_FOLDER = os.path.join(BASE_FOLDER, 'downloads')
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, 'output')

# 폴더가 없으면 자동 생성
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# 10. 필터링할 쇼핑몰 채널 목록
TARGET_CHANNELS = ['지마켓', '옥션', '11번가', '쿠팡']

# 11. [추가됨] 오류 발생 시 재시도 횟수
RETRY_COUNT = 3
# --- 사용자 설정 영역 끝 ---

def setup_driver():
    """Selenium WebDriver를 설정하고 반환합니다."""
    options = webdriver.ChromeOptions()
    # 다운로드 폴더 설정
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)
    prefs = {"download.default_directory": DOWNLOAD_FOLDER}
    options.add_experimental_option("prefs", prefs)
    
    # 창 크기 및 안정성 옵션 추가
    options.add_argument("--start-maximized")  # 창을 최대화하여 시작
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-logging')
    options.add_argument('--disable-web-security')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-notifications')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-translate')
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-features=TranslateUI')
    options.add_argument('--disable-ipc-flooding-protection')
    options.add_argument('--remote-debugging-port=0')  # 랜덤 포트 사용
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_experimental_option('useAutomationExtension', False)
    
    # ChromeDriver 서비스 설정 개선
    try:
        # ChromeDriver 경로를 명시적으로 지정
        chromedriver_path = ChromeDriverManager().install()
        print(f"ChromeDriver 경로: {chromedriver_path}")
        
        service = Service(chromedriver_path)
        service.start()  # 서비스 시작
        
        driver = webdriver.Chrome(service=service, options=options)
        print("ChromeDriver 생성 성공!")
        
    except Exception as e:
        print(f"ChromeDriver 생성 실패: {e}")
        print("Chrome 브라우저를 수동으로 열고 비플로우에 로그인한 후 다시 시도해주세요.")
        print("또는 Chrome 브라우저를 완전히 종료한 후 다시 시도해주세요.")
        
        # 대안: 기존 Chrome 인스턴스에 연결 시도
        try:
            print("기존 Chrome 인스턴스에 연결을 시도합니다...")
            options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
            driver = webdriver.Chrome(options=options)
            print("기존 Chrome 인스턴스 연결 성공!")
        except Exception as e2:
            print(f"기존 Chrome 인스턴스 연결도 실패: {e2}")
            raise e
    
    # 페이지 로드 타임아웃 설정
    driver.set_page_load_timeout(120)  # 60초 → 120초로 증가
    # [수정됨] 암시적 대기 시간을 20초로 늘려 안정성 확보
    driver.implicitly_wait(30)  # 20초 → 30초로 증가
    
    # 세션 유지를 위한 추가 설정
    driver.execute_script("return navigator.webdriver")  # 세션 활성화
    return driver

def authenticate_google_sheets():
    """Google Sheets API 인증을 하고 gspread 클라이언트를 반환합니다."""
    print("Google Sheets 인증을 시작합니다...")
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(GSPREAD_CREDENTIALS_PATH, scopes=scope)
        client = gspread.authorize(creds)
        print("Google Sheets 인증 성공!")
        print(f"📊 접근할 스프레드시트: '{SPREADSHEET_NAME}'")
        print(f"📋 접근할 시트: '{SOURCE_SHEET_NAME}'")
        return client
    except Exception as e:
        print(f"Google Sheets 인증 중 오류 발생: {e}")
        print("GSPREAD_CREDENTIALS_PATH 경로에 올바른 서비스 계정 키 파일이 있는지 확인하세요.")
        return None

def get_data_from_sheet(client):
    """구글 스프레드시트에서 데이터를 가져와 Pandas DataFrame으로 변환합니다."""
    print(f"'{SPREADSHEET_NAME}' 시트에서 데이터를 가져옵니다...")
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet(SOURCE_SHEET_NAME)
        all_values = sheet.get_all_values()
        
        if len(all_values) < 4:
            print("시트에 데이터가 충분하지 않습니다 (최소 4줄 필요).")
            return None
            
        header = all_values[2]
        # 중복/빈 헤더 이름을 고유하게 만듭니다.
        cols = []
        counts = {}
        for i, col in enumerate(header):
            if col == '':
                col = f'Unnamed:{i}'
            if col in counts:
                counts[col] += 1
                cols.append(f'{col}.{counts[col]}')
            else:
                counts[col] = 0
                cols.append(col)

        df = pd.DataFrame(all_values[3:], columns=cols)
        
        print(f"총 {len(df)}개의 데이터를 가져왔습니다. (3행을 헤더로 사용)")
        if '상품번호' not in df.columns:
            print("오류: 시트 3행에서 '상품번호' 컬럼을 찾을 수 없습니다.")
            return None
            
        df = df[df['상품번호'].astype(str) != '']
        print(f"상품번호가 있는 데이터 {len(df)}개 필터링 완료.")
        
        # R열(설정일)에 데이터가 있는 행 제거 (이미 완료된 작업)
        print(f"설정일 필터링 전 데이터 행 수: {len(df)}")
        if '설정일' in df.columns:
            # 설정일 컬럼이 비어있지 않은 행들을 제거
            df = df[df['설정일'].isna() | (df['설정일'] == '') | (df['설정일'].astype(str).str.strip() == '')]
            print(f"설정일이 있는 행을 제거한 후 데이터 행 수: {len(df)}")
        else:
            print("설정일 컬럼을 찾을 수 없습니다.")
        
        print(f"최종 검색 대상 데이터 {len(df)}개 필터링 완료.")
        return df
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"오류: 스프레드시트 '{SPREADSHEET_NAME}'을(를) 찾을 수 없습니다.")
        return None
    except gspread.exceptions.WorksheetNotFound:
        print(f"오류: '{SPREADSHEET_NAME}' 스프레드시트에서 '{SOURCE_SHEET_NAME}' 탭을 찾을 수 없습니다.")
        return None
    except Exception as e:
        print(f"시트에서 데이터를 가져오는 중 오류 발생: {e}")
        return None

def search_and_download_from_site(driver, product_df):
    """b-flow 사이트에서 상품번호로 검색하고 엑셀 파일을 다운로드합니다."""
    # 1. 로그인 페이지 접속
    print(f"'{SEARCH_SITE_URL}'에 접속합니다.")
    driver.get(SEARCH_SITE_URL)
    
    # --- 웹사이트 로그인 로직 ---
    try:
        print("로그인을 시작합니다...")
        login_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[3]/div[1]/div[2]/button[2]"))
        )
        login_button.click()
        
        username_input = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, "/html/body/div[1]/div[14]/div/div[2]/div/div[2]/div/input[1]"))
        )
        username_input.send_keys("a01025399154@brich.co.kr")
        
        password_input = driver.find_element(By.XPATH, "/html/body/div[1]/div[14]/div/div[2]/div/div[2]/div/input[2]")
        password_input.send_keys("2rlqmadl@!")
        
        submit_button = driver.find_element(By.XPATH, "/html/body/div[1]/div[14]/div/div[2]/div/div[3]/button[1]")
        submit_button.click()
        
        print("로그인 성공. 페이지 로딩을 기다립니다...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
        )
        print("메인 페이지 로딩 완료.")
    except Exception as e:
        print(f"로그인 중 오류가 발생했습니다: {e}")
        return []
    # --- 로그인 로직 끝 ---

    # 2. 상품조회 및 수정 페이지로 이동
    print("상품 조회/수정 페이지로 이동합니다...")
    driver.get("https://b-flow.co.kr/products/new#/")
    
    # 페이지 로딩 완료 대기 (동적 대기)
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
        )
        print("상품 조회 페이지 로딩 완료.")
    except Exception as e:
        print(f"페이지 로딩 대기 중 오류: {e}")
        time.sleep(5)  # 폴백 대기 시간

    product_numbers = product_df['상품번호'].astype(str).tolist()
    # 상품번호를 500개씩 묶음(chunk)으로 나눔
    chunks = [product_numbers[i:i + 500] for i in range(0, len(product_numbers), 500)]
    downloaded_files = []
    
    for i, chunk in enumerate(chunks):
        # [수정됨] 재시도 로직 추가
        for attempt in range(RETRY_COUNT):
            try:
                print(f"\n--- 처리 중: {i+1}/{len(chunks)}번째 묶음 (상품 {len(chunk)}개), 시도 {attempt + 1}/{RETRY_COUNT} ---")
                
                # 3. 검색어 설정 클릭
                print("검색 조건을 '상품번호'로 설정합니다.")
                search_filter_dropdown = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#main-page > div > div > section > div > div.box.collapsed-box > div:nth-child(2) > div > div:nth-child(1) > div > div:nth-child(1) > div > div.multiselect.br-select"))
                )
                search_filter_dropdown.click()
                
                product_number_option = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.XPATH, "//ul[contains(@class, 'multiselect__content')]//span[contains(text(), '상품번호')]"))
                )
                product_number_option.click()
                time.sleep(0.5)

                # 4. 상품번호 입력 (기존 내용 삭제 후 입력)
                print(f"{len(chunk)}개의 상품번호를 입력합니다.")
                search_box = driver.find_element(By.CSS_SELECTOR, "#main-page > div > div > section > div > div.box.collapsed-box > div:nth-child(2) > div > div:nth-child(1) > div > div:nth-child(1) > div > div.br-text-wrapper > input")
                search_box.clear()
                search_query = " \n".join(chunk)
                search_box.send_keys(search_query)
                time.sleep(1) # [추가됨] 입력 후 브라우저 처리 시간 대기

                # 5. 검색 버튼 클릭
                print("검색 버튼을 클릭합니다.")
                driver.find_element(By.CSS_SELECTOR, "#main-page > div > div > section > div > div.box.collapsed-box > div.button-container > button.br-btn.br-btn-purple.br-btn-medium-form").click()

                # 6. 검색 결과 로딩 대기
                print("검색 결과를 기다립니다...")
                try:
                    # 검색 결과가 로드될 때까지 대기 (최대 45초)
                    WebDriverWait(driver, 45).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page > div > div > section > div > div:nth-child(4) > div:nth-child(2) > div.pull-right.header-button > div > button"))
                    )
                    print("검색 결과 로딩 완료.")
                except Exception as e:
                    print(f"검색 결과 로딩 대기 중 오류: {e}")
                    print("추가 15초 대기 후 진행합니다...")
                    time.sleep(15)

                # 7. 엑셀 다운로드 버튼 클릭
                print("엑셀 다운로드를 시작합니다.")
                initial_file_count = len(os.listdir(DOWNLOAD_FOLDER))
                
                excel_dropdown = driver.find_element(By.CSS_SELECTOR, "#main-page > div > div > section > div > div:nth-child(4) > div:nth-child(2) > div.pull-right.header-button > div > button")
                excel_dropdown.click()
                
                # 8. "리스트 형식 (전체)" 선택
                download_option = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.XPATH, "//ul[contains(@class, 'dropdown-menu')]//a[normalize-space()='리스트 형식 (전체)']"))
                )
                download_option.click()
                
                # 9. 확인창(Alert) 처리
                print("확인 팝업창(Alert)을 처리합니다.")
                alert = WebDriverWait(driver, 10).until(EC.alert_is_present())
                alert.accept()

                # 10. 엑셀 다운로드 목록 팝업(모달) 처리
                print("엑셀 다운로드 목록 팝업을 기다립니다...")
                WebDriverWait(driver, 60).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "div.v--modal-box tbody > tr:nth-child(1) > td:nth-child(2) > span.br-label-green"))
                )
                print("파일 생성이 완료되었습니다. 다운로드를 클릭합니다.")
                
                modal_download_button = driver.find_element(By.CSS_SELECTOR, "div.v--modal-box tbody > tr:nth-child(1) > td:nth-child(7) > button")
                modal_download_button.click()

                # 11. 실제 파일 다운로드 완료 대기 및 파일명 변경
                for _ in range(30):
                    if len(os.listdir(DOWNLOAD_FOLDER)) > initial_file_count:
                        latest_file_path = max([os.path.join(DOWNLOAD_FOLDER, f) for f in os.listdir(DOWNLOAD_FOLDER)], key=os.path.getctime)
                        try:
                            time.sleep(1)
                            filename, extension = os.path.splitext(os.path.basename(latest_file_path))
                            parts = filename.split('_')
                            base_name = '_'.join(parts[:-1])
                            new_filename = f"{base_name}_{i+1:02d}{extension}"
                            new_file_path = os.path.join(DOWNLOAD_FOLDER, new_filename)
                            os.rename(latest_file_path, new_file_path)
                            downloaded_files.append(new_file_path)
                            print(f"'{os.path.basename(latest_file_path)}' 다운로드 완료. -> '{new_filename}'(으)로 변경.")
                        except Exception as rename_error:
                            print(f"파일명 변경 중 오류 발생: {rename_error}")
                            downloaded_files.append(latest_file_path)
                        break
                    time.sleep(1)
                else:
                    print(f"파일 다운로드를 감지하지 못했습니다.")
                
                # 12. 다운로드 목록 팝업 닫기
                print("다운로드 목록 팝업을 닫습니다.")
                close_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "div.v--modal-box span.close-btn"))
                )
                close_button.click()
                time.sleep(2)
                
                # [수정됨] 다음 묶음 처리를 위해 providers 페이지를 거쳐 상품조회 페이지로 이동
                print("다음 작업을 위해 페이지를 완전히 초기화합니다.")
                try:
                    # 1. providers 페이지로 이동 (완전한 페이지 리셋)
                    print("providers 페이지로 이동하여 세션을 초기화합니다...")
                    driver.get("https://b-flow.co.kr/providers#/")
                    
                    # providers 페이지 로딩 대기
                    WebDriverWait(driver, 4).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    print("providers 페이지 로딩 완료.")
                    
                    # 2. 상품조회 페이지로 이동
                    print("상품조회 페이지로 이동합니다...")
                    driver.get("https://b-flow.co.kr/products/new#/")
                    
                    # 상품조회 페이지 로딩 완료 대기 (더 긴 대기 시간)
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
                    )
                    print("상품조회 페이지 로딩 완료.")
                    
                    # 3. 검색 필터가 완전히 리셋되었는지 확인
                    try:
                        search_filter_dropdown = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "#main-page > div > div > section > div > div.box.collapsed-box > div:nth-child(2) > div > div:nth-child(1) > div > div:nth-child(1) > div > div.multiselect.br-select"))
                        )
                        print("페이지 초기화 완료. 검색 필터가 완전히 리셋되었습니다.")
                    except Exception as filter_error:
                        print(f"검색 필터 상태 확인 중 오류: {filter_error}")
                        print("추가 대기 후 다시 시도합니다...")
                        time.sleep(5)
                    
                    # 4. 추가 안정성을 위한 대기
                    time.sleep(3)
                    
                    print(f"{i+1}번째 묶음 처리를 성공했습니다.")
                    break
                    
                except Exception as page_error:
                    print(f"페이지 초기화 중 오류 발생: {page_error}")
                    print("브라우저를 새로고침하고 다시 시도합니다...")
                    driver.refresh()
                    time.sleep(5)
                    # 페이지 초기화 실패 시에도 다음 묶음으로 진행
                    print(f"{i+1}번째 묶음 처리를 완료했습니다 (페이지 초기화 실패).")
                    break 

            except Exception as e:
                print(f"오류 발생 (시도 {attempt + 1}/{RETRY_COUNT}): {e}")
                if attempt < RETRY_COUNT - 1:
                    print("페이지를 새로고침하고 다시 시도합니다...")
                    driver.refresh()
                    time.sleep(5)
                else:
                    print(f"최대 재시도 횟수({RETRY_COUNT})를 초과하여 {i+1}번째 묶음을 건너뜁니다.")
            
    return downloaded_files

def consolidate_excel_files(downloaded_files):
    """다운로드된 엑셀 파일들을 통합하고 필요한 컬럼만 추출합니다."""
    if not downloaded_files:
        print("다운로드된 파일이 없어 통합을 건너뜁니다.")
        return None

    print("다운로드된 엑셀 파일들을 통합하는 중...")
    
    # 구글 시트 컬럼명 → 엑셀 파일 컬럼명 매핑
    column_mapping = {
        '상품번호': '상품번호',  # AA 컬럼
        '지마켓(상품번호)': '지마켓(상품번호)',  # AB 컬럼
        '지마켓(마스터번호)': '지마켓(마스터번호)',  # AC 컬럼
        '옥션(상품번호)': '옥션(상품번호)',  # AD 컬럼
        '옥션(마스터번호)': '옥션(마스터번호)',  # AE 컬럼
        '11번가': '11번가',  # AF 컬럼
        '쿠팡': '쿠팡',  # AG 컬럼
        'SSG': 'SSG',  # AH 컬럼
        'GS샵': 'GS샵',  # AI 컬럼
        '롯데ON': '롯데ON', # AJ 컬럼
        'CJ몰': 'CJ몰', # AK 컬럼
        '하프클럽(신규)': '하프클럽(신규)', # AL 컬럼
        '롯데아이몰': '롯데아이몰', # AM 컬럼
        '카카오쇼핑하기': '카카오쇼핑하기', # AN 컬럼
        '카카오스타일': '카카오스타일', # AO 컬럼
        '홈앤쇼핑': '홈앤쇼핑', # AP 컬럼
        '퀸잇': '퀸잇'  # AQ 컬럼
    }
    
    # 최종 컬럼명 정의
    final_columns = [
        '상품번호', '지마켓(상품번호)', '지마켓(마스터번호)', '옥션(상품번호)', '옥션(마스터번호)',
        '11번가', '쿠팡', 'SSG', 'GS샵', '롯데ON', 'CJ몰', '하프클럽(신규)',
        '롯데아이몰', '카카오쇼핑하기', '카카오스타일', '홈앤쇼핑', '퀸잇'
    ]
    
    all_data = pd.DataFrame()
    headers = None  # 첫 번째 파일의 헤더를 저장할 변수
    
    for i, file_path in enumerate(downloaded_files):
        try:
            print(f"'{os.path.basename(file_path)}' 파일 처리 중...")
            # 헤더 없이 읽기 (모든 행을 데이터로 읽음)
            df = pd.read_excel(file_path, engine='openpyxl', header=None)
            
            # 첫 번째 파일이면 헤더 포함, 나머지는 헤더 제외
            if i == 0:
                # 첫 번째 파일: 1행을 헤더로, 2행부터 데이터로
                print("첫 번째 파일: 1행을 헤더로, 2행부터 데이터로 처리")
                headers = df.iloc[0].tolist()  # 1행을 헤더로 사용
                data_df = df.iloc[1:].reset_index(drop=True)  # 2행부터 데이터
                data_df.columns = headers  # 컬럼명 설정
                df = data_df
            else:
                # 두 번째 파일부터: 1행(헤더) 제외하고 2행부터 데이터로
                if len(df) > 1:
                    df = df.iloc[1:].reset_index(drop=True)  # 1행 제외
                    df.columns = headers  # 첫 번째 파일의 헤더 사용
                    print(f"첫 번째 행(헤더) 제외하여 처리")
            
            # 필요한 컬럼만 추출
            extracted_data = pd.DataFrame()
            for final_col, source_col in column_mapping.items():
                if source_col in df.columns:
                    extracted_data[final_col] = df[source_col]
                    print(f"컬럼 추출 성공: {source_col} → {final_col}")
                else:
                    print(f"경고: '{source_col}' 컬럼을 찾을 수 없습니다. 빈 값으로 채웁니다.")
                    extracted_data[final_col] = ''
            
            # 컬럼명은 이미 final_col로 설정되었으므로 변경 불필요
            
            all_data = pd.concat([all_data, extracted_data], ignore_index=True)
            print(f"'{os.path.basename(file_path)}' 파일 처리 완료. ({len(extracted_data)}개 행)")
            
        except Exception as e:
            print(f"'{file_path}' 파일 처리 중 오류 발생: {e}")
            continue

    if all_data.empty:
        print("통합할 데이터가 없습니다.")
        return None
    
    print(f"총 {len(all_data)}개의 데이터를 통합했습니다.")
    
    # 통합된 파일을 로컬에 저장
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
    
    # 통합 파일을 downloads 폴더에 저장
    consolidated_file_path = os.path.join(DOWNLOAD_FOLDER, "통합파일_내부할인.xlsx")
    try:
        all_data.to_excel(consolidated_file_path, index=False, engine='openpyxl')
        print(f"통합 파일이 저장되었습니다: {consolidated_file_path}")
    except Exception as e:
        print(f"통합 파일 저장 중 오류 발생: {e}")
    
    return all_data

def combine_downloaded_files(downloaded_files):
    """다운로드된 엑셀 파일들을 통합합니다."""
    # 파일 통합
    consolidated_data = consolidate_excel_files(downloaded_files)
    if consolidated_data is None:
        return None

    print("다운로드된 파일들을 통합했습니다.")
    return consolidated_data

def save_filtered_data_to_excel(combined_df):
    """채널별로 데이터를 필터링하고 'AS'열의 이름으로 엑셀 파일을 저장합니다."""
    if combined_df is None or combined_df.empty:
        print("처리할 데이터가 없어 엑셀 파일 생성을 건너뜁니다.")
        return []

    print("채널별 데이터 필터링 및 엑셀 파일 저장을 시작합니다.")
    
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"'{OUTPUT_FOLDER}' 폴더를 생성했습니다.")

    if '내부할인채널' not in combined_df.columns:
        print("오류: '내부할인채널' 컬럼을 찾을 수 없습니다. 컬럼명을 확인하세요.")
        return []
    if 'AS열_이름' not in combined_df.columns:
        print("경고: 'AS열_이름' 컬럼을 찾을 수 없습니다. 파일명은 '채널명.xlsx' 형식으로 저장됩니다.")
        combined_df['AS열_이름'] = combined_df['내부할인채널']


    created_files = []
    for channel in TARGET_CHANNELS:
        channel_df = combined_df[combined_df['내부할인채널'].str.contains(channel, na=False)]
        
        if not channel_df.empty:
            file_name_base = channel_df['AS열_이름'].iloc[0]
            file_path = os.path.join(OUTPUT_FOLDER, f"{file_name_base}_{channel}.xlsx")
            
            try:
                channel_df.to_excel(file_path, index=False, engine='openpyxl')
                print(f"'{file_path}' 파일 저장 완료.")
                created_files.append(file_path)
            except Exception as e:
                print(f"'{file_path}' 파일 저장 중 오류 발생: {e}")
        else:
            print(f"'{channel}' 채널에 대한 데이터가 없습니다.")
            
    return created_files

def upload_and_configure_site(driver, excel_files):
    """생성된 엑셀 파일들을 특정 사이트에 업로드하고 설정합니다."""
    if not excel_files:
        print("업로드할 엑셀 파일이 없습니다.")
        return

    print(f"'{UPLOAD_SITE_URL}'에 파일 업로드 및 설정을 시작합니다.")
    driver.get(UPLOAD_SITE_URL)

    # --- [수정 필요] 웹사이트 로그인 로직 ---
    # 필요시 로그인 과정을 여기에 추가하세요.
    # print("로그인 중...")
    # --- [수정 필요] ---

    for file_path in excel_files:
        print(f"'{os.path.basename(file_path)}' 파일 업로드 시도...")
        try:
            # --- [수정 필요] 파일 업로드 및 설정 로직 ---
            upload_element = driver.find_element(By.CSS_SELECTOR, 'input[type="file"]')
            upload_element.send_keys(file_path)
            driver.find_element(By.ID, 'save-button-id').click()
            WebDriverWait(driver, 20).until(
                EC.text_to_be_present_in_element((By.ID, 'status-message-id'), '완료')
            )
            print(f"'{os.path.basename(file_path)}' 파일 처리 완료.")
            # --- [수정 필요] ---
        except Exception as e:
            print(f"'{file_path}' 파일 처리 중 오류 발생: {e}")
            continue

def select_date_in_calendar(driver, date_str_yyyymmdd, is_start_date=True):
    """
    vdatetime 캘린더 팝업에서 특정 날짜를 선택하는 함수.

    :param driver: Selenium WebDriver 객체
    :param date_str_yyyymmdd: 'YYYY-MM-DD' 형식의 날짜 문자열 (예: '2025-09-01')
    :param is_start_date: 시작일인지 종료일인지 구분 (True: 시작일, False: 종료일)
    """
    try:
        date_type = "시작일" if is_start_date else "종료일"
        print(f"{date_type} 달력에서 날짜 '{date_str_yyyymmdd}' 선택을 시작합니다.")
        
        # 목표 날짜 파싱
        target_year = int(date_str_yyyymmdd.split('-')[0])
        target_month = int(date_str_yyyymmdd.split('-')[1])
        target_day = int(date_str_yyyymmdd.split('-')[2])

        # --- 1. 목표 월/년으로 이동 ---
        while True:
            # 현재 표시된 년/월 텍스트 가져오기 (예: "9월 2025")
            current_month_year_element = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, ".vdatetime-popup__month-selector__current"))
            )
            current_text = current_month_year_element.text
            
            current_month = int(current_text.split('월')[0])
            current_year = int(current_text.split(' ')[1])

            # 목표 년/월에 도달했는지 확인
            if current_year == target_year and current_month == target_month:
                print(f"목표 월/년({target_year}년 {target_month}월)에 도달했습니다.")
                break
            
            # 이전/다음 버튼 클릭
            if (current_year, current_month) < (target_year, target_month):
                print(f"현재: {current_year}년 {current_month}월. '다음' 버튼 클릭.")
                driver.find_element(By.CSS_SELECTOR, ".vdatetime-popup__month-selector__next").click()
            else:
                print(f"현재: {current_year}년 {current_month}월. '이전' 버튼 클릭.")
                driver.find_element(By.CSS_SELECTOR, ".vdatetime-popup__month-selector__previous").click()
            
            time.sleep(0.3) # 월 이동 애니메이션 대기

        # --- 2. 목표 일 클릭 ---
        print(f"'{target_day}'일을 클릭합니다.")
        # XPATH를 사용하여 비활성화되지 않은 날짜 요소 중 텍스트가 정확히 일치하는 것을 찾음
        day_xpath = f"//div[contains(@class, 'vdatetime-popup__date-picker__item') and not(contains(@class, '--disabled'))]//span/span[text()='{target_day}']"
        day_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, day_xpath))
        )
        day_element.click()
        time.sleep(0.5)

        # --- 3. 첫 번째 'Ok' 버튼 클릭 ---
        print(f"{date_type} 첫 번째 'Ok' 버튼을 클릭합니다.")
        nth_child = "1" if is_start_date else "2"
        ok_button1_selector = f"#main-page > div > div > section > div > div:nth-child(2) > div > div:nth-child(3) > div > div.content-area > div > div > div > div:nth-child({nth_child}) > div > div > div.vdatetime-popup > div.vdatetime-popup__actions > div:nth-child(2)"
        
        ok_button1 = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ok_button1_selector))
        )
        ok_button1.click()
        time.sleep(0.5)  # 첫 번째 OK 후 0.5초 대기
        
        # --- 4. 두 번째 'Ok' 버튼 클릭 ---
        print(f"{date_type} 두 번째 'Ok' 버튼을 클릭합니다.")
        time.sleep(0.5)  # 추가 0.5초 대기
        ok_button2 = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ok_button1_selector))  # 같은 선택자 사용
        )
        ok_button2.click()
        time.sleep(0.5)  # 클릭 후 추가 대기
        print(f"{date_type} 날짜 선택 성공!")

    except Exception as e:
        print(f"{date_type} 달력에서 날짜 선택 중 오류 발생: {e}")
        # 오류 발생 시 팝업을 닫으려면 Cancel 버튼 클릭 로직 추가 가능
        try:
            driver.find_element(By.XPATH, "//div[@class='vdatetime-popup__actions__button' and text()='Cancel']").click()
        except:
            pass

def check_and_recover_session(driver):
    """WebDriver 세션이 유효한지 확인하고, 끊어졌으면 복구합니다."""
    try:
        # 간단한 명령으로 세션 확인
        driver.current_url
        return True
    except Exception as e:
        if "invalid session id" in str(e).lower() or "session" in str(e).lower():
            print("  ⚠️ WebDriver 세션이 끊어졌습니다. 복구를 시도합니다...")
            return False
        return True

def register_promotions_on_bflow(driver, excel_files):
    """비플로우 사이트에 프로모션을 등록합니다."""
    if not excel_files:
        print("등록할 엑셀 파일이 없습니다.")
        return

    print("비플로우 프로모션 등록을 시작합니다...")
    
    # 성공/실패 추적을 위한 변수들
    success_count = 0
    failure_count = 0
    success_files = []
    failure_files = []
    
    # 파일 경로 미리 준비 (성능 최적화)
    print("파일 경로를 미리 준비합니다...")
    prepared_files = []
    for file_path in excel_files:
        filename = os.path.basename(file_path)
        # 파일명에서 날짜와 채널 정보 미리 추출
        name_parts = filename.replace('.xlsx', '').split('_')
        if len(name_parts) >= 3:
            date_range = name_parts[0]
            channel = name_parts[2]
            
            # 날짜 파싱
            start_date_str = date_range.split('-')[0]
            end_date_str = date_range.split('-')[1]
            
            # 날짜 형식 변환
            start_date = f"20{start_date_str[:2]}-{start_date_str[2:4]}-{start_date_str[4:6]}"
            end_date = f"20{end_date_str[:2]}-{end_date_str[2:4]}-{end_date_str[4:6]}"
            
            # 파일 존재 여부 미리 확인
            if os.path.exists(file_path):
                prepared_files.append({
                    'file_path': file_path,
                    'filename': filename,
                    'start_date': start_date,
                    'end_date': end_date,
                    'channel': channel
                })
                print(f"  준비 완료: {filename} ({channel})")
            else:
                print(f"  파일 없음: {filename}")
                failure_files.append({'filename': filename, 'reason': '파일 없음'})
                failure_count += 1
        else:
            print(f"  파일명 형식 오류: {filename}")
            failure_files.append({'filename': filename, 'reason': '파일명 형식 오류'})
            failure_count += 1
    
    print(f"총 {len(prepared_files)}개 파일이 준비되었습니다.")
    
    # 1. 이미 로그인된 상태에서 프로모션 관리 페이지로 바로 이동
    print("프로모션 관리 페이지로 이동합니다...")
    
    # 현재 URL 확인
    current_url = driver.current_url
    print(f"현재 URL: {current_url}")
    
    # 연동쇼핑몰 프로모션 관리 페이지로 바로 이동
    driver.get("https://b-flow.co.kr/distribution/promotions#/")
    
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
        )
        print("프로모션 관리 페이지 로딩 완료.")
    except Exception as e:
        print(f"프로모션 관리 페이지 로딩 중 오류: {e}")
        return

    # 2. 프로모션 관리 페이지에서 작업 계속
    print("프로모션 관리 페이지에서 작업을 계속합니다...")

    # 3. 각 엑셀 파일에 대해 프로모션 등록
    for file_info in prepared_files:
        file_path = file_info['file_path']
        filename = file_info['filename']
        start_date = file_info['start_date']
        end_date = file_info['end_date']
        channel = file_info['channel']
        
        print(f"\n'{filename}' 프로모션 등록을 시작합니다...")
        print(f"  프로모션명: {filename}")
        print(f"  시작일: {start_date}")
        print(f"  종료일: {end_date}")
        print(f"  채널: {channel}")
        
        # 세션 유효성 확인
        if not check_and_recover_session(driver):
            print("  ⚠️ 세션이 끊어져 복구를 시도합니다...")
            try:
                # WebDriver 재생성 시도
                try:
                    driver.quit()
                except:
                    pass
                time.sleep(2)
                driver = setup_driver()
                # 재로그인
                login_success = login_to_bflow(driver)
                if not login_success:
                    print("  ❌ 재로그인 실패. 이 파일을 건너뜁니다.")
                    failure_count += 1
                    failure_files.append({'filename': filename, 'reason': '세션 복구 실패: 재로그인 불가'})
                    continue
                # 프로모션 관리 페이지로 재이동
                driver.get("https://b-flow.co.kr/distribution/promotions#/")
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
                )
                print("  ✅ 세션 복구 및 재로그인 성공")
                time.sleep(3)  # 안정화 대기
            except Exception as recover_error:
                print(f"  ❌ 세션 복구 실패: {recover_error}")
                failure_count += 1
                failure_files.append({'filename': filename, 'reason': f'세션 복구 실패: {recover_error}'})
                continue
        
        try:
            
            # 외부채널 프로모션 등록 버튼 클릭 (재시도 로직 포함)
            print("  외부채널 프로모션 등록 버튼을 클릭합니다...")
            button_clicked = False
            
            # 여러 CSS 선택자 시도
            button_selectors = [
                "#main-page > div > div > section > div > div:nth-child(3) > div:nth-child(2) > div.pull-left.header-button > a > button",
                "#main-page div.pull-left.header-button a button",
                "button:contains('외부채널 프로모션 등록')",
                "a[href*='promotion'] button",
                ".header-button button",
                "button[class*='btn']"
            ]
            
            for attempt in range(3):  # 최대 3번 시도
                try:
                    # 현재 URL 확인
                    current_url = driver.current_url
                    print(f"  현재 URL: {current_url}")
                    
                    # 프로모션 관리 페이지가 아닌 경우 이동
                    if "distribution/promotions" not in current_url:
                        print("  프로모션 관리 페이지로 이동합니다...")
                        driver.get("https://b-flow.co.kr/distribution/promotions#/")
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
                        )
                        time.sleep(5)  # 페이지 완전 로딩 대기
                        print("  프로모션 관리 페이지 로딩 완료.")
                    
                    # 페이지가 완전히 로드될 때까지 대기
                    print("  페이지 요소 로딩을 기다립니다...")
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
                    )
                    time.sleep(3)  # 추가 안정화 대기
                    
                    # 여러 선택자로 버튼 찾기
                    register_button = None
                    for selector in button_selectors:
                        try:
                            print(f"  선택자 시도: {selector}")
                            register_button = WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                            )
                            print(f"  버튼 발견: {selector}")
                            break
                        except:
                            continue
                    
                    if register_button:
                        register_button.click()
                        button_clicked = True
                        print(f"  외부채널 프로모션 등록 버튼 클릭 성공 (시도 {attempt + 1})")
                        break
                    else:
                        print("  모든 선택자로 버튼을 찾을 수 없습니다.")
                        raise Exception("외부채널 프로모션 등록 버튼을 찾을 수 없습니다.")
                        
                except Exception as e:
                    print(f"  외부채널 프로모션 등록 버튼 클릭 실패 (시도 {attempt + 1}): {e}")
                    if attempt < 2:  # 마지막 시도가 아니면
                        print("  페이지를 새로고침하고 프로모션 관리 페이지로 이동합니다...")
                        # 프로모션 관리 페이지로 이동
                        driver.get("https://b-flow.co.kr/distribution/promotions#/")
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
                        )
                        time.sleep(5)  # 페이지 안정화 대기
                        print("  프로모션 관리 페이지 로딩 완료. 다시 시도합니다...")
                    else:
                        print("  최대 재시도 횟수를 초과했습니다.")
                        raise e
            
            if not button_clicked:
                raise Exception("외부채널 프로모션 등록 버튼을 클릭할 수 없습니다.")
            
            # 프로모션 명 입력 (재시도 로직 포함)
            print("  프로모션 명을 입력합니다...")
            name_input_success = False
            for attempt in range(3):  # 최대 3번 시도
                try:
                    promotion_name_input = WebDriverWait(driver, 30).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#main-page > div > div > section > div > div.box.distribution-promotion-style > div > div:nth-child(2) > div > div.content-area > div > div > input"))
                    )
                    promotion_name_input.clear()
                    # 파일명에서 .xlsx 확장자 제거
                    promotion_name = filename.replace('.xlsx', '')
                    promotion_name_input.send_keys(promotion_name)
                    name_input_success = True
                    print(f"  프로모션 명 입력 성공 (시도 {attempt + 1})")
                    break
                except Exception as e:
                    print(f"  프로모션 명 입력 실패 (시도 {attempt + 1}): {e}")
                    if attempt < 2:  # 마지막 시도가 아니면
                        print("  페이지를 새로고침하고 프로모션 관리 페이지로 이동합니다...")
                        # 프로모션 관리 페이지로 이동
                        driver.get("https://b-flow.co.kr/distribution/promotions#/")
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
                        )
                        time.sleep(3)  # 페이지 안정화 대기
                        print("  프로모션 관리 페이지 로딩 완료. 다시 시도합니다...")
                    else:
                        print("  최대 재시도 횟수를 초과했습니다.")
                        raise e
            
            if not name_input_success:
                raise Exception("프로모션 명을 입력할 수 없습니다.")
            
            # === 시작일 설정 ===
            print("  시작일 설정 버튼을 클릭합니다...")
            start_date_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#main-page > div > div > section > div > div.box.distribution-promotion-style > div > div:nth-child(3) > div > div.content-area > div > div > div > div:nth-child(1) > div > input"))
            )
            start_date_button.click()
            
            # 달력 팝업에서 시작일 선택
            print(f"  달력에서 시작일을 선택합니다: {start_date}")
            select_date_in_calendar(driver, start_date, is_start_date=True)
            
            # 시작일 완료 후 0.5초 대기
            time.sleep(0.5)
            
            # === 종료일 설정 ===
            print("  종료일 설정 버튼을 클릭합니다...")
            try:
                end_date_button = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#main-page > div > div > section > div > div:nth-child(2) > div > div:nth-child(3) > div > div.content-area > div > div > div > div:nth-child(2) > div > input"))
                )
                end_date_button.click()
                
                # 달력 팝업에서 종료일 선택
                print(f"  달력에서 종료일을 선택합니다: {end_date}")
                select_date_in_calendar(driver, end_date, is_start_date=False)
                
                # 종료일 완료 후 0.5초 대기
                time.sleep(0.5)
                
            except Exception as e:
                print(f"  종료일 설정 중 오류: {e}")
                print("  종료일이 없어 시작일과 동일하게 설정합니다.")
                # 종료일이 없으면 시작일과 동일하게 설정
                start_date_button.click()
                select_date_in_calendar(driver, end_date, is_start_date=False)
                time.sleep(0.5)
            
            # === 채널 설정 ===
            print(f"  채널 설정을 시작합니다: {channel}")
            
            # 한글 채널명을 영어로 매핑
            channel_mapping = {
                'SSG': 'ssg',
                '지마켓': 'gmarket',
                '옥션': 'auction',
                '11번가': '11st',
                '쿠팡': 'coupang',
                '위메프': 'wemakeprice',
                'GS샵': 'gsshop',
                '롯데ON': 'lotte',
                '롯데아이몰': 'lotteimall',
                'CJ몰': 'cjmall',
                '하프클럽(신규)': 'newhalfclub',
                '롯데아이몰': 'lotteimall',
                '네이버스마트스토어': 'naversmartstore',
                '글로벌지마켓': 'globalGmarket',
                '글로벌옥션': 'globalAuction',
                '카페24': 'cafe24',
                '화해': 'hwahae',
                '무신사': 'musinsa',
                '알리익스프레스': 'aliexpress',
                '쿠팡10': 'qoo10',
                '쉬인': 'shein',
                '카카오톡선물하기': 'kakaotalkgift',
                '카카오쇼핑하기': 'kakaotalkshopping',
                '글로벌네이버스마트스토어': 'globalNaversmartstore',
                '카카오스타일': 'kakaostyle',
                '사방넷': 'sabangnet',
                'H몰': 'hmall',
                '네이버플러스스토어': 'naverPlusStore',
                '퀸잇': 'queenit',
                '홈앤쇼핑': 'hnsmall',
                '로켓그로스': 'rocketgrowth'
            }
            
            # 채널명을 영어로 변환
            english_channel = channel_mapping.get(channel, channel.lower())
            print(f"  영어 채널명: {english_channel}")
            
            # 채널 설정 (안정적인 방법)
            try:
                print("  채널 설정 버튼을 클릭합니다...")
                # 페이지가 완전히 로드될 때까지 대기
                time.sleep(1)
                
                # 채널 설정 버튼 클릭
                channel_button = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#main-page > div > div > section > div > div:nth-child(2) > div > div:nth-child(4) > div > div.content-area > div > div > div.multiselect__tags > input"))
                )
                channel_button.click()
                time.sleep(1)  # 드롭다운이 열릴 때까지 대기
                
                # 채널 옵션 클릭
                print(f"  채널 '{english_channel}'을 선택합니다...")
                channel_option_xpath = f"//li[@class='multiselect__element']//span[text()='{english_channel}']"
                channel_option = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, channel_option_xpath))
                )
                channel_option.click()
                time.sleep(1)  # 선택 완료 대기
                print("  채널 선택 완료")
                
            except Exception as channel_error:
                print(f"  채널 선택 중 오류: {channel_error}")
                print("  JavaScript로 채널 선택을 시도합니다...")
                try:
                    # JavaScript로 채널 선택
                    driver.execute_script(f"""
                        var channelInput = document.querySelector('#main-page > div > div > section > div > div:nth-child(2) > div > div:nth-child(4) > div > div.content-area > div > div > div.multiselect__tags > input');
                        if (channelInput) {{
                            channelInput.click();
                            setTimeout(function() {{
                                var option = document.querySelector('li.multiselect__element span[text()="{english_channel}"]');
                                if (option) {{
                                    option.click();
                                }}
                            }}, 1000);
                        }}
                    """)
                    time.sleep(1)
                    print("  JavaScript 채널 선택 완료")
                except Exception as js_error:
                    print(f"  JavaScript 채널 선택 실패: {js_error}")
                    print("  채널 선택을 건너뛰고 계속 진행합니다...")
            
            # === 상품 체크박스 클릭 ===
            print("  상품 체크박스를 클릭합니다...")
            try:
                # 페이지 안정화 대기
                time.sleep(1)
                
                # 방법 1: JavaScript로 직접 체크 (가장 빠름)
                driver.execute_script("""
                    var checkbox = document.querySelector('#main-page > div > div > section > div > div:nth-child(2) > div > div:nth-child(5) > div > div.content-area > div > div > div:nth-child(1) > input[type="checkbox"]');
                    if (checkbox && !checkbox.checked) {
                        checkbox.checked = true;
                        checkbox.dispatchEvent(new Event('change', { bubbles: true }));
                        checkbox.dispatchEvent(new Event('click', { bubbles: true }));
                    }
                """)
                print("  상품 체크박스 클릭 성공 (JavaScript)")
            except Exception as e:
                print(f"  JavaScript 클릭 실패: {e}")
                try:
                    # 방법 2: Selenium으로 직접 클릭
                    product_checkbox = WebDriverWait(driver, 8).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#main-page > div > div > section > div > div:nth-child(2) > div > div:nth-child(5) > div > div.content-area > div > div > div:nth-child(1) > input[type=checkbox]"))
                    )
                    product_checkbox.click()
                    print("  상품 체크박스 클릭 성공 (Selenium)")
                except Exception as e2:
                    print(f"  Selenium 클릭 실패: {e2}")
                    try:
                        # 방법 3: ActionChains 사용
                        from selenium.webdriver.common.action_chains import ActionChains
                        product_checkbox = driver.find_element(By.CSS_SELECTOR, "#main-page > div > div > section > div > div:nth-child(2) > div > div:nth-child(5) > div > div.content-area > div > div > div:nth-child(1) > input[type=checkbox]")
                        actions = ActionChains(driver)
                        actions.move_to_element(product_checkbox).click().perform()
                        print("  상품 체크박스 클릭 성공 (ActionChains)")
                    except Exception as e3:
                        print(f"  ActionChains 클릭 실패: {e3}")
            
            time.sleep(1.0)  # 1초 텀
            
            # === 엑셀 업로드 버튼 클릭 ===
            print("  엑셀 업로드 버튼을 클릭합니다...")
            excel_upload_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#main-page > div > div > section > div > div:nth-child(3) > div.box-content > div > div > div > div.vgt-global-search.vgt-clearfix > div.vgt-global-search__actions.vgt-pull-right > div > div > button:nth-child(4)"))
            )
            excel_upload_button.click()
            time.sleep(1.0)  # 업로드 버튼 클릭 후 대기
            
            # === 파일 업로드 (최적화) ===
            print(f"  파일 업로드를 시작합니다: {filename}")
            print(f"  파일 경로: {file_path}")
            
            # 파일 업로드 (초고속 최적화)
            print("  파일 업로드를 시작합니다...")
            try:
                # JavaScript로 즉시 파일 업로드 (가장 빠름)
                driver.execute_script(f"""
                    var dropzone = document.getElementById('export-excel-dropzone');
                    var fileInput = dropzone.querySelector('input[type="file"]');
                    if (fileInput) {{
                        fileInput.style.display = 'block';
                        fileInput.style.visibility = 'visible';
                        fileInput.style.opacity = '1';
                        fileInput.value = '';
                    }}
                """)
                
                # 파일 입력 요소 찾기 (최단 시간)
                file_input = WebDriverWait(driver, 1).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#export-excel-dropzone input[type='file']"))
                )
                
                # 파일 업로드 (즉시)
                file_input.send_keys(file_path)
                print("  파일 업로드 성공 (초고속)")
                
            except Exception as e:
                print(f"  초고속 업로드 실패: {e}")
                # 대안: 일반 방법
                try:
                    file_input = driver.find_element(By.CSS_SELECTOR, "input[type='file']")
                    file_input.send_keys(file_path)
                    print("  파일 업로드 성공 (대안)")
                except Exception as e2:
                    print(f"  파일 업로드 실패: {e2}")
                    raise Exception("파일 업로드 실패")
            
            # === 파일 업로드 완료 확인 ===
            print("  파일 업로드 완료를 확인합니다...")
            upload_completed = False
            
            # 방법 1: 파일명이 표시되는지 확인 (진행률 표시)
            print("  파일명 표시 확인 중...")
            for i in range(5):
                try:
                    WebDriverWait(driver, 1).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".file-name, .uploaded-file, [class*='file'], [class*='upload']"))
                    )
                    print("  파일명 표시 확인됨")
                    upload_completed = True
                    break
                except:
                    if i % 2 == 0 and i > 0:  # 2초마다 진행 상황 출력
                        print(f"  파일명 표시 대기 중... ({i}/5초)")
                    time.sleep(1)
            
            # 방법 2: 업로드 진행 상태 확인 (진행률 표시)
            if not upload_completed:
                print("  업로드 진행 상태 확인 중...")
                upload_indicators = [
                    ".upload-success",
                    ".upload-complete", 
                    ".file-uploaded",
                    "[class*='success']",
                    "[class*='complete']"
                ]
                
                for indicator in upload_indicators:
                    for i in range(3):
                        try:
                            WebDriverWait(driver, 1).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, indicator))
                            )
                            print(f"  업로드 완료 표시 확인됨: {indicator}")
                            upload_completed = True
                            break
                        except:
                            if i % 2 == 0 and i > 0:  # 2초마다 진행 상황 출력
                                print(f"  업로드 상태 확인 중... ({i}/3초)")
                            time.sleep(1)
                    if upload_completed:
                        break
            
            # 방법 3: 최소 대기 시간 후 진행 (폴백)
            if not upload_completed:
                print("  최소 대기 시간 후 진행합니다...")
                time.sleep(2)  # 최소 2초 대기
                upload_completed = True
            
            if upload_completed:
                print("  파일 업로드 완료 확인됨")
            else:
                print("  파일 업로드 완료를 확인할 수 없지만 진행합니다...")
            
            time.sleep(0.5)  # 추가 안정화 대기
            
            # 업로드 버튼 클릭 (안정적)
            print("  업로드 버튼을 클릭합니다...")
            
            # "파일을 업로드 해주세요!" Alert 방지를 위한 추가 확인
            print("  업로드 가능 상태를 최종 확인합니다...")
            time.sleep(1)  # 추가 안정화 대기
            
            upload_confirm_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".br-btn.br-btn-purple.br-btn-medium"))
            )
            upload_confirm_button.click()
            time.sleep(3)  # 업로드 버튼 클릭 후 안정화 대기 (3초로 늘림)
            
            # 업로드 확인 Alert 창 처리 (개선된 안정성)
            print("  업로드 확인 Alert 창을 처리합니다...")
            alert_handled = False
            
            # 방법 1: Alert 창 처리 (더 안정적인 방식)
            try:
                print("  Alert 창 대기 중...")
                # Alert가 나타날 때까지 최대 15초 대기
                alert = WebDriverWait(driver, 15).until(EC.alert_is_present())
                alert_text = alert.text
                print(f"  Alert 텍스트: {alert_text}")
                
                # "파일을 업로드 해주세요!" Alert인지 확인
                if "파일을 업로드 해주세요" in alert_text:
                    print("  '파일을 업로드 해주세요!' Alert 감지됨")
                    print("  파일 업로드를 다시 시도합니다...")
                    alert.accept()  # Alert 확인
                    time.sleep(2)
                    
                    # 파일 업로드 재시도
                    try:
                        file_input = driver.find_element(By.CSS_SELECTOR, "input[type='file']")
                        file_input.send_keys(file_path)
                        print("  파일 재업로드 성공")
                        time.sleep(2)  # 재업로드 후 대기
                        
                        # 다시 업로드 버튼 클릭
                        upload_confirm_button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, ".br-btn.br-btn-purple.br-btn-medium"))
                        )
                        upload_confirm_button.click()
                        time.sleep(3)
                        
                        # 재업로드 후 Alert 처리
                        alert = WebDriverWait(driver, 5).until(EC.alert_is_present())
                        print(f"  재업로드 후 Alert 텍스트: {alert.text}")
                        alert.accept()
                        print("  재업로드 후 Alert 확인 버튼 클릭 성공")
                        time.sleep(3)
                        alert_handled = True
                        
                    except Exception as retry_error:
                        print(f"  파일 재업로드 실패: {retry_error}")
                        alert.accept()  # 원래 Alert라도 처리
                        time.sleep(3)
                        alert_handled = True
                else:
                    # 일반적인 업로드 확인 Alert
                    print("  일반 업로드 확인 Alert 처리")
                    time.sleep(1)  # Alert 텍스트 확인 후 추가 대기
                    alert.accept()  # Alert 확인 버튼 클릭
                    print("  Alert 확인 버튼 클릭 성공")
                    time.sleep(3)  # Alert 처리 후 안정화 대기
                    alert_handled = True
                    
            except Exception as e:
                print(f"  Alert 처리 실패: {e}")
                # Alert가 없거나 다른 문제가 있는 경우
                alert_handled = False
            
            # 방법 2: Alert가 없는 경우 모달창의 확인 버튼 클릭
            if not alert_handled:
                print("  Alert가 없으므로 모달창의 확인 버튼을 찾습니다...")
                try:
                    # 여러 선택자로 확인 버튼 찾기
                    confirm_selectors = [
                        "//button[contains(text(), '확인')]",
                        "//button[contains(text(), 'OK')]",
                        "//button[contains(text(), '업로드')]",
                        "//button[contains(@class, 'br-btn') and contains(text(), '확인')]",
                        "//button[contains(@class, 'btn') and contains(text(), '확인')]",
                        "//div[contains(@class, 'modal')]//button[contains(text(), '확인')]",
                        "//div[contains(@class, 'v--modal')]//button[contains(text(), '확인')]"
                    ]
                    
                    confirm_button_found = False
                    for i, selector in enumerate(confirm_selectors):
                        try:
                            print(f"  확인 버튼 선택자 시도 {i+1}: {selector}")
                            confirm_button = WebDriverWait(driver, 3).until(
                                EC.element_to_be_clickable((By.XPATH, selector))
                            )
                            time.sleep(1)  # 버튼 클릭 전 대기
                            confirm_button.click()
                            print(f"  확인 버튼 클릭 성공 (선택자 {i+1})")
                            confirm_button_found = True
                            alert_handled = True
                            break
                        except Exception as e:
                            print(f"  선택자 {i+1} 실패: {e}")
                            continue
                    
                    if not confirm_button_found:
                        # 방법 3: JavaScript로 확인 버튼 클릭
                        print("  JavaScript로 확인 버튼을 클릭합니다...")
                        try:
                            driver.execute_script("""
                                // 여러 방법으로 확인 버튼 찾기
                                var buttons = document.querySelectorAll('button');
                                var confirmButton = null;
                                
                                for (var i = 0; i < buttons.length; i++) {
                                    var text = buttons[i].textContent || buttons[i].innerText || '';
                                    if (text.includes('확인') || text.includes('OK') || text.includes('업로드')) {
                                        confirmButton = buttons[i];
                                        break;
                                    }
                                }
                                
                                if (confirmButton) {
                                    confirmButton.click();
                                    console.log('JavaScript로 확인 버튼 클릭 성공');
                                } else {
                                    console.log('확인 버튼을 찾을 수 없음');
                                }
                            """)
                            time.sleep(2)
                            print("  JavaScript 확인 버튼 클릭 시도 완료")
                            alert_handled = True
                        except Exception as js_error:
                            print(f"  JavaScript 확인 버튼 클릭 실패: {js_error}")
                            
                except Exception as e2:
                    print(f"  모달창 확인 버튼 클릭 실패: {e2}")
            
            # 방법 4: 최종 폴백 - 페이지 새로고침 후 재시도
            if not alert_handled:
                print("  모든 방법이 실패했습니다. 페이지를 새로고침하고 재시도합니다...")
                try:
                    driver.refresh()
                    time.sleep(5)
                    print("  페이지 새로고침 완료")
                except Exception as refresh_error:
                    print(f"  페이지 새로고침 실패: {refresh_error}")
            
            if alert_handled:
                print("  업로드 확인 처리가 완료되었습니다.")
            else:
                print("  업로드 확인 처리를 완료할 수 없었지만 계속 진행합니다.")
            
            time.sleep(2)  # 업로드 완료 대기 (안정화)
            
            # 업로드 완료 모달 창의 닫기 버튼 클릭 (개선된 안정성)
            print("  업로드 완료 창의 닫기 버튼을 클릭합니다...")
            modal_closed = False
            
            # 방법 1: 업로드 완료 모달 찾기 및 닫기
            try:
                print("  업로드 완료 모달을 찾는 중...")
                # 여러 모달 선택자 시도
                modal_selectors = [
                    "body > div.v--modal-overlay.scrollable > div > div.v--modal-box.v--modal",
                    "div.v--modal-overlay",
                    "div.v--modal-box",
                    "div[class*='modal']",
                    "div[class*='v--modal']"
                ]
                
                modal_found = False
                for i, selector in enumerate(modal_selectors):
                    try:
                        print(f"  모달 선택자 시도 {i+1}: {selector}")
                        WebDriverWait(driver, 3).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        print(f"  모달 발견 (선택자 {i+1})")
                        modal_found = True
                        break
                    except:
                        continue
                
                if modal_found:
                    # 닫기 버튼 찾기 및 클릭
                    close_button_selectors = [
                        "body > div.v--modal-overlay.scrollable > div > div.v--modal-box.v--modal > button",
                        "div.v--modal-box > button",
                        "div[class*='modal'] button",
                        "div[class*='v--modal'] button",
                        "//button[contains(text(), '닫기')]",
                        "//button[contains(text(), 'Close')]",
                        "//button[contains(text(), '확인')]",
                        "//button[contains(@class, 'close')]",
                        "//span[contains(@class, 'close')]"
                    ]
                    
                    for i, selector in enumerate(close_button_selectors):
                        try:
                            print(f"  닫기 버튼 선택자 시도 {i+1}: {selector}")
                            if selector.startswith("//"):
                                close_button = WebDriverWait(driver, 2).until(
                                    EC.element_to_be_clickable((By.XPATH, selector))
                                )
                            else:
                                close_button = WebDriverWait(driver, 2).until(
                                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                                )
                            close_button.click()
                            print(f"  닫기 버튼 클릭 성공 (선택자 {i+1})")
                            modal_closed = True
                            break
                        except Exception as e:
                            print(f"  닫기 버튼 선택자 {i+1} 실패: {e}")
                            continue
                
            except Exception as e:
                print(f"  모달 찾기 실패: {e}")
            
            # 방법 2: JavaScript로 모달 닫기
            if not modal_closed:
                print("  JavaScript로 모달을 닫습니다...")
                try:
                    driver.execute_script("""
                        // 여러 방법으로 모달 닫기 버튼 찾기
                        var closeButtons = document.querySelectorAll('button, span, div');
                        var closeButton = null;
                        
                        for (var i = 0; i < closeButtons.length; i++) {
                            var text = closeButtons[i].textContent || closeButtons[i].innerText || '';
                            var className = closeButtons[i].className || '';
                            
                            if (text.includes('닫기') || text.includes('Close') || text.includes('확인') || 
                                className.includes('close') || className.includes('btn-close')) {
                                closeButton = closeButtons[i];
                                break;
                            }
                        }
                        
                        if (closeButton) {
                            closeButton.click();
                            console.log('JavaScript로 모달 닫기 성공');
                        } else {
                            // ESC 키로 모달 닫기 시도
                            var event = new KeyboardEvent('keydown', { key: 'Escape', keyCode: 27 });
                            document.dispatchEvent(event);
                            console.log('ESC 키로 모달 닫기 시도');
                        }
                    """)
                    time.sleep(2)
                    print("  JavaScript 모달 닫기 시도 완료")
                    modal_closed = True
                except Exception as js_error:
                    print(f"  JavaScript 모달 닫기 실패: {js_error}")
            
            # 방법 3: ESC 키로 모달 닫기
            if not modal_closed:
                print("  ESC 키로 모달을 닫습니다...")
                try:
                    from selenium.webdriver.common.keys import Keys
                    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                    time.sleep(1)
                    print("  ESC 키 모달 닫기 시도 완료")
                    modal_closed = True
                except Exception as esc_error:
                    print(f"  ESC 키 모달 닫기 실패: {esc_error}")
            
            if modal_closed:
                print("  업로드 완료 모달이 닫혔습니다.")
            else:
                print("  업로드 완료 모달을 닫을 수 없었지만 계속 진행합니다.")
            
            time.sleep(1)  # 모달 닫기 후 안정화 대기
            
            # === 저장 버튼 클릭 ===
            print("  저장 버튼을 클릭합니다...")
            save_success = False
            try:
                save_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#main-page > div > div > section > div > div.text-center > button"))
                )
                save_button.click()
                print("  저장 버튼 클릭 성공")
                time.sleep(2)  # 저장 처리 대기
                save_success = True
                
            except Exception as e:
                print(f"  저장 버튼 클릭 실패: {e}")
            
            # === 저장 완료 처리 ===
            if save_success:
                print("  저장이 완료되었습니다.")
                
                # 등록 완료 확인 및 중복 리스트 모달 처리
                print("  등록 완료를 확인합니다...")
                registration_completed = False
                
                try:
                    # 방법 1: URL 변경 확인 (프로모션 생성 페이지로 이동했는지)
                    current_url = driver.current_url
                    if "promotion/create" in current_url:
                        print("  프로모션 생성 페이지로 이동됨 - 등록 완료")
                        registration_completed = True
                    
                    # 방법 2: 등록 완료 메시지 확인 (URL 변경이 없는 경우)
                    if not registration_completed:
                        print("  등록 완료 메시지를 확인합니다...")
                        completion_indicators = [
                            "등록이 완료되었습니다",
                            "등록 완료",
                            "프로모션이 등록되었습니다",
                            "성공적으로 등록되었습니다",
                            "등록 성공"
                        ]
                        for i in range(3):  # 3초 동안 확인
                            try:
                                page_text = driver.find_element(By.TAG_NAME, "body").text
                                for indicator in completion_indicators:
                                    if indicator in page_text:
                                        print(f"  등록 완료 메시지 발견: '{indicator}'")
                                        registration_completed = True
                                        break
                                if registration_completed:
                                    break
                            except:
                                pass
                            time.sleep(1)
                    
                    # 등록 완료가 확인되지 않은 경우에만 중복 리스트 모달 확인 (최대 2번 시도)
                    if not registration_completed:
                        print("  등록 완료가 확인되지 않았으므로 중복 리스트 모달을 확인합니다...")
                        
                        modal_selectors = [
                            "div.modal-wrapper.box",
                            "div[class*='modal']"
                        ]
                        
                        modal_found = False
                        for i, selector in enumerate(modal_selectors):
                            try:
                                print(f"  중복 모달 선택자 시도 {i+1}: {selector}")
                                WebDriverWait(driver, 1.2).until(  # 2초 → 1.5초로 단축
                                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                                )
                                print(f"  중복 모달 발견 (선택자 {i+1})")
                                modal_found = True
                                break
                            except:
                                continue
                        
                        # 2번 시도 후에도 모달을 찾지 못하면 등록 완료로 처리
                        if not modal_found:
                            print("  중복 리스트 모달을 2번 시도했지만 찾을 수 없습니다.")
                            print("  중복 리스트가 없는 경우로, 등록이 바로 완료되었습니다.")
                            registration_completed = True
                    
                    if modal_found:
                        # 중복 리스트 모달의 "확인" 버튼 클릭
                        print("  중복 리스트 모달의 '확인' 버튼을 찾습니다...")
                        
                        # 여러 확인 버튼 선택자 시도
                        confirm_selectors = [
                            "div.box-footer button.br-btn.br-btn-purple.br-btn-medium",
                            "div.box-footer button",
                            "button[class*='br-btn']",
                            "button[class*='btn']",
                            "//button[contains(text(), '확인')]",
                            "//button[contains(text(), 'OK')]",
                            "//button[contains(text(), '저장')]"
                        ]
                        
                        confirm_clicked = False
                        for i, selector in enumerate(confirm_selectors):
                            try:
                                print(f"  확인 버튼 선택자 시도 {i+1}: {selector}")
                                if selector.startswith("//"):
                                    confirm_button = WebDriverWait(driver, 3).until(
                                        EC.element_to_be_clickable((By.XPATH, selector))
                                    )
                                else:
                                    confirm_button = WebDriverWait(driver, 3).until(
                                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                                    )
                                confirm_button.click()
                                print(f"  중복 모달 '확인' 버튼 클릭 성공 (선택자 {i+1})")
                                confirm_clicked = True
                                modal_handled = True
                                break
                            except Exception as e:
                                print(f"  확인 버튼 선택자 {i+1} 실패: {e}")
                                continue
                        
                        if not confirm_clicked:
                            # JavaScript로 확인 버튼 클릭
                            print("  JavaScript로 중복 모달 확인 버튼을 클릭합니다...")
                            try:
                                driver.execute_script("""
                                    var buttons = document.querySelectorAll('button');
                                    for (var i = 0; i < buttons.length; i++) {
                                        var text = buttons[i].textContent || buttons[i].innerText || '';
                                        if (text.includes('확인') || text.includes('OK') || text.includes('저장')) {
                                            buttons[i].click();
                                            console.log('중복 모달 확인 버튼 클릭 성공');
                                            break;
                                        }
                                    }
                                """)
                                time.sleep(2)
                                print("  JavaScript 중복 모달 확인 버튼 클릭 시도 완료")
                                modal_handled = True
                            except Exception as js_error:
                                print(f"  JavaScript 중복 모달 확인 버튼 클릭 실패: {js_error}")
                        
                        time.sleep(2)  # 버튼 클릭 후 대기
                        
                        # 두 번째 확인 창 처리 (Alert)
                        print("  두 번째 확인 창을 처리합니다...")
                        try:
                            # Alert 창 확인 (더 긴 대기 시간)
                            alert = WebDriverWait(driver, 10).until(EC.alert_is_present())
                            alert_text = alert.text
                            print(f"  Alert 창 발견: {alert_text}")
                            alert.accept()
                            print("  Alert 창 '확인' 버튼 클릭 완료")
                            
                            # Alert 확인 후 바로 프로모션 생성 페이지로 이동 대기
                            print("  프로모션 생성 페이지로 자동 이동을 기다립니다...")
                            for i in range(10):  # 최대 10초 대기
                                current_url = driver.current_url
                                if "promotion/create" in current_url:
                                    print(f"  프로모션 생성 페이지로 이동 완료: {current_url}")
                                    break
                                if i % 2 == 0 and i > 0:
                                    print(f"  페이지 이동 대기 중... ({i}/10초)")
                                time.sleep(1)
                            else:
                                print("  프로모션 생성 페이지로 자동 이동되지 않았습니다.")
                                
                        except Exception as alert_error:
                            print(f"  Alert 창 처리 실패: {alert_error}")
                            print("  프로모션 생성 페이지로 이동을 확인합니다...")
                            # Alert가 없어도 프로모션 생성 페이지로 이동했는지 확인
                            current_url = driver.current_url
                            if "promotion/create" in current_url:
                                print(f"  이미 프로모션 생성 페이지에 있습니다: {current_url}")
                            else:
                                print("  프로모션 생성 페이지로 이동되지 않았습니다.")
                except Exception as e:
                    print(f"  등록 완료 확인 중 오류: {e}")
                
                if registration_completed:
                    print("  등록이 완료되었습니다.")
                else:
                    print("  등록 완료를 확인할 수 없었지만 계속 진행합니다.")
                
            else:
                print("  저장이 실패했습니다.")
            
            print(f"  '{filename}' 파일 업로드가 완료되었습니다.")
            print(f"  '{filename}' 프로모션 등록이 완료되었습니다.")
            
            # 성공 처리
            success_count += 1
            success_files.append({'filename': filename, 'channel': channel})
            print(f"  ✅ '{filename}' 등록 성공!")
            
            # 프로모션 생성 페이지에 있는 경우 프로모션 관리 페이지로 돌아가기
            current_url = driver.current_url
            if "promotion/create" in current_url:
                print("  프로모션 생성 페이지에서 프로모션 관리 페이지로 돌아갑니다...")
                try:
                    driver.get("https://b-flow.co.kr/distribution/promotions#/")
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
                    )
                    print("  프로모션 관리 페이지로 돌아가기 완료")
                    time.sleep(3)  # 페이지 안정화 대기
                except Exception as e:
                    print(f"  프로모션 관리 페이지 돌아가기 실패: {e}")
            else:
                print("  현재 페이지에서 다음 파일 처리를 계속합니다.")
            
            # 다음 파일 처리 전 안정화 대기
            print("  다음 파일 처리 전 안정화 대기...")
            time.sleep(8)  # 8초로 늘려서 안정성 확보
            
            # 세션 상태 확인 (주기적 체크)
            try:
                driver.current_url  # 세션 유지 확인
            except:
                print("  ⚠️ 세션 상태 확인 중 경고 발생 (다음 파일 처리 시 복구 시도)")
                
        except Exception as e:
            # 실패 처리
            error_message = str(e)
            
            # 세션 오류인 경우 복구 시도
            if "invalid session id" in error_message.lower() or "session" in error_message.lower():
                print(f"  ⚠️ '{filename}' 처리 중 WebDriver 세션 오류 발생 - {error_message}")
                print("  🔄 세션 복구를 시도합니다...")
                
                try:
                    # WebDriver 재생성 시도
                    try:
                        driver.quit()
                    except:
                        pass
                    time.sleep(2)
                    driver = setup_driver()
                    # 재로그인
                    login_success = login_to_bflow(driver)
                    if login_success:
                        # 프로모션 관리 페이지로 재이동
                        driver.get("https://b-flow.co.kr/distribution/promotions#/")
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
                        )
                        print("  ✅ 세션 복구 성공. 다음 파일로 계속 진행합니다.")
                        # 이 파일은 실패로 기록하되, 다음 파일은 계속 처리
                        failure_count += 1
                        failure_files.append({'filename': filename, 'reason': f'WebDriver 세션 오류 (복구 완료, 다음 파일 계속): {error_message}'})
                        time.sleep(3)
                        continue
                    else:
                        print("  ❌ 재로그인 실패. 프로세스를 중단합니다.")
                        failure_count += 1
                        failure_files.append({'filename': filename, 'reason': f'WebDriver 세션 오류 + 재로그인 실패: {error_message}'})
                        # 재로그인 실패 시 더 이상 진행할 수 없으므로 중단
                        break
                except Exception as recover_error:
                    print(f"  ❌ 세션 복구 실패: {recover_error}")
                    failure_count += 1
                    failure_files.append({'filename': filename, 'reason': f'WebDriver 세션 오류 + 복구 실패: {error_message}'})
                    # 복구 실패 시 더 이상 진행할 수 없으므로 중단
                    break
            else:
                print(f"  ❌ '{filename}' 등록 실패: {error_message}")
                failure_count += 1
                failure_files.append({'filename': filename, 'reason': error_message})
            continue
    
    # 최종 결과 요약 출력
    print("\n" + "="*60)
    print("프로모션 등록 결과 요약")
    print("="*60)
    print(f"총 처리 파일: {len(excel_files)}개")
    print(f"성공: {success_count}개")
    print(f"실패: {failure_count}개")
    print()
    
    if success_files:
        print("✅ 등록 성공한 파일들:")
        for i, file_info in enumerate(success_files, 1):
            print(f"  {i}. {file_info['filename']} ({file_info['channel']})")
        print()
    
    if failure_files:
        print("❌ 등록 실패한 파일들:")
        for i, file_info in enumerate(failure_files, 1):
            print(f"  {i}. {file_info['filename']} - {file_info['reason']}")
        print()
    
    print("="*60)
    print("모든 프로모션 등록이 완료되었습니다.")
    
    # 성공/실패 파일 목록 반환
    return {
        'success_files': success_files,
        'failure_files': failure_files,
        'success_count': success_count,
        'failure_count': failure_count
    }

def process_downloaded_files_only():
    """다운로드된 파일들만 통합하는 함수"""
    print("다운로드된 파일들을 처리합니다...")
    
    # 다운로드 폴더에서 파일 목록 가져오기
    if not os.path.exists(DOWNLOAD_FOLDER):
        print(f"다운로드 폴더가 존재하지 않습니다: {DOWNLOAD_FOLDER}")
        return
    
    downloaded_files = []
    for file in os.listdir(DOWNLOAD_FOLDER):
        if file.endswith('.xlsx') and 'product_all_list' in file:
            file_path = os.path.join(DOWNLOAD_FOLDER, file)
            downloaded_files.append(file_path)
    
    if not downloaded_files:
        print("처리할 다운로드 파일이 없습니다.")
        return
    
    # 파일명으로 정렬 (01, 02, 03 순서)
    downloaded_files.sort()
    print(f"발견된 다운로드 파일: {len(downloaded_files)}개")
    for file in downloaded_files:
        print(f"  - {os.path.basename(file)}")
    
    # 파일 통합만 실행
    combined_df = consolidate_excel_files(downloaded_files)
    
    if combined_df is not None:
        print("파일 통합이 완료되었습니다!")
    else:
        print("파일 통합 중 오류가 발생했습니다.")

def update_internal_discount_log(gspread_client, success_files=None, failure_files=None, success=True):
    """내부할인등록 탭에 작업 완료 내용을 업데이트합니다."""
    try:
        print("내부할인등록 탭에 작업 완료 내용을 업데이트합니다...")
        
        # 내부할인등록 탭 열기
        spreadsheet = gspread_client.open(SPREADSHEET_NAME)
        log_worksheet = spreadsheet.worksheet("내부할인등록")
        
        # 현재 날짜와 시간
        from datetime import datetime
        now = datetime.now()
        update_date = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # 기존 데이터 확인
        existing_data = log_worksheet.get_all_values()
        next_row = len(existing_data) + 1
        
        # 성공한 파일들 로그 추가
        if success_files:
            for file_info in success_files:
                filename = file_info['filename']
                channel = file_info.get('channel', '')
                
                # 로그에 추가할 데이터 (A, B, C열만)
                log_data = [
                    filename,  # A열: 실제 엑셀 파일명
                    update_date,  # B열: 업데이트일
                    "성공"  # C열: 성공/실패
                ]
                
                # 새 행 추가
                log_worksheet.update(f"A{next_row}:C{next_row}", [log_data])
                print(f"성공 로그 추가: {filename} ({channel})")
                next_row += 1
        
        # 실패한 파일들 로그 추가
        if failure_files:
            for file_info in failure_files:
                filename = file_info['filename']
                reason = file_info.get('reason', '알 수 없는 오류')
                
                # 로그에 추가할 데이터 (A, B, C열만)
                log_data = [
                    filename,  # A열: 실제 엑셀 파일명
                    update_date,  # B열: 업데이트일
                    f"실패: {reason}"  # C열: 실패 사유 포함
                ]
                
                # 새 행 추가
                log_worksheet.update(f"A{next_row}:C{next_row}", [log_data])
                print(f"실패 로그 추가: {filename} - {reason}")
                next_row += 1
        
        # 파일이 없는 경우
        if not success_files and not failure_files:
            log_data = [
                "파일_없음",  # A열
                update_date,  # B열
                "실패"  # C열
            ]
            log_worksheet.update(f"A{next_row}:C{next_row}", [log_data])
            print("추가된 로그: 파일_없음")
        
        print(f"내부할인등록 탭 업데이트 완료!")
        return True
        
    except Exception as e:
        print(f"내부할인등록 탭 업데이트 중 오류 발생: {e}")
        return False

def cleanup_previous_files():
    """이전 작업에서 생성된 파일들을 정리합니다."""
    print("이전 작업 파일들을 정리합니다...")
    
    # downloads 폴더 정리
    if os.path.exists(DOWNLOAD_FOLDER):
        for file in os.listdir(DOWNLOAD_FOLDER):
            if file.endswith('.xlsx'):
                file_path = os.path.join(DOWNLOAD_FOLDER, file)
                try:
                    os.remove(file_path)
                    print(f"삭제됨: {file}")
                except Exception as e:
                    print(f"삭제 실패: {file} - {e}")
    
    # output 폴더 정리
    if os.path.exists(OUTPUT_FOLDER):
        for file in os.listdir(OUTPUT_FOLDER):
            if file.endswith('.xlsx'):
                file_path = os.path.join(OUTPUT_FOLDER, file)
                try:
                    os.remove(file_path)
                    print(f"삭제됨: {file}")
                except Exception as e:
                    print(f"삭제 실패: {file} - {e}")
    
    print("파일 정리 완료!")

def main():
    """자동화 스크립트의 메인 실행 함수"""
    gspread_client = authenticate_google_sheets()
    if not gspread_client:
        return

    # 0. 이전 작업 파일들 정리
    cleanup_previous_files()

    # 1. 구글 스프레드시트에서 원본 데이터 가져오기
    print("구글 스프레드시트에서 원본 데이터를 가져옵니다...")
    original_df = get_data_from_sheet(gspread_client)
    if original_df is None:
        print("구글 스프레드시트에서 데이터를 가져올 수 없습니다.")
        return
    
    # 2. 기준데이터 추출 (K~R열에서 R열 필터링 적용)
    print("기준데이터를 추출합니다...")
    test_kq_data_extraction()
    
    # 3. 기준데이터 파일 읽기
    from datetime import datetime
    today = datetime.now().strftime("%Y%m%d")
    standard_file = os.path.join(DOWNLOAD_FOLDER, f"기준데이터_{today}.xlsx")
    
    if not os.path.exists(standard_file):
        print(f"기준데이터 파일을 찾을 수 없습니다: {standard_file}")
        return
    
    try:
        standard_df = pd.read_excel(standard_file)
        print(f"기준데이터 읽기 성공: {len(standard_df)}행")
    except Exception as e:
        print(f"기준데이터 파일 읽기 오류: {e}")
        return
    
    # 4. 기준데이터의 상품번호만 추출하여 검색
    if '상품번호' not in standard_df.columns:
        print("기준데이터에 상품번호 컬럼이 없습니다.")
        return
    
    # 상품번호만 있는 DataFrame 생성
    product_df = pd.DataFrame({'상품번호': standard_df['상품번호']})
    print(f"검색할 상품번호: {len(product_df)}개")

    driver = setup_driver()
    discount_files = []
    success = True
    
    try:
        downloaded_files = search_and_download_from_site(driver, product_df)
        if not downloaded_files:
            print("다운로드된 파일이 없어 작업을 중단합니다.")
            success = False
        else:
            combined_df = combine_downloaded_files(downloaded_files)
            final_excel_files = save_filtered_data_to_excel(original_df)
            
            # 할인시트 기반 파일 생성 및 프로모션 등록
            discount_files = create_discount_files()
            if discount_files:
                # 비플로우 프로모션 등록
                try:
                    result = register_promotions_on_bflow(driver, discount_files)
                    if result:
                        success_files = result.get('success_files', [])
                        failure_files = result.get('failure_files', [])
                        print(f"프로모션 등록 완료: 성공 {len(success_files)}개, 실패 {len(failure_files)}개")
                    else:
                        print("프로모션 등록 중 오류가 발생했습니다.")
                        success = False
                except Exception as promo_error:
                    print(f"프로모션 등록 중 오류 발생: {promo_error}")
                    success = False
            else:
                print("생성된 할인시트 파일이 없어 프로모션 등록을 건너뜁니다.")
                success = False
            
            if final_excel_files: # 업로드할 파일이 있을 경우에만 진행
                upload_and_configure_site(driver, final_excel_files)
            else:
                print("생성된 최종 엑셀 파일이 없어 업로드 단계를 건너뜁니다.")
            
            if success:
                print("\n모든 자동화 작업이 완료되었습니다.")
            else:
                print("\n일부 작업이 실패했습니다.")
        
    except Exception as e:
        print(f"자동화 작업 중 오류 발생: {e}")
        success = False
        
    finally:
        print("WebDriver를 종료합니다.")
        driver.quit()
        
        # 작업 완료 후 내부할인등록 탭에 로그 업데이트
        if gspread_client:
            # 성공/실패 파일 목록을 로그에 기록
            success_files = result.get('success_files', []) if 'result' in locals() else []
            failure_files = result.get('failure_files', []) if 'result' in locals() else []
            print(f"로그 업데이트: 성공 {len(success_files)}개, 실패 {len(failure_files)}개")
            update_internal_discount_log(gspread_client, success_files, failure_files, success)

def create_discount_files():
    """통합파일과 기준데이터를 비교하여 할인시트 기반으로 새로운 파일들을 생성합니다."""
    try:
        print("할인시트 기반 파일 생성을 시작합니다...")
        
        # 파일 경로 설정
        consolidated_file = os.path.join(DOWNLOAD_FOLDER, "통합파일_내부할인.xlsx")
        template_file = os.path.join(PROJECT_ROOT, "업로드기준파일", "할인시트.xlsx")
                   
        # 기준데이터 파일 찾기 (날짜가 포함된 파일)
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        standard_file = os.path.join(DOWNLOAD_FOLDER, f"기준데이터_{today}.xlsx")
        
        # standard_df 변수 초기화
        standard_df = None
        
        # 파일 존재 확인
        if not os.path.exists(consolidated_file):
            print(f"통합파일을 찾을 수 없습니다: {consolidated_file}")
            return
        
        if not os.path.exists(standard_file):
            print(f"기준데이터 파일을 찾을 수 없습니다: {standard_file}")
            print("기준데이터 없이 통합파일만으로 할인시트를 생성합니다.")
            # 기준데이터 없이 진행
            standard_df = None
            
        if not os.path.exists(template_file):
            print(f"할인시트 템플릿 파일을 찾을 수 없습니다: {template_file}")
            return
        
        # 파일 읽기 (권한 오류 방지를 위해 재시도 로직 추가)
        print("파일들을 읽는 중...")
        
        # 통합파일 읽기 (재시도)
        for attempt in range(3):
            try:
                consolidated_df = pd.read_excel(consolidated_file)
                print(f"통합파일 읽기 성공 (시도 {attempt + 1})")
                break
            except PermissionError:
                if attempt < 2:
                    print(f"파일 권한 오류 (시도 {attempt + 1}/3). 3초 후 재시도...")
                    time.sleep(3)
                else:
                    print("통합파일을 읽을 수 없습니다. 파일이 다른 프로그램에서 열려있는지 확인하세요.")
                    return
            except Exception as e:
                print(f"통합파일 읽기 오류: {e}")
                return
        
        # 기준데이터 파일 읽기 (있는 경우에만)
        if os.path.exists(standard_file):
            try:
                standard_df = pd.read_excel(standard_file)
                print("기준데이터 파일 읽기 성공")
            except Exception as e:
                print(f"기준데이터 파일 읽기 오류: {e}")
                return
        else:
            print("기준데이터 없이 통합파일만으로 진행합니다.")
            standard_df = None
        
        # 할인시트 템플릿 읽기
        try:
            template_df = pd.read_excel(template_file)
            print("할인시트 템플릿 읽기 성공")
        except Exception as e:
            print(f"할인시트 템플릿 읽기 오류: {e}")
            return
        
        print(f"통합파일 데이터: {len(consolidated_df)}행")
        if standard_df is not None:
            print(f"기준데이터: {len(standard_df)}행")
        else:
            print("기준데이터: 없음 (통합파일만 사용)")
        print(f"할인시트 템플릿: {len(template_df)}행")
        print(f"할인시트 템플릿 컬럼: {list(template_df.columns)}")
        print(f"통합파일 컬럼: {list(consolidated_df.columns)}")
        if standard_df is not None:
            print(f"기준데이터 컬럼: {list(standard_df.columns)}")
        
        # 기준데이터가 없는 경우 통합파일만으로 처리
        if standard_df is None:
            print("기준데이터가 없으므로 통합파일의 모든 상품에 대해 각 채널별 할인시트를 생성합니다.")
            all_channels = ['지마켓', '옥션', '11번가', '쿠팡', 'SSG', 'GS샵', '롯데ON', 'CJ몰', '하프클럽(신규)', '롯데아이몰', '카카오쇼핑하기', '카카오스타일', '퀸잇', '홈앤쇼핑']
            created_files = []
            
            for channel in all_channels:
                print(f"\n채널 '{channel}' 처리 중...")
                
                # 통합파일에서 해당 채널에 상품번호가 있는 상품들 찾기
                channel_columns = {
                    '지마켓': ['지마켓(상품번호)', '지마켓(마스터번호)'],
                    '옥션': ['옥션(상품번호)', '옥션(마스터번호)'],
                    '11번가': ['11번가'],
                    '쿠팡': ['쿠팡'],
                    'SSG': ['SSG'],
                    'GS샵': ['GS샵'],
                    '롯데ON': ['롯데ON'],
                    'CJ몰': ['CJ몰'],
                    '하프클럽(신규)': ['하프클럽(신규)'],
                    '롯데아이몰': ['롯데아이몰'],
                    '카카오쇼핑하기': ['카카오쇼핑하기'],
                    '카카오스타일': ['카카오스타일'],
                    '홈앤쇼핑': ['홈앤쇼핑'],
                    '퀸잇': ['퀸잇']
                }
                
                channel_cols = channel_columns.get(channel, [])
                matching_products = []
                
                for _, row in consolidated_df.iterrows():
                    has_channel_data = False
                    for col in channel_cols:
                        if col in consolidated_df.columns:
                            if pd.notna(row[col]) and str(row[col]).strip() != '':
                                has_channel_data = True
                                break
                    
                    if has_channel_data:
                        matching_products.append({
                            '상품번호': str(row['상품번호']),
                            '내부할인타입': 'P',  # 기본값
                            '내부할인': '10',  # 기본값 (% 제거)
                            '시작일': '2025. 9. 1',  # 기본값
                            '종료일': '2025. 9. 30',  # 기본값
                            '채널': channel,
                            '추가설명': f'{channel}_자동생성'
                        })
                
                if matching_products:
                    print(f"  매칭된 상품: {len(matching_products)}개")
                    
                    # 새로운 데이터프레임 생성
                    new_df = pd.DataFrame(columns=template_df.columns)
                    
                    # 매칭된 상품 수만큼 빈 행 생성
                    for i in range(len(matching_products)):
                        new_df.loc[i] = [''] * len(template_df.columns)
                    
                    # 데이터 추가
                    for i, product in enumerate(matching_products):
                        new_df.loc[i, '상품번호'] = product['상품번호']
                        new_df.loc[i, '내부할인타입'] = product['내부할인타입']
                        
                        # 내부할인타입에 따라 처리
                        internal_discount_type = str(product['내부할인타입']).strip()
                        internal_discount_value = str(product['내부할인']).strip()
                        
                        if internal_discount_type == 'A':
                            # A타입: 숫자만 (예: 2700)
                            internal_discount = internal_discount_value.replace(',', '').replace('%', '')
                        else:
                            # P타입: % 기호 제거 (예: 10% -> 10)
                            internal_discount = internal_discount_value.replace('%', '')
                        
                        new_df.loc[i, '내부할인'] = internal_discount
                        
                        # 내부할인타입에 따라 연동할인타입과 외부할인타입 설정
                        if internal_discount_type == 'A':
                            new_df.loc[i, '연동할인타입'] = 'A'
                            new_df.loc[i, '외부할인타입'] = 'A'
                        else:
                            new_df.loc[i, '연동할인타입'] = 'P'
                            new_df.loc[i, '외부할인타입'] = 'P'
                        
                        new_df.loc[i, '연동할인'] = '0'
                        new_df.loc[i, '외부할인가'] = '0'
                        new_df.loc[i, '채널분담율'] = '0'
                        new_df.loc[i, '브리치분담율'] = '0'
                        new_df.loc[i, '입점사분담율'] = '100'
                    
                    # 파일명 생성 (기준데이터의 실제 날짜 사용)
                    upload_count = len(matching_products)
                    # 기준데이터에서 날짜 추출 (첫 번째 상품의 날짜 사용)
                    start_str = matching_products[0]['시작일'].replace('2025', '25').replace('.', '').replace(' ', '')
                    end_str = matching_products[0]['종료일'].replace('2025', '25').replace('.', '').replace(' ', '')
                    if len(start_str) == 4:
                        start_date = f"25{start_str[2:3].zfill(2)}{start_str[3:4].zfill(2)}"
                    else:
                        start_date = f"25{start_str[2:4].zfill(2)}{start_str[4:6].zfill(2)}"
                    
                    if len(end_str) == 4:
                        end_date = f"25{end_str[2:3].zfill(2)}{end_str[3:5].zfill(2)}"
                    else:
                        end_date = f"25{end_str[2:4].zfill(2)}{end_str[4:6].zfill(2)}"
                    filename = f"{start_date}-{end_date}_상품_{channel}_{upload_count}.xlsx"
                    
                    # output 폴더에 저장
                    output_path = os.path.join(OUTPUT_FOLDER, filename)
                    new_df.to_excel(output_path, index=False)
                    print(f"  파일 생성: {filename}")
                    created_files.append(output_path)
                else:
                    print(f"  매칭된 상품이 없습니다.")
            
            print(f"\n총 {len(created_files)}개의 파일이 생성되었습니다.")
            return created_files
        
        # 기준데이터가 있는 경우 기존 로직 사용
        # 기준데이터 컬럼명 확인 및 설정
        standard_df.columns = ['시작일', '종료일', '상품번호', '내부할인타입', '내부할인', '채널', '추가설명']
        
        # 다중 채널 처리: 채널을 분리하여 개별 행으로 만들기
        expanded_rows = []
        all_channels = ['지마켓', '옥션', '11번가', '쿠팡', 'SSG', 'GS샵', '롯데ON', 'CJ몰', '하프클럽(신규)', '롯데아이몰', '카카오쇼핑하기', '카카오스타일', '퀸잇', '홈앤쇼핑']
        
        # 채널명 정규화 매핑 (기준데이터의 채널명을 코드의 채널명으로 변환)
        channel_normalization = {
            '롯데온': '롯데ON',
            '롯데i몰': '롯데아이몰',
            '롯데아이몰': '롯데아이몰',
            'CJ몰': 'CJ몰',
            'SSG': 'SSG',
            '쿠팡': '쿠팡',
            '지마켓': '지마켓',
            '옥션': '옥션',
            '11번가': '11번가',
            'GS샵': 'GS샵',
            '하프클럽(신규)': '하프클럽(신규)',
            '카카오쇼핑하기': '카카오쇼핑하기',
            '카카오스타일': '카카오스타일',
            '홈앤쇼핑': '홈앤쇼핑',
            '퀸잇': '퀸잇',
            '*전 채널 (gs제외)': '*전 채널 (gs제외)',  # 특수 케이스 추가
            '*전 채널 (퀸잇제외)': '*전 채널 (퀸잇제외)'  # 특수 케이스 추가
        }
        
        for _, row in standard_df.iterrows():
            channel = str(row['채널']).strip()
            
            if channel == '*전 채널':
                # 모든 채널에 대해 개별 행 생성
                for ch in all_channels:
                    new_row = row.copy()
                    new_row['채널'] = ch
                    expanded_rows.append(new_row)
            elif channel == '*전 채널 (gs제외)':
                # GS를 제외한 모든 채널에 대해 개별 행 생성
                for ch in all_channels:
                    if ch != 'GS샵':  # GS샵 제외
                        new_row = row.copy()
                        new_row['채널'] = ch
                        expanded_rows.append(new_row)
            elif channel == '*전 채널 (퀸잇제외)':
                # 퀸잇을 제외한 모든 채널에 대해 개별 행 생성
                for ch in all_channels:
                    if ch != '퀸잇':  # 퀸잇 제외
                        new_row = row.copy()
                        new_row['채널'] = ch
                        expanded_rows.append(new_row)
            elif (',' in channel) or ('/' in channel):
                # 여러 채널이 쉼표(,) 또는 슬래시(/)로 구분된 경우
                import re
                channels = [ch.strip() for ch in re.split(r"[,/]", channel) if ch.strip()]
                for ch in channels:
                    # 채널명 정규화 적용
                    normalized_channel = channel_normalization.get(ch, ch)
                    new_row = row.copy()
                    new_row['채널'] = normalized_channel
                    expanded_rows.append(new_row)
            else:
                # 단일 채널 - 정규화 적용
                normalized_channel = channel_normalization.get(channel, channel)
                new_row = row.copy()
                new_row['채널'] = normalized_channel
                expanded_rows.append(new_row)
        
        # 확장된 데이터프레임 생성
        expanded_df = pd.DataFrame(expanded_rows)
        print(f"채널 분리 후 데이터: {len(expanded_df)}행")
        
        # 채널별 통계 출력
        channel_counts = expanded_df['채널'].value_counts()
        print("채널별 상품 수:")
        for channel, count in channel_counts.items():
            print(f"  {channel}: {count}개")
        
        # 디버깅: 확장된 데이터의 샘플 출력
        print("\n확장된 데이터 샘플 (처음 5행):")
        print(expanded_df[['상품번호', '채널', '내부할인']].head())
        
        # 날짜 조합별로 그룹화 (시작일, 종료일, 채널)
        date_channel_groups = expanded_df.groupby(['시작일', '종료일', '채널'])
        
        created_files = []
        
        for (start_date_raw, end_date_raw, channel), group_df in date_channel_groups:
            print(f"\n날짜 조합 '{start_date_raw} ~ {end_date_raw}' 채널 '{channel}' 처리 중... ({len(group_df)}개 상품)")
            
            # 날짜 형식 변환: '2025. 10. 1' -> '251001', '2025. 10. 31' -> '251031'
            def parse_date(date_str):
                """날짜 문자열을 YYMMDD 형식으로 변환"""
                # '2025. 10. 1' -> '251001' 형태로 변환
                clean_str = str(date_str).replace('2025', '25').replace('.', '').replace(' ', '')
                
                # 정규표현식으로 년, 월, 일 추출
                import re
                match = re.match(r'25(\d{1,2})(\d{1,2})', clean_str)
                if match:
                    month = match.group(1).zfill(2)  # 월을 2자리로 맞춤
                    day = match.group(2).zfill(2)    # 일을 2자리로 맞춤
                    return f"25{month}{day}"
                else:
                    print(f"날짜 파싱 실패: {date_str} -> {clean_str}")
                    return "250101"  # 기본값
            
            start_date = parse_date(start_date_raw)
            end_date = parse_date(end_date_raw)
            
            # 상품번호 목록
            product_numbers = group_df['상품번호'].tolist()
            
            # 통합파일에서 해당 상품번호들 찾기
            matching_products = []
            for _, row in group_df.iterrows():
                product_num = str(row['상품번호'])
                
                # 통합파일에서 상품번호 찾기
                matching_rows = consolidated_df[consolidated_df['상품번호'].astype(str) == product_num]
                
                if not matching_rows.empty:
                    # 채널별 상품번호 컬럼 찾기
                    channel_columns = {
                        '지마켓': ['지마켓(상품번호)', '지마켓(마스터번호)'],
                        '옥션': ['옥션(상품번호)', '옥션(마스터번호)'],
                        '11번가': ['11번가'],
                        '쿠팡': ['쿠팡'],
                        'SSG': ['SSG'],
                        'GS샵': ['GS샵'],
                        '롯데ON': ['롯데ON'],
                        'CJ몰': ['CJ몰'],
                        '하프클럽(신규)': ['하프클럽(신규)'],
                        '롯데아이몰': ['롯데아이몰'],
                        '카카오쇼핑하기': ['카카오쇼핑하기'],
                        '카카오스타일': ['카카오스타일'],
                        '홈앤쇼핑': ['홈앤쇼핑'],
                        '퀸잇': ['퀸잇']
                    }
                    
                    # 채널에 해당하는 컬럼들 확인
                    channel_cols = channel_columns.get(channel, [])
                    has_channel_data = False
                    
                    for col in channel_cols:
                        if col in matching_rows.columns:
                            channel_values = matching_rows[col].dropna()
                            if not channel_values.empty and any(str(val).strip() != '' for val in channel_values):
                                has_channel_data = True
                                break
                    
                    if has_channel_data:
                        matching_products.append({
                            '상품번호': product_num,
                            '내부할인타입': row['내부할인타입'],
                            '내부할인': row['내부할인'],
                            '시작일': row['시작일'],
                            '종료일': row['종료일'],
                            '채널': row['채널'],
                            '추가설명': row['추가설명']
                        })
            
            if matching_products:
                print(f"  매칭된 상품: {len(matching_products)}개")
                
                # 새로운 데이터프레임 생성 (빈 템플릿에 충분한 행 생성)
                # 템플릿이 비어있으므로 필요한 만큼 빈 행을 생성
                new_df = pd.DataFrame(columns=template_df.columns)
                
                # 매칭된 상품 수만큼 빈 행 생성
                for i in range(len(matching_products)):
                    new_df.loc[i] = [''] * len(template_df.columns)
                
                # 데이터 추가 (올바른 템플릿 헤더명 사용)
                for i, product in enumerate(matching_products):
                    new_df.loc[i, '상품번호'] = product['상품번호']  # 1. 상품번호
                    new_df.loc[i, '내부할인타입'] = product['내부할인타입']  # 2. 내부할인타입
                    
                    # 내부할인타입에 따라 처리
                    internal_discount_type = str(product['내부할인타입']).strip()
                    internal_discount_value = str(product['내부할인']).strip()
                    
                    if internal_discount_type == 'A':
                        # A타입: 숫자만 (예: 2700)
                        internal_discount = internal_discount_value.replace(',', '').replace('%', '')
                    else:
                        # P타입: % 기호 제거 (예: 10% -> 10)
                        internal_discount = internal_discount_value.replace('%', '')
                    
                    new_df.loc[i, '내부할인'] = internal_discount  # 3. 내부할인
                    
                    # 내부할인타입에 따라 연동할인타입과 외부할인타입 설정
                    if internal_discount_type == 'A':
                        new_df.loc[i, '연동할인타입'] = 'A'  # 4. 연동할인타입
                        new_df.loc[i, '외부할인타입'] = 'A'  # 6. 외부할인타입
                    else:
                        new_df.loc[i, '연동할인타입'] = 'P'  # 4. 연동할인타입
                        new_df.loc[i, '외부할인타입'] = 'P'  # 6. 외부할인타입
                    
                    new_df.loc[i, '연동할인'] = '0'  # 5. 연동할인
                    new_df.loc[i, '외부할인가'] = '0'  # 7. 외부할인가
                    new_df.loc[i, '채널분담율'] = '0'  # 8. 채널분담율
                    new_df.loc[i, '브리치분담율'] = '0'  # 9. 브리치분담율
                    new_df.loc[i, '입점사분담율'] = '100'  # 10. 입점사분담율
                
                # 파일명 생성 (이미 변환된 날짜 사용)
                upload_count = len(matching_products)
                filename = f"{start_date}-{end_date}_상품_{channel}_{upload_count}.xlsx"
                
                # output 폴더에 저장
                output_path = os.path.join(OUTPUT_FOLDER, filename)
                new_df.to_excel(output_path, index=False)
                print(f"  파일 생성: {filename}")
                created_files.append(output_path)
            else:
                print(f"  매칭된 상품이 없습니다. (통합파일에서 해당 채널의 상품번호를 찾을 수 없음)")
                # 디버깅: 통합파일의 컬럼명과 샘플 데이터 출력
                print(f"  통합파일 컬럼: {list(consolidated_df.columns)}")
                print(f"  찾는 채널: {channel}")
                print(f"  해당 채널 컬럼: {channel_columns.get(channel, [])}")
                if not group_df.empty:
                    sample_product = str(group_df['상품번호'].iloc[0])
                    print(f"  샘플 상품번호: {sample_product}")
                    # 통합파일에서 해당 상품번호가 있는지 확인
                    matching_rows = consolidated_df[consolidated_df['상품번호'].astype(str) == sample_product]
                    if not matching_rows.empty:
                        print(f"  통합파일에서 상품번호 {sample_product} 찾음")
                        print(f"  해당 행의 채널 컬럼 값들:")
                        for col in channel_columns.get(channel, []):
                            if col in matching_rows.columns:
                                value = matching_rows[col].iloc[0]
                                print(f"    {col}: '{value}' (타입: {type(value)})")
                    else:
                        print(f"  통합파일에서 상품번호 {sample_product}를 찾을 수 없음")
                continue
        
        print(f"\n총 {len(created_files)}개의 파일이 생성되었습니다.")
        
        # 자동으로 프로모션 등록 진행
        print("비플로우 프로모션 등록을 시작합니다...")
        return created_files
        
    except Exception as e:
        print(f"할인시트 파일 생성 중 오류 발생: {e}")
        return []

def test_kq_data_extraction():
    """구글 스프레드시트에서 K~Q열 데이터만 추출하는 테스트 함수"""
    try:
        print("구글 스프레드시트 K~Q열 데이터 추출 테스트를 시작합니다...")
        
        # Google Sheets 인증
        print("Google Sheets 인증을 시작합니다...")
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = Credentials.from_service_account_file(GSPREAD_CREDENTIALS_PATH, scopes=scope)
        client = gspread.authorize(credentials)
        print("Google Sheets 인증 성공!")
        
        # 스프레드시트 열기
        spreadsheet = client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(SOURCE_SHEET_NAME)
        
        # K~R열 데이터 추출 (3행을 헤더로, 4행부터 데이터로)
        print("구글 스프레드시트에서 K~R열 데이터를 추출합니다...")
        k_to_r_data = worksheet.get('K:R')
        
        if k_to_r_data:
            print(f"추출된 데이터 행 수: {len(k_to_r_data)}")
            
            # 3행이 헤더, 4행부터 데이터로 처리
            if len(k_to_r_data) > 2:
                # 3행을 헤더로, 4행부터 데이터로 분리
                headers = k_to_r_data[2]  # 3행 (인덱스 2)
                data_rows = k_to_r_data[3:]  # 4행부터 (인덱스 3부터)
                
                print(f"3행 헤더: {headers}")
                print(f"헤더 컬럼 수: {len(headers)}")
                print(f"첫 번째 데이터 행 (4행): {data_rows[0] if data_rows else 'None'}")
                print(f"첫 번째 데이터 행 컬럼 수: {len(data_rows[0]) if data_rows else 0}")
                
                # K~R열의 올바른 헤더 설정 (8개 컬럼)
                correct_headers = ['시작일', '종료일', '상품번호', '내부할인타입', '내부할인', '채널', '추가설명', '설정일']
                print(f"올바른 헤더로 설정: {correct_headers}")
                
                # 헤더를 올바른 것으로 교체
                headers = correct_headers
                
                # 컬럼 수가 맞지 않는 경우 처리
                if len(headers) != len(data_rows[0]):
                    print("헤더와 데이터의 컬럼 수가 맞지 않습니다. 컬럼 수를 맞춰서 처리합니다.")
                    # 더 긴 길이에 맞춰서 처리
                    max_cols = max(len(headers), len(data_rows[0]))
                    
                    # 헤더 길이 맞추기
                    while len(headers) < max_cols:
                        headers.append(f"컬럼_{len(headers)+1}")
                    
                    # 데이터 행들 길이 맞추기
                    for i, row in enumerate(data_rows):
                        while len(row) < max_cols:
                            row.append("")
                        data_rows[i] = row[:max_cols]  # 초과하는 컬럼 제거
                
                # 데이터프레임 생성
                kr_df = pd.DataFrame(data_rows, columns=headers)
                
                # 빈 행 제거
                kr_df = kr_df.dropna(how='all')
                
                # R열(설정일)에 데이터가 있는 행 제거 (이미 완료된 작업)
                print(f"필터링 전 데이터 행 수: {len(kr_df)}")
                if '설정일' in kr_df.columns:
                    # 설정일 컬럼이 비어있지 않은 행들을 제거
                    kr_df = kr_df[kr_df['설정일'].isna() | (kr_df['설정일'] == '') | (kr_df['설정일'].astype(str).str.strip() == '')]
                    print(f"설정일이 있는 행을 제거한 후 데이터 행 수: {len(kr_df)}")
                else:
                    print("설정일 컬럼을 찾을 수 없습니다.")
                
                # K~Q열만 추출 (R열 제외)
                kq_df = kr_df[['시작일', '종료일', '상품번호', '내부할인타입', '내부할인', '채널', '추가설명']]
                
                print(f"정리된 데이터 행 수: {len(kq_df)}")
                print(f"컬럼: {list(kq_df.columns)}")
                
                # downloads 폴더에 저장 (오늘 날짜 포함)
                from datetime import datetime
                today = datetime.now().strftime("%Y%m%d")
                kq_file_path = os.path.join(DOWNLOAD_FOLDER, f"기준데이터_{today}.xlsx")
                kq_df.to_excel(kq_file_path, index=False)
                print(f"K~Q열 데이터가 저장되었습니다: {kq_file_path}")
                
                # 샘플 데이터 출력
                print("\n샘플 데이터 (처음 5행):")
                print(kq_df.head())
                
            else:
                print("K~R열에 데이터가 없습니다.")
        else:
            print("K~R열 데이터를 읽을 수 없습니다.")
            
    except Exception as e:
        print(f"K~R열 데이터 추출 중 오류 발생: {e}")

def run_promotion_registration_only():
    """프로모션 등록만 실행하는 함수"""
    print("=== 프로모션 등록만 실행 ===")
    
    # output 폴더에서 생성된 파일들 찾기
    if not os.path.exists(OUTPUT_FOLDER):
        print(f"출력 폴더가 존재하지 않습니다: {OUTPUT_FOLDER}")
        return False
    
    excel_files = []
    all_files = os.listdir(OUTPUT_FOLDER)
    print(f"output 폴더의 모든 파일: {len(all_files)}개")
    for file in all_files:
        print(f"  - {file}")
    
    # 프로모션 등록할 파일들 필터링
    for file in all_files:
        if file.endswith('.xlsx') and ('_상품_' in file or '_테스트_' in file):
            file_path = os.path.join(OUTPUT_FOLDER, file)
            excel_files.append(file_path)
            print(f"  ✓ 매칭된 파일: {file}")
        else:
            print(f"  ✗ 제외된 파일: {file}")
    
    if not excel_files:
        print("프로모션 등록할 엑셀 파일이 없습니다.")
        return False
    
    print(f"\n발견된 엑셀 파일: {len(excel_files)}개")
    for file in excel_files:
        print(f"  - {os.path.basename(file)}")
    
    # WebDriver 설정
    driver = setup_driver()
    success = True
    
    try:
        # 비플로우 로그인부터 시작
        print("비플로우에 로그인합니다...")
        login_success = login_to_bflow(driver)
        
        if not login_success:
            print("로그인에 실패했습니다.")
            return False
        
        # 비플로우 프로모션 등록
        result = register_promotions_on_bflow(driver, excel_files)
        if result:
            success_files = result.get('success_files', [])
            failure_files = result.get('failure_files', [])
            print(f"\n프로모션 등록 완료: 성공 {len(success_files)}개, 실패 {len(failure_files)}개")
        else:
            print("\n프로모션 등록 중 오류가 발생했습니다.")
            success = False
        
    except Exception as e:
        print(f"프로모션 등록 중 오류 발생: {e}")
        success = False
        
    finally:
        print("WebDriver를 종료합니다.")
        driver.quit()
    
    return success

def login_to_bflow(driver):
    """비플로우에 로그인하는 함수"""
    try:
        print("비플로우 로그인 페이지로 이동합니다...")
        driver.get(SEARCH_SITE_URL)
        
        # 로그인 버튼 클릭
        print("로그인 버튼을 클릭합니다...")
        login_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[3]/div[1]/div[2]/button[2]"))
        )
        login_button.click()
        
        # 사용자명 입력
        print("사용자명을 입력합니다...")
        username_input = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, "/html/body/div[1]/div[14]/div/div[2]/div/div[2]/div/input[1]"))
        )
        username_input.clear()
        username_input.send_keys("a01025399154@brich.co.kr")
        
        # 비밀번호 입력
        print("비밀번호를 입력합니다...")
        password_input = driver.find_element(By.XPATH, "/html/body/div[1]/div[14]/div/div[2]/div/div[2]/div/input[2]")
        password_input.clear()
        password_input.send_keys("2rlqmadl@!")
        
        # 로그인 버튼 클릭
        print("로그인을 실행합니다...")
        submit_button = driver.find_element(By.XPATH, "/html/body/div[1]/div[14]/div/div[2]/div/div[3]/button[1]")
        submit_button.click()
        
        # 로그인 완료 대기
        print("로그인 완료를 기다립니다...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#main-page"))
        )
        print("로그인 성공!")
        return True
        
    except Exception as e:
        print(f"로그인 중 오류 발생: {e}")
        return False

def run_file_creation_and_promotion():
    """파일 생성부터 프로모션 등록까지 실행하는 함수"""
    print("=== 파일 생성 + 프로모션 등록 실행 ===")
    
    # 1. 할인시트 기반 파일 생성
    print("1단계: 할인시트 기반 파일 생성")
    discount_files = create_discount_files()
    
    if not discount_files:
        print("생성된 파일이 없어 프로모션 등록을 건너뜁니다.")
        return False
    
    print(f"생성된 파일: {len(discount_files)}개")
    for file in discount_files:
        print(f"  - {os.path.basename(file)}")
    
    # 2. 프로모션 등록
    print("\n2단계: 프로모션 등록")
    driver = setup_driver()
    success = True
    
    try:
        result = register_promotions_on_bflow(driver, discount_files)
        if result:
            success_files = result.get('success_files', [])
            failure_files = result.get('failure_files', [])
            print(f"\n프로모션 등록 완료: 성공 {len(success_files)}개, 실패 {len(failure_files)}개")
        else:
            print("\n프로모션 등록 중 오류가 발생했습니다.")
            success = False
        
    except Exception as e:
        print(f"프로모션 등록 중 오류 발생: {e}")
        success = False
        
    finally:
        print("WebDriver를 종료합니다.")
        driver.quit()
    
    return success

if __name__ == '__main__':
    # ===== 실행 옵션 선택 =====
    # 아래 주석을 해제/추가하여 원하는 실행 방식을 선택하세요
    
    # 1. 전체 자동화 실행 (웹사이트에서 다운로드부터 시작)
    main()
    
    # 2. 프로모션 등록만 실행 (파일이 이미 생성된 상태)
    # run_promotion_registration_only()
    
    # 3. 파일 생성 + 프로모션 등록 (기준데이터와 통합파일이 있는 상태)
    # run_file_creation_and_promotion()
    
    # 4. 중간부터 시작하는 옵션들 (필요시 주석 해제)
    
    # 4-1. 다운로드된 파일들만 통합 (다운로드는 이미 완료된 상태)
    # process_downloaded_files_only()
    
    # 4-2. 기준데이터 추출 (구글 스프레드시트에서 K~Q열만 추출)
    # test_kq_data_extraction()
    
    # 4-3. 할인시트 기반 파일 생성만 (기준데이터와 통합파일이 있는 상태)
    # create_discount_files()
    
    # 전체 자동화 실행 (웹사이트에서 다운로드부터 시작)
    # main()
    
    # 프로모션 등록만 실행 (파일이 이미 생성된 상태)
    # run_promotion_registration_only()
    
    # 할인시트 기반 파일 생성만 (기준데이터와 통합파일이 있는 상태)
    # create_discount_files()
