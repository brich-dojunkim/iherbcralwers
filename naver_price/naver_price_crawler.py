"""
네이버 쇼핑 가격 크롤링 및 구글 스프레드시트 연동
- undetected_chromedriver 사용
- 네이버 로그인
- 최저가 조사
- 아이허브 판매처 확인
- 봇 캡차 자동 감지 및 대기
"""

import time
import re
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup


class NaverPriceCrawler:
    def __init__(self, spreadsheet_id, credentials_file='credentials.json'):
        """
        초기화
        
        Args:
            spreadsheet_id: 구글 스프레드시트 ID
            credentials_file: Google API 인증 파일 경로
        """
        self.spreadsheet_id = spreadsheet_id
        self.driver = None
        self.wait = None
        
        # 구글 스프레드시트 연결
        self.sheet = self._connect_google_sheets(credentials_file)
    
    def _connect_google_sheets(self, credentials_file):
        """구글 스프레드시트 연결"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_file(
                credentials_file, 
                scopes=scope
            )
            client = gspread.authorize(creds)
            
            # 스프레드시트 열기
            spreadsheet = client.open_by_key(self.spreadsheet_id)
            
            # "0" 시트 가져오기
            sheet = spreadsheet.worksheet("0")
            print(f"✓ 구글 스프레드시트 연결 성공: {spreadsheet.title}")
            return sheet
            
        except Exception as e:
            print(f"✗ 스프레드시트 연결 실패: {e}")
            raise
    
    def setup_driver(self):
        """undetected_chromedriver 설정"""
        options = uc.ChromeOptions()
        
        # 옵션 설정
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        
        # headless 모드 (필요시 주석 해제)
        # options.add_argument('--headless=new')
        
        self.driver = uc.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 10)
        
        print("✓ 크롬 드라이버 설정 완료")
    
    def naver_login(self, user_id, user_pw):
        """
        네이버 로그인
        
        Args:
            user_id: 네이버 아이디
            user_pw: 네이버 비밀번호
        """
        try:
            print("네이버 로그인 시작...")
            self.driver.get('https://nid.naver.com/nidlogin.login')
            time.sleep(2)
            
            # 아이디 입력
            id_input = self.wait.until(
                EC.presence_of_element_located((By.ID, 'id'))
            )
            id_input.clear()
            id_input.send_keys(user_id)
            time.sleep(0.5)
            
            # 비밀번호 입력
            pw_input = self.driver.find_element(By.ID, 'pw')
            pw_input.clear()
            pw_input.send_keys(user_pw)
            time.sleep(0.5)
            
            # 로그인 버튼 클릭
            login_btn = self.driver.find_element(By.ID, 'log.login')
            login_btn.click()
            
            time.sleep(3)
            
            # 로그인 시 캡차 확인
            self._wait_for_captcha()
            
            # 로그인 확인
            if 'nid.naver.com' not in self.driver.current_url:
                print("✓ 네이버 로그인 성공")
                return True
            else:
                print("✗ 네이버 로그인 실패 - 캡챠 또는 인증 필요할 수 있음")
                input("로그인을 수동으로 완료한 후 Enter를 누르세요...")
                return True
                
        except Exception as e:
            print(f"✗ 로그인 오류: {e}")
            return False
    
    def scroll_to_element(self, element):
        """요소까지 스크롤"""
        self.driver.execute_script(
            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", 
            element
        )
        time.sleep(0.5)
    
    def _wait_for_captcha(self):
        """봇 캡차가 나타나면 사용자가 해결할 때까지 대기"""
        try:
            # 캡차 관련 요소들
            captcha_selectors = [
                '#captcha_wrap',
                '#rcpt_form',
                '.captcha_wrap',
                '[data-component="rcpt_wrap"]',
                '[data-component="vcpt_wrap"]',
                '.captcha_img'
            ]
            
            captcha_detected = False
            for selector in captcha_selectors:
                try:
                    captcha = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if captcha.is_displayed():
                        captcha_detected = True
                        break
                except:
                    continue
            
            if captcha_detected:
                print("\n" + "="*60)
                print("⚠️  봇 캡차가 감지되었습니다!")
                print("   브라우저에서 캡차를 해결해주세요.")
                print("   (이미지에서 정답을 입력하고 확인 버튼 클릭)")
                print("="*60)
                
                # 캡차가 사라질 때까지 대기
                while True:
                    captcha_exists = False
                    for selector in captcha_selectors:
                        try:
                            captcha = self.driver.find_element(By.CSS_SELECTOR, selector)
                            if captcha.is_displayed():
                                captcha_exists = True
                                break
                        except:
                            continue
                    
                    if not captcha_exists:
                        break
                    
                    time.sleep(1)
                
                print("✓ 캡차 해결 완료")
                time.sleep(2)
                    
        except Exception as e:
            # 캡차가 없으면 그냥 진행
            pass
    
    def get_lowest_price(self, url):
        """
        최저가 조회
        
        Args:
            url: 네이버 쇼핑 상품 URL
            
        Returns:
            dict: {'price': 최저가, 'shipping': 배송비, 'mall': 판매처}
        """
        try:
            print(f"\n상품 URL 접속: {url}")
            self.driver.get(url)
            time.sleep(2)
            
            # 봇 캡차 확인
            self._wait_for_captcha()
            
            # 페이지 스크롤 (판매처 선택보기 요소가 보이도록)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(1)
            
            # HTML 파싱
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # 최저가 추출
            lowest_price = None
            price_elem = soup.select_one('.lowestPrice_num__adgCI')
            if price_elem:
                price_text = price_elem.get_text(strip=True).replace(',', '')
                lowest_price = int(price_text)
            
            # 배송비 추출
            shipping_text = "확인필요"
            shipping_elem = soup.select_one('.lowestPrice_delivery_fee__COSVN')
            if shipping_elem:
                shipping_text = shipping_elem.get_text(strip=True)
            
            # 판매처 추출
            mall_name = "확인필요"
            mall_elem = soup.select_one('.lowestPrice_cell__1_Cz0:nth-child(2)')
            if mall_elem:
                mall_name = mall_elem.get_text(strip=True)
            
            print(f"  최저가: {lowest_price}원")
            print(f"  배송비: {shipping_text}")
            print(f"  판매처: {mall_name}")
            
            return {
                'price': lowest_price,
                'shipping': shipping_text,
                'mall': mall_name
            }
            
        except Exception as e:
            print(f"✗ 가격 조회 실패: {e}")
            return {
                'price': None,
                'shipping': "오류",
                'mall': "오류"
            }
    
    def check_iherb_available(self, url):
        """
        아이허브 판매 여부 확인
        
        Args:
            url: 네이버 쇼핑 상품 URL
            
        Returns:
            bool: 아이허브 판매 여부
        """
        try:
            print("아이허브 판매처 확인 중...")
            
            # 이미 해당 페이지에 있다고 가정
            if self.driver.current_url != url:
                self.driver.get(url)
                time.sleep(2)
                
                # 봇 캡차 확인
                self._wait_for_captcha()
            
            # 페이지 하단으로 스크롤 (헤더 피하기)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(1)
            
            # 판매처 선택보기 버튼 찾기 및 클릭
            try:
                # 올바른 선택자 사용
                select_mall_btn = self.wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, '.filter_check_mall__IK03K')
                    )
                )
                
                # 요소가 보이도록 추가 스크롤
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", 
                    select_mall_btn
                )
                time.sleep(1)
                
                # JavaScript로 클릭 (헤더 간섭 방지)
                self.driver.execute_script("arguments[0].click();", select_mall_btn)
                print("  판매처 선택 레이어 열림")
                time.sleep(2)
                
                # 레이어 오픈 후 캡차 확인
                self._wait_for_captcha()
                
                # 판매처 목록에서 아이허브 검색
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # 아이허브 키워드
                iherb_keywords = ['아이허브', 'iherb', 'iHerb', '아이허브 공식']
                
                # 판매처 목록 검색
                mall_labels = soup.select('.filter_text__yBa_v')
                
                for mall in mall_labels:
                    mall_text = mall.get_text(strip=True)
                    for keyword in iherb_keywords:
                        if keyword.lower() in mall_text.lower():
                            print(f"  ✓ 아이허브 판매처 발견: {mall_text}")
                            
                            # 레이어 닫기 (취소 버튼 클릭)
                            try:
                                cancel_btn = self.driver.find_element(
                                    By.CSS_SELECTOR, 
                                    '.filter_btn_cancel__wIx02'
                                )
                                self.driver.execute_script("arguments[0].click();", cancel_btn)
                                time.sleep(0.5)
                            except:
                                # ESC 키로 닫기 시도
                                from selenium.webdriver.common.action_chains import ActionChains
                                ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                                time.sleep(0.5)
                            
                            return True
                
                print("  ✗ 아이허브 판매처 없음")
                
                # 레이어 닫기
                try:
                    cancel_btn = self.driver.find_element(
                        By.CSS_SELECTOR, 
                        '.filter_btn_cancel__wIx02'
                    )
                    self.driver.execute_script("arguments[0].click();", cancel_btn)
                    time.sleep(0.5)
                except:
                    from selenium.webdriver.common.action_chains import ActionChains
                    ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                    time.sleep(0.5)
                
                return False
                
            except TimeoutException:
                print("  ! 판매처 선택보기 버튼을 찾을 수 없음")
                
                # 페이지 전체에서 아이허브 키워드 검색 (대안)
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                page_text = soup.get_text().lower()
                
                if '아이허브' in page_text or 'iherb' in page_text:
                    print("  ✓ 페이지 내 아이허브 키워드 발견")
                    return True
                
                return False
                
        except Exception as e:
            print(f"✗ 아이허브 확인 실패: {e}")
            return False
    
    def close(self):
        """드라이버 종료"""
        if self.driver:
            self.driver.quit()
            print("\n브라우저 종료")
        
    def process_urls(self):
        """
        URL 일괄 처리 (날짜별 누적 + 날짜 행 병합)
        """
        try:
            # 모든 데이터 가져오기
            all_data = self.sheet.get_all_values()
            
            if not all_data or len(all_data) < 2:
                print("스프레드시트에 데이터가 없습니다.")
                return
            
            # 1행: 날짜 (병합용)
            # 2행: 항목명 (아이허브, 최저가)
            date_row = all_data[0] if len(all_data) > 0 else []
            headers = all_data[1] if len(all_data) > 1 else []
            
            print(f"현재 헤더 개수: {len(headers)}개")
            
            # URL 컬럼 찾기 (D 컬럼)
            url_col_idx = None
            for idx, header in enumerate(headers):
                if header and ('url' in header.lower() or header == 'URL'):
                    url_col_idx = idx
                    break
            
            if url_col_idx is None:
                print("URL 컬럼을 찾을 수 없습니다.")
                return
            
            print(f"URL 컬럼 위치: {chr(65+url_col_idx)} (인덱스 {url_col_idx})")
            
            # 오늘 날짜
            today = datetime.now().strftime('%Y-%m-%d')
            print(f"\n📅 처리 날짜: {today}")
            
            # 오늘 날짜의 컬럼이 이미 있는지 확인 (1행의 날짜 확인)
            today_start_col = None
            for idx in range(len(date_row)):
                if date_row[idx] and today in date_row[idx]:
                    today_start_col = idx
                    print(f"⚠️  같은 날짜 컬럼 발견 → 덮어쓰기 모드")
                    break
            
            # 오늘 날짜 컬럼이 없으면 새로 추가
            if today_start_col is None:
                # 마지막 컬럼 다음에 추가
                today_start_col = len(headers)
                
                # 1행에 날짜 추가 (병합용)
                new_date_row = date_row.copy()
                while len(new_date_row) < today_start_col:
                    new_date_row.append('')
                new_date_row.append(today)  # E열에 날짜
                new_date_row.append('')      # F열은 비움 (병합될 부분)
                
                # 2행에 항목명 추가
                new_headers = headers.copy()
                while len(new_headers) < today_start_col:
                    new_headers.append('')
                new_headers.append('아이허브')
                new_headers.append('최저가')
                
                # 1행과 2행 업데이트
                update_range = f'A1:{chr(65+len(new_date_row)-1)}2'
                self.sheet.update([new_date_row, new_headers], update_range)
                
                # 셀 병합 (E1:F1 병합)
                merge_range = f'{chr(65+today_start_col)}1:{chr(65+today_start_col+1)}1'
                self.sheet.merge_cells(merge_range, merge_type='MERGE_ALL')
                
                print(f"✓ 새로운 날짜 컬럼 추가: {chr(65+today_start_col)}~{chr(65+today_start_col+1)}열")
                print(f"✓ 날짜 병합: {merge_range}")
                
                date_row = new_date_row
                headers = new_headers
            else:
                print(f"✓ 기존 컬럼 사용: {chr(65+today_start_col)}열부터")
            
            # 컬럼 위치
            iherb_col = today_start_col
            price_col = today_start_col + 1
            
            print(f"  📍 아이허브: {chr(65+iherb_col)}열")
            print(f"  📍 최저가: {chr(65+price_col)}열")
            
            # 실제 URL이 있는 행 찾기 (3행부터 - 1행 날짜, 2행 헤더)
            url_rows = []
            for row_idx in range(2, len(all_data)):
                row_data = all_data[row_idx]
                
                if len(row_data) > url_col_idx:
                    url = row_data[url_col_idx]
                    if url and url.strip() and url.startswith('http'):
                        url_rows.append(row_idx)
            
            if not url_rows:
                print("\n처리할 URL이 없습니다.")
                return
            
            print(f"\n📊 총 {len(url_rows)}개의 URL 발견")
            print(f"   시작 행: {url_rows[0] + 1}")
            print(f"   종료 행: {url_rows[-1] + 1}")
            
            # URL 처리
            for idx, row_idx in enumerate(url_rows):
                row_data = all_data[row_idx]
                url = row_data[url_col_idx]
                
                print(f"\n{'='*60}")
                print(f"[{idx + 1}/{len(url_rows)}] 행 {row_idx + 1} 처리 시작")
                print(f"{'='*60}")
                
                # 최저가 조회
                price_info = self.get_lowest_price(url)
                
                # 아이허브 판매 확인
                has_iherb = self.check_iherb_available(url)
                
                # 스프레드시트 업데이트
                result_row = row_idx + 1
                
                # 배치 업데이트 (2개 셀만)
                update_range = f'{chr(65+iherb_col)}{result_row}:{chr(65+price_col)}{result_row}'
                update_values = [[
                    'O' if has_iherb else 'X',
                    price_info['price']
                ]]
                
                self.sheet.update(update_values, update_range)
                
                print(f"\n✅ 행 {result_row} 업데이트 완료")
                print(f"   아이허브: {'O' if has_iherb else 'X'}")
                print(f"   최저가: {price_info['price']}원")
                
                # 요청 간 딜레이
                time.sleep(2)
            
            print(f"\n{'='*60}")
            print(f"✅ 전체 처리 완료!")
            print(f"   처리된 URL: {len(url_rows)}개")
            print(f"   저장 위치: {chr(65+iherb_col)}~{chr(65+price_col)}열")
            print(f"{'='*60}")
            
        except Exception as e:
            print(f"✗ URL 처리 중 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            raise

def main():
    """메인 함수"""
    
    # 설정
    SPREADSHEET_ID = '1o2xj4R02Wr4QDhfR3VdQSYbtoaueZzEZend15DqObR8'
    CREDENTIALS_FILE = 'credentials.json'  # Google API 인증 파일
    
    # 네이버 계정 (환경변수 또는 안전한 방법으로 관리 권장)
    NAVER_ID = input("네이버 ID: ")
    NAVER_PW = input("네이버 PW: ")
    
    crawler = None
    
    try:
        # 크롤러 초기화
        crawler = NaverPriceCrawler(SPREADSHEET_ID, CREDENTIALS_FILE)
        
        # 드라이버 설정
        crawler.setup_driver()
        
        # 네이버 로그인
        if not crawler.naver_login(NAVER_ID, NAVER_PW):
            print("로그인 실패. 프로그램을 종료합니다.")
            return
        
        # URL 처리 (자동으로 모든 URL 처리)
        crawler.process_urls()
        
    except KeyboardInterrupt:
        print("\n\n사용자가 중단했습니다.")
        
    except Exception as e:
        print(f"\n오류 발생: {e}")
        
    finally:
        if crawler:
            crawler.close()


if __name__ == "__main__":
    main()