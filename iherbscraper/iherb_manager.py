"""
브라우저 관리 모듈 - 개선된 재시작 및 오류 처리
"""

import time
import random
import subprocess
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from iherb_config import IHerbConfig

class BrowserManager:
    """브라우저 초기화, 관리, 안전한 페이지 로딩 담당"""
    
    def __init__(self, headless=False, delay_range=None):
        self.headless = headless
        self.delay_range = delay_range or IHerbConfig.DEFAULT_DELAY_RANGE
        self.driver = None
        
        self._initialize_browser()
    
    def _initialize_browser(self):
        """브라우저 초기화"""
        try:
            print("  브라우저 초기화 중...")
            options = uc.ChromeOptions()
            
            if self.headless:
                options.add_argument("--headless=new")
            
            # 설정에서 옵션 추가
            for option in IHerbConfig.CHROME_OPTIONS:
                options.add_argument(option)
            
            self.driver = uc.Chrome(options=options, version_main=None)
            
            # 타임아웃 설정
            self.driver.set_page_load_timeout(IHerbConfig.PAGE_LOAD_TIMEOUT)
            self.driver.implicitly_wait(IHerbConfig.IMPLICIT_WAIT)
            
            # WebDriver 감지 방지
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            print("  브라우저 초기화 완료 ✓")
            
        except Exception as e:
            print(f"  브라우저 초기화 실패: {e}")
            raise
    
    def safe_get(self, url, max_retries=None):
        """안전한 페이지 로딩"""
        max_retries = max_retries or IHerbConfig.MAX_RETRIES
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"    페이지 로딩 재시도 {attempt + 1}/{max_retries}")
                
                # 브라우저가 죽었는지 확인
                if not self._is_browser_alive():
                    print("    브라우저 세션이 끊어짐 - 재시작")
                    self._initialize_browser()
                
                # 페이지 로딩
                self.driver.get(url)
                
                # JavaScript 실행이 완료될 때까지 대기
                WebDriverWait(self.driver, 8).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
                
                # 추가 대기 시간
                time.sleep(random.uniform(0.5, 1))
                
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
                    if self._is_critical_error(str(e)):
                        try:
                            self._safe_restart_browser()
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
    
    def _is_browser_alive(self):
        """브라우저가 살아있는지 확인"""
        try:
            if self.driver is None:
                return False
            # 간단한 명령으로 브라우저 상태 확인
            self.driver.current_url
            return True
        except:
            return False
    
    def _safe_restart_browser(self):
        """안전한 브라우저 재시작"""
        try:
            print("    브라우저 안전 재시작 중...")
            
            # 기존 브라우저 종료
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None
            
            # 메모리 정리 대기
            time.sleep(3)
            
            # 새 브라우저 시작
            self._initialize_browser()
            
            print("    브라우저 안전 재시작 완료 ✓")
            
        except Exception as e:
            print(f"    브라우저 재시작 실패: {e}")
            raise
    
    def _is_critical_error(self, error_message):
        """심각한 오류 여부 판단"""
        critical_keywords = [
            'chrome not reachable',
            'session deleted',
            'connection refused',
            'disconnected',
            'max retries exceeded',
            'failed to establish a new connection',
            'httpconnectionpool'
        ]
        
        return any(keyword in error_message.lower() for keyword in critical_keywords)
    
    def restart_with_cleanup(self):
        """브라우저 재시작 + 완전한 정리 (강화된 버전)"""
        try:
            print("  브라우저 완전 재시작 중...")
            
            # 1. 현재 브라우저 안전 종료
            if self.driver:
                try:
                    # 모든 쿠키 및 캐시 삭제
                    self.driver.delete_all_cookies()
                    self.driver.execute_script("window.localStorage.clear();")
                    self.driver.execute_script("window.sessionStorage.clear();")
                except:
                    pass
                
                try:
                    # 브라우저 종료
                    self.driver.quit()
                except:
                    pass
                
                self.driver = None
            
            # 2. Chrome 프로세스 강제 종료 (macOS)
            try:
                subprocess.run(['pkill', '-f', 'chrome'], check=False, capture_output=True)
                subprocess.run(['pkill', '-f', 'chromedriver'], check=False, capture_output=True)
                print("    Chrome 프로세스 정리 완료")
            except:
                pass
            
            # 3. 충분한 대기 (포트 해제 대기)
            time.sleep(12)  # 8초 -> 12초로 증가
            
            # 4. 새 브라우저 시작
            self._initialize_browser()
            
            # 5. 안정화 대기
            time.sleep(8)  # 5초 -> 8초로 증가
            
            print("  브라우저 완전 재시작 완료 ✓")
            print("    - Chrome 프로세스 완전 정리")
            print("    - 포트 충돌 해결")
            print("    - 새 세션 안정화")
            
        except Exception as e:
            print(f"  브라우저 재시작 실패: {e}")
            raise
    
    def random_delay(self):
        """랜덤 딜레이"""
        delay = random.uniform(0.3, 0.8)
        time.sleep(delay)
    
    def wait_for_element(self, selector, timeout=10):
        """요소 대기"""
        try:
            return WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located(selector)
            )
        except TimeoutException:
            return None
    
    def wait_for_clickable(self, selector, timeout=10):
        """클릭 가능한 요소 대기"""
        try:
            return WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable(selector)
            )
        except TimeoutException:
            return None
    
    def close(self):
        """브라우저 종료"""
        try:
            if self.driver:
                self.driver.quit()
                print("  브라우저 종료 ✓")
        except:
            pass
    
    @property
    def current_url(self):
        """현재 URL"""
        try:
            return self.driver.current_url if self.driver else None
        except:
            return None
    
    @property
    def page_source(self):
        """페이지 소스"""
        try:
            return self.driver.page_source if self.driver else None
        except:
            return None