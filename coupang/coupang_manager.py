import undetected_chromedriver as uc
import time

class BrowserManager:
    def __init__(self, headless=False):
        self.headless = headless
        self.driver = None
        self.options = uc.ChromeOptions()
        
        # macOS에서 안정적으로 작동하는 옵션들만 사용
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_argument('--disable-extensions')
        self.options.add_argument('--disable-plugins-discovery')
        
        # 실험적 옵션 (macOS 호환)
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        
        # User-Agent 설정
        self.options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        if headless:
            self.options.add_argument('--headless')
            
        # 윈도우 크기 설정
        self.options.add_argument('--window-size=1920,1080')
    
    def start_driver(self):
        """Chrome 드라이버 시작 (macOS 최적화)"""
        try:
            print("Chrome 드라이버 시작 중... (macOS)")
            
            # macOS에서는 버전 감지를 수동으로 설정할 수도 있음
            self.driver = uc.Chrome(
                options=self.options,
                version_main=None,  # 자동 감지
                driver_executable_path=None,  # 자동 다운로드
                browser_executable_path=None,  # 시스템 Chrome 사용
            )
            
            # JavaScript로 웹드라이버 속성 숨기기
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['ko-KR', 'ko', 'en-US', 'en']
                });
                
                window.chrome = {
                    runtime: {}
                };
            """)
            
            print("Chrome 드라이버 시작 완료")
            return True
            
        except Exception as e:
            print(f"드라이버 시작 실패: {e}")
            
            # 대체 방법 시도
            try:
                print("대체 방법으로 드라이버 시작 시도...")
                # 더 단순한 옵션으로 재시도
                simple_options = uc.ChromeOptions()
                simple_options.add_argument('--no-sandbox')
                simple_options.add_argument('--disable-dev-shm-usage')
                
                if self.headless:
                    simple_options.add_argument('--headless')
                
                self.driver = uc.Chrome(options=simple_options)
                print("대체 방법으로 드라이버 시작 성공")
                return True
                
            except Exception as e2:
                print(f"대체 방법도 실패: {e2}")
                return False
    
    def close(self):
        """브라우저 종료"""
        try:
            if self.driver:
                print("브라우저 종료 중...")
                self.driver.quit()
        except:
            pass