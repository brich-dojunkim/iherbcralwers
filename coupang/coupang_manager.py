import undetected_chromedriver as uc
import time
from coupang_config import CoupangConfig

class BrowserManager:
    def __init__(self, headless=False):
        self.headless = headless
        self.driver = None
    
    def start_driver(self):
        """Chrome 드라이버 시작 - 설정 활용 버전"""
        try:
            print("Chrome 드라이버 시작 중... (macOS)")
            print("단순 옵션으로 드라이버 시작...")
            
            # 설정에서 단순 옵션들 가져오기
            simple_options = uc.ChromeOptions()
            for option in CoupangConfig.CHROME_OPTIONS_SIMPLE:
                simple_options.add_argument(option)
            
            if self.headless:
                simple_options.add_argument('--headless')
            
            # 드라이버 시작
            self.driver = uc.Chrome(options=simple_options)
            
            # 설정에서 웹드라이버 숨기기 스크립트 실행
            try:
                self.driver.execute_script(CoupangConfig.WEBDRIVER_STEALTH_SCRIPT)
            except:
                pass  # 실패해도 계속 진행
            
            print("Chrome 드라이버 시작 완료")
            return True
            
        except Exception as e:
            print(f"드라이버 시작 실패: {e}")
            
            # 최후의 수단: 아무 옵션 없이
            try:
                print("최후 수단: 기본 드라이버 시작...")
                self.driver = uc.Chrome()
                print("기본 드라이버 시작 성공")
                return True
            except Exception as final_e:
                print(f"모든 시도 실패: {final_e}")
                return False
    
    def close(self):
        """브라우저 종료"""
        try:
            if self.driver:
                print("브라우저 종료 중...")
                self.driver.quit()
        except:
            pass