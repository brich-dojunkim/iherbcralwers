"""
쿠팡 브라우저 관리 - undetected-chromedriver 사용 (봇 감지 완전 우회)
"""

import undetected_chromedriver as uc
import time
import random


class BrowserManager:
    """undetected-chromedriver 기반 브라우저 관리자"""
    
    def __init__(self, headless=False):
        """
        Args:
            headless: 헤드리스 모드 여부
        """
        print("  undetected-chromedriver로 초기화 중...")
        
        options = uc.ChromeOptions()
        
        # 기본 설정
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-web-security')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        
        # 언어 설정
        options.add_argument('--lang=ko-KR')
        options.add_experimental_option('prefs', {
            'intl.accept_languages': 'ko-KR,ko,en-US,en'
        })
        
        if headless:
            options.add_argument('--headless=new')
        
        # undetected-chromedriver 초기화
        self.driver = uc.Chrome(
            options=options,
            use_subprocess=False,
            version_main=None  # 자동 감지
        )
        
        self._last_request_time = time.time()
        
        print("  ✓ undetected-chromedriver 준비 완료")
    
    def _rate_limit_wait(self):
        """요청 간 최소 대기 시간 보장"""
        current_time = time.time()
        elapsed = current_time - self._last_request_time
        
        if elapsed < 1.5:
            wait_time = 1.5 - elapsed + random.uniform(0.2, 0.8)
            time.sleep(wait_time)
        
        self._last_request_time = time.time()
    
    def get_with_coupang_referrer(self, url: str):
        """
        쿠팡 URL 접근 (Referer 설정)
        
        Args:
            url: 접근할 URL
        """
        self._rate_limit_wait()
        
        # 메인 페이지가 아니면 먼저 메인 방문
        if "coupang.com" not in self.driver.current_url:
            self.driver.get("https://www.coupang.com")
            time.sleep(random.uniform(2, 4))
        
        # JavaScript 클릭으로 이동
        script = """
            const a = document.createElement('a');
            a.href = arguments[0];
            a.rel = 'noopener';
            a.target = '_self';
            document.body.appendChild(a);
            a.click();
        """
        
        self.driver.execute_script(script, url)
        time.sleep(random.uniform(3, 5))
        
        self._last_request_time = time.time()
    
    def close(self):
        """브라우저 종료"""
        if self.driver:
            self.driver.quit()
            print("  브라우저 종료")