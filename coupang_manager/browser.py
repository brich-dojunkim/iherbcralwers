"""
쿠팡 브라우저 관리
undetected-chromedriver 기반
"""

import undetected_chromedriver as uc
import time
import random


class BrowserManager:
    """쿠팡 전용 브라우저 관리자"""
    
    def __init__(self, headless=False):
        """
        Args:
            headless: 헤드리스 모드 여부
        """
        print("  undetected-chromedriver로 초기화 중...")
        
        options = uc.ChromeOptions()
        
        # 최소한의 설정
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--lang=ko-KR')
        
        if headless:
            options.add_argument('--headless=new')
        
        self.driver = uc.Chrome(
            options=options,
            use_subprocess=False,
            version_main=None
        )
        
        self._last_request_time = time.time()
        
        print("  ✓ 브라우저 준비 완료")
    
    def get_with_coupang_referrer(self, url: str):
        """
        쿠팡 URL 접근 (Referer 설정)
        
        Args:
            url: 접근할 URL
        """
        # 최소 2초 대기
        elapsed = time.time() - self._last_request_time
        if elapsed < 2:
            time.sleep(2 - elapsed + random.uniform(0.5, 1.5))
        
        # 메인 페이지가 아니면 먼저 메인 방문
        if "coupang.com" not in self.driver.current_url:
            self.driver.get("https://www.coupang.com")
            time.sleep(3)
        
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
            print("  ✓ 브라우저 종료")
