"""
긴급 수정: 쿠팡 차단 우회 - 어제까지 됐는데 오늘 갑자기 안될 때
"""

import undetected_chromedriver as uc
import time
from coupang_config import CoupangConfig


class BrowserManager:
    def __init__(self, headless=False):
        self.headless = headless
        self.driver = None
    
    def start_driver(self):
        """Chrome 드라이버 시작 - 긴급 수정 버전"""
        try:
            print("Chrome 드라이버 시작 중... (긴급 수정 버전)")
            
            # 방법 1: undetected-chromedriver 강제 업데이트 후 재시작
            print("\n🔧 시도 1: 최신 버전으로 드라이버 강제 업데이트")
            try:
                # version_main 파라미터로 최신 Chrome 버전 강제 지정
                self.driver = uc.Chrome(
                    headless=self.headless,
                    use_subprocess=True,  # 서브프로세스 사용
                    version_main=None,     # 자동 감지
                )
                
                # 추가 스텔스 설정
                self._apply_stealth_settings()
                
                print("✅ 드라이버 시작 성공 (방법 1)")
                return True
                
            except Exception as e1:
                print(f"⚠️ 방법 1 실패: {e1}")
            
            # 방법 2: 옵션 최소화 + 드라이버 캐시 무시
            print("\n🔧 시도 2: 최소 옵션 + 캐시 무시")
            try:
                options = uc.ChromeOptions()
                
                # 최소한의 옵션만
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-blink-features=AutomationControlled')
                
                # 캐시 무시하고 새로 다운로드
                self.driver = uc.Chrome(
                    options=options,
                    headless=self.headless,
                    driver_executable_path=None,  # 캐시 무시
                )
                
                self._apply_stealth_settings()
                
                print("✅ 드라이버 시작 성공 (방법 2)")
                return True
                
            except Exception as e2:
                print(f"⚠️ 방법 2 실패: {e2}")
            
            # 방법 3: 완전 초기화
            print("\n🔧 시도 3: 완전 기본 설정")
            try:
                self.driver = uc.Chrome(headless=self.headless)
                self._apply_stealth_settings()
                
                print("✅ 드라이버 시작 성공 (방법 3)")
                return True
                
            except Exception as e3:
                print(f"❌ 모든 시도 실패: {e3}")
                return False
                
        except Exception as e:
            print(f"❌ 예상치 못한 오류: {e}")
            return False
    
    def _apply_stealth_settings(self):
        """스텔스 설정 적용"""
        try:
            # 웹드라이버 감지 제거
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    
                    // Chrome 객체 추가
                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                    
                    // Permissions API 오버라이드
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );
                    
                    // Plugin 배열
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    
                    // 언어
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['ko-KR', 'ko', 'en-US', 'en']
                    });
                '''
            })
            
            print("✅ 스텔스 설정 적용 완료")
            
        except Exception as e:
            print(f"⚠️ 스텔스 설정 실패 (계속 진행): {e}")
    
    def close(self):
        """브라우저 종료"""
        try:
            if self.driver:
                print("브라우저 종료 중...")
                self.driver.quit()
        except:
            pass


# ============ 테스트용 ============
if __name__ == "__main__":
    print("="*80)
    print("🚨 긴급 수정 버전 테스트")
    print("="*80)
    
    manager = BrowserManager(headless=False)
    
    if manager.start_driver():
        print("\n✅ 드라이버 시작 성공!")
        print("쿠팡 접속 테스트 중...")
        
        try:
            manager.driver.get("https://www.coupang.com")
            time.sleep(5)
            
            title = manager.driver.title
            print(f"페이지 제목: {title}")
            
            if "차단" in title.lower() or "blocked" in title.lower():
                print("❌ 여전히 차단됨")
            else:
                print("✅ 정상 접속!")
            
            input("\n확인 후 Enter를 눌러 종료...")
            
        except Exception as e:
            print(f"❌ 테스트 실패: {e}")
        
        finally:
            manager.close()
    else:
        print("❌ 드라이버 시작 실패")