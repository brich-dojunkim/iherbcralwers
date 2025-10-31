# coupang_manager.py
import os
import sys
import undetected_chromedriver as uc
import time
from typing import Optional

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from coupang_config import CoupangConfig

COUPANG_DOMAINS = ("coupang.com",)
# 쿠팡에서 딥링크로 접근 시 차단 가능성이 높은 경로 키워드
COUPANG_DEEPLINK_HINTS = ("/np/search", "/vp/product", "/vp/products", "/shop.coupang.com")

class BrowserManager:
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.driver = None

    # -----------------------
    # 내부 유틸
    # -----------------------
    def _is_coupang_deeplink(self, url: str) -> bool:
        if not url:
            return False
        return any(d in url for d in COUPANG_DOMAINS) and any(h in url for h in COUPANG_DEEPLINK_HINTS)

    def _is_coupang(self, url: str) -> bool:
        if not url:
            return False
        return any(d in url for d in COUPANG_DOMAINS)

    # -----------------------
    # 핵심: ‘클릭으로 이동’(Referer/쿠키 유지)
    # -----------------------
    def open_with_referrer(self, target_url: str, home: str = "https://www.coupang.com", settle_sec: float = 4.0):
        """쿠팡 홈에서 referer/쿠키 컨텍스트 확보 후 '클릭' 방식으로 target_url 진입"""
        d = self.driver
        if d is None:
            raise RuntimeError("driver not started. call start_driver() first.")

        cur = (d.current_url or "")
        if "coupang.com" not in cur:
            d.get(home)
            time.sleep(settle_sec)

        # 홈 문서에서 실제 '클릭' 이벤트로 이동 (브라우저가 Referer 자동 추가)
        d.execute_script("""
            const a = document.createElement('a');
            a.href = arguments[0];
            a.rel = 'noopener';
            a.target = '_self';
            document.body.appendChild(a);
            a.click();
        """, target_url)

    # 편의: 쿠팡이면 referrer 보존 이동, 아니면 일반 get
    def get_with_coupang_referrer(self, url: str, settle_sec: float = 4.0):
        if self._is_coupang(url):
            return self.open_with_referrer(url, settle_sec=settle_sec)
        else:
            return self.driver.get(url)

    # -----------------------
    # 드라이버 시작
    # -----------------------
    def start_driver(self) -> bool:
        """Chrome 드라이버 시작 - 설정 활용 + 안정화 옵션"""
        try:
            print("Chrome 드라이버 시작 중... (macOS)")
            print("단순 옵션으로 드라이버 시작...")

            opts = uc.ChromeOptions()
            # 설정에서 옵션 주입
            for option in getattr(CoupangConfig, "CHROME_OPTIONS_SIMPLE", []):
                opts.add_argument(option)

            # 안정화 권장 옵션(중복되면 Chrome이 무시)
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--window-size=1920,1080")
            if self.headless:
                opts.add_argument("--headless=new")

            # ✅ 핵심: use_subprocess=True, version_main=None
            self.driver = uc.Chrome(
                options=opts,
                use_subprocess=True,
                version_main=None
            )

            # 설정에서 제공하는 스텔스 스크립트 실행 (실패해도 무시)
            try:
                stealth_js = getattr(CoupangConfig, "WEBDRIVER_STEALTH_SCRIPT", None)
                if stealth_js:
                    self.driver.execute_script(stealth_js)
            except Exception:
                pass

            # -----------------------
            # ✅ 핵심: .get() 오토패치
            # -----------------------
            original_get = self.driver.get

            def smart_get(url: Optional[str] = None):
                # 쿠팡 + 딥링크 라면: referrer/쿠키 컨텍스트로 ‘클릭 이동’
                if url and self._is_coupang_deeplink(url):
                    return self.open_with_referrer(url)
                # 그 외: 기존 동작
                return original_get(url)

            # driver.get 를 스마트 버전으로 바꿉니다 (monitoring.py 변경 불필요)
            self.driver.get = smart_get  # type: ignore[attr-defined]

            print("Chrome 드라이버 시작 완료")
            return True

        except Exception as e:
            print(f"드라이버 시작 실패: {e}")

            # 최후의 수단: 아무 옵션 없이(그래도 use_subprocess 유지 권장)
            try:
                print("최후 수단: 기본 드라이버 시작...")
                self.driver = uc.Chrome(use_subprocess=True, version_main=None)
                print("기본 드라이버 시작 성공")

                # .get 오토패치 (옵션 없는 드라이버에도 동일 적용)
                original_get = self.driver.get
                def smart_get(url: Optional[str] = None):
                    if url and self._is_coupang_deeplink(url):
                        return self.open_with_referrer(url)
                    return original_get(url)
                self.driver.get = smart_get  # type: ignore[attr-defined]

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
