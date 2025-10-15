#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
네이버 쇼핑 → 스마트스토어 URL 수집 (키워드 배치, 자동 재개)
- URL 단위 재개(progress/*.done)
- 키워드마다 브라우저 리셋
- 기본: 키워드 시작 시 로그인 페이지로 이동 → 인증/보안문자 해결 → 타겟 URL 복귀
- 로그인/캡차 감지 시:
  * 기본(권장): --auto-resume (ON) → 폴링으로 자동 재개
  * 옵션: --interactive → 수동 프롬프트(Enter/force/skip/open)
- 캡차 복귀 후 '껍데기 DOM' 대응: 재하이드레이션 루틴(제스처/강스크롤/필터터치/리프레시) 후 재수집
- 페이지별 부분 결과(progress/*.stores.txt) 저장, 종료 시 병합하여 --out에 중복 제거 저장

예시:
  python3 batch_collect.py --url-file urls.txt --pages 10 --out stores_unique.txt
  # 수동 프롬프트 모드
  python3 batch_collect.py --url-file urls.txt --pages 10 --out stores_unique.txt --interactive
"""

import os, re, time, html, json, argparse, sys, hashlib
from pathlib import Path
from typing import List, Set, Iterable, Optional
from urllib.parse import urlparse, parse_qs, unquote, urlencode, urlunparse, quote
from datetime import datetime

import undetected_chromedriver as uc
from selenium.webdriver.common.by   import By
from selenium.webdriver.support.ui  import WebDriverWait
from selenium.webdriver.support     import expected_conditions as EC
from selenium.common.exceptions     import TimeoutException, UnexpectedAlertPresentException, JavascriptException, WebDriverException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys

# ============ 설정 ============
PAGE_WAIT = 12
SCROLL_MAX_STEPS = 30
SCROLL_PAUSE = 0.35
SCROLL_SETTLE_PAUSE = 0.6
SAFE_GET_MAX_RETRIES = 3
SAFE_GET_STABILIZE_SLEEP = 0.8

USER_DATA_DIR = os.path.expanduser("~/.cache/ossomall_chrome_batch")
os.makedirs(USER_DATA_DIR, exist_ok=True)

PROGRESS_DIR = Path("./progress")
PROGRESS_DIR.mkdir(exist_ok=True)
DEBUG_DIR = Path("./debug")
DEBUG_DIR.mkdir(exist_ok=True)

LOGIN_URL_TMPL = "https://nid.naver.com/nidlogin.login?mode=form&url={return_url}"
GLOBAL_CAPTCHA_HOST_SUBSTRING = "ncpt.naver.com"
GLOBAL_CAPTCHA_INDICATORS = [
    "자동입력 방지", "보안문자", "비정상적인 접근", "접속이 일시적으로 제한",
    "captcha", "blocked"
]
LOGIN_HOST_SUBSTRING = "nid.naver.com"

SEARCH_SUCCESS_XPATHS = [
    "//div[contains(@class,'basicList_list') or contains(@class,'list_basis') or contains(@class,'product_list')]",
    "//ul[contains(@class,'list_basis') or contains(@class,'list_area')]",
    "//a[contains(@href,'smartstore.naver.com')]"
]

SMARTSTORE_BASE_RE = re.compile(r"https?://smartstore\.naver\.com/([A-Za-z0-9._-]+)", re.I)
OUTLINK_RE = re.compile(
    r"https?://smartstore\.naver\.com/inflow/outlink/url\?[^\"'>]*\burl=([^\"'&]+)",
    re.I
)

# ============ 유틸 ============
def normalize_url(u: Optional[str]) -> Optional[str]:
    if not u: return None
    u = u.strip()
    if not u: return None
    if not u.startswith(("http://", "https://")):
        u = "https://" + u.lstrip("/")
    return u

def with_paging_index(url: str, index: int) -> str:
    pr = urlparse(url)
    qs = parse_qs(pr.query, keep_blank_values=True)
    qs["pagingIndex"] = [str(index)]
    new_q = urlencode(qs, doseq=True)
    return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))

def to_store_base(url: str) -> Optional[str]:
    m = SMARTSTORE_BASE_RE.search(url.strip())
    if not m: return None
    return f"https://smartstore.naver.com/{m.group(1)}"

def extract_from_outlink(url: str) -> Optional[str]:
    m = OUTLINK_RE.search(url)
    if not m:
        return None
    raw = m.group(1)
    decoded = html.unescape(unquote(raw))
    return to_store_base(decoded)

def url_hash(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:12]

def paths_for_url(idx: int, url: str):
    h = url_hash(url)
    base = f"{idx:04d}_{h}"
    done = PROGRESS_DIR / f"{base}.done"
    stores_txt = PROGRESS_DIR / f"{base}.stores.txt"
    return done, stores_txt, base

# ============ DOM/상태 판정 ============
def page_contains_any(driver, xpaths: List[str]) -> bool:
    try:
        for xp in xpaths:
            if driver.find_elements(By.XPATH, xp):
                return True
    except Exception:
        pass
    return False

def search_dom_looks_ok(driver) -> bool:
    return page_contains_any(driver, SEARCH_SUCCESS_XPATHS)

def is_login_redirect(driver) -> bool:
    try:
        return LOGIN_HOST_SUBSTRING in (driver.current_url or "").lower()
    except Exception:
        return False

def is_global_captcha_page(driver) -> bool:
    try:
        cur = (driver.current_url or "").lower()
        htmlsrc = (driver.page_source or "").lower()
        looks_like_captcha = (GLOBAL_CAPTCHA_HOST_SUBSTRING in cur) or any(ind in htmlsrc for ind in GLOBAL_CAPTCHA_INDICATORS)
        if looks_like_captcha and search_dom_looks_ok(driver):
            return False
        return looks_like_captcha
    except Exception:
        return False

def _dump_debug_snapshot(driver, prefix="captcha"):
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        cur = (driver.current_url or "about:blank")
        title = (driver.title or "").strip()
        html_src = driver.page_source or ""
        p = DEBUG_DIR / f"{prefix}_{ts}.html"
        p.write_text(html_src, encoding="utf-8")
        print(f"[DEBUG] current_url: {cur}")
        print(f"[DEBUG] title      : {title}")
        print(f"[DEBUG] snapshot   : {p}")
    except Exception as e:
        print(f"[DEBUG] snapshot error: {e}")

# ============ 대기/재개: 자동 폴링 & 수동 프롬프트 ============
def auto_wait_issue_clear(driver, expected_url: Optional[str], reason: str,
                          poll_interval: float, poll_timeout: int,
                          auto_refresh_every: int = 30) -> bool:
    """
    자동 폴링으로 로그인/캡차 해제 여부를 확인하고, 해제되면 True.
    poll_timeout(초) 내 해제 안 되면 False.
    """
    print(f"\n🔒 {reason} 상태 감지 → 자동 폴링으로 해제 감시 시작 "
          f"(interval={poll_interval}s, timeout={poll_timeout}s)")
    _dump_debug_snapshot(driver, prefix=reason.replace('/', '_'))

    start = time.time()
    last_refresh = time.time()
    while True:
        # 타임아웃
        if time.time() - start > poll_timeout:
            print("⏱️ 타임아웃: 자동 재개 실패")
            return False

        # 주기적 새로고침 시도(옵션)
        if expected_url and (time.time() - last_refresh) >= auto_refresh_every:
            try:
                driver.get(expected_url)
                last_refresh = time.time()
            except Exception:
                pass

        time.sleep(poll_interval)

        # 해제 확인
        if not is_login_redirect(driver) and not is_global_captcha_page(driver):
            # 검색결과면 DOM도 확인
            if "search.shopping.naver.com" in (driver.current_url or ""):
                if search_dom_looks_ok(driver):
                    print("✅ 자동 감지: 정상 DOM 확인 → 수집 재개")
                    return True
                else:
                    # 아직 하이드레이션 전일 수 있음 → 다음 루프
                    continue
            else:
                print("✅ 자동 감지: 문제 해제 → 수집 재개")
                return True

def prompt_wait_issue_clear(driver, expected_url: Optional[str], reason: str):
    """
    기존 수동 프롬프트 모드.
    """
    while True:
        print(f"\n🔒 {reason} 상태 감지")
        _dump_debug_snapshot(driver, prefix=reason.replace('/', '_'))
        print("브라우저에서 문제(캡차/로그인)를 해결한 뒤, 아래 중 하나를 입력하세요.")
        print("  [Enter] 다시 검사 / [force] 강제 진행 / [skip] 이 키워드 건너뛰기 / [open] 새로고침")
        try:
            cmd = input("> ").strip().lower()
        except KeyboardInterrupt:
            raise
        except EOFError:
            cmd = ""

        if cmd == "skip":
            raise RuntimeError("USER_SKIPPED")

        if cmd == "open" and expected_url:
            try:
                driver.get(expected_url)
                time.sleep(1.0)
            except Exception:
                pass

        if cmd == "force":
            print("✅ 사용자 강제 진행 선택 → 계속 진행합니다.")
            return True

        if not is_login_redirect(driver) and not is_global_captcha_page(driver):
            if "search.shopping.naver.com" in (driver.current_url or ""):
                if search_dom_looks_ok(driver):
                    print("✅ 정상 DOM 확인 → 수집 재개")
                    return True
                else:
                    print("… 아직 검색 결과 DOM이 보이지 않습니다.")
                    continue
            else:
                print("✅ 문제 해제 → 수집 재개")
                return True

        print("⚠️ 여전히 캡차/로그인 상태로 보입니다.")

# ============ 안전 이동 ============
def safe_get(driver, url: str, wait_complete_seconds: int = PAGE_WAIT,
             auto_resume: bool = True, poll_interval: float = 3.0,
             poll_timeout: int = 600, interactive: bool = False):
    url = normalize_url(url) or url
    for attempt in range(1, SAFE_GET_MAX_RETRIES + 1):
        driver.get(url)
        try:
            WebDriverWait(driver, wait_complete_seconds).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except Exception:
            pass

        # 상태 처리
        if is_login_redirect(driver) or is_global_captcha_page(driver):
            reason = "로그인" if is_login_redirect(driver) else "캡차"
            if auto_resume and not interactive:
                ok = auto_wait_issue_clear(driver, expected_url=url, reason=reason,
                                           poll_interval=poll_interval, poll_timeout=poll_timeout)
                if not ok:
                    # 자동 실패 → 마지막에 한번 더 수동 시도 가능
                    print("자동 재개 실패 → 수동 모드 시도")
                    ok = prompt_wait_issue_clear(driver, expected_url=url, reason=reason)
                if ok:
                    # 해제된 상태에서 루프 마무리
                    pass
                else:
                    raise RuntimeError("AUTO_RESUME_FAILED")
            else:
                # 수동 프롬프트 모드
                ok = prompt_wait_issue_clear(driver, expected_url=url, reason=reason)
                if not ok:
                    raise RuntimeError("USER_ABORT")

        time.sleep(SAFE_GET_STABILIZE_SLEEP)

        # 안정화 판정
        if "search.shopping.naver.com" in (driver.current_url or ""):
            if search_dom_looks_ok(driver):
                return
        else:
            if not (is_login_redirect(driver) or is_global_captcha_page(driver)):
                return

        print(f"… 페이지 안정화 재시도 {attempt}/{SAFE_GET_MAX_RETRIES}")
        time.sleep(0.7)

    # 최대 재시도 후에도 문제면 마지막으로 사용자 개입 요청
    if is_login_redirect(driver) or is_global_captcha_page(driver):
        reason = "로그인" if is_login_redirect(driver) else "캡차"
        if auto_resume and not interactive:
            ok = auto_wait_issue_clear(driver, expected_url=url, reason=reason,
                                       poll_interval=poll_interval, poll_timeout=poll_timeout)
            if ok:
                return
        # 그래도 안되면 수동으로 한 번 더
        prompt_wait_issue_clear(driver, expected_url=url, reason=reason)

# ============ 드라이버 ============
def make_driver(headless=False):
    opt = uc.ChromeOptions()
    opt.add_argument("--disable-popup-blocking")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    opt.add_argument("--profile-directory=Default")
    opt.add_argument("--disable-features=SameSiteByDefaultCookies,CookiesWithoutSameSiteMustBeSecure")
    if headless:
        opt.add_argument("--headless=new")
    d = uc.Chrome(options=opt)
    d.set_page_load_timeout(45)
    return d

# ============ 스크롤 & 수집 ============
def scroll_full_page(driver, max_steps=SCROLL_MAX_STEPS, pause=SCROLL_PAUSE):
    last_h = -1
    same_count = 0
    for _ in range(max_steps):
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        except WebDriverException:
            pass
        time.sleep(pause)
        try:
            h = driver.execute_script("return document.body.scrollHeight;")
        except WebDriverException:
            h = last_h
        if h == last_h:
            same_count += 1
            if same_count >= 2:
                break
        else:
            same_count = 0
        last_h = h
    time.sleep(SCROLL_SETTLE_PAUSE)

def js_collect_candidates(driver) -> List[str]:
    script = r"""
    const urls = new Set();
    document.querySelectorAll('a[href]').forEach(a => {
      let u = a.getAttribute('href') || '';
      if (u) urls.add(u);
    });
    const ATTRS = ['data-href','data-url','data-link','data-mall-url','data-nv-mall-url','data-nv-url'];
    ATTRS.forEach(attr => {
      document.querySelectorAll(`[${attr}]`).forEach(el => {
        let v = el.getAttribute(attr);
        if (v) urls.add(v);
      });
    });
    document.querySelectorAll('[onclick]').forEach(el => {
      let v = el.getAttribute('onclick') || '';
      const m = v.match(/https?:\/\/[^\s"'<>]+/g);
      if (m) m.forEach(x => urls.add(x));
    });
    document.querySelectorAll('[role="link"]').forEach(el => {
      let v = el.getAttribute('href') || el.getAttribute('data-href') || el.getAttribute('data-url');
      if (v) urls.add(v);
    });
    return Array.from(urls);
    """
    try:
        return driver.execute_script(script)
    except JavascriptException:
        return []

def extract_smartstores_from_sources(sources: Iterable[str]) -> Set[str]:
    stores: Set[str] = set()
    for href in sources:
        if not href: continue
        href = html.unescape(href)
        lo = href.lower()
        if "/adcr" in lo:
            continue
        base = extract_from_outlink(href)
        if not base and "smartstore.naver.com" in lo:
            base = to_store_base(href)
        if base:
            stores.add(base)
    return stores

def collect_from_current_page(driver) -> List[str]:
    scroll_full_page(driver)
    dom_candidates = js_collect_candidates(driver)
    dom_stores = extract_smartstores_from_sources(dom_candidates)
    src = driver.page_source or ""
    src_candidates = []
    for m in OUTLINK_RE.finditer(src):
        src_candidates.append(f"https://smartstore.naver.com/inflow/outlink/url?url={m.group(1)}")
    for m in SMARTSTORE_BASE_RE.finditer(src):
        src_candidates.append(m.group(0))
    src_stores = extract_smartstores_from_sources(src_candidates)
    all_stores = sorted(dom_stores | src_stores)
    print(f"  - DOM후보:{len(dom_candidates)} / DOM스토어:{len(dom_stores)} / HTML스토어:{len(src_stores)}")
    return all_stores

# ============ 재하이드레이션 ============
def ensure_results_hydrated(driver, page_url: str, attempts: int = 2) -> bool:
    for attempt in range(1, attempts + 1):
        try:
            try:
                ActionChains(driver).move_by_offset(10, 10).perform()
            except Exception:
                pass
            try:
                ActionChains(driver).send_keys(Keys.PAGE_DOWN).perform()
                time.sleep(0.2)
                ActionChains(driver).send_keys(Keys.PAGE_UP).perform()
            except Exception:
                pass
            for _ in range(3):
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(0.2)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                driver.execute_script("window.dispatchEvent(new Event('scroll'));")
                time.sleep(0.4)
            try:
                candidates = driver.find_elements(By.XPATH, "//a[contains(@role,'tab') or contains(@class,'_filter') or contains(@class,'tab')]")
                if candidates:
                    try:
                        driver.execute_script("arguments[0].click();", candidates[0])
                    except Exception:
                        candidates[0].click()
                    time.sleep(0.6)
            except Exception:
                pass
            time.sleep(0.8)
            if search_dom_looks_ok(driver):
                anchors = driver.find_elements(By.XPATH, "//a[contains(@href,'smartstore.naver.com')]")
                if anchors:
                    return True
            if attempt == attempts:
                driver.refresh()
                time.sleep(1.0)
                if search_dom_looks_ok(driver):
                    anchors = driver.find_elements(By.XPATH, "//a[contains(@href,'smartstore.naver.com')]")
                    if anchors:
                        return True
        except Exception:
            time.sleep(0.5)
    return False

def collect_with_rehydrate(driver, page_url: str) -> List[str]:
    stores = collect_from_current_page(driver)
    if len(stores) > 0:
        return stores
    has_many_candidates = False
    try:
        cands = driver.find_elements(By.CSS_SELECTOR, 'a[href], [data-href], [data-url]')
        has_many_candidates = len(cands) >= 80
    except Exception:
        pass
    hydrated = ensure_results_hydrated(driver, page_url, attempts=2)
    if hydrated or has_many_candidates:
        stores_retry = collect_from_current_page(driver)
        if len(stores_retry) > 0:
            print("  ↻ 재하이드레이션 후 재수집 성공")
            return stores_retry
    return stores

# ============ 로그인 & 이동 ============
def go_login_then_return(driver, target_url: str, auto_resume: bool, poll_interval: float,
                         poll_timeout: int, interactive: bool):
    login_url = LOGIN_URL_TMPL.format(return_url=quote(normalize_url(target_url) or target_url, safe=""))
    print("🔐 로그인 페이지로 이동:", login_url)
    safe_get(driver, login_url, auto_resume=auto_resume, poll_interval=poll_interval,
             poll_timeout=poll_timeout, interactive=interactive)
    print("↩️ 로그인 후 타겟 URL 안정화 확인:", target_url)
    safe_get(driver, target_url, auto_resume=auto_resume, poll_interval=poll_interval,
             poll_timeout=poll_timeout, interactive=interactive)

# ============ 키워드(검색 URL) 단위 수집 ============
def make_driver(headless=False):
    opt = uc.ChromeOptions()
    opt.add_argument("--disable-popup-blocking")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    opt.add_argument("--profile-directory=Default")
    opt.add_argument("--disable-features=SameSiteByDefaultCookies,CookiesWithoutSameSiteMustBeSecure")
    if headless:
        opt.add_argument("--headless=new")
    d = uc.Chrome(options=opt)
    d.set_page_load_timeout(45)
    return d

def collect_one_search(idx: int, start_url: str, pages: int, headless: bool,
                       no_login_first: bool, stores_txt_path: Path,
                       auto_resume: bool, poll_interval: float, poll_timeout: int,
                       interactive: bool) -> Set[str]:
    stores: Set[str] = set()
    url = normalize_url(start_url) or start_url
    driver = make_driver(headless=headless)
    try:
        if not no_login_first:
            go_login_then_return(driver, url, auto_resume=auto_resume,
                                 poll_interval=poll_interval, poll_timeout=poll_timeout,
                                 interactive=interactive)
        for p in range(1, pages + 1):
            page_url = with_paging_index(url, p)
            print(f"📄 [{idx+1}] 페이지 {p}/{pages}: {page_url}")
            safe_get(driver, page_url, auto_resume=auto_resume,
                     poll_interval=poll_interval, poll_timeout=poll_timeout,
                     interactive=interactive)
            page_stores = collect_with_rehydrate(driver, page_url)
            print(f"  → 수집: {len(page_stores)}개")
            stores.update(page_stores)
            if stores_txt_path:
                try:
                    with stores_txt_path.open("a", encoding="utf-8") as f:
                        for s in page_stores:
                            f.write(s + "\n")
                except Exception as e:
                    print(f"부분 저장 실패: {e}")
    except KeyboardInterrupt:
        print("⏸️ 페이지 루프 중단됨(KeyboardInterrupt). 현재까지 수집분 반환.")
        return stores
    finally:
        try: driver.quit()
        except Exception: pass
    return stores

# ============ 메인 ============
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url-file", required=True)
    ap.add_argument("--pages", type=int, default=5)
    ap.add_argument("--out", default="stores_unique.txt")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--no-login-first", action="store_true",
                    help="키워드 시작 시 로그인 선행 비활성화")
    # 자동 재개 관련
    ap.add_argument("--interactive", action="store_true",
                    help="캡차/로그인 시 수동 프롬프트 모드 사용")
    ap.add_argument("--poll-interval", type=float, default=3.0,
                    help="자동 재개 폴링 간격(초)")
    ap.add_argument("--poll-timeout", type=int, default=600,
                    help="자동 재개 타임아웃(초)")
    args = ap.parse_args()

    url_lines = [line.strip() for line in open(args.url_file, encoding="utf-8") if line.strip()]
    if not url_lines:
        print("URL 목록이 비어 있습니다."); return
    print(f"총 키워드(검색 URL): {len(url_lines)}")

    all_unique: Set[str] = set()

    for idx, start_url in enumerate(url_lines):
        done_path, stores_txt_path, base = paths_for_url(idx, start_url)
        if done_path.exists():
            print(f"\n=== [{idx+1}/{len(url_lines)}] 이미 완료(.done) → 건너뜀: {start_url}")
            if stores_txt_path.exists():
                try:
                    for line in stores_txt_path.read_text(encoding="utf-8").splitlines():
                        if line.strip(): all_unique.add(line.strip())
                except Exception as e:
                    print(f"부분 결과 읽기 실패: {e}")
            continue

        print(f"\n=== [{idx+1}/{len(url_lines)}] 수집 시작 ===")
        print(f"키워드 URL: {start_url}")
        if stores_txt_path.exists():
            try: stores_txt_path.unlink()
            except Exception: pass

        try:
            stores = collect_one_search(
                idx, start_url, pages=args.pages, headless=args.headless,
                no_login_first=args.no_login_first, stores_txt_path=stores_txt_path,
                auto_resume=(not args.interactive),
                poll_interval=args.poll_interval, poll_timeout=args.poll_timeout,
                interactive=args.interactive
            )
        except RuntimeError as e:
            if str(e) == "USER_SKIPPED":
                print("↷ 사용자 요청으로 이 키워드 건너뜀.")
                stores = set()
            else:
                print(f"⚠️ 런타임 예외: {e}")
                break
        except KeyboardInterrupt:
            print("\n⏹️ 사용자 중단 감지 → 현재 URL까지 부분 결과 반영 후 종료")
            break
        except Exception as e:
            print(f"⚠️ 수집 중 예외 발생: {e}")
            break

        all_unique.update(stores)
        try:
            done_path.write_text("done", encoding="utf-8")
        except Exception as e:
            print(f".done 파일 생성 실패: {e}")

        print(f"→ 키워드 완료. 누적 고유 스토어 수: {len(all_unique)}")
        try:
            Path(args.out).write_text("\n".join(sorted(all_unique)) + "\n", encoding="utf-8")
            print(f"  (중간 저장) {args.out}")
        except Exception as e:
            print(f"중간 저장 실패: {e}")

    # 종료 시 부분결과 병합
    try:
        for p in PROGRESS_DIR.glob("*.stores.txt"):
            try:
                for line in p.read_text(encoding="utf-8").splitlines():
                    if line.strip(): all_unique.add(line.strip())
            except Exception:
                pass
    except Exception:
        pass

    final_sorted = sorted(all_unique)
    try:
        Path(args.out).write_text("\n".join(final_sorted) + "\n", encoding="utf-8")
        print(f"\n✅ 최종 저장: {args.out} (고유 스토어 {len(final_sorted)}개)")
    except Exception as e:
        print(f"\n⚠️ 최종 저장 실패: {e}")
    print("작업 종료")

if __name__ == "__main__":
    main()
