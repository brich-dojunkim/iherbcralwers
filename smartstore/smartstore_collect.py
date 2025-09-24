#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
네이버 쇼핑 검색결과 실시간 수집 (페이지네이션 지원)
→ 페이지를 실제로 열고 스크롤하면서 '현재 DOM/HTML'을 **적극 파싱**하여
   스마트스토어 베이스 URL(https://smartstore.naver.com/<store>)을 최대한 많이 수집.

강화 포인트
- 스크롤로 지연로딩된 결과까지 모두 노출 (가변 높이 대응)
- DOM 수집: a[href], [data-href], [data-url], [onclick] 등 광범위 추출
- HTML 정규식 파싱: outlink(url=...) 및 직접 smartstore 링크를 page_source에서 추가 추출
- outlink의 url 파라미터를 디코드해 실제 스마트스토어로 복원
- 로그인 후 warm-up 순회 제거 (요청사항 반영)

사용:
  pip install undetected-chromedriver

예:
  python3 smartstore_collect.py \
    --start-url "https://search.shopping.naver.com/search/all?adQuery=%ED%97%8C%ED%84%B0&agency=true&origQuery=%ED%97%8C%ED%84%B0&pagingIndex=1&pagingSize=40&productSet=total&query=%ED%97%8C%ED%84%B0&sort=rel&timestamp=&viewType=list" \
    --pages 10 \
    --out stores_hunter.txt
"""

import os, re, time, html, argparse, json
from typing import List, Set, Optional, Iterable
from urllib.parse import urlparse, parse_qs, unquote, urlencode, urlunparse, quote

import undetected_chromedriver as uc
from selenium.webdriver.common.by   import By
from selenium.webdriver.support.ui  import WebDriverWait
from selenium.webdriver.support     import expected_conditions as EC
from selenium.common.exceptions     import TimeoutException, UnexpectedAlertPresentException, JavascriptException

# ===== 설정 =====
PAGE_WAIT = 12
CAPTCHA_WAIT = 120
SCROLL_MAX_STEPS = 30          # 스크롤 단계 수(페이지 길이에 따라 충분히 크게)
SCROLL_PAUSE = 0.35            # 스크롤 사이 대기
SCROLL_SETTLE_PAUSE = 0.6      # 마지막 안정화 대기
USER_DATA_DIR = os.path.expanduser("~/.cache/ossomall_chrome")  # 영구 프로필
os.makedirs(USER_DATA_DIR, exist_ok=True)

NID_LOGIN_URL_TMPL = "https://nid.naver.com/nidlogin.login?mode=form&url={return_url}"
GLOBAL_CAPTCHA_HOST_SUBSTRING = "ncpt.naver.com"

SMARTSTORE_BASE_RE = re.compile(r"https?://smartstore\.naver\.com/([A-Za-z0-9._-]+)", re.I)
OUTLINK_RE = re.compile(
    r"https?://smartstore\.naver\.com/inflow/outlink/url\?[^\"'>]*\burl=([^\"'&]+)",
    re.I
)

def make_driver(headless=False):
    opt = uc.ChromeOptions()
    opt.add_argument("--disable-popup-blocking")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-blink-features=AutomationControlled")
    # 영구 프로필 + SameSite 완화
    opt.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    opt.add_argument("--profile-directory=Default")
    opt.add_argument("--disable-features=SameSiteByDefaultCookies,CookiesWithoutSameSiteMustBeSecure")
    if headless:
        opt.add_argument("--headless=new")
    d = uc.Chrome(options=opt)
    d.set_page_load_timeout(45)
    return d

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

def is_global_captcha_page(driver) -> bool:
    try:
        return GLOBAL_CAPTCHA_HOST_SUBSTRING in (driver.current_url or "").lower()
    except UnexpectedAlertPresentException:
        try: driver.switch_to.alert.accept()
        except Exception: pass
        return False

def maybe_handle_global_captcha(driver, expected_url=None, where=""):
    if not is_global_captcha_page(driver): return
    print(f"🔒 전역 보안 캡차({where}). 캡차 해결 후 Enter…")
    try: input()
    except EOFError: time.sleep(CAPTCHA_WAIT)
    if expected_url and is_global_captcha_page(driver):
        driver.get(expected_url)
    time.sleep(0.7)

def check_captcha_or_block(driver) -> bool:
    try:
        cur = (driver.current_url or "").lower()
        htmlsrc = (driver.page_source or "").lower()
        if GLOBAL_CAPTCHA_HOST_SUBSTRING in cur: return True
        indicators = ['접속이 일시적으로 제한','비정상적인 접근','자동입력 방지','보안문자','captcha','blocked','로그인','nidlogin']
        if any(s in cur for s in ['captcha','blocked','nidlogin']) or any(s in htmlsrc for s in indicators):
            return True
        return False
    except Exception:
        return False

def prelogin_once(driver, target_url: str):
    """로그인 필요 시 NID로 1회만 → 통과 후 곧바로 target_url 복귀 (warm-up 없음)."""
    target_url = normalize_url(target_url) or target_url
    print(f"🔗 초기 진입: {target_url}")
    driver.get("https://www.naver.com")
    try: WebDriverWait(driver, PAGE_WAIT).until(lambda d: d.execute_script("return document.readyState")=="complete")
    except Exception: pass
    driver.get(target_url)
    try: WebDriverWait(driver, PAGE_WAIT).until(lambda d: d.execute_script("return document.readyState")=="complete")
    except Exception: pass
    maybe_handle_global_captcha(driver, expected_url=target_url, where="초기 진입")
    if check_captcha_or_block(driver):
        login_url = NID_LOGIN_URL_TMPL.format(return_url=quote(target_url, safe=''))
        print("🟡 로그인/차단 감지 → NID 로그인 이동")
        driver.get(login_url)
        print("🟢 로그인/보안문자 해결 후 Enter…")
        try: input()
        except EOFError: time.sleep(CAPTCHA_WAIT)
        print("↩️  원래 URL로 복귀")
        driver.get(target_url)
        try: WebDriverWait(driver, PAGE_WAIT).until(lambda d: d.execute_script("return document.readyState")=="complete")
        except Exception: pass
        maybe_handle_global_captcha(driver, expected_url=target_url, where="복귀")

# ---------- 실시간(라이브) 파싱 핵심 ----------
def scroll_full_page(driver, max_steps=SCROLL_MAX_STEPS, pause=SCROLL_PAUSE):
    last_h = -1
    same_count = 0
    for _ in range(max_steps):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h:
            same_count += 1
            if same_count >= 2:  # 두 번 연속 높이 그대로면 종료
                break
        else:
            same_count = 0
        last_h = h
    time.sleep(SCROLL_SETTLE_PAUSE)

def js_collect_candidates(driver) -> List[str]:
    """
    현재 DOM에서 링크 후보를 광범위로 수집:
    - a[href]
    - [data-href], [data-url]
    - [onclick]에 포함된 URL
    - role="link"인 요소의 href-like 속성
    """
    script = r"""
    const urls = new Set();

    // 1) a[href]
    document.querySelectorAll('a[href]').forEach(a => {
      let u = a.getAttribute('href') || '';
      if (u) urls.add(u);
    });

    // 2) data-href, data-url, data-link, data-mall-url 등 일반적인 data 속성들
    const ATTRS = ['data-href','data-url','data-link','data-mall-url','data-nclick','data-nv-mall-url','data-nv-url'];
    ATTRS.forEach(attr => {
      document.querySelectorAll(`[${attr}]`).forEach(el => {
        let v = el.getAttribute(attr);
        if (v) urls.add(v);
      });
    });

    // 3) onclick 내부의 URL
    document.querySelectorAll('[onclick]').forEach(el => {
      let v = el.getAttribute('onclick') || '';
      // crude extraction
      const m = v.match(/https?:\/\/[^\s"'<>]+/g);
      if (m) m.forEach(x => urls.add(x));
    });

    // 4) role="link"인 요소가 href-like 속성을 가진 경우
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

        # 광고(adcr)는 네비게이션 없이는 해석 불가 → 스킵
        if "/adcr" in lo:
            continue

        # 1) outlink에서 추출
        base = extract_from_outlink(href)
        if not base:
            # 2) 직접 smartstore 링크
            if "smartstore.naver.com" in lo:
                base = to_store_base(href)

        if base:
            stores.add(base)
    return stores

def collect_from_current_page(driver) -> List[str]:
    """
    1) 스크롤로 지연 로딩 요소 노출
    2) JS로 DOM 기반 후보 수집
    3) page_source 정규식으로 보강
    """
    scroll_full_page(driver)

    # DOM 기반
    dom_candidates = js_collect_candidates(driver)
    dom_stores = extract_smartstores_from_sources(dom_candidates)

    # HTML 정규식 기반 (page_source 전체 스캔)
    src = driver.page_source or ""
    src_candidates = []

    # outlink url= 파라미터
    for m in OUTLINK_RE.finditer(src):
        src_candidates.append(f"https://smartstore.naver.com/inflow/outlink/url?url={m.group(1)}")

    # 직접 스마트스토어 링크
    for m in SMARTSTORE_BASE_RE.finditer(src):
        src_candidates.append(m.group(0))

    src_stores = extract_smartstores_from_sources(src_candidates)

    all_stores = sorted((dom_stores | src_stores))
    print(f"  - DOM후보: {len(dom_candidates)} / DOM스마트스토어: {len(dom_stores)} / HTML스마트스토어: {len(src_stores)}")
    return all_stores

# ===== 메인 =====
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-url", required=True, help="네이버 쇼핑 검색결과 URL(1페이지 기준)")
    ap.add_argument("--pages", type=int, default=1, help="가져올 페이지 수(기본=1)")
    ap.add_argument("--out", default="stores.txt", help="저장 파일(.txt, 한 줄당 1개)")
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()

    base_url = normalize_url(args.start_url) or args.start_url
    driver = make_driver(headless=args.headless)
    all_stores: Set[str] = set()

    try:
        # 로그인 1회 (warm-up 없음)
        prelogin_once(driver, base_url)

        # 페이지네이션 순회 (검색결과 페이지만 방문)
        for idx in range(1, args.pages + 1):
            page_url = with_paging_index(base_url, idx)
            print(f"\n📄 페이지 {idx}/{args.pages}: {page_url}")
            driver.get(page_url)
            try:
                WebDriverWait(driver, PAGE_WAIT).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except Exception:
                pass
            maybe_handle_global_captcha(driver, expected_url=page_url, where=f"{idx}페이지 로드")

            stores = collect_from_current_page(driver)
            print(f"  → 수집: {len(stores)}개")
            all_stores.update(stores)

        if not all_stores:
            print("⚠️ 스마트스토어 링크를 찾지 못했습니다.")
            return

        with open(args.out, "w", encoding="utf-8") as f:
            for u in sorted(all_stores):
                f.write(u + "\n")
        print(f"\n✅ 저장 완료: {args.out} (총 {len(all_stores)}개 고유 스토어)")

    finally:
        try: driver.quit()
        except Exception: pass

if __name__ == "__main__":
    main()
