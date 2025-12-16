#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Selenium 관련 유틸리티
"""

from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote

import undetected_chromedriver as uc


def create_driver(headless: bool = False, profile_name: str = "default"):
    """undetected-chromedriver 생성"""
    options = uc.ChromeOptions()
    
    profile_base = Path.home() / ".chrome_automation_profiles"
    profile_base.mkdir(parents=True, exist_ok=True)
    
    profile_dir = profile_base / profile_name
    profile_dir.mkdir(parents=True, exist_ok=True)
    
    options.add_argument(f"--user-data-dir={profile_dir}")
    
    if headless:
        options.add_argument("--headless=new")
    
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1400,1000")
    options.add_argument("--lang=en-US")
    
    driver = uc.Chrome(options=options, version_main=None)
    driver.set_page_load_timeout(60)
    
    return driver


def save_debug_info(driver, step_name: str, seq: str = "unknown", debug_dir: Path = None):
    """디버그 정보 저장"""
    if debug_dir is None:
        debug_dir = Path(__file__).parent.parent / "debug"
    
    debug_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = f"{timestamp}_{seq}_{step_name}"
    
    try:
        html_path = debug_dir / f"{prefix}.html"
        html_content = driver.page_source
        html_path.write_text(html_content, encoding='utf-8')
        print(f"  [DEBUG] HTML 저장: {html_path.name}")
    except Exception as e:
        print(f"  [DEBUG ERROR] HTML 저장 실패: {e}")
    
    try:
        screenshot_path = debug_dir / f"{prefix}.png"
        driver.save_screenshot(str(screenshot_path))
        print(f"  [DEBUG] 스크린샷 저장: {screenshot_path.name}")
    except Exception as e:
        print(f"  [DEBUG ERROR] 스크린샷 저장 실패: {e}")


def extract_real_url(google_redirect_url: str) -> str:
    """구글 redirect URL에서 실제 URL 추출"""
    try:
        if "/url?q=" in google_redirect_url:
            parsed = urlparse(google_redirect_url)
            params = parse_qs(parsed.query)
            if 'q' in params:
                return unquote(params['q'][0])
        return google_redirect_url
    except:
        return google_redirect_url