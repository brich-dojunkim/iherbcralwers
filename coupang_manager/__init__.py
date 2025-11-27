"""
Coupang Manager - 통합 쿠팡 크롤링 모듈

undetected-chromedriver 기반 (selenium import 없음)

사용 예시:
    from coupang_manager import CoupangBrowser, CoupangCrawler, CoupangSelectors
    
    browser = CoupangBrowser()
    crawler = CoupangCrawler(browser)
    products = crawler.search_products("비타민C")

의존성:
    pip install undetected-chromedriver
    
주의:
    selenium을 별도로 설치하지 마세요.
"""

__version__ = "1.0.0"
__author__ = "BRICH"
__date__ = "2024-11-25"

# 브라우저 관리
from .browser import BrowserManager as CoupangBrowser

# HTML 선택자 & 헬퍼
from .selectors import (
    CoupangSelectors,
    CoupangHTMLHelper,
    CoupangHTMLPatterns
)

# 데이터 모델
from .models import CoupangProduct


__all__ = [
    # 브라우저
    'CoupangBrowser',
    
    # 크롤러
    'CoupangCrawler',
    
    # 선택자
    'CoupangSelectors',
    'CoupangHTMLHelper',
    'CoupangHTMLPatterns',
    'CoupangHTMLStructure',
    
    # 모델
    'CoupangProduct',
]
