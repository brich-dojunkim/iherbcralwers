"""
iHerb 스크래퍼 패키지 초기화
"""

from .main import EnglishIHerbScraper
from .config import Config
from .browser_manager import BrowserManager
from .iherb_client import IHerbClient
from .product_matcher import ProductMatcher
from .data_manager import DataManager

__version__ = "1.0.0"
__author__ = "iHerb Scraper Team"

__all__ = [
    'EnglishIHerbScraper',
    'Config',
    'BrowserManager',
    'IHerbClient',
    'ProductMatcher',
    'DataManager'
]