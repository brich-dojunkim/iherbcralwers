"""
쿠팡 제품 자동 매칭 시스템 (개선 버전)
- 실시간 저장 강화
- 이어서 실행 로직 개선
- utils.py 모듈 통합
"""

import sys
import os
import pandas as pd
import time
import re
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import json

# 현재 디렉토리를 sys.path에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# utils 모듈 import
from utils import ColumnManager, ResultSaver, ProgressTracker, StageManager

# 프로젝트 루트를 sys.path에 추가 (coupang 모듈 import용)
project_root = os.path.dirname(os.path.dirname(current_dir))  # iherb_price 디렉토리
if project_root not in sys.path:
    sys.path.insert(0, project_root)

@dataclass
class ProductInfo:
    """제품 정보 데이터 클래스"""
    name: str
    count: Optional[int] = None
    quantity: Optional[int] = None
    brand: Optional[str] = None
    product_type: Optional[str] = None
    original_name: str = ""
    

@dataclass
class CoupangProduct:
    """쿠팡 검색 결과 제품"""
    rank: int
    name: str
    count: Optional[int]
    quantity: Optional[int]
    price: int
    shipping_fee: int
    final_price: int
    unit_price: Optional[float]
    url: str
    brand: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    seller_type: str = "3P"


@dataclass
class MatchResult:
    """매칭 결과"""
    original_product: ProductInfo
    matched_product: Optional[CoupangProduct]
    confidence_level: str
    reason: str


class ProductParser:
    """제품명 파싱 유틸리티"""
    
    # Part Number 접두사 -> 브랜드 매핑 (실제 CSV 데이터 기반 확장)
    PART_NUMBER_BRAND_MAP = {
        # 주요 브랜드
        'DRB': '닥터스베스트',
        'NOW': '나우푸드',
        'LEX': '라이프익스텐션',
        'THR': '쏜리서치',
        'JRW': '재로우',
        'SRE': '스포츠리서치',
        'SOL': '솔가',
        'BLB': '블루보넷',
        
        # 추가 브랜드
        'QLL': 'Quality of Life',
        'NFS': '네추럴팩터스',
        'CDL': '차일드라이프',
        'INV': 'InnovixLabs',
        'AUN': '오로라뉴트라사이언스',
        'NOR': '노르딕내추럴스',
        'GOL': '가든오브라이프',
        'AGE': 'Amazing Grass',
        'PHV': 'Pure Hawaiian',
        'BOI': 'Boiron',
        'BGA': 'BioGaia',
        'PAR': '파라다이스허브',
        'WAK': '쿄릭',
        'ENZ': 'Enzymedica',
        'FHH': '이지맘',
        'OPN': '옵티멈뉴트리션',
        'RSH': 'RevitaLash',
        'ATK': '앳킨스',
        'GAI': '가이아허브',
        'CLF': '컨트리라이프',
        'ADC': '더마이',
        'NWY': '네이처스웨이',
        'OGA': '올게인',
        'MCL': '닥터메르콜라',
        'MLV': '마더러브',
        'EMT': 'Enzymatic Therapy',
        'NUT': '뉴트리콜로지',
        'CAL': '캘리포니아골드',
        'ZYM': 'Zymase',
        'AOR': 'AOR',
        'HEB': 'Herb Pharm',
        'MRM': 'MRM',
        'VSL': 'VSL',
        'REN': 'Renew Life',
        'ALI': 'Alive',
        'BIO': 'Bioray',
        'CHE': 'Cherimoya',
        'COL': 'Collagen',
        'DER': 'Derma E',
        'ECL': 'Eclectic',
        'FLO': 'Flora',
        'FUC': 'Fucoidanz',
        'GEN': 'Genestra',
        'HOS': 'Host Defense',
        'INT': 'Integrative',
        'KAL': 'KAL',
        'LIF': 'LifeSeasons',
        'MAG': 'Magnesium',
        'MET': 'Metabolic',
        'NAT': 'Natrol',
        'NEU': 'Neurobiologix',
        'OPT': 'Optimum',
        'PRO': 'Protocol',
        'QUE': 'Quest',
        'REG': 'Regenemax',
        'SOU': 'Source Naturals',
        'SUN': 'Sundown',
        'SWA': '스완슨',
        'THE': 'Thera',
        'ULT': 'Ultimate',
        'VEG': 'VeganSmart',
        'WOM': 'Womens',
        'ZEN': 'Zenwise',
    }
    
    # 영문 브랜드 -> 한글 매핑 (정규화)
    BRAND_NAME_MAP = {
        # === 메이저 브랜드 (영문 -> 한글) ===
        "doctor's best": '닥터스베스트',
        'doctors best': '닥터스베스트',
        'dr best': '닥터스베스트',
        'now foods': '나우푸드',
        'now': '나우푸드',
        'life extension': '라이프익스텐션',
        'lifeextension': '라이프익스텐션',
        'thorne': '쏜리서치',
        'thorne research': '쏜리서치',
        'jarrow': '재로우',
        'jarrow formulas': '재로우',
        'sports research': '스포츠리서치',
        'sportsresearch': '스포츠리서치',
        'solgar': '솔가',
        'garden of life': '가든오브라이프',
        'gardenoflife': '가든오브라이프',
        "nature's way": '네이처스웨이',
        'natures way': '네이처스웨이',
        'naturesway': '네이처스웨이',
        'bluebonnet': '블루보넷',
        'blue bonnet': '블루보넷',
        
        # === 추가 브랜드 (영문 -> 한글) ===
        'quality of life': 'Quality of Life',
        'nutricology': '뉴트리콜로지',
        'natural factors': '네추럴팩터스',
        'naturalfactors': '네추럴팩터스',
        'childlife': '차일드라이프',
        'child life': '차일드라이프',
        'innovixlabs': 'InnovixLabs',
        'innovix labs': 'InnovixLabs',
        'aurora nutrascience': '오로라뉴트라사이언스',
        'aurora': '오로라뉴트라사이언스',
        'nordic naturals': '노르딕내추럴스',
        'nordicnaturals': '노르딕내추럴스',
        'amazing grass': 'Amazing Grass',
        'amazinggrass': 'Amazing Grass',
        'pure hawaiian': 'Pure Hawaiian',
        'boiron': 'Boiron',
        'biogaia': 'BioGaia',
        'paradise herbs': '파라다이스허브',
        'paradiseherbs': '파라다이스허브',
        'wakunaga': '쿄릭',
        'kyolic': '쿄릭',
        'enzymedica': 'Enzymedica',
        'fairhaven': '이지맘',
        'fairhaven health': '이지맘',
        'optimum nutrition': '옵티멈뉴트리션',
        'optimumnutrition': '옵티멈뉴트리션',
        'revitalash': 'RevitaLash',
        'atkins': '앳킨스',
        'gaia herbs': '가이아허브',
        'gaiaherbs': '가이아허브',
        'country life': '컨트리라이프',
        'countrylife': '컨트리라이프',
        'derma e': '더마이',
        'dermae': '더마이',
        'orgain': '올게인',
        'dr mercola': '닥터메르콜라',
        'dr. mercola': '닥터메르콜라',
        'mercola': '닥터메르콜라',
        'motherlove': '마더러브',
        'mother love': '마더러브',
        'enzymatic therapy': 'Enzymatic Therapy',
        'enzymatictherapy': 'Enzymatic Therapy',
        'california gold': '캘리포니아골드',
        'california gold nutrition': '캘리포니아골드',
        'swanson': '스완슨',
        
        # === 추가 영문 브랜드 ===
        'source naturals': '소스내추럴스',
        'sourcenaturals': '소스내추럴스',
        'nature made': '네이처메이드',
        'naturemade': '네이처메이드',
        'kirkland': '커클랜드',
        'kirkland signature': '커클랜드',
        'puritan': '퓨리탄',
        "puritan's pride": '퓨리탄',
        'puritans pride': '퓨리탄',
        'vitafusion': '비타퓨전',
        'viva naturals': '비바내추럴스',
        'vivanaturals': '비바내추럴스',
        'natrol': '내트롤',
        'nature bounty': '네이처바운티',
        "nature's bounty": '네이처바운티',
        'natures bounty': '네이처바운티',
        'gnc': 'GNC',
        'centrum': '센트룸',
        'mega food': '메가푸드',
        'megafood': '메가푸드',
        'new chapter': '뉴챕터',
        'newchapter': '뉴챕터',
        'rainbow light': '레인보우라이트',
        'rainbowlight': '레인보우라이트',
        'bluebonnet nutrition': '블루보넷',
        'bluebonnetnutrition': '블루보넷',
        'renew life': '리뉴라이프',
        'renewlife': '리뉴라이프',
        'native': '네이티브',
        'quest': '퀘스트',
        'quest nutrition': '퀘스트',
        'kal': 'KAL',
        'alvita': '알비타',
        'herb pharm': '허브팜',
        'herbpharm': '허브팜',
        'eclectic institute': '이클렉틱',
        'eclecticinstitute': '이클렉틱',
        'planetary herbals': '플래네터리허벌스',
        'planetaryherbals': '플래네터리허벌스',
        'mrm': 'MRM',
        'allergy research': '알러지리서치',
        'allergyresearch': '알러지리서치',
        'pure encapsulations': '퓨어인캡슐레이션스',
        'pureencapsulations': '퓨어인캡슐레이션스',
        'integrative therapeutics': '인테그러티브',
        'integrativetherapeutics': '인테그러티브',
        'douglas labs': '더글라스',
        'douglaslabs': '더글라스',
        'designs for health': '디자인스포헬스',
        'designsforhealth': '디자인스포헬스',
        'klaire labs': '클레어랩스',
        'klairelabs': '클레어랩스',
        'biotics research': '바이오틱스',
        'bioticsresearch': '바이오틱스',
        'xymogen': '자이모젠',
        'ortho molecular': '오르소몰레큘러',
        'orthomolecular': '오르소몰레큘러',
        'metagenics': '메타제닉스',
        'pure': '퓨어',
        'bluebonnet': '블루보넷',
        'zhou': '저우',
        'zhou nutrition': '저우',
        'sports nutrition': '스포츠뉴트리션',
        'bsn': 'BSN',
        'cellucor': '셀루코어',
        'muscletech': '머슬테크',
        'muscle tech': '머슬테크',
        'dymatize': '다이마타이즈',
        'universal nutrition': '유니버셜',
        'universal': '유니버셜',
        'iso': 'ISO',
        'bulk supplements': '벌크서플리먼츠',
        'bulksupplements': '벌크서플리먼츠',
        'nutrition': '뉴트리션',
        'nutricost': '뉴트리코스트',
        'sports': '스포츠',
        'vital proteins': '바이탈프로틴스',
        'vitalproteins': '바이탈프로틴스',
        'ancient nutrition': '에인션트뉴트리션',
        'ancientnutrition': '에인션트뉴트리션',
        'primal': '프라이멀',
        'primal kitchen': '프라이멀',
        'paleovalley': '팔레오밸리',
        'paleo valley': '팔레오밸리',
        'naturelo': '내츄렐로',
        'ritual': '리추얼',
        'hum': 'HUM',
        'hum nutrition': 'HUM',
        'olly': '올리',
        'smartypants': '스마티팬츠',
        'smarty pants': '스마티팬츠',
        
        # === 한글 브랜드 (정규화 - 띄어쓰기 제거) ===
        '닥터스베스트': '닥터스베스트',
        '닥터스 베스트': '닥터스베스트',
        '닥터 베스트': '닥터스베스트',
        '닥터베스트': '닥터스베스트',
        '나우푸드': '나우푸드',
        '나우 푸드': '나우푸드',
        '나우 후드': '나우푸드',
        '라이프익스텐션': '라이프익스텐션',
        '라이프 익스텐션': '라이프익스텐션',
        '라이프 엑스텐션': '라이프익스텐션',
        '쏜리서치': '쏜리서치',
        '쏜 리서치': '쏜리서치',
        '쏜': '쏜리서치',
        '재로우': '재로우',
        '자로우': '재로우',
        '제로우': '재로우',
        '스포츠리서치': '스포츠리서치',
        '스포츠 리서치': '스포츠리서치',
        '솔가': '솔가',
        '가든오브라이프': '가든오브라이프',
        '가든 오브 라이프': '가든오브라이프',
        '가든 오프 라이프': '가든오브라이프',
        '네이처스웨이': '네이처스웨이',
        '네이처스 웨이': '네이처스웨이',
        '네추럴팩터스': '네추럴팩터스',
        '네추럴 팩터스': '네추럴팩터스',
        '내추럴팩터스': '네추럴팩터스',
        '블루보넷': '블루보넷',
        '차일드라이프': '차일드라이프',
        '오로라뉴트라사이언스': '오로라뉴트라사이언스',
        '오로라 뉴트라사이언스': '오로라뉴트라사이언스',
        '노르딕내추럴스': '노르딕내추럴스',
        '노르딕 내추럴스': '노르딕내추럴스',
        '파라다이스허브': '파라다이스허브',
        '파라다이스 허브': '파라다이스허브',
        '쿄릭': '쿄릭',
        '이지맘': '이지맘',
        '옵티멈뉴트리션': '옵티멈뉴트리션',
        '옵티멈 뉴트리션': '옵티멈뉴트리션',
        '앳킨스': '앳킨스',
        '가이아허브': '가이아허브',
        '가이아 허브': '가이아허브',
        '컨트리라이프': '컨트리라이프',
        '컨트리 라이프': '컨트리라이프',
        '더마이': '더마이',
        '올게인': '올게인',
        '닥터메르콜라': '닥터메르콜라',
        '닥터 메르콜라': '닥터메르콜라',
        '마더러브': '마더러브',
        '뉴트리콜로지': '뉴트리콜로지',
        '캘리포니아골드': '캘리포니아골드',
        '캘리포니아 골드': '캘리포니아골드',
        '스완슨': '스완슨',
        '소스내추럴스': '소스내추럴스',
        '소스 내추럴스': '소스내추럴스',
        '네이처메이드': '네이처메이드',
        '네이처 메이드': '네이처메이드',
        '커클랜드': '커클랜드',
        '퓨리탄': '퓨리탄',
        '비타퓨전': '비타퓨전',
        '비바내추럴스': '비바내추럴스',
        '내트롤': '내트롤',
        '네이처바운티': '네이처바운티',
        '네이처 바운티': '네이처바운티',
        '센트룸': '센트룸',
        '메가푸드': '메가푸드',
        '뉴챕터': '뉴챕터',
        '레인보우라이트': '레인보우라이트',
        '리뉴라이프': '리뉴라이프',
        '네이티브': '네이티브',
        '퀘스트': '퀘스트',
        '알비타': '알비타',
        '허브팜': '허브팜',
        '이클렉틱': '이클렉틱',
        '플래네터리허벌스': '플래네터리허벌스',
        '알러지리서치': '알러지리서치',
        '퓨어인캡슐레이션스': '퓨어인캡슐레이션스',
        '인테그러티브': '인테그러티브',
        '더글라스': '더글라스',
        '디자인스포헬스': '디자인스포헬스',
        '클레어랩스': '클레어랩스',
        '바이오틱스': '바이오틱스',
        '자이모젠': '자이모젠',
        '오르소몰레큘러': '오르소몰레큘러',
        '메타제닉스': '메타제닉스',
        '퓨어': '퓨어',
        '저우': '저우',
        '스포츠뉴트리션': '스포츠뉴트리션',
        '셀루코어': '셀루코어',
        '머슬테크': '머슬테크',
        '다이마타이즈': '다이마타이즈',
        '유니버셜': '유니버셜',
        '벌크서플리먼츠': '벌크서플리먼츠',
        '뉴트리션': '뉴트리션',
        '뉴트리코스트': '뉴트리코스트',
        '바이탈프로틴스': '바이탈프로틴스',
        '에인션트뉴트리션': '에인션트뉴트리션',
        '프라이멀': '프라이멀',
        '팔레오밸리': '팔레오밸리',
        '내츄렐로': '내츄렐로',
        '리추얼': '리추얼',
        '올리': '올리',
        '스마티팬츠': '스마티팬츠',
    }
    
    # 정수 추출 패턴
    COUNT_PATTERNS = [
        r'(\d+)정',
        r'(\d+)개입',
        r'(\d+)캡슐',
        r'(\d+)캡',
        r'(\d+)베지캡',
        r'(\d+)소프트젤',
        r'(\d+)소프트겔',
        r'(\d+)알',
        r'(\d+)tablets?',
        r'(\d+)capsules?',
        r'(\d+)softgels?',
    ]
    
    # 개수 추출 패턴
    QUANTITY_PATTERNS = [
        r'(\d+)개',
        r'(\d+)병',
        r'(\d+)팩',
    ]
    
    @staticmethod
    def extract_count(text: str) -> Optional[int]:
        """텍스트에서 정수 추출"""
        if not text:
            return None
        
        text = text.lower()
        for pattern in ProductParser.COUNT_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    count = int(match.group(1))
                    if 10 <= count <= 1000:
                        return count
                except (ValueError, IndexError):
                    continue
        return None
    
    @staticmethod
    def extract_quantity(text: str) -> Optional[int]:
        """텍스트에서 개수 추출"""
        if not text:
            return None
        
        text = text.lower()
        for pattern in ProductParser.QUANTITY_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    quantity = int(match.group(1))
                    if 1 <= quantity <= 20:
                        return quantity
                except (ValueError, IndexError):
                    continue
        return None
    
    @staticmethod
    def extract_brand_from_partnumber(part_number: str) -> Optional[str]:
        """Part Number에서 브랜드 추출"""
        if not part_number:
            return None
        
        prefix = part_number[:3].upper()
        return ProductParser.PART_NUMBER_BRAND_MAP.get(prefix)
    
    @staticmethod
    def extract_brand(text: str) -> Optional[str]:
        """텍스트에서 브랜드명 추출"""
        if not text:
            return None
        
        text_lower = text.lower()
        
        for key, normalized_brand in ProductParser.BRAND_NAME_MAP.items():
            if key in text_lower:
                return normalized_brand
        
        return None
    
    @staticmethod
    def parse_product_name(name: str, part_number: str = None) -> ProductInfo:
        """제품명 전체 파싱"""
        brand = ProductParser.extract_brand_from_partnumber(part_number) if part_number else None
        
        if not brand:
            brand = ProductParser.extract_brand(name)
        
        return ProductInfo(
            name=name,
            count=ProductParser.extract_count(name),
            quantity=ProductParser.extract_quantity(name),
            brand=brand,
            original_name=name
        )
    
    @staticmethod
    def clean_url(url: str) -> str:
        """쿠팡 URL 정리"""
        if not url:
            return url
        
        try:
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
            
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            
            essential_params = {}
            if 'itemId' in params:
                essential_params['itemId'] = params['itemId'][0]
            
            if 'vendorItemId' in params:
                essential_params['vendorItemId'] = params['vendorItemId'][0]
            elif 'lptag' in params:
                essential_params['lptag'] = params['lptag'][0]
            
            new_query = urlencode(essential_params)
            clean_url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                '',
                new_query,
                ''
            ))
            
            return clean_url
        except:
            return url


class CoupangSearcher:
    """쿠팡 검색 및 파싱"""
    
    def __init__(self, browser_manager=None):
        self.browser = browser_manager
    
    def search_and_get_top_products(self, query: str, top_n: int = 4) -> List[CoupangProduct]:
        """쿠팡에서 검색하고 상위 N개 제품 반환"""
        if not self.browser:
            raise ValueError("Browser manager is not initialized")
        
        products = []
        
        try:
            search_url = f"https://www.coupang.com/np/search?q={query}"
            
            print(f"  검색 URL: {search_url}")
            self.browser.get_with_coupang_referrer(search_url)
            time.sleep(3)
            
            self._apply_single_item_filter()
            time.sleep(2)
            
            products = self._parse_search_results(top_n)
            
        except Exception as e:
            print(f"  ✗ 검색 중 오류: {e}")
        
        return products
    
    def _apply_single_item_filter(self):
        """낱개상품 필터 적용"""
        try:
            driver = self.browser.driver
            
            filter_script = """
            const filterLabels = document.querySelectorAll('label');
            for (let label of filterLabels) {
                const text = label.textContent.trim();
                if (text.includes('낱개상품')) {
                    label.click();
                    return true;
                }
            }
            return false;
            """
            
            result = driver.execute_script(filter_script)
            if result:
                print("  ✓ 낱개상품 필터 적용")
            else:
                print("  ⚠ 낱개상품 필터 없음")
                
        except Exception as e:
            print(f"  ✗ 필터 적용 실패: {e}")
    
    def _parse_search_results(self, top_n: int) -> List[CoupangProduct]:
        """검색 결과 파싱"""
        products = []
        
        try:
            driver = self.browser.driver
            product_items = driver.find_elements("css selector", "li.ProductUnit_productUnit__Qd6sv")
            
            for idx, item in enumerate(product_items[:top_n]):
                try:
                    name_elem = item.find_element("css selector", "div.ProductUnit_productNameV2__cV9cw")
                    name = name_elem.text.strip()
                    
                    link_elem = item.find_element("css selector", "a")
                    raw_url = link_elem.get_attribute("href")
                    url = ProductParser.clean_url(raw_url)
                    
                    try:
                        price_elem = item.find_element("css selector", "div.custom-oos.fw-text-\\[20px\\]\\/\\[24px\\]")
                        price_text = price_elem.text.strip().replace(",", "").replace("원", "")
                        price = int(price_text)
                    except:
                        price = 0
                    
                    shipping_fee = 0
                    try:
                        fee_elem = item.find_element("css selector", "div.TextBadge_feePrice__n_gta")
                        fee_text = fee_elem.text.strip()
                        
                        if "무료배송" in fee_text and "조건부" not in fee_text:
                            shipping_fee = 0
                        else:
                            import re
                            match = re.search(r'배송비\s*([\d,]+)원', fee_text)
                            if match:
                                shipping_fee = int(match.group(1).replace(",", ""))
                    except:
                        shipping_fee = 0
                    
                    final_price = price + shipping_fee
                    
                    count = ProductParser.extract_count(name)
                    quantity = ProductParser.extract_quantity(name)
                    brand = ProductParser.extract_brand(name)
                    
                    seller_type = "3P"
                    try:
                        item.find_element("css selector", "img[src*='logo_jikgu']")
                        seller_type = "로켓직구"
                    except:
                        seller_type = "3P"
                    
                    product = CoupangProduct(
                        rank=idx + 1,
                        name=name,
                        count=count,
                        quantity=quantity,
                        price=price,
                        shipping_fee=shipping_fee,
                        final_price=final_price,
                        unit_price=None,
                        url=url,
                        brand=brand,
                        rating=None,
                        review_count=None,
                        seller_type=seller_type
                    )
                    
                    products.append(product)
                    
                    if shipping_fee > 0:
                        print(f"  ✓ [{idx+1}] {name[:50]}... ({count}정 x {quantity}개, {final_price:,}원)")
                    else:
                        print(f"  ✓ [{idx+1}] {name[:50]}... ({count}정 x {quantity}개, {final_price:,}원)")
                    
                except Exception as e:
                    print(f"  ✗ 제품 파싱 실패 [{idx+1}]: {e}")
                    continue
            
        except Exception as e:
            print(f"  ✗ 검색 결과 파싱 실패: {e}")
        
        return products


class SimpleMatcher:
    """정수, 개수, 가격 기반 간단한 매칭"""
    
    def match_products(
        self, 
        original: ProductInfo, 
        candidates: List[CoupangProduct]
    ) -> MatchResult:
        """정수와 개수가 모두 일치하는 제품 중 최저가 선택"""
        if not candidates:
            return MatchResult(
                original_product=original,
                matched_product=None,
                confidence_level="검토필요",
                reason="검색 결과 없음"
            )
        
        original_qty = original.quantity if original.quantity else 1
        
        matched = [
            c for c in candidates 
            if c.count == original.count and (c.quantity if c.quantity else 1) == original_qty
        ]
        
        if not matched:
            return MatchResult(
                original_product=original,
                matched_product=None,
                confidence_level="검토필요",
                reason=f"정수·개수 일치하는 제품 없음 (원본: {original.count}정 x {original_qty}개)"
            )
        
        brand_matched = []
        if original.brand:
            for product in matched:
                product_brand = ProductParser.extract_brand(product.name)
                if product_brand and original.brand == product_brand:
                    brand_matched.append(product)
        
        if brand_matched:
            selected = min(brand_matched, key=lambda x: x.final_price if x.final_price > 0 else float('inf'))
            confidence = "확신"
            brand_match = True
        else:
            selected = min(matched, key=lambda x: x.final_price if x.final_price > 0 else float('inf'))
            confidence = "검토필요"
            brand_match = False
        
        if selected.shipping_fee > 0:
            reason = f"정수·개수 일치({original.count}정 x {original_qty}개), 가격 {selected.final_price:,}원 (상품: {selected.price:,}원 + 배송비: {selected.shipping_fee:,}원)"
        else:
            reason = f"정수·개수 일치({original.count}정 x {original_qty}개), 가격 {selected.final_price:,}원"
        
        if brand_match:
            reason += f", 브랜드 일치({original.brand})"
        
        return MatchResult(
            original_product=original,
            matched_product=selected,
            confidence_level=confidence,
            reason=reason
        )


class ProductMatchingSystem:
    """전체 제품 매칭 시스템 (개선 버전)"""
    
    def __init__(self, csv_path: str, browser_manager=None, output_path: str = None):
        """
        Args:
            csv_path: 입력 CSV 파일 경로
            browser_manager: BrowserManager 인스턴스
            output_path: 출력 CSV 경로
        """
        self.csv_path = csv_path
        
        if output_path is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            output_dir = os.path.join(current_dir, 'outputs')
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, 'matching_results.csv')
        
        self.output_path = output_path
        self.df = None
        self.searcher = CoupangSearcher(browser_manager)
        self.matcher = SimpleMatcher()
        self.saver = ResultSaver(output_path)
    
    def initialize(self):
        """
        초기화 및 컬럼 준비
        - CSV 로드
        - 기존 결과 확인
        - 필요한 컬럼 생성
        """
        print(f"\n{'='*60}")
        print(f"초기화 및 컬럼 준비")
        print(f"{'='*60}\n")
        
        # CSV 로드
        if os.path.exists(self.output_path):
            print(f"✓ 기존 결과 파일 로드: {self.output_path}")
            self.df = pd.read_csv(self.output_path, encoding='utf-8-sig')
        else:
            print(f"✓ 새 작업 시작: {self.csv_path}")
            self.df = pd.read_csv(self.csv_path, encoding='utf-8')
            self.df = self.df[self.df['상품명_20251024'].notna()]
            
            # 원본제품명 컬럼 생성
            if '원본제품명' not in self.df.columns:
                self.df['원본제품명'] = self.df['상품명_20251024']
        
        # 모든 컬럼 생성
        self.df = ColumnManager.ensure_columns(self.df)
        
        # 사유 컬럼 파싱
        self.df = ColumnManager.parse_reason_column(self.df)
        
        # 저장
        self.saver.save(self.df, "초기화 완료")
        
        print(f"✓ 전체 데이터: {len(self.df)}개")
        print(f"{'='*60}\n")
        
        return True
    
    def process_matching(self, start_idx: int = 0, end_idx: Optional[int] = None):
        """
        제품 매칭 단계
        """
        StageManager.print_stage_header('matching')
        
        # 이미 처리된 제품 확인
        processed_ids = self.saver.get_processed_ids(self.df, 'matching')
        print(f"✓ 이미 처리 완료: {len(processed_ids)}개\n")
        
        # 처리할 제품 필터링
        end_idx = end_idx or len(self.df)
        to_process = self.df.iloc[start_idx:end_idx]
        to_process = to_process[~to_process['Part Number'].astype(str).isin(processed_ids)]
        
        if len(to_process) == 0:
            print("✓ 매칭할 제품이 없습니다\n")
            return
        
        print(f"✓ 매칭 대상: {len(to_process)}개\n")
        
        # 진행 상황 추적
        tracker = ProgressTracker(len(to_process), "제품 매칭")
        
        for idx, row in to_process.iterrows():
            part_number = str(row['Part Number'])
            original_name = row['원본제품명']
            
            tracker.print_progress(part_number)
            print(f"  원본: {original_name}")
            
            # 1. 제품 정보 파싱
            original = ProductParser.parse_product_name(original_name, part_number)
            print(f"  정수: {original.count}, 개수: {original.quantity}개, 브랜드: {original.brand}")
            
            # 2. 쿠팡 검색
            print(f"  쿠팡 검색 중...")
            search_query = original_name
            
            if original.brand:
                product_name_normalized = search_query.lower().replace(" ", "")
                brand_normalized = original.brand.lower().replace(" ", "")
                
                if brand_normalized not in product_name_normalized:
                    search_query = f"{original.brand} {search_query}"
                    print(f"  브랜드 추가: {original.brand}")
            
            candidates = self.searcher.search_and_get_top_products(search_query, top_n=4)
            
            if not candidates:
                print(f"  ✗ 검색 결과 없음")
                self.df.at[idx, '사유'] = '검색 결과 없음'
                self.df.at[idx, '정수개수일치'] = False
                self.df.at[idx, '브랜드일치'] = False
                self.df.at[idx, '정수개수일치_상세'] = ''
                self.df.at[idx, '브랜드일치_상세'] = ''
                self.saver.save(self.df, f"저장 ({tracker.current}/{tracker.total})")
                tracker.update(success=False)
                time.sleep(2)
                continue
            
            # 3. 매칭
            print(f"  매칭 중...")
            match_result = self.matcher.match_products(original, candidates)
            
            # 4. 결과 저장
            if match_result.matched_product:
                print(f"  ✓ 매칭: {match_result.matched_product.name[:50]}...")
                print(f"    신뢰도: {match_result.confidence_level}")
                
                self.df.at[idx, '매칭제품명'] = match_result.matched_product.name
                self.df.at[idx, '매칭URL'] = match_result.matched_product.url
                self.df.at[idx, '가격'] = match_result.matched_product.final_price
                self.df.at[idx, '판매유형'] = match_result.matched_product.seller_type
                
                tracker.update(success=True)
            else:
                print(f"  ✗ 매칭 실패: {match_result.reason}")
                tracker.update(success=False)
            
            # 사유 저장
            self.df.at[idx, '사유'] = match_result.reason
            
            # 사유에서 4개 컬럼 추출
            import re
            reason = match_result.reason
            
            # 정수·개수 일치 파싱
            count_pattern = r'정수[^(]*\(([^)]+)\)'
            count_match = re.search(count_pattern, reason)
            if count_match:
                self.df.at[idx, '정수개수일치'] = True
                self.df.at[idx, '정수개수일치_상세'] = count_match.group(1)
            else:
                self.df.at[idx, '정수개수일치'] = False
                self.df.at[idx, '정수개수일치_상세'] = ''
            
            # 브랜드 일치 파싱
            brand_pattern = r'브랜드[^(]*\(([^)]+)\)'
            brand_match = re.search(brand_pattern, reason)
            if brand_match:
                self.df.at[idx, '브랜드일치'] = True
                self.df.at[idx, '브랜드일치_상세'] = brand_match.group(1)
            else:
                self.df.at[idx, '브랜드일치'] = False
                self.df.at[idx, '브랜드일치_상세'] = ''
            
            # 실시간 저장
            self.saver.save(self.df, f"저장 ({tracker.current}/{tracker.total})")
            
            time.sleep(2)
        
        # 최종 컬럼 순서 정렬
        self.df = ColumnManager.reorder_columns(self.df)
        self.saver.save(self.df, "최종 저장 (컬럼 순서 정렬)")
        
        tracker.print_summary()


# 사용 예시
if __name__ == "__main__":
    print("product_matcher.py 모듈 로드 완료 (개선 버전)")