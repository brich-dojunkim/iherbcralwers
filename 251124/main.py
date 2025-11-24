"""
GNC-쿠팡 상품 자동 매칭 시스템
기존 프로젝트의 coupang_manager.BrowserManager 사용
"""

import pandas as pd
from openpyxl import load_workbook
import time
import sys
import os
from typing import List, Optional, Dict
from dataclasses import dataclass, asdict
from datetime import datetime

# 프로젝트 루트 추가  
# 실행 경로: iherb_price/251124/
current_dir = os.path.dirname(os.path.abspath(__file__))  # 251124
project_root = os.path.dirname(current_dir)  # iherb_price
sys.path.insert(0, project_root)

# 기존 BrowserManager import
try:
    from coupang.coupang_manager import BrowserManager
    print("✓ BrowserManager 로드 성공")
except ImportError as e:
    print(f"✗ BrowserManager 로드 실패: {e}")
    print(f"현재 디렉토리: {current_dir}")
    print(f"프로젝트 루트: {project_root}")
    print("경로를 확인하세요")
    sys.exit(1)

from gnc_crawler import GNCCrawler, GNCProduct
from coupang_crawler import CoupangCrawler, CoupangProduct
from image_matcher import ImageMatcher, SimpleImageMatcher, ProductNameMatcher, CandidateSelector


@dataclass
class MatchResult:
    """매칭 결과"""
    no: int
    brand: str
    product_code: str
    product_name: str
    
    # GNC 정보
    gnc_search_result: str = ""
    gnc_url: str = ""
    gnc_thumbnail: str = ""
    gnc_count: Optional[int] = None
    
    # 쿠팡 검색
    coupang_query: str = ""
    coupang_candidates_count: int = 0
    
    # 선택된 쿠팡 상품
    coupang_name: str = ""
    coupang_url: str = ""
    coupang_price: int = 0
    coupang_shipping: int = 0
    coupang_final_price: int = 0
    coupang_count: Optional[int] = None
    coupang_brand: str = ""
    coupang_rating: Optional[float] = None
    coupang_reviews: Optional[int] = None
    coupang_seller: str = ""
    
    # 상품명 매칭 (Gemini)
    name_match: str = ""  # 일치/불일치
    name_confidence: str = ""  # high/medium/low
    name_reason: str = ""
    
    # 이미지 매칭
    image_match: str = ""
    image_confidence: str = ""
    image_reason: str = ""
    
    # 상태
    status: str = ""
    error_message: str = ""
    processed_at: str = ""


class ProductMatchingSystem:
    """전체 매칭 시스템"""
    
    def __init__(self, excel_path: str, gemini_api_key: Optional[str] = None, headless: bool = False):
        """
        Args:
            excel_path: 엑셀 파일 경로
            gemini_api_key: Gemini API 키
            headless: 헤드리스 모드
        """
        self.excel_path = excel_path
        self.headless = headless
        
        # BrowserManager
        self.browser = None
        
        # 크롤러
        self.gnc_crawler = None
        self.coupang_crawler = None
        
        # 이미지 매처
        if gemini_api_key:
            self.image_matcher = ImageMatcher(gemini_api_key)
            self.name_matcher = ProductNameMatcher(gemini_api_key)
            self.candidate_selector = CandidateSelector(gemini_api_key)
            self.use_gemini = True
            print("✓ Gemini Vision API 사용")
            print("✓ Gemini 상품명 매칭 사용")
            print("✓ Gemini 후보 선택 사용")
        else:
            self.image_matcher = SimpleImageMatcher()
            self.name_matcher = None
            self.candidate_selector = None
            self.use_gemini = False
            print("⚠ 단순 이미지 비교 사용")
            print("⚠ 상품명 매칭 비활성화")
            print("⚠ 자동 후보 선택 (정수·가격 기준)")
        
        # 결과
        self.results: List[MatchResult] = []
        self.output_path = f"matching_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    def initialize_crawlers(self):
        """크롤러 초기화"""
        print("\n크롤러 초기화 중...")
        
        # BrowserManager 생성
        self.browser = BrowserManager(headless=self.headless)
        print("✓ BrowserManager 초기화")
        
        # 크롤러 생성
        self.gnc_crawler = GNCCrawler(browser_manager=self.browser)
        self.coupang_crawler = CoupangCrawler(browser_manager=self.browser)
        
        print("✓ 크롤러 준비 완료\n")
    
    def load_products(self, priority_numbers: Optional[List[int]] = None) -> tuple[List[Dict], List[Dict]]:
        """
        엑셀에서 상품 로드
        
        Args:
            priority_numbers: 우선 처리할 상품 NO 리스트 (예: [1, 2, 3, ...])
        
        Returns:
            (우선순위 상품, 일반 상품)
        """
        print(f"엑셀 파일 로드: {self.excel_path}")
        
        wb = load_workbook(self.excel_path)
        ws = wb.active
        
        priority_products = []
        normal_products = []
        
        headers = [cell.value for cell in ws[1]]
        
        for row_idx in range(2, ws.max_row + 1):
            row_data = {}
            for col_idx, header in enumerate(headers, 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                row_data[header] = cell.value
            
            # NO 기준 우선순위 확인
            product_no = row_data.get('NO')
            
            if priority_numbers and product_no in priority_numbers:
                priority_products.append(row_data)
            else:
                normal_products.append(row_data)
        
        wb.close()
        
        print(f"✓ 우선순위: {len(priority_products)}개")
        print(f"✓ 일반: {len(normal_products)}개")
        
        if priority_numbers:
            print(f"  우선순위 NO: {priority_numbers[:10]}{'...' if len(priority_numbers) > 10 else ''}\n")
        
        return priority_products, normal_products
    
    def process_product(self, product_data: Dict) -> MatchResult:
        """개별 상품 처리"""
        no = product_data.get('NO', 0)
        brand = product_data.get('브랜드', '')
        product_code = product_data.get('상품코드', '')
        product_name = product_data.get('상품명', '')
        
        print(f"\n{'='*60}")
        print(f"[{no}] {brand} - {product_name}")
        print(f"상품코드: {product_code}")
        print(f"{'='*60}")
        
        result = MatchResult(
            no=no,
            brand=brand,
            product_code=str(product_code),
            product_name=product_name,
            processed_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        
        try:
            # 1. GNC 검색
            print("\n[1] GNC 검색...")
            gnc_product = self.gnc_crawler.search_product(product_code)
            
            if not gnc_product:
                result.status = "실패"
                result.error_message = "GNC 검색 결과 없음"
                return result
            
            result.gnc_search_result = gnc_product.product_name
            result.gnc_url = gnc_product.gnc_url
            result.gnc_thumbnail = gnc_product.thumbnail_url or ""
            result.gnc_count = gnc_product.count
            
            print(f"  ✓ {gnc_product.product_name[:50]}...")
            
            # 2. 쿠팡 쿼리
            print("\n[2] 쿠팡 쿼리 생성...")
            query = self._generate_query(gnc_product)
            result.coupang_query = query
            print(f"  {query}")
            
            # 3. 쿠팡 검색
            print("\n[3] 쿠팡 검색...")
            candidates = self.coupang_crawler.search_products(query, top_n=5)
            result.coupang_candidates_count = len(candidates)
            
            if not candidates:
                result.status = "실패"
                result.error_message = "쿠팡 검색 결과 없음"
                return result
            
            print(f"  ✓ {len(candidates)}개 후보")
            
            # 4. 최적 후보 선택
            print("\n[4] 후보 선택...")
            if self.candidate_selector:
                # Gemini로 선택
                print("  Gemini로 최적 후보 분석 중...")
                best, reason = self.candidate_selector.select_best_candidate(gnc_product, candidates)
                print(f"  ✓ 선택 이유: {reason[:100]}...")
            else:
                # 자동 선택 (정수·브랜드·가격)
                print("  자동 선택 (정수·브랜드·가격 기준)")
                best = self._select_best(gnc_product, candidates)
                reason = "자동 선택"
            
            if not best:
                result.status = "실패"
                result.error_message = "매칭 불가"
                return result
            
            print(f"  ✓ {best.name[:40]}...")
            
            # 5. 상세 정보
            print("\n[5] 상세 정보...")
            detail = self.coupang_crawler.get_product_detail(best.url)
            
            result.coupang_name = best.name
            result.coupang_url = best.url
            result.coupang_price = best.price
            result.coupang_shipping = best.shipping_fee
            result.coupang_final_price = best.final_price
            result.coupang_count = best.count
            result.coupang_brand = best.brand or ""
            result.coupang_rating = best.rating
            result.coupang_reviews = best.review_count
            
            if detail:
                result.coupang_seller = detail.get('seller_name', '')
                if detail.get('thumbnail_url'):
                    best.thumbnail_url = detail['thumbnail_url']
            
            # 6-1. 상품명 매칭 (Gemini 사용 시)
            if self.name_matcher:
                print("\n[6-1] 상품명 매칭...")
                is_name_match, name_conf, name_reason = self.name_matcher.match_product_names(
                    gnc_product.product_name,
                    best.name
                )
                
                result.name_match = "일치" if is_name_match else "불일치"
                result.name_confidence = name_conf
                result.name_reason = name_reason
                
                print(f"  상품명: {result.name_match} ({name_conf})")
            else:
                result.name_match = "비활성화"
                result.name_confidence = "N/A"
                result.name_reason = "Gemini API 없음"
            
            # 6-2. 이미지 비교
            print("\n[6-2] 이미지 비교...")
            if gnc_product.thumbnail_url and best.thumbnail_url:
                if self.use_gemini:
                    is_match, conf, reason = self.image_matcher.compare_images(
                        gnc_product.thumbnail_url, best.thumbnail_url
                    )
                else:
                    is_match, conf, reason = self.image_matcher.compare_images_simple(
                        gnc_product.thumbnail_url, best.thumbnail_url
                    )
                
                result.image_match = "일치" if is_match else "불일치"
                result.image_confidence = conf
                result.image_reason = reason
                
                # 상품명 + 이미지 종합 판정
                if self.name_matcher:
                    # 둘 다 일치하면 성공
                    if is_match and result.name_match == "일치":
                        result.status = "성공"
                    # 하나라도 불일치면 검토필요
                    elif not is_match or result.name_match == "불일치":
                        result.status = "검토필요"
                        if not is_match and result.name_match == "불일치":
                            result.error_message = "상품명·이미지 모두 불일치"
                        elif not is_match:
                            result.error_message = "이미지 불일치"
                        else:
                            result.error_message = "상품명 불일치"
                    else:
                        result.status = "검토필요"
                else:
                    # Gemini 없으면 이미지만 판단
                    result.status = "성공" if is_match else "검토필요"
                    if not is_match:
                        result.error_message = "이미지 불일치"
                
                print(f"  이미지: {result.image_match} ({conf})")
                if self.name_matcher:
                    print(f"  종합판정: {result.status}")
            else:
                result.image_match = "비교불가"
                result.status = "검토필요"
            
            return result
            
        except Exception as e:
            print(f"\n✗ 오류: {e}")
            result.status = "오류"
            result.error_message = str(e)
            return result
    
    def _generate_query(self, gnc: GNCProduct) -> str:
        """쿼리 생성"""
        parts = []
        
        if gnc.brand and gnc.brand != 'GNC':
            parts.append(gnc.brand)
        
        words = gnc.product_name.split()
        stopwords = ['GNC', 'mg', 'mcg', 'servings', 'tablets', 'capsules']
        keywords = [w for w in words if w.lower() not in [s.lower() for s in stopwords]]
        parts.extend(keywords[:4])
        
        if gnc.count:
            parts.append(f"{gnc.count}정")
        
        return ' '.join(parts)
    
    def _select_best(self, gnc: GNCProduct, candidates: List[CoupangProduct]) -> Optional[CoupangProduct]:
        """최적 후보"""
        if not candidates:
            return None
        
        # 정수 일치
        if gnc.count:
            matched = [c for c in candidates if c.count == gnc.count]
            if matched:
                return min(matched, key=lambda x: x.final_price)
        
        # 브랜드 일치
        if gnc.brand:
            matched = [c for c in candidates if c.brand and gnc.brand.lower() in c.brand.lower()]
            if matched:
                return min(matched, key=lambda x: x.final_price)
        
        return min(candidates, key=lambda x: x.final_price)
    
    def save_results(self):
        """결과 저장"""
        if not self.results:
            return
        
        df = pd.DataFrame([asdict(r) for r in self.results])
        df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        print(f"\n✓ 저장: {self.output_path}")
    
    def run(self, priority_numbers: Optional[List[int]] = None):
        """
        실행
        
        Args:
            priority_numbers: 우선 처리할 상품 NO 리스트 (예: [1, 2, 3, 4, 5])
        """
        try:
            self.initialize_crawlers()
            priority, normal = self.load_products(priority_numbers)
            
            # 우선순위
            if priority:
                print("\n" + "="*60)
                print("우선순위 상품 처리")
                print("="*60)
                
                for idx, p in enumerate(priority, 1):
                    print(f"\n진행: {idx}/{len(priority)}")
                    result = self.process_product(p)
                    self.results.append(result)
                    
                    if idx % 10 == 0:
                        self.save_results()
                    time.sleep(2)
            
            # 일반
            print("\n" + "="*60)
            print("일반 상품 처리" if priority else "전체 상품 처리")
            print("="*60)
            # 일반
            print("\n" + "="*60)
            print("일반 상품 처리" if priority else "전체 상품 처리")
            print("="*60)
            
            for idx, p in enumerate(normal, 1):
                print(f"\n진행: {idx}/{len(normal)}")
                result = self.process_product(p)
                self.results.append(result)
                
                if idx % 10 == 0:
                    self.save_results()
                time.sleep(2)
            
            self.save_results()
            
            # 통계
            print("\n" + "="*60)
            print("완료")
            print("="*60)
            success = len([r for r in self.results if r.status == "성공"])
            review = len([r for r in self.results if r.status == "검토필요"])
            failed = len([r for r in self.results if r.status in ["실패", "오류"]])
            
            print(f"성공: {success}")
            print(f"검토필요: {review}")
            print(f"실패: {failed}")
            
        finally:
            if self.browser:
                self.browser.close()


def main():
    excel_path = "GNC_상품_리스트_외산.xlsx"
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    
    # 우선순위 상품 번호 지정 (필요시 수정)
    # 예: [1, 2, 3, 4, 5] 또는 None (전체 순차 처리)
    priority_numbers = None  # 우선순위 없음
    # priority_numbers = list(range(1, 72))  # 1-71번 우선 처리
    
    print("="*60)
    print("GNC-쿠팡 자동 매칭")
    print("="*60)
    
    if priority_numbers:
        print(f"우선순위 상품: {len(priority_numbers)}개")
    
    system = ProductMatchingSystem(
        excel_path=excel_path,
        gemini_api_key=gemini_api_key,
        headless=False
    )
    
    system.run(priority_numbers=priority_numbers)


if __name__ == '__main__':
    main()