"""
GNC-쿠팡 자동 매칭 시스템 v3.0
✨ 검색 결과 페이지만 활용 (상세 페이지 불필요)
✨ 30개 칼럼으로 확장 (배송 정보, 할인율, 배지 등 추가)
"""

import pandas as pd
from openpyxl import load_workbook
import time
import sys
import os
from typing import List, Optional, Dict
from dataclasses import dataclass, asdict
from datetime import datetime

# 프로젝트 루트
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from coupang_manager import CoupangBrowser
from gnc_crawler import GNCCrawler, GNCProduct
from coupang_crawler import CoupangCrawler, CoupangProduct
from gemini_matcher import ImageMatcher, CandidateSelector
from priority_detector import detect_red_font_rows


def clean_reason(text: str) -> str:
    """reason 텍스트 정리"""
    if not text:
        return ""
    
    # "이유:" 이후만 추출
    for keyword in ["이유:", "Reason:"]:
        if keyword in text:
            parts = text.split(keyword, 1)
            if len(parts) > 1:
                return parts[1].strip()
    
    return text.strip()


@dataclass
class MatchResult:
    """매칭 결과 v3.0 - 30개 칼럼"""
    no: int
    brand: str
    product_code: str
    product_name: str
    
    # GNC
    gnc_search_result: str = ""
    gnc_url: str = ""
    gnc_thumbnail: str = ""
    gnc_count: Optional[int] = None
    
    # 쿠팡 검색
    coupang_query: str = ""
    coupang_candidates_count: int = 0
    
    # 쿠팡 상품
    coupang_name: str = ""
    coupang_url: str = ""
    
    # 가격 정보 (확장)
    coupang_original_price: int = 0      # 정가
    coupang_sale_price: int = 0          # 판매가
    coupang_discount_rate: int = 0       # 할인율 (%)
    coupang_shipping: int = 0            # 배송비
    coupang_final_price: int = 0         # 최종가
    coupang_unit_price: Optional[int] = None  # 1정당 가격
    
    # 기타
    coupang_count: Optional[int] = None
    coupang_brand: str = ""
    coupang_rating: Optional[float] = None
    coupang_reviews: Optional[int] = None
    
    # 배송 정보 (NEW)
    coupang_delivery_type: str = ""      # 로켓배송, 직구, etc.
    coupang_delivery_date: str = ""      # 내일(수) 도착
    coupang_is_rocket: str = ""          # Y/N
    coupang_is_free_shipping: str = ""   # Y/N
    coupang_badges: str = ""             # 쿠팡PICK;로켓배송
    
    # 매칭 결과
    selection_reason: str = ""
    image_match: str = ""
    image_reason: str = ""
    
    processed_at: str = ""


class ProductMatchingSystem:
    """GNC-쿠팡 자동 매칭 v3.0"""
    
    def __init__(self, excel_path: str, gemini_api_key: Optional[str] = None, headless: bool = False):
        self.excel_path = excel_path
        self.headless = headless
        self.browser = None
        
        # AI 매처
        if gemini_api_key:
            self.image_matcher = ImageMatcher(gemini_api_key)
            self.candidate_selector = CandidateSelector(gemini_api_key)
            self.use_gemini = True
            print("✓ Gemini API 사용")
        else:
            self.image_matcher = None
            self.candidate_selector = None
            self.use_gemini = False
            print("⚠ Gemini API 없음")
        
        self.results: List[MatchResult] = []
        self.output_path = f"matching_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # 기존 결과 파일 확인
        existing_files = sorted([f for f in os.listdir('.') if f.startswith('matching_results_') and f.endswith('.csv')])
        if existing_files:
            latest_file = existing_files[-1]
            print(f"\n⚠ 기존 결과 파일 발견: {latest_file}")
            response = input("이어서 진행하시겠습니까? (y/n): ").lower()
            if response == 'y':
                self.output_path = latest_file
                print(f"✓ 이어서 실행: {self.output_path}\n")
            else:
                print(f"✓ 새로 시작: {self.output_path}\n")
    
    def load_existing_results(self) -> set:
        """기존 처리된 NO 로드"""
        if os.path.exists(self.output_path):
            try:
                df = pd.read_csv(self.output_path, encoding='utf-8-sig')
                processed_nos = set(df['no'].tolist())
                print(f"✓ 기존 결과 {len(processed_nos)}개 로드")
                return processed_nos
            except Exception as e:
                print(f"⚠ 기존 결과 로드 실패: {e}")
        return set()
    
    def initialize_crawlers(self):
        """크롤러 초기화"""
        print("\n크롤러 초기화...")
        self.browser = CoupangBrowser(headless=self.headless)
        
        self.gnc_crawler = GNCCrawler(browser_manager=self.browser)
        self.coupang_crawler = CoupangCrawler(browser_manager=self.browser)
        print("✓ 준비 완료\n")
    
    def load_products(self, priority_numbers: Optional[List[int]] = None):
        """엑셀 로드"""
        print(f"엑셀 로드: {self.excel_path}")
        
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
            
            product_no = row_data.get('NO')
            
            if priority_numbers and product_no in priority_numbers:
                priority_products.append(row_data)
            else:
                normal_products.append(row_data)
        
        wb.close()
        
        print(f"✓ 우선순위: {len(priority_products)}개")
        print(f"✓ 일반: {len(normal_products)}개\n")
        
        return priority_products, normal_products
    
    def process_product(self, product_data: Dict) -> MatchResult:
        """개별 상품 처리"""
        no = product_data.get('NO', 0)
        brand = product_data.get('브랜드', '')
        product_code = product_data.get('상품코드', '')
        product_name = product_data.get('상품명', '')
        
        print(f"\n{'='*60}")
        print(f"[{no}] {brand} - {product_name}")
        print(f"{'='*60}")
        
        result = MatchResult(
            no=no,
            brand=brand,
            product_code=str(product_code),
            product_name=product_name,
            processed_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        
        try:
            # [1] GNC 검색
            print("\n[1] GNC 검색...")
            gnc = self.gnc_crawler.search_product(product_code)
            
            if not gnc:
                return result
            
            result.gnc_search_result = gnc.product_name
            result.gnc_url = gnc.gnc_url
            result.gnc_thumbnail = gnc.thumbnail_url or ""
            result.gnc_count = gnc.count
            
            # [2] 쿠팡 쿼리
            print("\n[2] 쿼리 생성...")
            query = self._generate_query(gnc)
            result.coupang_query = query
            print(f"  {query}")
            
            # [3] 쿠팡 검색 (검색 결과에서 모든 정보 수집)
            print("\n[3] 쿠팡 검색...")
            candidates = self.coupang_crawler.search_products(query, top_n=8)
            result.coupang_candidates_count = len(candidates)
            
            if not candidates:
                print(f"  ✗ 후보 없음")
                return result
            
            print(f"  ✓ {len(candidates)}개 발견")
            
            # [4] 후보 선택
            print("\n[4] 후보 선택...")
            if self.use_gemini and self.candidate_selector:
                selected, confidence, reason = self.candidate_selector.select_best_candidate(
                    gnc_product=gnc,  # ✅ 수정: gnc_name → gnc_product
                    candidates=candidates
                )
            else:
                selected = None
                confidence = "none"
                reason = "Gemini API 없음"
            
            if not selected:
                print(f"  ✗ 매칭 불가")
                return result
            
            print(f"  선택: {selected.name[:50]}...")
            
            # 쿠팡 상품 정보 저장 (검색 결과에서 추출)
            result.coupang_name = selected.name
            result.coupang_url = selected.url
            result.coupang_original_price = selected.original_price
            result.coupang_sale_price = selected.sale_price
            result.coupang_discount_rate = selected.discount_rate
            result.coupang_shipping = selected.shipping_fee
            result.coupang_final_price = selected.final_price
            result.coupang_unit_price = selected.unit_price
            
            # count 추출
            from coupang_manager.selectors import CoupangHTMLHelper
            result.coupang_count = CoupangHTMLHelper.extract_count(selected.name)
            result.coupang_brand = brand  # GNC 브랜드 사용
            result.coupang_rating = selected.rating
            result.coupang_reviews = selected.review_count
            
            # 배송 정보
            result.coupang_delivery_type = selected.delivery_type or ""
            result.coupang_delivery_date = selected.delivery_date or ""
            result.coupang_is_rocket = "Y" if selected.is_rocket else "N"
            result.coupang_is_free_shipping = "Y" if selected.is_free_shipping else "N"
            result.coupang_badges = ";".join(selected.badges) if selected.badges else ""
            
            # reason 정리
            result.selection_reason = clean_reason(reason)
            
            # [5] 이미지 비교 (검색 결과 썸네일 사용) ✅ 수정
            print("\n[5] 이미지 비교...")
            if self.use_gemini and self.image_matcher and gnc.thumbnail_url and selected.thumbnail_url:
                is_match, img_confidence, img_reason = self.image_matcher.compare_images(gnc.thumbnail_url, selected.thumbnail_url)
                result.image_match = "일치" if is_match else "불일치"
                result.image_reason = clean_reason(img_reason)
                print(f"  이미지: {'일치' if is_match else '불일치'} (신뢰도: {img_confidence})")
            else:
                result.image_match = "비교불가"
                result.image_reason = "이미지 없음"
                if not gnc.thumbnail_url:
                    print(f"  ⚠ GNC 썸네일 없음")
                if not selected.thumbnail_url:
                    print(f"  ⚠ 쿠팡 썸네일 없음")
            
            print(f"\n✓ 완료")
            return result
            
        except Exception as e:
            print(f"\n✗ 오류: {e}")
            return result
    
    def _generate_query(self, gnc: GNCProduct) -> str:
        """쿠팡 검색 쿼리 생성"""
        if gnc.brand:
            return f"{gnc.brand} {gnc.product_name}"
        return gnc.product_name
    
    def save_results(self):
        """실시간 저장 (append 모드)"""
        if not self.results:
            return
        
        df = pd.DataFrame([asdict(r) for r in self.results])
        
        # 파일이 존재하면 append, 없으면 새로 생성
        if os.path.exists(self.output_path):
            df.to_csv(self.output_path, mode='a', header=False, index=False, encoding='utf-8-sig')
        else:
            df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        
        # 저장 후 results 초기화 (메모리 절약)
        self.results.clear()
        print(f"  💾 실시간 저장 완료")
    
    def run(self, priority_numbers: Optional[List[int]] = None):
        """실행"""
        try:
            self.initialize_crawlers()
            
            # 기존 처리된 NO 로드
            processed_nos = self.load_existing_results()
            
            priority, normal = self.load_products(priority_numbers)
            
            # 우선순위 처리
            if priority:
                print(f"\n{'='*60}")
                print("우선순위 처리")
                print(f"{'='*60}")
                
                # 미처리 상품만 필터링
                priority_to_process = [p for p in priority if p.get('NO') not in processed_nos]
                
                if processed_nos and priority_to_process:
                    print(f"✓ 이미 처리됨: {len(priority) - len(priority_to_process)}개")
                    print(f"✓ 처리 예정: {len(priority_to_process)}개")
                
                for idx, p in enumerate(priority_to_process, 1):
                    print(f"\n진행: {idx}/{len(priority_to_process)}")
                    result = self.process_product(p)
                    self.results.append(result)
                    self.save_results()
                    time.sleep(2)
            
            # 일반 처리
            if normal:
                print(f"\n{'='*60}")
                print("일반 상품 처리")
                print(f"{'='*60}")
                
                # 미처리 상품만 필터링
                normal_to_process = [p for p in normal if p.get('NO') not in processed_nos]
                
                if processed_nos and normal_to_process:
                    print(f"✓ 이미 처리됨: {len(normal) - len(normal_to_process)}개")
                    print(f"✓ 처리 예정: {len(normal_to_process)}개")
                
                for idx, p in enumerate(normal_to_process, 1):
                    print(f"\n진행: {idx}/{len(normal_to_process)}")
                    result = self.process_product(p)
                    self.results.append(result)
                    
                    if idx % 10 == 0:
                        self.save_results()
                    time.sleep(2)
                
                self.save_results()
            
            print(f"\n✓ 완료")
            print(f"✓ 결과 파일: {self.output_path}")
        
        except KeyboardInterrupt:
            print("\n\n⚠️  사용자가 중단했습니다")
            print("✓ 처리된 결과는 이미 저장되었습니다")
            print(f"✓ 결과 파일: {self.output_path}")
        
        finally:
            if self.browser:
                self.browser.close()


def main():
    excel_path = "GNC_상품_리스트_외산.xlsx"
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    
    if not gemini_api_key:
        print("\n⚠️  GEMINI_API_KEY 없음")
        return
    
    # 우선순위 감지
    print("\n우선순위 감지 중...")
    priority_numbers = detect_red_font_rows(excel_path)
    
    if priority_numbers:
        print(f"✓ 빨간색 폰트 {len(priority_numbers)}개")
    
    print(f"\n{'='*60}")
    print("GNC-쿠팡 자동 매칭 v3.0")
    print(f"{'='*60}")
    print("✨ 개선사항:")
    print("  - 검색 결과 페이지만 활용 (속도 2-3배)")
    print("  - 정가/판매가/할인율 분리")
    print("  - 배송 정보 추가 (로켓/직구/도착일)")
    print("  - 1정당 가격 추가")
    print("  - 배지 정보 추가")
    print(f"{'='*60}\n")
    
    system = ProductMatchingSystem(
        excel_path=excel_path,
        gemini_api_key=gemini_api_key,
        headless=False
    )
    
    system.run(priority_numbers=priority_numbers)


if __name__ == '__main__':
    main()