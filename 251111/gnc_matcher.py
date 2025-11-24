"""
GNC 제품 쿠팡 매칭 시스템
- 브랜드 + 상품명으로 검색
- 단순 매칭 (정수/개수 무시)
- 1순위 제품 선택
"""

import sys
import os
import pandas as pd
import time
from typing import List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

# 현재 파일 위치에서 프로젝트 루트 찾기
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 상위 디렉토리 (iherb_price)

# coupang 모듈 경로 추가
coupang_dir = os.path.join(project_root, "coupang")
if coupang_dir not in sys.path:
    sys.path.insert(0, coupang_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)


@dataclass
class GNCProduct:
    """GNC 제품 정보"""
    no: int
    brand: str
    product_code: str
    product_name: str
    search_keyword: str


class BrandMatcher:
    """브랜드명 유사도 판정"""
    
    # GNC 브랜드 변형 패턴
    GNC_VARIANTS = [
        'gnc',
        '지앤씨',
        '지엔씨',
    ]
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """텍스트 정규화 (공백, 특수문자 제거)"""
        import re
        text = text.lower()
        text = re.sub(r'[^a-z0-9가-힣]', '', text)
        return text
    
    @staticmethod
    def is_gnc_brand(product_name: str) -> bool:
        """
        제품명에 GNC 브랜드가 포함되어 있는지 확인
        
        Args:
            product_name: 제품명
        
        Returns:
            GNC 브랜드 여부
        """
        normalized_name = BrandMatcher.normalize_text(product_name)
        
        for variant in BrandMatcher.GNC_VARIANTS:
            normalized_variant = BrandMatcher.normalize_text(variant)
            if normalized_variant in normalized_name:
                return True
        
        return False


@dataclass
class CoupangMatch:
    """쿠팡 매칭 결과"""
    product_name: str
    url: str
    price: int
    shipping_fee: int
    final_price: int
    seller_type: str
    rank: int


@dataclass
class MatchResult:
    """최종 매칭 결과"""
    gnc_product: GNCProduct
    coupang_match: Optional[CoupangMatch]
    confidence: str  # high/medium/low
    reason: str


class GNCCoupangSearcher:
    """GNC 제품용 쿠팡 검색"""
    
    def __init__(self, browser_manager):
        """
        Args:
            browser_manager: BrowserManager 인스턴스
        """
        self.browser = browser_manager
    
    def search_products(self, keyword: str, top_n: int = 4) -> List[CoupangMatch]:
        """
        쿠팡에서 제품 검색
        
        Args:
            keyword: 검색 키워드 (브랜드 + 상품명)
            top_n: 상위 N개 제품
        
        Returns:
            CoupangMatch 리스트
        """
        products = []
        
        try:
            search_url = f"https://www.coupang.com/np/search?q={keyword}"
            print(f"  검색: {search_url}")
            
            self.browser.get_with_coupang_referrer(search_url)
            time.sleep(3)
            
            # 낱개상품 필터 적용
            self._apply_single_item_filter()
            time.sleep(2)
            
            # 검색 결과 파싱
            products = self._parse_results(top_n)
            
        except Exception as e:
            print(f"  ✗ 검색 오류: {e}")
        
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
            
        except Exception as e:
            print(f"  ⚠ 필터 적용 실패: {e}")
    
    def _parse_results(self, top_n: int) -> List[CoupangMatch]:
        """검색 결과 파싱"""
        products = []
        
        try:
            driver = self.browser.driver
            product_items = driver.find_elements("css selector", "li.ProductUnit_productUnit__Qd6sv")
            
            for idx, item in enumerate(product_items[:top_n]):
                try:
                    # 제품명
                    name_elem = item.find_element("css selector", "div.ProductUnit_productNameV2__cV9cw")
                    name = name_elem.text.strip()
                    
                    # URL
                    link_elem = item.find_element("css selector", "a")
                    raw_url = link_elem.get_attribute("href")
                    url = self._clean_url(raw_url)
                    
                    # 가격
                    try:
                        price_elem = item.find_element("css selector", "div.custom-oos.fw-text-\\[20px\\]\\/\\[24px\\]")
                        price_text = price_elem.text.strip().replace(",", "").replace("원", "")
                        price = int(price_text)
                    except:
                        price = 0
                    
                    # 배송비
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
                    
                    # 판매 유형
                    seller_type = "3P"
                    try:
                        item.find_element("css selector", "img[src*='logo_jikgu']")
                        seller_type = "로켓직구"
                    except:
                        seller_type = "3P"
                    
                    match = CoupangMatch(
                        product_name=name,
                        url=url,
                        price=price,
                        shipping_fee=shipping_fee,
                        final_price=final_price,
                        seller_type=seller_type,
                        rank=idx + 1
                    )
                    
                    products.append(match)
                    print(f"  [{idx+1}] {name[:50]}... ({final_price:,}원)")
                    
                except Exception as e:
                    print(f"  ✗ 제품 파싱 실패 [{idx+1}]: {e}")
                    continue
            
        except Exception as e:
            print(f"  ✗ 결과 파싱 실패: {e}")
        
        return products
    
    @staticmethod
    def _clean_url(url: str) -> str:
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


class GNCMatcher:
    """GNC 제품 매칭 시스템"""
    
    def __init__(self, xlsx_path: str, output_path: str, browser_manager):
        """
        Args:
            xlsx_path: GNC 엑셀 파일 경로
            output_path: 출력 CSV 경로
            browser_manager: BrowserManager 인스턴스
        """
        self.xlsx_path = xlsx_path
        self.output_path = output_path
        self.searcher = GNCCoupangSearcher(browser_manager)
        self.df = None
        self.results_df = None
    
    def load_data(self):
        """엑셀 데이터 로드"""
        print(f"\n{'='*60}")
        print(f"데이터 로드")
        print(f"{'='*60}")
        
        try:
            # 엑셀 로드
            self.df = pd.read_excel(self.xlsx_path)
            print(f"✓ GNC 제품: {len(self.df)}개")
            
            # 기존 결과 파일 확인
            if os.path.exists(self.output_path):
                self.results_df = pd.read_csv(self.output_path, encoding='utf-8-sig')
                print(f"✓ 기존 결과 로드: {len(self.results_df)}개")
            else:
                # 새 결과 DataFrame 생성
                self.results_df = pd.DataFrame(columns=[
                    'NO',
                    '원본_브랜드',
                    '원본_상품코드',
                    '원본_상품명',
                    '검색_키워드',
                    '매칭_제품명',
                    '매칭_URL',
                    '가격',
                    '배송비',
                    '최종가격',
                    '판매유형',
                    '순위',
                    '신뢰도',
                    '사유',
                    '처리시각'
                ])
                print(f"✓ 새 결과 파일 생성")
            
            print(f"{'='*60}\n")
            return True
            
        except Exception as e:
            print(f"✗ 데이터 로드 실패: {e}")
            return False
    
    def match_product(self, gnc_product: GNCProduct) -> MatchResult:
        """
        단일 제품 매칭 (브랜드 일치 필수, GNC 변형 포함)
        
        Args:
            gnc_product: GNC 제품 정보
        
        Returns:
            MatchResult
        """
        # 쿠팡 검색
        candidates = self.searcher.search_products(gnc_product.search_keyword, top_n=4)
        
        if not candidates:
            return MatchResult(
                gnc_product=gnc_product,
                coupang_match=None,
                confidence="low",
                reason="검색 결과 없음"
            )
        
        # GNC 브랜드 일치하는 제품만 찾기 (변형 포함)
        brand_matched = []
        
        for candidate in candidates:
            if BrandMatcher.is_gnc_brand(candidate.product_name):
                brand_matched.append(candidate)
        
        # 브랜드 일치 제품이 없으면 매칭 안 함
        if not brand_matched:
            print(f"  ⚠ GNC 브랜드 일치하는 제품 없음")
            # 검색 결과 제품명 출력 (디버깅용)
            for i, c in enumerate(candidates, 1):
                print(f"     [{i}] {c.product_name[:60]}...")
            
            return MatchResult(
                gnc_product=gnc_product,
                coupang_match=None,
                confidence="low",
                reason=f"GNC 브랜드 일치하는 제품 없음"
            )
        
        # 브랜드 일치 제품 중 최저가 선택
        selected = min(brand_matched, key=lambda x: x.final_price if x.final_price > 0 else float('inf'))
        confidence = "high" if selected.rank == 1 else "medium"
        reason = f"브랜드 일치, 순위 {selected.rank}, 최저가"
        
        return MatchResult(
            gnc_product=gnc_product,
            coupang_match=selected,
            confidence=confidence,
            reason=reason
        )
    
    def process_all(self, start_idx: int = 0, end_idx: Optional[int] = None):
        """
        전체 제품 처리
        
        Args:
            start_idx: 시작 인덱스 (0부터)
            end_idx: 종료 인덱스 (None이면 끝까지)
        """
        print(f"\n{'='*60}")
        print(f"제품 매칭 시작")
        print(f"{'='*60}\n")
        
        # 이미 처리된 제품 확인
        processed_nos = set()
        if len(self.results_df) > 0:
            processed_nos = set(self.results_df['NO'].astype(int))
            print(f"✓ 이미 처리 완료: {len(processed_nos)}개\n")
        
        # 처리 범위 설정
        end_idx = end_idx or len(self.df)
        to_process = self.df.iloc[start_idx:end_idx]
        
        # 미처리 제품만 필터링
        to_process = to_process[~to_process['NO'].isin(processed_nos)]
        
        if len(to_process) == 0:
            print("✓ 처리할 제품이 없습니다\n")
            return
        
        print(f"✓ 처리 대상: {len(to_process)}개\n")
        
        # 진행 상황 추적
        success_count = 0
        fail_count = 0
        
        for i, (idx, row) in enumerate(to_process.iterrows(), 1):
            no = int(row['NO'])
            brand = str(row['브랜드'])
            product_code = str(row['상품코드'])
            product_name = str(row['상품명'])
            
            # 검색 키워드 생성
            search_keyword = f"{brand} {product_name}"
            
            print(f"[{i}/{len(to_process)}] NO.{no}")
            print(f"  원본: [{brand}] {product_name}")
            print(f"  검색: {search_keyword}")
            
            # GNC 제품 객체 생성
            gnc_product = GNCProduct(
                no=no,
                brand=brand,
                product_code=product_code,
                product_name=product_name,
                search_keyword=search_keyword
            )
            
            try:
                # 매칭 실행
                match_result = self.match_product(gnc_product)
                
                # 결과 저장
                result_row = {
                    'NO': no,
                    '원본_브랜드': brand,
                    '원본_상품코드': product_code,
                    '원본_상품명': product_name,
                    '검색_키워드': search_keyword,
                    '처리시각': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                if match_result.coupang_match:
                    result_row.update({
                        '매칭_제품명': match_result.coupang_match.product_name,
                        '매칭_URL': match_result.coupang_match.url,
                        '가격': match_result.coupang_match.price,
                        '배송비': match_result.coupang_match.shipping_fee,
                        '최종가격': match_result.coupang_match.final_price,
                        '판매유형': match_result.coupang_match.seller_type,
                        '순위': match_result.coupang_match.rank,
                        '신뢰도': match_result.confidence,
                        '사유': match_result.reason
                    })
                    print(f"  ✓ 매칭: {match_result.coupang_match.product_name[:50]}...")
                    print(f"    신뢰도: {match_result.confidence}, 사유: {match_result.reason}")
                    success_count += 1
                else:
                    result_row.update({
                        '매칭_제품명': None,
                        '매칭_URL': None,
                        '가격': None,
                        '배송비': None,
                        '최종가격': None,
                        '판매유형': None,
                        '순위': None,
                        '신뢰도': match_result.confidence,
                        '사유': match_result.reason
                    })
                    print(f"  ✗ 매칭 실패: {match_result.reason}")
                    fail_count += 1
                
                # DataFrame에 추가
                self.results_df = pd.concat([
                    self.results_df,
                    pd.DataFrame([result_row])
                ], ignore_index=True)
                
                # 실시간 저장
                self.save_results()
                print(f"  💾 저장 완료 ({i}/{len(to_process)})\n")
                
            except Exception as e:
                print(f"  ✗ 오류 발생: {e}")
                fail_count += 1
                
                # 오류도 기록
                result_row = {
                    'NO': no,
                    '원본_브랜드': brand,
                    '원본_상품코드': product_code,
                    '원본_상품명': product_name,
                    '검색_키워드': search_keyword,
                    '매칭_제품명': None,
                    '매칭_URL': None,
                    '가격': None,
                    '배송비': None,
                    '최종가격': None,
                    '판매유형': None,
                    '순위': None,
                    '신뢰도': 'error',
                    '사유': f'오류: {str(e)}',
                    '처리시각': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                self.results_df = pd.concat([
                    self.results_df,
                    pd.DataFrame([result_row])
                ], ignore_index=True)
                
                self.save_results()
                print(f"  💾 오류 기록 저장\n")
            
            # 대기
            time.sleep(2)
        
        # 최종 통계
        print(f"\n{'='*60}")
        print(f"매칭 완료!")
        print(f"{'='*60}")
        print(f"전체: {len(to_process)}개")
        print(f"성공: {success_count}개 ({success_count/len(to_process)*100:.1f}%)")
        print(f"실패: {fail_count}개 ({fail_count/len(to_process)*100:.1f}%)")
        print(f"{'='*60}\n")
    
    def save_results(self):
        """결과 저장"""
        try:
            self.results_df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"  ✗ 저장 실패: {e}")
    
    def print_summary(self):
        """최종 통계 출력"""
        if self.results_df is None or len(self.results_df) == 0:
            print("처리된 데이터가 없습니다")
            return
        
        print(f"\n{'='*60}")
        print(f"최종 통계")
        print(f"{'='*60}")
        
        total = len(self.results_df)
        matched = len(self.results_df[self.results_df['매칭_제품명'].notna()])
        
        print(f"전체 처리: {total}개")
        print(f"매칭 성공: {matched}개 ({matched/total*100:.1f}%)")
        print(f"매칭 실패: {total-matched}개 ({(total-matched)/total*100:.1f}%)")
        
        # 신뢰도 분포
        if '신뢰도' in self.results_df.columns:
            print(f"\n[신뢰도 분포]")
            confidence_counts = self.results_df['신뢰도'].value_counts()
            for level, count in confidence_counts.items():
                print(f"  {level}: {count}개 ({count/total*100:.1f}%)")
        
        print(f"{'='*60}\n")


# 실행 예시
if __name__ == "__main__":
    print("gnc_matcher.py 모듈 로드 완료")