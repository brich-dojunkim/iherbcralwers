"""
쿠팡 가격/링크 업데이트 - 기존 결과 파일에 이어서 작업
"""

import sys
import os
import pandas as pd
import time
import random
from urllib.parse import quote
from datetime import datetime

# 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
coupang_dir = os.path.join(current_dir, 'coupang')
sys.path.insert(0, coupang_dir)

from coupang.coupang_manager import BrowserManager
from coupang.scraper import ProductScraper
import google.generativeai as genai

# config는 config 폴더에서 import
config_dir = os.path.join(current_dir, 'config')
sys.path.insert(0, config_dir)
from config.global_config import APIConfig


class CoupangPriceLinkUpdater:
    """CSV 상품의 쿠팡 가격/링크 업데이트"""
    
    def __init__(self, original_csv: str, result_csv: str = None, max_rows: int = 200):
        """
        Args:
            original_csv: 원본 CSV 파일 (전체 데이터)
            result_csv: 기존 결과 CSV 파일 (있으면 이어받기)
            max_rows: 총 처리할 최대 개수
        """
        self.original_csv = original_csv
        self.result_csv = result_csv
        self.max_rows = max_rows
        
        # 브라우저 초기화
        self.browser = BrowserManager(headless=False)
        self.scraper = ProductScraper()
        
        # Gemini 초기화
        genai.configure(api_key=APIConfig.GEMINI_API_KEY)
        self.model = genai.GenerativeModel(APIConfig.GEMINI_TEXT_MODEL)
        
        self.processed_count = 0
        self.success_count = 0
        self.fail_count = 0
        self.save_interval = 10
    
    def start(self):
        """전체 프로세스 실행"""
        
        # 1. 원본 CSV 읽기
        try:
            full_df = pd.read_csv(self.original_csv, encoding='utf-8-sig')
            print(f"\n원본 CSV 로드: {len(full_df)}개 상품")
        except Exception as e:
            print(f"원본 CSV 로드 실패: {e}")
            return
        
        # 2. 기존 결과 파일 확인
        start_index = 0
        existing_results = []
        
        if self.result_csv and os.path.exists(self.result_csv):
            try:
                result_df = pd.read_csv(self.result_csv, encoding='utf-8-sig')
                
                # 처리 완료된 항목 확인
                if '처리상태' in result_df.columns:
                    processed_mask = result_df['처리상태'].notna() & (result_df['처리상태'] != '')
                    processed_count = processed_mask.sum()
                    
                    print(f"기존 결과 파일: {os.path.basename(self.result_csv)}")
                    print(f"이미 처리된 항목: {processed_count}개")
                    
                    start_index = processed_count
                    existing_results = result_df.to_dict('records')
                    
            except Exception as e:
                print(f"기존 결과 로드 실패: {e}")
                start_index = 0
        
        # 3. 처리 범위 결정
        end_index = min(self.max_rows, len(full_df))
        
        print(f"\n{'='*80}")
        print(f"작업 계획")
        print(f"{'='*80}")
        print(f"원본 파일: {os.path.basename(self.original_csv)}")
        print(f"결과 파일: {os.path.basename(self.result_csv) if self.result_csv else '새로 생성'}")
        print(f"전체 데이터: {len(full_df)}개")
        print(f"목표: 상위 {self.max_rows}개 처리")
        print(f"이미 처리: {start_index}개")
        print(f"이번 작업: {start_index}번 ~ {end_index-1}번 ({end_index - start_index}개)")
        print(f"{'='*80}\n")
        
        if start_index >= end_index:
            print(f"✓ 목표({self.max_rows}개)를 이미 달성했습니다!")
            return
        
        response = input(f"{end_index - start_index}개 항목을 처리하시겠습니까? (y/n): ").strip().lower()
        if response != 'y':
            print("작업을 취소했습니다.")
            return
        
        # 4. 결과 DataFrame 준비
        # 기존 결과에 새로운 행 추가
        if existing_results:
            result_df = pd.DataFrame(existing_results)
        else:
            result_df = full_df.head(0).copy()  # 빈 DataFrame with same columns
            result_df['처리상태'] = ''
            result_df['매칭된_쿠팡상품명'] = ''
        
        # 5. 브라우저 시작
        print("\n브라우저 시작 중...")
        if not self.browser.start_driver():
            print("브라우저 시작 실패")
            return
        print("브라우저 시작 완료\n")
        
        # 6. 출력 파일명 결정
        if self.result_csv:
            output_path = self.result_csv  # 기존 파일 덮어쓰기
        else:
            # 새 파일 생성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(os.path.basename(self.original_csv))[0]
            output_dir = os.path.dirname(self.original_csv)
            output_path = os.path.join(output_dir, f'{base_name}_결과_{timestamp}.csv')
        
        try:
            # 7. 처리 시작
            for idx in range(start_index, end_index):
                row = full_df.iloc[idx]
                
                # 처리
                result = self._process_row(row, idx, end_index, start_index)
                
                # 결과 추가
                result_df = pd.concat([result_df, pd.DataFrame([result])], ignore_index=True)
                
                # 주기적으로 저장
                if self.processed_count % self.save_interval == 0 and self.processed_count > 0:
                    result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                    print(f"\n{'*'*60}")
                    print(f"체크포인트 저장 완료")
                    print(f"진행: {start_index + self.processed_count}/{end_index}개")
                    print(f"{'*'*60}\n")
            
            # 8. 최종 저장
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            # 9. 통계 출력
            self._print_summary(start_index, end_index, output_path, len(full_df))
            
        except KeyboardInterrupt:
            print("\n\n" + "="*80)
            print("사용자가 중단했습니다 (Ctrl+C)")
            print("="*80)
            print("현재까지 처리된 결과를 저장합니다...")
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            current_total = start_index + self.processed_count
            print(f"\n저장 완료!")
            print(f"현재까지: {current_total}/{self.max_rows}개 처리")
            print(f"남은 작업: {self.max_rows - current_total}개")
            print(f"\n이어서 작업하려면:")
            print(f"python3 coupang1.py '{self.original_csv}' '{output_path}' {self.max_rows}")
            print("="*80)
            
        except Exception as e:
            print(f"\n오류 발생: {e}")
            import traceback
            traceback.print_exc()
            print("\n현재까지 처리된 결과를 저장합니다...")
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
        finally:
            self.browser.close()
            print("\n브라우저 종료 완료")
    
    def _process_row(self, row: pd.Series, idx: int, end_index: int, start_index: int) -> dict:
        """단일 상품 처리"""
        self.processed_count += 1
        current_position = start_index + self.processed_count
        
        # 원본 데이터 복사
        result = row.to_dict()
        result['처리상태'] = ''
        result['매칭된_쿠팡상품명'] = ''
        
        # 상품명 추출
        product_name = str(row.get('쿠팡 상품명', '')).strip()
        
        if not product_name or product_name == 'nan':
            print(f"[{idx+1}번째] ({current_position}/{end_index}) 상품명 없음")
            result['처리상태'] = '상품명 없음'
            self.fail_count += 1
            return result
        
        print(f"\n{'='*80}")
        print(f"[{idx+1}번째 행] 진행: {current_position}/{end_index}")
        print(f"상품명: {product_name[:60]}...")
        print(f"{'='*80}")
        
        try:
            # 1. 쿠팡 검색
            search_url = self._build_search_url(product_name)
            print(f"검색 중...")
            
            self.browser.driver.get(search_url)
            time.sleep(random.uniform(3, 5))
            
            # 2. 검색 결과 추출
            products = self._extract_search_results()
            
            if not products:
                print("검색 결과 없음")
                result['처리상태'] = '검색 결과 없음'
                self.fail_count += 1
                return result
            
            print(f"검색 결과: {len(products)}개")
            
            # 3. Gemini로 가장 유사한 상품 찾기
            best_match = self._find_best_match(product_name, products)
            
            if not best_match:
                print("유사 상품 찾기 실패")
                result['처리상태'] = 'Gemini 매칭 실패'
                self.fail_count += 1
                return result
            
            # 4. 결과 업데이트
            result['로켓직구 최저가 or 최저가'] = best_match.get('current_price', '')
            result['최저가'] = '쿠팡로켓' if best_match.get('is_rocket', False) else ''
            result['쿠팡링크'] = best_match.get('product_url', '')
            result['매칭된_쿠팡상품명'] = best_match.get('product_name', '')
            result['처리상태'] = '성공'
            
            print(f"✓ 업데이트 완료")
            self.success_count += 1
            
            # 딜레이
            time.sleep(random.uniform(2, 4))
            
        except Exception as e:
            print(f"✗ 처리 실패: {e}")
            result['처리상태'] = f'오류: {str(e)[:50]}'
            self.fail_count += 1
        
        return result
    
    def _build_search_url(self, product_name: str) -> str:
        """검색 URL 생성"""
        encoded_name = quote(product_name)
        return f"https://www.coupang.com/np/search?q={encoded_name}&channel=user"
    
    def _extract_search_results(self) -> list:
        """검색 결과 페이지에서 상품 추출"""
        from bs4 import BeautifulSoup
        
        try:
            html = self.browser.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            product_list = soup.find('ul', id='product-list')
            if not product_list:
                return []
            
            items = product_list.select('li.ProductUnit_productUnit__Qd6sv, li[data-id]')
            
            products = []
            for item in items[:20]:
                try:
                    product = self.scraper.extract_product_info(item)
                    if product and product.get('product_name'):
                        products.append(product)
                except:
                    continue
            
            return products
            
        except Exception as e:
            print(f"검색 결과 추출 오류: {e}")
            return []
    
    def _find_best_match(self, query_name: str, products: list) -> dict:
        """Gemini로 가장 유사한 상품 찾기"""
        try:
            product_names = [p['product_name'] for p in products]
            search_results = "\n".join([f"- {name}" for name in product_names])
            
            prompt = f"""You are matching products for price comparison.

ORIGINAL PRODUCT:
{query_name}

COUPANG SEARCH RESULTS:
{search_results}

Find the search result that is the EXACT SAME product as the original.
Consider: brand name, product type, ingredients, form, quantity, dosage.

Return ONLY the exact product name from the search results that matches.
If no match exists, return "NO_MATCH".

Your answer:"""

            response = self.model.generate_content(
                prompt,
                generation_config={
                    'temperature': 0.1,
                    'max_output_tokens': 200
                }
            )
            
            answer = response.text.strip()
            
            if answer == "NO_MATCH":
                print(f"Gemini: 매칭 없음")
                return None
            
            for product in products:
                if answer in product['product_name'] or product['product_name'] in answer:
                    print(f"Gemini 매칭 성공:")
                    print(f"  원본: {query_name[:50]}...")
                    print(f"  매칭: {product['product_name'][:50]}...")
                    print(f"  가격: {product['current_price']}")
                    print(f"  로켓: {'예' if product['is_rocket'] else '아니오'}")
                    return product
            
            print(f"Gemini 응답을 목록에서 찾을 수 없음: {answer[:50]}...")
            return None
            
        except Exception as e:
            print(f"Gemini 매칭 오류: {e}")
            return None
    
    def _print_summary(self, start_index: int, end_index: int, output_path: str, total_in_csv: int):
        """통계 출력"""
        print(f"\n{'='*80}")
        print(f"작업 완료!")
        print(f"{'='*80}")
        print(f"이번 작업 범위: {start_index}번 ~ {end_index-1}번")
        print(f"이번 처리: {self.processed_count}개")
        print(f"누적 처리: {end_index}개 (목표: {self.max_rows}개)")
        print(f"")
        print(f"이번 성공: {self.success_count}개")
        print(f"이번 실패: {self.fail_count}개")
        if self.processed_count > 0:
            print(f"이번 성공률: {self.success_count/self.processed_count*100:.1f}%")
        print(f"")
        
        if end_index < self.max_rows and end_index < total_in_csv:
            remaining = min(self.max_rows, total_in_csv) - end_index
            print(f"⚠️  남은 작업: {remaining}개")
            print(f"이어서 작업하려면:")
            print(f"  python3 coupang1.py '{self.original_csv}' '{output_path}' {self.max_rows}")
        else:
            print(f"✓ 목표 달성! ({end_index}개 처리 완료)")
        
        print(f"")
        print(f"결과 파일:")
        print(f"  {output_path}")
        print(f"{'='*80}")


def main():
    """실행"""
    
    if len(sys.argv) < 2:
        print("\n사용법:")
        print("  python3 coupang1.py <원본CSV> [결과CSV] [최대개수]")
        print("\n예시:")
        print("  # 처음 시작 (200개 목표)")
        print("  python3 coupang1.py '취합노트 - 할인율요청.csv' 200")
        print("\n  # 이어서 작업 (114개 처리된 상태에서 200개까지)")
        print("  python3 coupang1.py '취합노트 - 할인율요청.csv' '취합노트 - 할인율요청_결과_20250930_153337.csv' 200")
        return
    
    original_csv = sys.argv[1]
    
    # 파일 존재 확인
    if not os.path.exists(original_csv):
        print(f"파일을 찾을 수 없음: {original_csv}")
        return
    
    # 인자 파싱
    result_csv = None
    max_rows = 200
    
    if len(sys.argv) >= 3:
        # 두번째 인자가 숫자면 max_rows, 아니면 result_csv
        try:
            max_rows = int(sys.argv[2])
        except ValueError:
            result_csv = sys.argv[2]
            if len(sys.argv) >= 4:
                max_rows = int(sys.argv[3])
    
    updater = CoupangPriceLinkUpdater(original_csv, result_csv, max_rows)
    updater.start()


if __name__ == "__main__":
    main()