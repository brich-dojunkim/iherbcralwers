"""
영어 번역 기반 iHerb 스크래퍼 - 메인 실행 파일
"""

import os
import pandas as pd
from browser_manager import BrowserManager
from iherb_client import IHerbClient
from product_matcher import ProductMatcher
from data_manager import DataManager
from config import Config


class EnglishIHerbScraper:
    """메인 오케스트레이터 - 모든 모듈을 조합하여 완전한 스크래핑 실행"""
    
    def __init__(self, headless=False, delay_range=None, max_products_to_compare=None):
        # 설정 초기화
        self.delay_range = delay_range or Config.DEFAULT_DELAY_RANGE
        self.max_products_to_compare = max_products_to_compare or Config.MAX_PRODUCTS_TO_COMPARE
        self.success_count = 0
        
        # 모듈 초기화
        self.browser_manager = BrowserManager(headless, self.delay_range)
        self.iherb_client = IHerbClient(self.browser_manager)
        self.product_matcher = ProductMatcher(self.iherb_client)
        self.data_manager = DataManager()
        
        # 아이허브 언어를 영어로 설정
        self.iherb_client.set_language_to_english()
    
    def process_products_complete(self, csv_file_path, output_file_path, limit=None, start_from=None):
        """완전한 상품 처리 - 영어 번역명 기반"""
        try:
            # 1. CSV 검증 및 로딩
            df = self.data_manager.validate_input_csv(csv_file_path)
            
            if limit:
                df = df.head(limit)
            
            # 2. 시작점 자동 감지
            if start_from is None:
                start_from = self.data_manager.auto_detect_start_point(csv_file_path, output_file_path)
            
            # 3. 데이터 프레임 슬라이싱
            original_df_length = len(df)
            if start_from > 0:
                df = df.iloc[start_from:].reset_index(drop=True)
                if start_from >= original_df_length:
                    print("  모든 상품이 이미 처리되었습니다!")
                    return output_file_path
                print(f"  {start_from+1}번째 상품부터 재시작")
            
            print("영어 번역 기반 iHerb 가격 비교 스크래퍼 시작")
            print(f"  총 처리 상품: {len(df)}개 (전체: {original_df_length}개)")
            
            # 4. CSV 헤더 초기화 (새로 시작하는 경우만)
            if start_from == 0:
                self.data_manager.initialize_output_csv(output_file_path)
            
            # 5. 메인 처리 루프
            for idx, (index, row) in enumerate(df.iterrows()):
                actual_idx = idx + start_from
                self._process_single_product(row, actual_idx, original_df_length, output_file_path)
            
            # 6. 최종 요약
            try:
                final_df = pd.read_csv(output_file_path)
                self.data_manager.print_summary(final_df)
            except:
                print("최종 요약 생성 실패")
            
            return output_file_path
            
        except KeyboardInterrupt:
            print("\n사용자에 의해 중단됨")
            print(f"현재까지 결과는 {output_file_path}에 저장되어 있습니다.")
            return output_file_path
            
        except Exception as e:
            print(f"처리 중 오류: {e}")
            print(f"현재까지 결과는 {output_file_path}에 저장되어 있습니다.")
            return output_file_path
    
    def _process_single_product(self, row, actual_idx, total_count, output_file_path):
        """단일 상품 처리"""
        korean_name = row['product_name']
        english_name = row['product_name_english']
        
        print(f"\n[{actual_idx+1}/{total_count}] {korean_name[:40]}...")
        print(f"  영어명: {english_name[:50]}...")
        
        # 브라우저 재시작 체크
        if actual_idx > 0 and actual_idx % Config.BROWSER_RESTART_INTERVAL == 0:
            self._restart_browser_if_needed()
        
        # 쿠팡 가격 정보 표시
        coupang_price_info = self.data_manager.extract_coupang_price_info(row)
        self._display_coupang_price(coupang_price_info)
        
        # 아이허브 검색 및 정보 추출
        product_url, product_code, iherb_product_name, iherb_price_info, similarity_score = \
            self._search_and_extract_iherb_info(korean_name, english_name)
        
        # 결과 생성 및 저장
        result = self.data_manager.create_result_record(
            row, actual_idx, english_name, product_url, similarity_score,
            product_code, iherb_product_name, coupang_price_info, iherb_price_info
        )
        
        self.data_manager.append_result_to_csv(result, output_file_path)
        
        # 결과 출력
        self._display_results(product_code, similarity_score, coupang_price_info, iherb_price_info)
        
        # 진행률 표시
        self._display_progress(actual_idx, total_count, output_file_path)
        
        # 딜레이
        self.browser_manager.random_delay()
    
    def _restart_browser_if_needed(self):
        """브라우저 재시작 처리"""
        print(f"\n  === 브라우저 완전 재시작 (매 {Config.BROWSER_RESTART_INTERVAL}개마다) ===")
        try:
            self.browser_manager.restart_with_cleanup()
            # 재시작 후 언어 설정 다시 적용
            self.iherb_client.set_language_to_english()
        except Exception as e:
            print(f"  브라우저 재시작 실패: {e}")
            raise
    
    def _display_coupang_price(self, coupang_price_info):
        """쿠팡 가격 정보 표시"""
        coupang_summary = []
        if coupang_price_info.get('current_price'):
            coupang_summary.append(f"{int(coupang_price_info['current_price']):,}원")
        if coupang_price_info.get('discount_rate'):
            coupang_summary.append(f"{coupang_price_info['discount_rate']}% 할인")
        
        print(f"  쿠팡 가격: {' '.join(coupang_summary) if coupang_summary else '정보 없음'}")
    
    def _search_and_extract_iherb_info(self, korean_name, english_name):
        """아이허브 검색 및 정보 추출 (재시도 로직 포함)"""
        product_url = None
        similarity_score = 0
        product_code = None
        iherb_product_name = None
        iherb_price_info = {}
        
        for retry in range(Config.MAX_RETRIES):
            try:
                if retry > 0:
                    print(f"  재시도 {retry + 1}/{Config.MAX_RETRIES}")
                    import time
                    time.sleep(5)
                
                # 검색 실행
                product_url, similarity_score, match_details = self.product_matcher.search_product_enhanced(
                    korean_name, english_name
                )
                
                if product_url:
                    # 상품 정보 추출
                    product_code, iherb_product_name, iherb_price_info = \
                        self.iherb_client.extract_product_info_with_price(product_url)
                    
                    if product_code:
                        self.success_count += 1
                    break  # 성공하면 재시도 루프 종료
                else:
                    print("  매칭된 상품 없음")
                    break  # 검색 결과가 없으면 재시도할 필요 없음
                    
            except Exception as e:
                print(f"  처리 중 오류 (시도 {retry + 1}): {str(e)[:100]}...")
                if retry == Config.MAX_RETRIES - 1:
                    print("  최대 재시도 횟수 초과 - 건너뜀")
                else:
                    # 심각한 오류의 경우 브라우저 재시작
                    if self.browser_manager._is_critical_error(str(e)):
                        try:
                            print("  심각한 오류 감지 - 브라우저 완전 재시작")
                            self.browser_manager.restart_with_cleanup()
                            self.iherb_client.set_language_to_english()  # 언어 설정 재적용
                        except:
                            print("  브라우저 재시작 실패")
                            break
        
        return product_url, product_code, iherb_product_name, iherb_price_info, similarity_score
    
    def _display_results(self, product_code, similarity_score, coupang_price_info, iherb_price_info):
        """결과 출력"""
        print()
        if product_code:
            print(f"  결과: 매칭 성공 ✓")
            print(f"    상품코드: {product_code}")
            print(f"    유사도: {similarity_score:.3f}")
            
            # 가격 비교 (USD vs KRW)
            coupang_price_str, iherb_price_str = self.data_manager.format_price_comparison(
                coupang_price_info, iherb_price_info
            )
            
            if coupang_price_str and iherb_price_str:
                print(f"    쿠팡   : {coupang_price_str}")
                print(f"    아이허브: {iherb_price_str}")
                print(f"    주의   : 환율 적용하여 비교 필요")
            
        elif similarity_score > 0:
            print(f"  결과: 상품은 찾았으나 상품코드 추출 실패 ✗")
            print(f"    유사도: {similarity_score:.3f}")
        else:
            print(f"  결과: 매칭된 상품 없음 ✗")
    
    def _display_progress(self, actual_idx, total_count, output_file_path):
        """진행률 표시"""
        print(f"  진행률: {actual_idx+1}/{total_count} ({(actual_idx+1)/total_count*100:.1f}%)")
        print(f"  성공률: {self.success_count}/{actual_idx+1} ({self.success_count/(actual_idx+1)*100:.1f}%)")
        print(f"  결과 저장: {output_file_path} (실시간 누적)")
    
    def close(self):
        """브라우저 종료"""
        self.browser_manager.close()


# 실행
if __name__ == "__main__":
    scraper = None
    try:
        print("영어 번역 기반 iHerb 가격 비교 스크래퍼 - 모듈화 버전")
        print("모듈 구조:")
        print("- config.py: 설정 관리")
        print("- browser_manager.py: 브라우저 관리")
        print("- iherb_client.py: 아이허브 사이트 상호작용")
        print("- product_matcher.py: 상품 매칭 로직")
        print("- data_manager.py: 데이터 처리")
        print("- main.py: 메인 오케스트레이터")
        print()
        
        scraper = EnglishIHerbScraper(
            headless=False,
            delay_range=(2, 4),
            max_products_to_compare=4
        )
        
        # 절대경로로 coupang 폴더 사용
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        coupang_dir = os.path.join(project_root, "coupang")
        
        input_csv = os.path.join(coupang_dir, "coupang_products_translated.csv")
        output_csv = os.path.join(coupang_dir, "iherb_english_results_modular.csv")
        
        print(f"입력 파일: {input_csv}")
        print(f"출력 파일: {output_csv}")
        print()
        
        # start_from을 None으로 설정하면 자동으로 감지
        results = scraper.process_products_complete(
            csv_file_path=input_csv,
            output_file_path=output_csv,
            limit=5,  # 테스트용으로 5개로 설정
            start_from=None  # None이면 자동 감지, 숫자 입력시 해당 지점부터 시작
        )
        
        if results is not None:
            print(f"\n최종 결과: {results} ✓")
            print("\n모듈화 완료 기능:")
            print("- 기능별 모듈 분리로 유지보수성 향상")
            print("- 설정 파일을 통한 중앙화된 관리")
            print("- 각 모듈별 단위 테스트 가능")
            print("- 재사용 가능한 컴포넌트 구조")
            print("- 깔끔한 코드 구조와 명확한 책임 분담")
    
    except KeyboardInterrupt:
        print("\n중단됨")
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper:
            scraper.close()