"""
영어 번역 기반 iHerb 스크래퍼 - 메인 실행 파일
"""

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
        """완전한 상품 처리 - 영어 번역명 기반 + 실패 상품 자동 재시도"""
        try:
            # 1. CSV 검증 및 로딩
            df = self.data_manager.validate_input_csv(csv_file_path)
            
            if limit:
                df = df.head(limit)
            
            # 2. 시작점 자동 감지 및 실패 상품 목록 수집
            if start_from is None:
                start_from, failed_indices = self.data_manager.auto_detect_start_point(csv_file_path, output_file_path)
            else:
                failed_indices = []
            
            # 3. 처리할 상품 목록 생성
            process_list = []
            
            # 실패한 상품들 먼저 추가 (재시도)
            if failed_indices:
                for idx in failed_indices:
                    if idx < len(df):
                        process_list.append((idx, "재시도"))
            
            # 새로 처리할 상품들 추가
            original_df_length = len(df)
            if start_from < original_df_length:
                for idx in range(start_from, original_df_length):
                    process_list.append((idx, "신규"))
            
            if not process_list:
                print("  처리할 상품이 없습니다!")
                return output_file_path
            
            print("영어 번역 기반 iHerb 가격 비교 스크래퍼 시작")
            print(f"  총 처리 상품: {len(process_list)}개")
            
            retry_count = len([x for x in process_list if x[1] == "재시도"])
            new_count = len([x for x in process_list if x[1] == "신규"])
            
            if retry_count > 0:
                print(f"  - 재시도 상품: {retry_count}개")
            if new_count > 0:
                print(f"  - 신규 상품: {new_count}개")
            
            # 4. CSV 헤더 초기화 (완전히 새로 시작하는 경우만)
            if start_from == 0 and not failed_indices:
                self.data_manager.initialize_output_csv(output_file_path)
            
            # 5. 메인 처리 루프
            for process_idx, (actual_idx, process_type) in enumerate(process_list):
                row = df.iloc[actual_idx]
                
                print(f"\n[{process_idx+1}/{len(process_list)}] [{actual_idx}] {row['product_name']}")
                if process_type == "재시도":
                    print(f"  🔄 실패 상품 재시도")
                
                self._process_single_product(row, actual_idx, len(process_list), output_file_path, process_idx)
            
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
    
    def _process_single_product(self, row, actual_idx, total_count, output_file_path, process_idx):
        """단일 상품 처리"""
        korean_name = row['product_name']
        english_name = row['product_name_english']
        
        print(f"  영어명: {english_name}")
        
        # 브라우저 재시작 체크
        if process_idx > 0 and process_idx % Config.BROWSER_RESTART_INTERVAL == 0:
            self._restart_browser_if_needed()
        
        # 쿠팡 가격 정보 표시
        coupang_price_info = self.data_manager.extract_coupang_price_info(row)
        self._display_coupang_price(coupang_price_info)
        
        # 아이허브 검색 및 정보 추출 (6개 값 반환)
        product_url, product_code, iherb_product_name, iherb_price_info, similarity_score, matching_reason = \
            self._search_and_extract_iherb_info(korean_name, english_name)
        
        # 결과 생성 및 저장
        result = self.data_manager.create_result_record(
            row, actual_idx, english_name, product_url, similarity_score,
            product_code, iherb_product_name, coupang_price_info, iherb_price_info, matching_reason
        )
        
        self.data_manager.append_result_to_csv(result, output_file_path)
        
        # 결과 출력
        self._display_results(product_code, iherb_product_name, similarity_score, 
                            coupang_price_info, iherb_price_info, matching_reason)
        
        # 진행률 표시
        self._display_progress(process_idx, total_count, output_file_path)
        
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
        """아이허브 검색 및 정보 추출 (재시도 로직 포함) - 6개 값 반환"""
        product_url = None
        similarity_score = 0
        product_code = None
        iherb_product_name = None
        iherb_price_info = {}
        matching_reason = "매칭 시도되지 않음"
        
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
                
                # 매칭 사유 추출
                if match_details and isinstance(match_details, dict):
                    if match_details.get('rejected', False):
                        if match_details['reason'] == 'count_mismatch':
                            matching_reason = "개수 불일치로 탈락"
                        elif match_details['reason'] == 'dosage_mismatch':
                            matching_reason = "용량(mg) 불일치로 탈락"
                    elif similarity_score >= Config.MATCHING_THRESHOLDS['success_threshold']:
                        if match_details.get('exact_count_match') and match_details.get('dosage_match'):
                            matching_reason = "개수/용량 정확 매칭"
                        elif match_details.get('exact_count_match'):
                            matching_reason = "개수 정확 매칭"
                        elif match_details.get('dosage_match'):
                            matching_reason = "용량 정확 매칭"
                        else:
                            eng_sim = match_details.get('english_similarity', 0)
                            matching_reason = f"높은 유사도 (영어:{eng_sim:.2f})"
                    else:
                        eng_sim = match_details.get('english_similarity', 0)
                        matching_reason = f"낮은 유사도 (영어:{eng_sim:.2f})"
                else:
                    matching_reason = "매칭 상세 정보 없음"
                
                if product_url:
                    # 상품 정보 추출
                    product_code, iherb_product_name, iherb_price_info = \
                        self.iherb_client.extract_product_info_with_price(product_url)
                    
                    if product_code:
                        self.success_count += 1
                    break  # 성공하면 재시도 루프 종료
                else:
                    print("  매칭된 상품 없음")
                    matching_reason = "검색 결과 없음"
                    break  # 검색 결과가 없으면 재시도할 필요 없음
                    
            except Exception as e:
                print(f"  처리 중 오류 (시도 {retry + 1}): {str(e)[:100]}...")
                matching_reason = f"처리 오류: {str(e)[:50]}"
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
        
        return product_url, product_code, iherb_product_name, iherb_price_info, similarity_score, matching_reason
    
    def _display_results(self, product_code, iherb_product_name, similarity_score, 
                        coupang_price_info, iherb_price_info, matching_reason):
        """결과 출력 (개선된 버전)"""
        print()
        if product_code:
            print(f"  ✅ 매칭 성공!")
            print(f"     상품코드: {product_code}")
            print(f"     아이허브명: {iherb_product_name}")
            print(f"     유사도: {similarity_score:.3f}")
            print(f"     매칭 사유: {matching_reason}")
            
            # 가격 비교 상세 정보
            print(f"  💰 가격 정보:")
            
            # 쿠팡 가격
            if coupang_price_info.get('current_price'):
                coupang_price = int(coupang_price_info['current_price'])
                coupang_discount = coupang_price_info.get('discount_rate', '')
                discount_str = f" ({coupang_discount}% 할인)" if coupang_discount else ""
                print(f"     쿠팡   : {coupang_price:,}원{discount_str}")
            
            # 아이허브 가격
            if iherb_price_info.get('discount_price'):
                iherb_discount_price = int(iherb_price_info['discount_price'])
                iherb_discount_percent = iherb_price_info.get('discount_percent', '')
                subscription_discount = iherb_price_info.get('subscription_discount', '')
                
                discount_str = f" ({iherb_discount_percent}% 할인)" if iherb_discount_percent else ""
                subscription_str = f" + 정기배송 {subscription_discount}% 추가할인" if subscription_discount else ""
                
                print(f"     아이허브: {iherb_discount_price:,}원{discount_str}{subscription_str}")
                
                # 가격 차이 계산
                if coupang_price_info.get('current_price'):
                    coupang_price = int(coupang_price_info['current_price'])
                    price_diff = coupang_price - iherb_discount_price
                    if price_diff > 0:
                        print(f"     💡 아이허브가 {price_diff:,}원 더 저렴!")
                    elif price_diff < 0:
                        print(f"     💡 쿠팡이 {abs(price_diff):,}원 더 저렴!")
                    else:
                        print(f"     💡 가격 동일!")
                        
            elif iherb_price_info.get('list_price'):
                iherb_list_price = int(iherb_price_info['list_price'])
                print(f"     아이허브: {iherb_list_price:,}원 (정가)")
            else:
                print(f"     아이허브: 가격 정보 없음")
                
        elif similarity_score > 0:
            print(f"  ⚠️  상품은 찾았으나 상품코드 추출 실패")
            print(f"     아이허브명: {iherb_product_name}")
            print(f"     유사도: {similarity_score:.3f}")
            print(f"     매칭 사유: {matching_reason}")
        else:
            print(f"  ❌ 매칭된 상품 없음")
            print(f"     매칭 사유: {matching_reason}")
    
    def _display_progress(self, process_idx, total_count, output_file_path):
        """진행률 표시"""
        print(f"  📊 진행률: {process_idx+1}/{total_count} ({(process_idx+1)/total_count*100:.1f}%)")
        print(f"     성공률: {self.success_count}/{process_idx+1} ({self.success_count/(process_idx+1)*100:.1f}%)")
        print(f"     결과 저장: {output_file_path} (실시간 누적)")
    
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
        
        input_csv = "/Users/brich/Desktop/iherb_price/coupang/coupang_products_translated.csv"
        output_csv = "/Users/brich/Desktop/iherb_price/coupang/iherb_english_results_modular.csv"
        
        # 간단한 처리 (실패 상품 자동 재시도 포함)
        results = scraper.process_products_complete(
            csv_file_path=input_csv,
            output_file_path=output_csv,
            limit=None,  # 전체 처리
            start_from=None  # 자동 감지
        )
        
        if results is not None:
            print(f"\n최종 결과: {results}")
            print("\n모듈화 완료 기능:")
            print("- 기능별 모듈 분리로 유지보수성 향상")
            print("- 설정 파일을 통한 중앙화된 관리")
            print("- 각 모듈별 단위 테스트 가능")
            print("- 재사용 가능한 컴포넌트 구조")
            print("- 깔끔한 코드 구조와 명확한 책임 분담")
            print("- 실패한 상품 자동 재시도 기능")
    
    except KeyboardInterrupt:
        print("\n중단됨")
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper:
            scraper.close()