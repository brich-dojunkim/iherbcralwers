"""
한글 제품명 기반 iHerb 스크래퍼 - Gemini AI 통합 메인 실행 파일
"""

import os
import pandas as pd
import subprocess
import time
from browser_manager import BrowserManager
from iherb_client import IHerbClient
from product_matcher import ProductMatcher
from data_manager import DataManager
from config import Config, FailureType


class EnglishIHerbScraper:
    """메인 오케스트레이터 - Gemini AI 기반 상품 매칭"""
    
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
        """완전한 상품 처리 - Gemini AI 기반 매칭 + 자동 재시작 지원"""
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
            
            print("한글 제품명 기반 iHerb 가격 비교 스크래퍼 시작 (Gemini AI 통합)")
            print(f"  총 처리 상품: {len(process_list)}개")
            
            retry_count = len([x for x in process_list if x[1] == "재시도"])
            new_count = len([x for x in process_list if x[1] == "신규"])
            
            if retry_count > 0:
                print(f"  - 재시도 상품: {retry_count}개")
            if new_count > 0:
                print(f"  - 신규 상품: {new_count}개")
            
            print("  주요 개선사항:")
            print("    - 한글 제품명으로 직접 검색")
            print("    - 용량/개수 엄격 필터링")
            print("    - Gemini AI 최종 매칭 판단")
            print("    - API 할당량 모니터링")
            print("    - 자동 재시작 지원")
            
            # 4. CSV 헤더 초기화 (완전히 새로 시작하는 경우만)
            if start_from == 0 and not failed_indices:
                self.data_manager.initialize_output_csv(output_file_path)
            
            # 5. 메인 처리 루프
            for process_idx, (actual_idx, process_type) in enumerate(process_list):
                row = df.iloc[actual_idx]
                
                # 처리 직전에 한 번 더 중복 체크
                if self.is_already_processed(actual_idx, output_file_path):
                    print(f"\n[{process_idx+1}/{len(process_list)}] [{actual_idx}] 이미 처리됨 - 건너뜀")
                    continue
                
                print(f"\n[{process_idx+1}/{len(process_list)}] [{actual_idx}] {row['product_name']}")
                if process_type == "재시도":
                    print(f"  🔄 실패 상품 재시도 (Gemini AI 적용)")
                
                try:
                    self._process_single_product(row, actual_idx, len(process_list), output_file_path, process_idx)
                except KeyboardInterrupt:
                    print(f"\n⚠️ 사용자 중단 또는 API 제한으로 인한 안전 종료")
                    print(f"   현재까지 결과는 {output_file_path}에 저장되었습니다.")
                    print(f"   다시 실행하면 actual_index {actual_idx}부터 재시작됩니다.")
                    raise
            
            # 6. 최종 요약
            try:
                final_df = pd.read_csv(output_file_path)
                self.data_manager.print_summary(final_df)
                print("\nGemini AI 통합 효과:")
                print("  - 한글 제품명 직접 검색으로 정확도 향상")
                print("  - 용량/개수 엄격 필터링으로 오매칭 방지")
                print("  - AI 기반 최종 매칭으로 신뢰성 확보")
                print(f"  - 총 Gemini API 호출: {self.product_matcher.api_call_count}회")
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
        """단일 상품 처리 - Gemini AI 매칭 적용"""
        korean_name = row['product_name']
        english_name = row.get('product_name_english', '')  # 영어명은 참고용으로만 사용
        
        print(f"  한글명: {korean_name}")
        if english_name:
            print(f"  영어명 (참고): {english_name}")
        
        # 브라우저 재시작 체크
        if process_idx > 0 and process_idx % Config.BROWSER_RESTART_INTERVAL == 0:
            self._restart_browser_if_needed()
        
        # 쿠팡 가격 정보 표시
        coupang_price_info = self.data_manager.extract_coupang_price_info(row)
        self._display_coupang_price(coupang_price_info)
        
        # 아이허브 검색 및 정보 추출 (Gemini AI 적용)
        product_url, product_code, iherb_product_name, iherb_price_info, similarity_score, matching_reason, failure_type = \
            self._search_and_extract_iherb_info(korean_name, actual_idx)
        
        # 결과 생성 및 저장
        result = self.data_manager.create_result_record(
            row, actual_idx, english_name, product_url, similarity_score,
            product_code, iherb_product_name, coupang_price_info, iherb_price_info, 
            matching_reason, failure_type, self.product_matcher.api_call_count
        )
        
        self.data_manager.append_result_to_csv(result, output_file_path)
        
        # 결과 출력
        self._display_results(product_code, iherb_product_name, similarity_score, 
                            coupang_price_info, iherb_price_info, matching_reason, failure_type)
        
        # 진행률 표시
        self._display_progress(process_idx, total_count, output_file_path)
        
        # 딜레이
        self.browser_manager.random_delay()
    
    def _restart_browser_if_needed(self):
        """브라우저 재시작 처리"""
        print(f"\n  === 브라우저 완전 재시작 (매 {Config.BROWSER_RESTART_INTERVAL}개마다) ===")
        try:
            self._safe_browser_restart()
        except Exception as e:
            print(f"  브라우저 재시작 실패: {e}")
            raise
    
    def _safe_browser_restart(self):
        """안전한 브라우저 재시작"""
        try:
            print("  브라우저 안전 재시작 중...")
            
            # 1. 현재 브라우저 강제 종료
            if self.browser_manager.driver:
                try:
                    self.browser_manager.driver.quit()
                except:
                    pass
                self.browser_manager.driver = None
            
            # 2. Chrome 프로세스 완전 정리 (macOS)
            try:
                subprocess.run(['pkill', '-f', 'chrome'], check=False, capture_output=True)
                subprocess.run(['pkill', '-f', 'chromedriver'], check=False, capture_output=True)
                print("    Chrome 프로세스 정리 완료")
            except:
                pass
            
            # 3. 충분한 대기 시간 (포트 해제 대기)
            time.sleep(12)
            
            # 4. 새 브라우저 인스턴스 생성
            self.browser_manager = BrowserManager(headless=False, delay_range=self.delay_range)
            self.iherb_client = IHerbClient(self.browser_manager)
            self.product_matcher = ProductMatcher(self.iherb_client)
            
            # 5. 언어 설정 재적용
            self.iherb_client.set_language_to_english()
            
            # 6. 안정화 대기
            time.sleep(8)
            
            print("  브라우저 안전 재시작 완료 ✓")
            
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
    
    def _search_and_extract_iherb_info(self, korean_name, actual_idx):
        """아이허브 검색 및 정보 추출 - Gemini AI 적용"""
        product_url = None
        similarity_score = 0
        product_code = None
        iherb_product_name = None
        iherb_price_info = {}
        matching_reason = "처리 시작"
        failure_type = FailureType.UNPROCESSED
        
        for retry in range(Config.MAX_RETRIES):
            try:
                if retry > 0:
                    print(f"  재시도 {retry + 1}/{Config.MAX_RETRIES}")
                    time.sleep(5)
                
                # Gemini AI 기반 검색 실행
                product_url, similarity_score, match_details = self.product_matcher.search_product_enhanced(korean_name)
                
                # 매칭 결과에 따른 분류
                if not product_url:
                    if match_details and match_details.get('no_results'):
                        failure_type = FailureType.NO_SEARCH_RESULTS
                        matching_reason = "검색 결과 없음"
                    elif match_details and match_details.get('reason') == 'no_products_after_filtering':
                        # 용량/개수 불일치로 모든 제품 탈락
                        original_count = match_details.get('original_count', 0)
                        if original_count > 0:
                            failure_type = FailureType.COUNT_MISMATCH  # 또는 DOSAGE_MISMATCH
                            matching_reason = f"검색 결과 {original_count}개 중 용량/개수 일치하는 제품 없음"
                        else:
                            failure_type = FailureType.NO_SEARCH_RESULTS
                            matching_reason = "검색 결과 없음"
                    elif match_details and match_details.get('reason') == 'gemini_no_match':
                        failure_type = FailureType.GEMINI_NO_MATCH
                        matching_reason = "Gemini 판단: 동일 제품 없음"
                    else:
                        failure_type = FailureType.NO_MATCHING_PRODUCT
                        matching_reason = "매칭되는 상품 없음"
                    break
                
                # 매칭 성공 시 세부 사유
                if match_details and isinstance(match_details, dict):
                    if match_details.get('reason') == 'gemini_match':
                        failure_type = FailureType.SUCCESS
                        selected_product = match_details.get('selected_product', '')
                        matching_reason = f"Gemini AI 매칭: {selected_product[:30]}..."
                        similarity_score = 0.9  # Gemini 매칭 시 고정 점수
                
                if product_url:
                    # 상품 정보 추출
                    product_code, iherb_product_name, iherb_price_info = \
                        self.iherb_client.extract_product_info_with_price(product_url)
                    
                    if product_code:
                        failure_type = FailureType.SUCCESS
                        self.success_count += 1
                    break
                    
            except Exception as e:
                error_msg = str(e)
                print(f"  처리 중 오류 (시도 {retry + 1}): {error_msg[:100]}...")
                
                # Gemini API 할당량 초과 - 즉시 중단
                if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                    failure_type = FailureType.GEMINI_QUOTA_EXCEEDED
                    matching_reason = f"Gemini API 할당량 초과"
                    print(f"  ⚠️ Gemini API 할당량 초과 감지")
                    print(f"  현재까지 API 호출: {self.product_matcher.api_call_count}회")
                    print(f"  다시 실행하면 actual_index {actual_idx}부터 재시작됩니다.")
                    raise KeyboardInterrupt("Gemini API 할당량 초과로 인한 안전 종료")
                
                # Gemini API 타임아웃
                elif "GEMINI_TIMEOUT" in error_msg:
                    failure_type = FailureType.GEMINI_TIMEOUT
                    matching_reason = f"Gemini API 타임아웃"
                
                # Gemini API 일반 오류
                elif "GEMINI_API_ERROR" in error_msg:
                    failure_type = FailureType.GEMINI_API_ERROR
                    matching_reason = f"Gemini API 오류: {error_msg[:50]}"
                
                # 시스템 오류 타입별 분류
                elif "HTTPConnectionPool" in error_msg or "Connection refused" in error_msg:
                    failure_type = FailureType.NETWORK_ERROR
                    matching_reason = f"네트워크 연결 오류: {error_msg[:50]}"
                elif "WebDriverException" in error_msg or "selenium" in error_msg.lower():
                    failure_type = FailureType.WEBDRIVER_ERROR
                    matching_reason = f"웹드라이버 오류: {error_msg[:50]}"
                elif "TimeoutException" in error_msg or "timeout" in error_msg.lower():
                    failure_type = FailureType.TIMEOUT_ERROR
                    matching_reason = f"타임아웃 오류: {error_msg[:50]}"
                elif "chrome" in error_msg.lower():
                    failure_type = FailureType.BROWSER_ERROR
                    matching_reason = f"브라우저 오류: {error_msg[:50]}"
                else:
                    failure_type = FailureType.PROCESSING_ERROR
                    matching_reason = f"처리 오류: {error_msg[:50]}"
                
                if retry == Config.MAX_RETRIES - 1:
                    print("  최대 재시도 횟수 초과 - 건너뜀")
                else:
                    # 심각한 오류의 경우 브라우저 재시작
                    if self.browser_manager._is_critical_error(error_msg):
                        try:
                            print("  심각한 오류 감지 - 브라우저 완전 재시작")
                            self._safe_browser_restart()
                        except Exception as restart_error:
                            print(f"  브라우저 재시작 실패: {restart_error}")
                            failure_type = FailureType.BROWSER_ERROR
                            matching_reason = f"브라우저 재시작 실패: {str(restart_error)[:50]}"
                            break
        
        return product_url, product_code, iherb_product_name, iherb_price_info, similarity_score, matching_reason, failure_type
    
    def _display_results(self, product_code, iherb_product_name, similarity_score, 
                        coupang_price_info, iherb_price_info, matching_reason, failure_type):
        """결과 출력 - Gemini AI 정보 포함"""
        print()
        if product_code:
            print(f"  ✅ 매칭 성공! (Gemini AI)")
            print(f"     상품코드: {product_code}")
            print(f"     아이허브명: {iherb_product_name}")
            print(f"     매칭 점수: {similarity_score:.3f}")
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
            
            # 단위당 가격 표시
            if iherb_price_info.get('price_per_unit'):
                print(f"     단위가격: {iherb_price_info['price_per_unit']}")
            
            # 품절 상태 표시
            if not iherb_price_info.get('is_in_stock', True):
                print(f"     ⚠️ 품절 상태")
                if iherb_price_info.get('back_in_stock_date'):
                    print(f"     재입고: {iherb_price_info['back_in_stock_date']}")
                        
        elif similarity_score > 0:
            print(f"  ⚠️  상품은 찾았으나 상품코드 추출 실패")
            print(f"     아이허브명: {iherb_product_name}")
            print(f"     매칭 점수: {similarity_score:.3f}")
            print(f"     매칭 사유: {matching_reason}")
            print(f"     실패 유형: {FailureType.get_description(failure_type)}")
        else:
            print(f"  ❌ 매칭된 상품 없음")
            print(f"     매칭 사유: {matching_reason}")
            print(f"     실패 유형: {FailureType.get_description(failure_type)}")
    
    def _display_progress(self, process_idx, total_count, output_file_path):
        """진행률 표시 - Gemini API 사용량 포함"""
        print(f"  📊 진행률: {process_idx+1}/{total_count} ({(process_idx+1)/total_count*100:.1f}%)")
        print(f"     성공률: {self.success_count}/{process_idx+1} ({self.success_count/(process_idx+1)*100:.1f}%)")
        
        # Gemini API 사용량 표시 (Vision 포함)
        if hasattr(self.product_matcher, 'get_api_usage_stats'):
            api_stats = self.product_matcher.get_api_usage_stats()
            total_calls = api_stats['total_calls']
            vision_calls = api_stats['vision_calls']
            text_calls = api_stats['text_calls']
            
            print(f"     Gemini API 호출: {total_calls}회 (텍스트: {text_calls}회, Vision: {vision_calls}회)")
            
            if Config.IMAGE_COMPARISON_ENABLED and vision_calls > 0:
                print(f"     이미지 비교율: {vision_calls}/{process_idx+1} ({vision_calls/(process_idx+1)*100:.1f}%)")
        
        print(f"     결과 저장: {output_file_path} (실시간 누적)")
        print(f"     ⚠️ 중단 시 actual_index {process_idx}부터 재시작 가능")
        print(f"     적용 기술: Gemini AI + Vision 이미지 비교")
    
    def close(self):
        """브라우저 종료"""
        self.browser_manager.close()
    
    def is_already_processed(self, actual_index, output_csv_path):
        """특정 인덱스가 이미 처리되었는지 확인"""
        try:
            if not os.path.exists(output_csv_path):
                return False
            
            existing_df = pd.read_csv(output_csv_path, encoding='utf-8-sig')
            
            if 'actual_index' in existing_df.columns:
                processed_indices = existing_df['actual_index'].dropna().astype(int).tolist()
                return actual_index in processed_indices
            
            return False
            
        except Exception:
            return False

# 실행
if __name__ == "__main__":
    scraper = None
    try:
        print("한글 제품명 기반 iHerb 가격 비교 스크래퍼 - Gemini AI 통합 버전")
        print("주요 혁신사항:")
        print("- 한글 제품명으로 직접 검색 (번역 오류 제거)")
        print("- 용량/개수 엄격 필터링 (오매칭 방지)")
        print("- Gemini AI 최종 매칭 판단 (정확도 극대화)")
        print("- API 할당량 모니터링 및 자동 재시작")
        print("- 실시간 누적 저장으로 안전성 확보")
        print()
        print("모듈 구조:")
        print("- config.py: Gemini AI 설정 및 실패 유형 확장")
        print("- product_matcher.py: Gemini AI 통합 매칭 로직")
        print("- browser_manager.py: 브라우저 관리")
        print("- iherb_client.py: 가격 정보 추출")
        print("- data_manager.py: 데이터 처리 및 재시작 지원")
        print("- main.py: Gemini AI 통합 오케스트레이터")
        print()
        
        scraper = EnglishIHerbScraper(
            headless=False,
            delay_range=(2, 4),
            max_products_to_compare=4
        )
        
        input_csv = "/Users/brich/Desktop/iherb_price/iherbscraper/input/coupang_products_20250911_130214.csv"
        output_csv = "/output/iherb_results.csv"
        
        # Gemini AI 통합 처리 (자동 재시작 지원)
        results = scraper.process_products_complete(
            csv_file_path=input_csv,
            output_file_path=output_csv,
            limit=None,  # 전체 처리
            start_from=None  # 자동 감지
        )
        
        if results is not None:
            print(f"\n최종 결과: {results}")
            print("\nGemini AI 통합 완료 기능:")
            print("- 한글 제품명 직접 검색으로 정확도 향상")
            print("- 용량/개수 엄격 필터링으로 오매칭 방지")
            print("- Gemini AI 최종 매칭으로 신뢰성 확보")
            print("- API 할당량 모니터링으로 안전성 확보")
            print("- 자동 재시작으로 연속성 보장")
            print("- 실시간 누적 저장으로 데이터 보호")
            print(f"- 총 Gemini API 호출: {scraper.product_matcher.api_call_count}회")
    
    except KeyboardInterrupt:
        print("\n중단됨 (API 제한 또는 사용자 중단)")
        print("다시 실행하면 중단된 지점부터 자동으로 재시작됩니다.")
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper:
            scraper.close()