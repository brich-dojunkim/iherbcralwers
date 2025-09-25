"""
영어명 기반 iHerb 스크래퍼 - Gemini AI 통합 메인 실행 파일
주요 수정사항:
1. 매칭 관련 파라미터 제거 (similarity_score, matching_reason, gemini_confidence)
2. 쿠팡 재고 정보 출력 추가
3. create_result_record 호출 방식 수정
4. 진행률 표시 단순화
"""

import sys
import os
import pandas as pd
import subprocess
import time
import google.generativeai as genai

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import PathConfig

from iherb_config import IHerbConfig, FailureType
from iherb_manager import BrowserManager
from iherb_client import IHerbClient
from product_matcher import ProductMatcher
from data_manager import DataManager

class EnglishIHerbScraper:
    """영어명 기반 상품 매칭 - Gemini AI 통합"""
    
    def __init__(self, headless=False, delay_range=None, max_products_to_compare=None):
        self.delay_range = delay_range or IHerbConfig.DEFAULT_DELAY_RANGE
        self.max_products_to_compare = max_products_to_compare or IHerbConfig.MAX_PRODUCTS_TO_COMPARE
        self.success_count = 0

        # Gemini 번역 모델 초기화
        genai.configure(api_key=IHerbConfig.GEMINI_API_KEY)
        self.translator = genai.GenerativeModel(IHerbConfig.GEMINI_TEXT_MODEL)

        # 모듈 초기화
        self.browser_manager = BrowserManager(headless, self.delay_range)
        self.iherb_client = IHerbClient(self.browser_manager)
        self.product_matcher = ProductMatcher(self.iherb_client)
        self.data_manager = DataManager()

        # 아이허브 언어를 영어로 설정
        self.iherb_client.set_language_to_english()
    
    def translate_product_name(self, korean_name):
        """한글 제품명을 영어로 번역"""
        try:
            prompt = f"Translate this Korean supplement product name to English: {korean_name}\n\nAnswer with English product name only:"
            
            response = self.translator.generate_content(
                prompt,
                generation_config={
                    'temperature': 0.1,
                    'max_output_tokens': 100
                }
            )
            
            if response and response.text:
                return response.text.strip()
            else:
                return None
                
        except Exception as e:
            print(f"    번역 실패: {e}")
            return None
    
    def process_products_complete(self, csv_file_path, output_file_path, limit=None, start_from=None):
        """완전한 상품 처리 - 영어명 우선 검색"""
        try:
            # CSV 검증 및 로딩
            df = self.data_manager.validate_input_csv(csv_file_path)
            
            if limit:
                df = df.head(limit)
            
            # 시작점 자동 감지
            if start_from is None:
                start_from, failed_indices = self.data_manager.auto_detect_start_point(csv_file_path, output_file_path)
            else:
                failed_indices = []
            
            # 출력 디렉토리 생성
            output_dir = os.path.dirname(output_file_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                print(f"  출력 디렉토리 생성: {output_dir}")
            
            # 처리할 상품 목록 생성
            process_list = []
            
            if failed_indices:
                for idx in failed_indices:
                    if idx < len(df):
                        process_list.append((idx, "재시도"))
            
            original_df_length = len(df)
            if start_from < original_df_length:
                for idx in range(start_from, original_df_length):
                    process_list.append((idx, "신규"))
            
            if not process_list:
                print("  처리할 상품이 없습니다!")
                return output_file_path
            
            print("영어명 기반 iHerb 가격 비교 스크래퍼 시작 (Gemini AI 통합)")
            print(f"  총 처리 상품: {len(process_list)}개")
            
            retry_count = len([x for x in process_list if x[1] == "재시도"])
            new_count = len([x for x in process_list if x[1] == "신규"])
            
            if retry_count > 0:
                print(f"  - 재시도 상품: {retry_count}개")
            if new_count > 0:
                print(f"  - 신규 상품: {new_count}개")
            
            print("  주요 개선사항:")
            print("    - 영어명 우선 검색 (번역 기능 통합)")
            print("    - Gemini AI 매칭 판단")
            print("    - 이미지 비교 활성화")
            print("    - 쿠팡 재고 정보 포함")
            
            # CSV 헤더 초기화
            if start_from == 0 and not failed_indices:
                self.data_manager.initialize_output_csv(output_file_path)
            
            # 메인 처리 루프
            for process_idx, (actual_idx, process_type) in enumerate(process_list):
                row = df.iloc[actual_idx]
                
                print(f"\n[{process_idx+1}/{len(process_list)}] [{actual_idx}] {row['product_name']}")
                if process_type == "재시도":
                    print(f"  🔄 실패 상품 재시도")
                
                try:
                    self._process_single_product(row, actual_idx, len(process_list), output_file_path, process_idx)
                except KeyboardInterrupt:
                    print(f"\n⚠️ 사용자 중단 또는 API 제한으로 인한 안전 종료")
                    print(f"   현재까지 결과는 {output_file_path}에 저장되었습니다.")
                    print(f"   다시 실행하면 인덱스 {actual_idx}부터 재시작됩니다.")
                    raise
            
            # 최종 요약
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
        english_name = row.get('product_name_english', '')
        coupang_product_id = row.get('product_id', '')
        
        print(f"  한글명: {korean_name}")
        
        # 영어명 결정
        search_name = None
        if english_name and english_name.strip():
            search_name = english_name.strip()
            print(f"  영어명 (기존): {search_name}")
        else:
            print("  영어명 없음 - 실시간 번역 시도 중...")
            translated = self.translate_product_name(korean_name)
            if translated:
                search_name = translated
                print(f"  영어명 (번역): {search_name}")
            else:
                search_name = korean_name
                print(f"  번역 실패 - 한글명 사용: {search_name}")
        
        # 브라우저 재시작 체크
        if process_idx > 0 and process_idx % IHerbConfig.BROWSER_RESTART_INTERVAL == 0:
            self._restart_browser_if_needed()
        
        # 쿠팡 가격 정보 표시
        coupang_price_info = self.data_manager.extract_coupang_price_info(row)
        self._display_coupang_info(row, coupang_price_info)
        
        # 아이허브 검색 및 정보 추출
        result = self._search_and_extract_iherb_info(search_name, coupang_product_id, actual_idx)
        product_url, product_code, iherb_product_name, iherb_price_info, failure_type = result
        
        # 결과 생성 및 저장
        result_record = self.data_manager.create_result_record(
            row=row,
            english_name=search_name,
            product_url=product_url,
            product_code=product_code,
            iherb_product_name=iherb_product_name,
            coupang_price_info=coupang_price_info,
            iherb_price_info=iherb_price_info,
            failure_type=failure_type
        )
        
        self.data_manager.append_result_to_csv(result_record, output_file_path)
        
        # 결과 출력
        self._display_results(product_code, iherb_product_name, coupang_price_info, iherb_price_info, failure_type)
        
        # 진행률 표시
        self._display_progress(process_idx, total_count)
        
        # 딜레이
        self.browser_manager.random_delay()
    
    def _restart_browser_if_needed(self):
        """브라우저 재시작 처리"""
        print(f"\n  === 브라우저 완전 재시작 (매 {IHerbConfig.BROWSER_RESTART_INTERVAL}개마다) ===")
        try:
            self._safe_browser_restart()
        except Exception as e:
            print(f"  브라우저 재시작 실패: {e}")
            raise
    
    def _safe_browser_restart(self):
        """안전한 브라우저 재시작"""
        try:
            print("  브라우저 안전 재시작 중...")
            
            if self.browser_manager.driver:
                try:
                    self.browser_manager.driver.quit()
                except:
                    pass
                self.browser_manager.driver = None
            
            try:
                subprocess.run(['pkill', '-f', 'chrome'], check=False, capture_output=True)
                subprocess.run(['pkill', '-f', 'chromedriver'], check=False, capture_output=True)
                print("    Chrome 프로세스 정리 완료")
            except:
                pass
            
            time.sleep(12)
            
            self.browser_manager = BrowserManager(headless=False, delay_range=self.delay_range)
            self.iherb_client = IHerbClient(self.browser_manager)
            self.product_matcher = ProductMatcher(self.iherb_client)
            
            # 🔁 여기서 다시 영어 설정 호출
            self.iherb_client.set_language_to_english()
            
            time.sleep(8)
            
            print("  브라우저 안전 재시작 완료 ✓")
            
        except Exception as e:
            print(f"  브라우저 재시작 실패: {e}")
            raise
    
    def _display_coupang_info(self, row, coupang_price_info):
        """쿠팡 정보 표시 - 가격 + 재고 정보 (numpy 타입 안전 처리)"""
        
        # 안전한 문자열 변환 함수
        def safe_get_str(value):
            if pd.isna(value):
                return ''
            return str(value).strip()
        
        # 가격 정보
        coupang_summary = []
        if coupang_price_info.get('current_price'):
            try:
                price_val = int(float(coupang_price_info['current_price']))
                coupang_summary.append(f"{price_val:,}원")
            except (ValueError, TypeError):
                coupang_summary.append(f"{coupang_price_info['current_price']}원")
        
        if coupang_price_info.get('discount_rate'):
            coupang_summary.append(f"{coupang_price_info['discount_rate']}% 할인")
        
        price_text = ' '.join(coupang_summary) if coupang_summary else '정보 없음'
        
        # 재고 정보
        stock_info = []
        
        stock_status = safe_get_str(row.get('stock_status', ''))
        if stock_status == 'in_stock':
            stock_info.append('재고있음')
        elif stock_status == 'out_of_stock':
            stock_info.append('품절')
        
        delivery_badge = safe_get_str(row.get('delivery_badge', ''))
        if delivery_badge:
            stock_info.append(delivery_badge)
        
        origin = safe_get_str(row.get('origin_country', ''))
        if origin:
            stock_info.append(f'원산지:{origin}')
        
        unit_price = safe_get_str(row.get('unit_price', ''))
        if unit_price:
            stock_info.append(f'단위가격:{unit_price}')
        
        stock_text = ', '.join(stock_info) if stock_info else ''
        
        print(f"  쿠팡: {price_text}")
        if stock_text:
            print(f"  쿠팡 정보: {stock_text}")

    def _search_and_extract_iherb_info(self, search_name, coupang_product_id, actual_idx):
        """아이허브 검색 및 정보 추출 - 단순화된 버전"""
        product_url = None
        product_code = None
        iherb_product_name = None
        iherb_price_info = {}
        failure_type = FailureType.UNPROCESSED
        
        for retry in range(IHerbConfig.MAX_RETRIES):
            try:
                if retry > 0:
                    print(f"  재시도 {retry + 1}/{IHerbConfig.MAX_RETRIES}")
                    time.sleep(5)
                
                # Gemini AI 기반 검색 실행
                search_result = self.product_matcher.search_product_enhanced(search_name, coupang_product_id)
                
                if len(search_result) == 3:
                    product_url, similarity_score, match_details = search_result
                else:
                    print(f"  검색 결과 형식 오류")
                    failure_type = FailureType.PROCESSING_ERROR
                    break
                
                # 매칭 결과 분류
                if not product_url:
                    if match_details and match_details.get('no_results'):
                        failure_type = FailureType.NO_SEARCH_RESULTS
                    elif match_details and match_details.get('reason') == 'gemini_no_match':
                        failure_type = FailureType.GEMINI_NO_MATCH
                    else:
                        failure_type = FailureType.NO_MATCHING_PRODUCT
                    break
                
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
                    print(f"  ⚠️ Gemini API 할당량 초과 감지")
                    raise KeyboardInterrupt("Gemini API 할당량 초과로 인한 안전 종료")
                
                elif "GEMINI_TIMEOUT" in error_msg:
                    failure_type = FailureType.GEMINI_TIMEOUT
                elif "GEMINI_API_ERROR" in error_msg:
                    failure_type = FailureType.GEMINI_API_ERROR
                elif "HTTPConnectionPool" in error_msg or "Connection refused" in error_msg:
                    failure_type = FailureType.NETWORK_ERROR
                elif "WebDriverException" in error_msg or "selenium" in error_msg.lower():
                    failure_type = FailureType.WEBDRIVER_ERROR
                elif "TimeoutException" in error_msg or "timeout" in error_msg.lower():
                    failure_type = FailureType.TIMEOUT_ERROR
                elif "chrome" in error_msg.lower():
                    failure_type = FailureType.BROWSER_ERROR
                else:
                    failure_type = FailureType.PROCESSING_ERROR
                
                if retry == IHerbConfig.MAX_RETRIES - 1:
                    print("  최대 재시도 횟수 초과 - 건너뜀")
                else:
                    if self.browser_manager._is_critical_error(error_msg):
                        try:
                            print("  심각한 오류 감지 - 브라우저 완전 재시작")
                            self._safe_browser_restart()
                        except Exception as restart_error:
                            print(f"  브라우저 재시작 실패: {restart_error}")
                            failure_type = FailureType.BROWSER_ERROR
                            break
        
        return product_url, product_code, iherb_product_name, iherb_price_info, failure_type
    
    def _display_results(self, product_code, iherb_product_name, coupang_price_info, iherb_price_info, failure_type):
        """결과 출력 - 단순화된 버전"""
        print()
        if product_code:
            print(f"  ✅ 매칭 성공!")
            print(f"     상품코드: {product_code}")
            print(f"     아이허브명: {iherb_product_name}")
            
            print(f"  💰 가격 정보:")
            
            if coupang_price_info.get('current_price'):
                coupang_price = int(coupang_price_info['current_price'])
                coupang_discount = coupang_price_info.get('discount_rate', '')
                discount_str = f" ({coupang_discount}% 할인)" if coupang_discount else ""
                print(f"     쿠팡   : {coupang_price:,}원{discount_str}")
            
            if iherb_price_info.get('discount_price'):
                iherb_discount_price = int(iherb_price_info['discount_price'])
                iherb_discount_percent = iherb_price_info.get('discount_percent', '')
                subscription_discount = iherb_price_info.get('subscription_discount', '')
                
                discount_str = f" ({iherb_discount_percent}% 할인)" if iherb_discount_percent else ""
                subscription_str = f" + 정기배송 {subscription_discount}% 추가할인" if subscription_discount else ""
                
                print(f"     아이허브: {iherb_discount_price:,}원{discount_str}{subscription_str}")
                
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
            
            if iherb_price_info.get('price_per_unit'):
                print(f"     단위가격: {iherb_price_info['price_per_unit']}")
            
            if not iherb_price_info.get('is_in_stock', True):
                print(f"     ⚠️ 품절 상태")
                if iherb_price_info.get('back_in_stock_date'):
                    print(f"     재입고: {iherb_price_info['back_in_stock_date']}")
                        
        else:
            print(f"  ❌ 매칭된 상품 없음")
            print(f"     실패 유형: {FailureType.get_description(failure_type)}")
    
    def _display_progress(self, process_idx, total_count):
        """진행률 표시 - 단순화"""
        print(f"  📊 진행률: {process_idx+1}/{total_count} ({(process_idx+1)/total_count*100:.1f}%)")
        print(f"     성공률: {self.success_count}/{process_idx+1} ({self.success_count/(process_idx+1)*100:.1f}%)")
        
        # Gemini API 사용량 (간단히)
        if hasattr(self.product_matcher, 'api_call_count'):
            print(f"     Gemini API: {self.product_matcher.api_call_count}회")
    
    def close(self):
        """브라우저 종료"""
        self.browser_manager.close()


# 실행
if __name__ == "__main__":
    scraper = None
    try:
        print("영어명 기반 iHerb 가격 비교 스크래퍼 - Gemini AI 통합")
        print("주요 기능:")
        print("- 영어명 우선 검색 (실시간 번역 지원)")
        print("- Gemini AI 매칭 판단")
        print("- 이미지 비교 활성화")
        print("- 쿠팡 재고 정보 포함")
        print("- 실시간 누적 저장")
        print()
        
        scraper = EnglishIHerbScraper(
            headless=False,
            delay_range=(2, 4),
            max_products_to_compare=4
        )
        
        input_csv = "/Users/brich/Desktop/iherb_price/coupang/coupang_csv/coupang_products_v2_20250918_141436_translated.csv"
        output_csv = os.path.join(PathConfig.UNIFIED_OUTPUTS_DIR, "coupang_iherb_products_{timestamp}.csv")
        
        results = scraper.process_products_complete(
            csv_file_path=input_csv,
            output_file_path=output_csv,
            limit=None,
            start_from=None
        )
        
        if results is not None:
            print(f"\n최종 결과: {results}")
            print("\n완료된 기능:")
            print("- 34개 컬럼 완전한 가격 비교")
            print("- 쿠팡 재고 정보 포함")
            print("- 대칭적인 정보 구조")
            print("- 자동 재시작 지원")
    
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