"""
데이터 관리 모듈 - Gemini API 사용량 추적 및 재시작 지원
"""

import os
import pandas as pd
from datetime import datetime
from config import Config, FailureType


class DataManager:
    """CSV 파일 처리, 결과 저장, 진행상황 관리 담당 - Gemini API 지원"""
    
    def __init__(self):
        pass
    
    def auto_detect_start_point(self, input_csv_path, output_csv_path):
        """기존 결과를 분석해서 시작점 자동 감지 - Gemini API 제한 고려"""
        try:
            if not os.path.exists(output_csv_path):
                print("  결과 파일 없음 - 처음부터 시작")
                return 0, []
            
            existing_df = pd.read_csv(output_csv_path, encoding='utf-8-sig')
            
            if len(existing_df) == 0:
                print("  빈 결과 파일 - 처음부터 시작")
                return 0, []
            
            print(f"  기존 결과 파일 분석:")
            print(f"    총 레코드: {len(existing_df)}개")
            
            # Gemini API 사용량 확인
            if 'gemini_api_calls' in existing_df.columns:
                total_api_calls = existing_df['gemini_api_calls'].fillna(0).sum()
                print(f"    기존 Gemini API 총 호출: {int(total_api_calls)}회")
            
            if 'actual_index' in existing_df.columns:
                processed_indices = set()
                failed_indices = []
                
                # 모든 처리된 인덱스 수집
                for idx, row in existing_df.iterrows():
                    actual_index = row['actual_index']
                    if pd.notna(actual_index):
                        processed_indices.add(int(actual_index))
                        
                        # 재시도가 필요한 실패 상품만 failed_indices에 추가
                        if 'status' in row and row['status'] != 'success':
                            if 'failure_type' in row:
                                failure_type = row.get('failure_type', 'UNPROCESSED')
                                if FailureType.is_system_error(failure_type):
                                    # Gemini API 할당량 초과는 재시도하지 않음
                                    if failure_type != FailureType.GEMINI_QUOTA_EXCEEDED:
                                        failed_indices.append(int(actual_index))
                
                # 다음 시작점 계산: 처리된 인덱스 중 최대값 + 1
                if processed_indices:
                    next_start_index = max(processed_indices) + 1
                else:
                    next_start_index = 0
                
                print(f"    처리된 인덱스 범위: {min(processed_indices) if processed_indices else 0} ~ {max(processed_indices) if processed_indices else 0}")
                print(f"    재시도 대상: {len(failed_indices)}개 (API 할당량 초과 제외)")
                print(f"  시작점: {next_start_index}번째 상품부터 (실제 인덱스 기준)")
                
                return next_start_index, failed_indices
            
            else:
                print("  ⚠️  actual_index 컬럼이 없습니다 - 처음부터 시작")
                return 0, []
        
        except Exception as e:
            print(f"  시작점 자동 감지 실패: {e}")
            print("  안전을 위해 처음부터 시작")
            return 0, []
            
    def should_retry_product(self, status, failure_type):
        """상품이 재시도 대상인지 판단 - Gemini API 제한 고려"""
        if status == 'success':
            return False
        
        if not failure_type:
            return True  # failure_type이 없으면 재시도
        
        # Gemini API 할당량 초과는 재시도하지 않음
        if failure_type == FailureType.GEMINI_QUOTA_EXCEEDED:
            return False
        
        return FailureType.is_system_error(failure_type)
    
    def validate_input_csv(self, csv_file_path):
        """입력 CSV 파일 검증 - 영어 번역 선택적"""
        try:
            df = pd.read_csv(csv_file_path)
            
            # 기본 제품명 확인
            if 'product_name' not in df.columns:
                raise ValueError("CSV에 'product_name' 컬럼이 없습니다.")
            
            # 영어 번역 컬럼은 선택사항 (Gemini는 한글명으로 직접 검색)
            original_count = len(df)
            df = df.dropna(subset=['product_name'])
            df = df[df['product_name'].str.strip() != '']
            filtered_count = len(df)
            
            print(f"  원본 상품: {original_count}개")
            print(f"  유효한 제품명: {filtered_count}개")
            if original_count != filtered_count:
                print(f"  제품명 없는 상품: {original_count - filtered_count}개 (제외됨)")
            
            # 영어 번역 정보 확인 (있으면 참고용)
            if 'product_name_english' in df.columns:
                english_count = len(df[df['product_name_english'].notna() & (df['product_name_english'].str.strip() != '')])
                print(f"  영어 번역된 상품: {english_count}개 (참고용)")
            else:
                print("  영어 번역 없음 - 한글명으로 직접 검색")
            
            return df
            
        except Exception as e:
            print(f"  CSV 검증 실패: {e}")
            raise
    
    def initialize_output_csv(self, output_file_path):
        """CSV 파일 헤더 초기화 - Gemini API 컬럼 포함"""
        try:
            empty_df = pd.DataFrame(columns=Config.OUTPUT_COLUMNS)
            empty_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
            print(f"  결과 파일 초기화: {output_file_path}")
            print(f"  Gemini API 사용량 추적 컬럼 포함")
            
        except Exception as e:
            print(f"  CSV 초기화 실패: {e}")
    
    def append_result_to_csv(self, result, output_file_path):
        """결과를 CSV에 즉시 추가 (누적 방식) - 컬럼 순서 보장"""
        try:
            # Config.OUTPUT_COLUMNS 순서대로 값들을 리스트로 정렬
            ordered_values = [result.get(col, '') for col in Config.OUTPUT_COLUMNS]
            
            # DataFrame 생성 시 명시적으로 컬럼 순서 지정
            result_df = pd.DataFrame([ordered_values], columns=Config.OUTPUT_COLUMNS)
            
            # 기존 파일에 추가 (헤더 제외)
            result_df.to_csv(
                output_file_path, 
                mode='a',  # append 모드
                header=False,  # 헤더 제외 (이미 있음)
                index=False, 
                encoding='utf-8-sig'
            )
            
        except Exception as e:
            print(f"    CSV 저장 실패: {e}")
    
    def extract_coupang_price_info(self, row):
        """쿠팡 가격 정보 추출"""
        try:
            price_info = {}
            
            # 현재 가격
            if 'current_price' in row:
                current_price = str(row['current_price']).replace('원', '').replace(',', '').strip()
                if current_price and current_price != 'nan':
                    price_info['current_price'] = current_price
            
            # 원가
            if 'original_price' in row:
                original_price = str(row['original_price']).replace('원', '').replace(',', '').strip()
                if original_price and original_price != 'nan':
                    price_info['original_price'] = original_price
            
            # 할인율
            if 'discount_rate' in row:
                discount_rate = str(row['discount_rate']).replace('%', '').strip()
                if discount_rate and discount_rate != 'nan':
                    price_info['discount_rate'] = discount_rate
            
            # 쿠팡 URL
            if 'product_url' in row:
                price_info['url'] = str(row['product_url'])
            
            return price_info
            
        except Exception as e:
            return {}
    
    def calculate_price_comparison(self, coupang_price_info, iherb_price_info):
        """플랫폼 간 가격 비교 계산"""
        comparison_result = {
            'price_difference_krw': '',
            'cheaper_platform': '',
            'savings_amount': '',
            'savings_percentage': '',
            'price_difference_note': '원화 기준 직접 비교 가능'
        }
        
        try:
            # 쿠팡 가격 (현재 가격 우선, 없으면 원가)
            coupang_price = None
            if coupang_price_info.get('current_price'):
                coupang_price = int(coupang_price_info['current_price'])
            elif coupang_price_info.get('original_price'):
                coupang_price = int(coupang_price_info['original_price'])
            
            # 아이허브 가격 (할인가 우선, 없으면 정가)
            iherb_price = None
            if iherb_price_info.get('discount_price'):
                iherb_price = int(iherb_price_info['discount_price'])
            elif iherb_price_info.get('list_price'):
                iherb_price = int(iherb_price_info['list_price'])
            
            # 두 가격이 모두 있을 때만 비교
            if coupang_price and iherb_price:
                price_diff = coupang_price - iherb_price
                comparison_result['price_difference_krw'] = str(price_diff)
                
                if price_diff > 0:
                    # 아이허브가 더 저렴
                    comparison_result['cheaper_platform'] = '아이허브'
                    comparison_result['savings_amount'] = str(price_diff)
                    savings_pct = round((price_diff / coupang_price) * 100, 1)
                    comparison_result['savings_percentage'] = str(savings_pct)
                    comparison_result['price_difference_note'] = f'아이허브가 {price_diff:,}원 ({savings_pct}%) 더 저렴'
                    
                elif price_diff < 0:
                    # 쿠팡이 더 저렴
                    abs_diff = abs(price_diff)
                    comparison_result['cheaper_platform'] = '쿠팡'
                    comparison_result['savings_amount'] = str(abs_diff)
                    savings_pct = round((abs_diff / iherb_price) * 100, 1)
                    comparison_result['savings_percentage'] = str(savings_pct)
                    comparison_result['price_difference_note'] = f'쿠팡이 {abs_diff:,}원 ({savings_pct}%) 더 저렴'
                    
                else:
                    # 가격 동일
                    comparison_result['cheaper_platform'] = '동일'
                    comparison_result['savings_amount'] = '0'
                    comparison_result['savings_percentage'] = '0'
                    comparison_result['price_difference_note'] = '두 플랫폼 가격 동일'
            
            elif coupang_price and not iherb_price:
                comparison_result['price_difference_note'] = '아이허브 가격 정보 없음'
            elif not coupang_price and iherb_price:
                comparison_result['price_difference_note'] = '쿠팡 가격 정보 없음'
            else:
                comparison_result['price_difference_note'] = '양쪽 플랫폼 가격 정보 없음'
                
        except Exception as e:
            comparison_result['price_difference_note'] = f'가격 비교 계산 오류: {str(e)}'
        
        return comparison_result
    
    def create_result_record(self, row, actual_idx, english_name, product_url, 
                        similarity_score, product_code, iherb_product_name, 
                        coupang_price_info, iherb_price_info, matching_reason=None, 
                        failure_type=None, gemini_api_calls=0, gemini_confidence=None):
        """결과 레코드 생성 - Gemini API 사용량 추가"""
        
        # 가격 비교 계산
        price_comparison = self.calculate_price_comparison(coupang_price_info, iherb_price_info)
        
        # status 결정
        if product_code:
            status = 'success'
            failure_type = failure_type or FailureType.SUCCESS
        elif product_url:
            status = 'code_not_found'
            failure_type = failure_type or FailureType.PROCESSING_ERROR
        else:
            status = 'not_found'
            failure_type = failure_type or FailureType.NO_MATCHING_PRODUCT
        
        result = {
            'iherb_product_name': iherb_product_name or '',
            'coupang_product_name_english': english_name or '',
            'coupang_product_name': row.get('product_name', ''),
            'similarity_score': round(similarity_score, 3) if similarity_score else 0,
            'matching_reason': matching_reason or '매칭 정보 없음',
            'gemini_confidence': gemini_confidence or 'NONE',
            'failure_type': failure_type,
            'coupang_url': coupang_price_info.get('url', ''),
            'iherb_product_url': product_url or '',
            'coupang_product_id': row.get('product_id', ''),
            'iherb_product_code': product_code or '',
            'status': status,
            'coupang_current_price_krw': coupang_price_info.get('current_price', ''),
            'coupang_original_price_krw': coupang_price_info.get('original_price', ''),
            'coupang_discount_rate': coupang_price_info.get('discount_rate', ''),
            'iherb_list_price_krw': iherb_price_info.get('list_price', ''),
            'iherb_discount_price_krw': iherb_price_info.get('discount_price', ''),
            'iherb_discount_percent': iherb_price_info.get('discount_percent', ''),
            'iherb_subscription_discount': iherb_price_info.get('subscription_discount', ''),
            'iherb_price_per_unit': iherb_price_info.get('price_per_unit', ''),
            'is_in_stock': iherb_price_info.get('is_in_stock', True),
            'stock_message': iherb_price_info.get('stock_message', ''),
            'back_in_stock_date': iherb_price_info.get('back_in_stock_date', ''),
            'price_difference_krw': price_comparison['price_difference_krw'],
            'cheaper_platform': price_comparison['cheaper_platform'],
            'savings_amount': price_comparison['savings_amount'],
            'savings_percentage': price_comparison['savings_percentage'],
            'price_difference_note': price_comparison['price_difference_note'],
            'processed_at': datetime.now().isoformat(),
            'actual_index': actual_idx,
            'search_language': 'korean_direct',  # 한글 직접 검색
            'gemini_api_calls': gemini_api_calls  # Gemini API 사용량 추가
        }
        
        return result
    
    def print_summary(self, results_df):
        """결과 요약 - Gemini API 사용량 포함"""
        total = len(results_df)
        successful = len(results_df[results_df['status'] == 'success'])
        price_extracted = len(results_df[
            (results_df['iherb_discount_price_krw'] != '') | 
            (results_df['iherb_list_price_krw'] != '')
        ])
        
        print(f"\n처리 완료")
        print(f"  총 처리: {total}개 상품")
        print(f"  매칭 성공: {successful}개 ({successful/total*100:.1f}%)")
        print(f"  가격 추출: {price_extracted}개 ({price_extracted/total*100:.1f}%)")
        
        # Gemini API 사용량 통계
        if 'gemini_api_calls' in results_df.columns:
            total_api_calls = results_df['gemini_api_calls'].fillna(0).sum()
            avg_api_calls_per_product = total_api_calls / total if total > 0 else 0
            print(f"  Gemini API 총 호출: {int(total_api_calls)}회")
            print(f"  상품당 평균 API 호출: {avg_api_calls_per_product:.1f}회")
        
        # 실패 유형별 통계
        if 'failure_type' in results_df.columns and total > 0:
            print(f"\n실패 유형별 통계:")
            
            # Gemini 관련 통계 별도 표시
            gemini_successes = len(results_df[results_df['failure_type'] == FailureType.SUCCESS])
            gemini_no_matches = len(results_df[results_df['failure_type'] == FailureType.GEMINI_NO_MATCH])
            gemini_quota_exceeded = len(results_df[results_df['failure_type'] == FailureType.GEMINI_QUOTA_EXCEEDED])
            gemini_api_errors = len(results_df[results_df['failure_type'] == FailureType.GEMINI_API_ERROR])
            
            if gemini_successes + gemini_no_matches + gemini_quota_exceeded + gemini_api_errors > 0:
                print(f"  Gemini AI 관련:")
                if gemini_successes > 0:
                    print(f"    - 매칭 성공: {gemini_successes}개")
                if gemini_no_matches > 0:
                    print(f"    - 동일 제품 없음: {gemini_no_matches}개")
                if gemini_quota_exceeded > 0:
                    print(f"    - API 할당량 초과: {gemini_quota_exceeded}개")
                if gemini_api_errors > 0:
                    print(f"    - API 오류: {gemini_api_errors}개")
            
            # 시스템 오류
            system_errors = results_df[results_df['failure_type'].apply(FailureType.is_system_error)]
            system_errors = system_errors[system_errors['failure_type'] != FailureType.GEMINI_QUOTA_EXCEEDED]  # 제외
            if len(system_errors) > 0:
                print(f"  시스템 오류 ({len(system_errors)}개, 재시도 대상):")
                error_counts = system_errors['failure_type'].value_counts()
                for error_type, count in error_counts.items():
                    description = FailureType.get_description(error_type)
                    print(f"    - {description}: {count}개")
            
            # 정당한 실패
            legitimate_errors = results_df[~results_df['failure_type'].apply(FailureType.is_system_error)]
            legitimate_errors = legitimate_errors[legitimate_errors['status'] != 'success']
            if len(legitimate_errors) > 0:
                print(f"  정당한 실패 ({len(legitimate_errors)}개, 재시도 불필요):")
                legit_counts = legitimate_errors['failure_type'].value_counts()
                for error_type, count in legit_counts.items():
                    description = FailureType.get_description(error_type)
                    print(f"    - {description}: {count}개")
        
        # 성공한 상품 샘플
        if successful > 0:
            print(f"\n주요 성공 사례 (Gemini AI 매칭):")
            successful_df = results_df[results_df['status'] == 'success']
            
            for idx, (_, row) in enumerate(successful_df.head(5).iterrows()):
                korean_name = row['coupang_product_name'][:30] + "..."
                iherb_name = row.get('iherb_product_name', '')[:30] + "..."
                
                coupang_price = row.get('coupang_current_price_krw', '')
                iherb_price = row.get('iherb_discount_price_krw', '')
                
                print(f"  {idx+1}. {korean_name}")
                print(f"     매칭: {iherb_name}")
                
                if coupang_price and iherb_price:
                    try:
                        print(f"     가격: {int(coupang_price):,}원 vs {int(iherb_price):,}원")
                    except:
                        print(f"     가격: {coupang_price}원 vs {iherb_price}원")
                
                print()
    
    def format_price_comparison(self, coupang_price_info, iherb_price_info):
        """가격 비교 문자열 포맷팅 (원화 기준)"""
        coupang_price_str = ""
        if coupang_price_info.get('current_price'):
            coupang_price_str = f"{int(coupang_price_info['current_price']):,}원"
            if coupang_price_info.get('discount_rate'):
                coupang_price_str += f" ({coupang_price_info['discount_rate']}% 할인)"
        
        iherb_price_str = ""
        if iherb_price_info.get('discount_price'):
            iherb_price_str = f"{int(iherb_price_info['discount_price']):,}원"
            if iherb_price_info.get('discount_percent'):
                iherb_price_str += f" ({iherb_price_info['discount_percent']}% 할인)"
            if iherb_price_info.get('subscription_discount'):
                iherb_price_str += f" + 정기배송 {iherb_price_info['subscription_discount']}% 추가할인"
        elif iherb_price_info.get('list_price'):
            iherb_price_str = f"{int(iherb_price_info['list_price']):,}원 (정가)"
        
        return coupang_price_str, iherb_price_str