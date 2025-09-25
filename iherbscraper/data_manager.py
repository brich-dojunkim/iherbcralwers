"""
데이터 관리 모듈 - Gemini API 사용량 추적 및 재시작 지원
주요 수정사항:
1. 매칭 관련 4개 컬럼 제거 (similarity_score, matching_reason, gemini_confidence, failure_type)
2. 쿠팡 재고 관련 5개 컬럼 추가
3. create_result_record 메서드 수정
4. 사용되지 않는 메서드 제거
"""

import os
import pandas as pd
from iherb_config import IHerbConfig, FailureType

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
            
            if 'status' in existing_df.columns:
                processed_indices = set()
                failed_indices = []
                
                for idx, row in existing_df.iterrows():
                    if 'status' in row and row['status'] == 'success':
                        processed_indices.add(idx)
                    else:
                        # 시스템 오류만 재시도 대상에 포함
                        if 'failure_type' in row:
                            failure_type = row.get('failure_type', 'UNPROCESSED')
                            if FailureType.is_system_error(failure_type):
                                if failure_type != FailureType.GEMINI_QUOTA_EXCEEDED:
                                    failed_indices.append(idx)
                
                next_start_index = max(processed_indices) + 1 if processed_indices else 0
                
                print(f"    처리된 상품: {len(processed_indices)}개")
                print(f"    재시도 대상: {len(failed_indices)}개")
                print(f"  시작점: {next_start_index}번째 상품부터")
                
                return next_start_index, failed_indices
            else:
                print("  상태 컬럼이 없습니다 - 처음부터 시작")
                return 0, []
        
        except Exception as e:
            print(f"  시작점 자동 감지 실패: {e}")
            print("  안전을 위해 처음부터 시작")
            return 0, []
    
    def validate_input_csv(self, csv_file_path):
        """입력 CSV 파일 검증"""
        try:
            df = pd.read_csv(csv_file_path)
            
            if 'product_name' not in df.columns:
                raise ValueError("CSV에 'product_name' 컬럼이 없습니다.")
            
            original_count = len(df)
            df = df.dropna(subset=['product_name'])
            df = df[df['product_name'].str.strip() != '']
            filtered_count = len(df)
            
            print(f"  원본 상품: {original_count}개")
            print(f"  유효한 제품명: {filtered_count}개")
            
            if original_count != filtered_count:
                print(f"  제품명 없는 상품: {original_count - filtered_count}개 (제외됨)")
            
            if 'product_name_english' in df.columns:
                english_count = len(df[df['product_name_english'].notna() & (df['product_name_english'].str.strip() != '')])
                print(f"  영어 번역된 상품: {english_count}개")
            else:
                print("  영어 번역 없음 - 한글명으로 직접 검색")
            
            return df
            
        except Exception as e:
            print(f"  CSV 검증 실패: {e}")
            raise
    
    def initialize_output_csv(self, output_file_path):
        """CSV 파일 헤더 초기화"""
        try:
            empty_df = pd.DataFrame(columns=IHerbConfig.OUTPUT_COLUMNS)
            empty_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
            print(f"  결과 파일 초기화: {output_file_path}")
            print(f"  총 {len(IHerbConfig.OUTPUT_COLUMNS)}개 컬럼 생성")
            
        except Exception as e:
            print(f"  CSV 초기화 실패: {e}")
    
    def append_result_to_csv(self, result, output_file_path):
        """결과를 CSV에 즉시 추가 (누적 방식) - 컬럼 순서 보장"""
        try:
            ordered_values = [result.get(col, '') for col in IHerbConfig.OUTPUT_COLUMNS]
            result_df = pd.DataFrame([ordered_values], columns=IHerbConfig.OUTPUT_COLUMNS)
            
            result_df.to_csv(
                output_file_path, 
                mode='a',
                header=False,
                index=False, 
                encoding='utf-8-sig'
            )
            
        except Exception as e:
            print(f"    CSV 저장 실패: {e}")
    
    def extract_coupang_price_info(self, row):
        """쿠팡 가격 정보 추출 - numpy 타입 안전 처리"""
        try:
            price_info = {}
            
            # 안전한 문자열 변환 함수
            def safe_str_convert(value):
                if pd.isna(value):
                    return ''
                return str(value).replace('원', '').replace(',', '').strip()
            
            if 'current_price' in row:
                current_price = safe_str_convert(row['current_price'])
                if current_price and current_price != 'nan':
                    price_info['current_price'] = current_price
            
            if 'original_price' in row:
                original_price = safe_str_convert(row['original_price'])
                if original_price and original_price != 'nan':
                    price_info['original_price'] = original_price
            
            if 'discount_rate' in row:
                discount_rate = safe_str_convert(row['discount_rate']).replace('%', '')
                if discount_rate and discount_rate != 'nan':
                    price_info['discount_rate'] = discount_rate
            
            if 'product_url' in row:
                url = row.get('product_url', '')
                if pd.notna(url):
                    price_info['url'] = str(url)
            
            return price_info
            
        except Exception as e:
            print(f"    가격 정보 추출 오류: {e}")
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
            coupang_price = None
            if coupang_price_info.get('current_price'):
                coupang_price = int(coupang_price_info['current_price'])
            elif coupang_price_info.get('original_price'):
                coupang_price = int(coupang_price_info['original_price'])
            
            iherb_price = None
            if iherb_price_info.get('discount_price'):
                iherb_price = int(iherb_price_info['discount_price'])
            elif iherb_price_info.get('list_price'):
                iherb_price = int(iherb_price_info['list_price'])
            
            if coupang_price and iherb_price:
                price_diff = coupang_price - iherb_price
                comparison_result['price_difference_krw'] = str(price_diff)
                
                if price_diff > 0:
                    comparison_result['cheaper_platform'] = '아이허브'
                    comparison_result['savings_amount'] = str(price_diff)
                    savings_pct = round((price_diff / coupang_price) * 100, 1)
                    comparison_result['savings_percentage'] = str(savings_pct)
                    comparison_result['price_difference_note'] = f'아이허브가 {price_diff:,}원 ({savings_pct}%) 더 저렴'
                    
                elif price_diff < 0:
                    abs_diff = abs(price_diff)
                    comparison_result['cheaper_platform'] = '쿠팡'
                    comparison_result['savings_amount'] = str(abs_diff)
                    savings_pct = round((abs_diff / iherb_price) * 100, 1)
                    comparison_result['savings_percentage'] = str(savings_pct)
                    comparison_result['price_difference_note'] = f'쿠팡이 {abs_diff:,}원 ({savings_pct}%) 더 저렴'
                    
                else:
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
    
    def create_result_record(self, row, english_name, product_url, 
                        product_code, iherb_product_name, 
                        coupang_price_info, iherb_price_info, failure_type=None):
        """결과 레코드 생성 - 쿠팡 재고 정보 추가 + 매칭 정보 제거"""
        
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
            # 상품정보 (5개)
            'iherb_product_name': iherb_product_name or '',
            'coupang_product_name_english': english_name or '',
            'coupang_product_name': row.get('product_name', ''),
            'coupang_product_id': row.get('product_id', ''),
            'iherb_product_code': product_code or '',
            
            # URL정보 (2개)
            'coupang_url': coupang_price_info.get('url', ''),
            'iherb_product_url': product_url or '',
            
            # 상태정보 (1개)
            'status': status,
            
            # 쿠팡가격 (3개)
            'coupang_current_price_krw': coupang_price_info.get('current_price', ''),
            'coupang_original_price_krw': coupang_price_info.get('original_price', ''),
            'coupang_discount_rate': coupang_price_info.get('discount_rate', ''),
            
            # 아이허브가격 (4개)
            'iherb_list_price_krw': iherb_price_info.get('list_price', ''),
            'iherb_discount_price_krw': iherb_price_info.get('discount_price', ''),
            'iherb_discount_percent': iherb_price_info.get('discount_percent', ''),
            'iherb_subscription_discount': iherb_price_info.get('subscription_discount', ''),
            
            # 🆕 쿠팡재고 (5개) - 새로 추가
            'coupang_stock_status': row.get('stock_status', ''),
            'coupang_delivery_badge': row.get('delivery_badge', ''),
            'coupang_origin_country': row.get('origin_country', ''),
            'coupang_unit_price': row.get('unit_price', ''),
            
            # 아이허브재고 (4개)
            'iherb_price_per_unit': iherb_price_info.get('price_per_unit', ''),
            'is_in_stock': iherb_price_info.get('is_in_stock', True),
            'stock_message': iherb_price_info.get('stock_message', ''),
            'back_in_stock_date': iherb_price_info.get('back_in_stock_date', ''),
            
            # 가격비교 (5개)
            'price_difference_krw': price_comparison['price_difference_krw'],
            'cheaper_platform': price_comparison['cheaper_platform'],
            'savings_amount': price_comparison['savings_amount'],
            'savings_percentage': price_comparison['savings_percentage'],
            'price_difference_note': price_comparison['price_difference_note']
        }
        
        return result
    
    def print_summary(self, results_df):
        """결과 요약"""
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
        
        # 쿠팡 재고 정보 통계
        if 'coupang_stock_status' in results_df.columns:
            in_stock_count = len(results_df[results_df['coupang_stock_status'] == 'in_stock'])
            print(f"  쿠팡 재고 있음: {in_stock_count}개")
        
        # 실패 유형별 통계
        if 'status' in results_df.columns and total > 0:
            print(f"\n실패 유형별 통계:")
            
            success_count = len(results_df[results_df['status'] == 'success'])
            not_found_count = len(results_df[results_df['status'] == 'not_found'])
            code_not_found_count = len(results_df[results_df['status'] == 'code_not_found'])
            
            if success_count > 0:
                print(f"  매칭 성공: {success_count}개")
            if not_found_count > 0:
                print(f"  매칭 실패: {not_found_count}개")
            if code_not_found_count > 0:
                print(f"  코드 추출 실패: {code_not_found_count}개")
        
        # 성공한 상품 샘플
        if successful > 0:
            print(f"\n주요 성공 사례:")
            successful_df = results_df[results_df['status'] == 'success']
            
            for idx, (_, row) in enumerate(successful_df.head(3).iterrows()):
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
                
                # 쿠팡 재고 정보 표시
                stock_status = row.get('coupang_stock_status', '')
                if stock_status:
                    stock_info = []
                    if stock_status == 'in_stock':
                        stock_info.append('재고있음')
                    elif stock_status == 'out_of_stock':
                        stock_info.append('품절')
                    if stock_info:
                        print(f"     쿠팡: {', '.join(stock_info)}")
                
                print()