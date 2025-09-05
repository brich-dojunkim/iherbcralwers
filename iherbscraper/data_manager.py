"""
데이터 관리 모듈
"""

import os
import pandas as pd
from datetime import datetime
from config import Config


class DataManager:
    """CSV 파일 처리, 결과 저장, 진행상황 관리 담당"""
    
    def __init__(self):
        pass
    
    def auto_detect_start_point(self, input_csv_path, output_csv_path):
        """기존 결과를 분석해서 시작점 자동 감지 (성공한 상품 개수 기준)"""
        try:
            if not os.path.exists(output_csv_path):
                print("  결과 파일 없음 - 처음부터 시작")
                return 0, []
            
            # 기존 결과 파일 읽기
            existing_df = pd.read_csv(output_csv_path)
            
            if len(existing_df) == 0:
                print("  빈 결과 파일 - 처음부터 시작")
                return 0, []
            
            # 원본 CSV 읽기
            input_df = pd.read_csv(input_csv_path)
            total_products = len(input_df)
            
            # 성공한 상품 개수만 카운트해서 시작점 결정
            if 'status' in existing_df.columns:
                successful_count = len(existing_df[existing_df['status'] == 'success'])
                failed_indices = []
                
                # 실패한 상품들의 인덱스 수집 (재시도용)
                if 'actual_index' in existing_df.columns:
                    failed_mask = existing_df['status'] != 'success'
                    failed_indices = existing_df[failed_mask]['actual_index'].tolist()
                
                print(f"  성공한 상품: {successful_count}개")
                print(f"  실패한 상품: {len(failed_indices)}개 (재시도 예정)")
                print(f"  시작점: {successful_count}번째 상품부터 (성공 기준)")
                
                return successful_count, failed_indices
            
            # 기존 방식 (호환성)
            else:
                processed_products = set(existing_df['coupang_product_name'].tolist())
                processed_count = len(processed_products)
                
                print(f"  처리된 상품: {processed_count}개")
                print(f"  시작점: {processed_count}번째 상품부터")
                
                return processed_count, []
        
        except Exception as e:
            print(f"  시작점 자동 감지 실패: {e}")
            print("  안전을 위해 처음부터 시작")
            return 0, []
    
    def validate_input_csv(self, csv_file_path):
        """입력 CSV 파일 검증"""
        try:
            df = pd.read_csv(csv_file_path)
            
            # 영어 번역 컬럼 확인
            if 'product_name_english' not in df.columns:
                raise ValueError("CSV에 'product_name_english' 컬럼이 없습니다.")
            
            # 영어 번역이 없는 행 필터링
            original_count = len(df)
            df = df.dropna(subset=['product_name_english'])
            df = df[df['product_name_english'].str.strip() != '']
            filtered_count = len(df)
            
            print(f"  원본 상품: {original_count}개")
            print(f"  영어 번역된 상품: {filtered_count}개")
            if original_count != filtered_count:
                print(f"  번역 없는 상품: {original_count - filtered_count}개 (제외됨)")
            
            return df
            
        except Exception as e:
            print(f"  CSV 검증 실패: {e}")
            raise
    
    def initialize_output_csv(self, output_file_path):
        """CSV 파일 헤더 초기화 - 영어 번역 기반"""
        try:
            empty_df = pd.DataFrame(columns=Config.OUTPUT_COLUMNS)
            empty_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
            print(f"  결과 파일 초기화: {output_file_path}")
            
        except Exception as e:
            print(f"  CSV 초기화 실패: {e}")
    
    def append_result_to_csv(self, result, output_file_path):
        """결과를 CSV에 즉시 추가 (누적 방식)"""
        try:
            # 결과를 DataFrame으로 변환
            result_df = pd.DataFrame([result])
            
            # 기존 파일에 추가 (헤더 제외)
            result_df.to_csv(
                output_file_path, 
                mode='a',  # append 모드
                header=False,  # 헤더 제외
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
                           coupang_price_info, iherb_price_info, matching_reason=None):
        """결과 레코드 생성 (원화 기준 + 가격 비교 + 매칭 사유 포함)"""
        
        # 가격 비교 계산
        price_comparison = self.calculate_price_comparison(coupang_price_info, iherb_price_info)
        
        return {
            # 1. 기본 식별 정보 (순서 변경됨)
            'iherb_product_name': iherb_product_name,
            'coupang_product_name_english': english_name,
            'coupang_product_name': row['product_name'],
            'similarity_score': round(similarity_score, 3) if similarity_score else 0,
            'matching_reason': matching_reason or '매칭 정보 없음',
            'coupang_url': coupang_price_info.get('url', ''),
            'iherb_product_url': product_url,
            'coupang_product_id': row['product_id'],
            'iherb_product_code': product_code,
            
            # 2. 상태 정보
            'status': 'success' if product_code else ('not_found' if not product_url else 'code_not_found'),
            
            # 3. 쿠팡 가격 정보 (KRW)
            'coupang_current_price_krw': coupang_price_info.get('current_price', ''),
            'coupang_original_price_krw': coupang_price_info.get('original_price', ''),
            'coupang_discount_rate': coupang_price_info.get('discount_rate', ''),
            
            # 4. 아이허브 가격 정보 (KRW)
            'iherb_list_price_krw': iherb_price_info.get('list_price', ''),
            'iherb_discount_price_krw': iherb_price_info.get('discount_price', ''),
            'iherb_discount_percent': iherb_price_info.get('discount_percent', ''),
            'iherb_subscription_discount': iherb_price_info.get('subscription_discount', ''),
            'iherb_price_per_unit': iherb_price_info.get('price_per_unit', ''),
            
            # 5. 가격 비교
            'price_difference_krw': price_comparison['price_difference_krw'],
            'cheaper_platform': price_comparison['cheaper_platform'],
            'savings_amount': price_comparison['savings_amount'],
            'savings_percentage': price_comparison['savings_percentage'],
            'price_difference_note': price_comparison['price_difference_note'],
            
            # 6. 메타데이터
            'processed_at': datetime.now().isoformat(),
            'actual_index': actual_idx,
            'search_language': 'english'
        }
    
    def print_summary(self, results_df):
        """결과 요약 - 영어 번역 기반"""
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
        
        if successful > 0:
            print(f"\n주요 결과:")
            successful_df = results_df[results_df['status'] == 'success']
            
            for idx, (_, row) in enumerate(successful_df.head(5).iterrows()):
                korean_name = row['coupang_product_name'][:30] + "..."
                english_name = row.get('coupang_product_name_english', '')[:30] + "..."
                
                coupang_price = row.get('coupang_current_price_krw', '')
                iherb_price = row.get('iherb_discount_price_krw', '')
                
                print(f"  {idx+1}. {korean_name}")
                print(f"     영어: {english_name}")
                
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