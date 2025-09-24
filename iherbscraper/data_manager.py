"""
데이터 관리 모듈 - 미사용 컬럼 제거 버전
"""

import os
import pandas as pd
from datetime import datetime


class DataManager:
    """CSV 파일 처리, 결과 저장 - 핵심 기능만"""
    
    def __init__(self):
        pass
    
    def validate_input_csv(self, csv_file_path):
        """입력 CSV 파일 검증"""
        try:
            df = pd.read_csv(csv_file_path)
            
            if 'product_name' not in df.columns:
                raise ValueError("CSV에 'product_name' 컬럼이 없습니다.")
            
            # 유효한 제품명만 필터링
            original_count = len(df)
            df = df.dropna(subset=['product_name'])
            df = df[df['product_name'].str.strip() != '']
            filtered_count = len(df)
            
            print(f"  원본 상품: {original_count}개")
            print(f"  유효한 제품명: {filtered_count}개")
            
            if 'product_name_english' in df.columns:
                english_count = len(df[df['product_name_english'].notna() & (df['product_name_english'].str.strip() != '')])
                print(f"  영어 번역: {english_count}개")
            
            return df
            
        except Exception as e:
            print(f"  CSV 검증 실패: {e}")
            raise
    
    def initialize_output_csv(self, output_file_path):
        """CSV 파일 헤더 초기화 - iHerb 매칭 전용"""
        essential_columns = [
            # 기본 상품 정보
            'iherb_product_name',
            'coupang_product_name_english', 
            'coupang_product_name',
            'coupang_url',
            'iherb_product_url',
            'coupang_product_id',
            'iherb_product_code',
            'status',
            
            # 매칭 정보
            'similarity_score',
            'matching_reason',
            'gemini_confidence',
            
            # 가격 정보
            'coupang_current_price_krw',
            'coupang_original_price_krw', 
            'coupang_discount_rate',
            'iherb_list_price_krw',
            'iherb_discount_price_krw',
            'iherb_discount_percent',
            'iherb_subscription_discount',
            'iherb_price_per_unit',
            
            # 재고 정보
            'is_in_stock',
            'stock_message',
            'back_in_stock_date',
            
            # 가격 비교
            'cheaper_platform',
            'savings_amount',
            'savings_percentage', 
            'price_difference_note'
        ]
        
        try:
            empty_df = pd.DataFrame(columns=essential_columns)
            empty_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
            print(f"  결과 파일 초기화: {output_file_path}")
            print(f"  매칭 전용 컬럼 {len(essential_columns)}개 사용")
            
        except Exception as e:
            print(f"  CSV 초기화 실패: {e}")
    
    def create_result_record(self, row, product_url, product_code, iherb_product_name, 
                            coupang_price_info, iherb_price_info, similarity_score, 
                            matching_reason, gemini_confidence):
        """결과 레코드 생성 - iHerb 매칭 전용 (상태 추적 제거)"""
        
        # 가격 비교 계산
        price_comparison = self._calculate_price_comparison(coupang_price_info, iherb_price_info)
        
        # 매칭 상태 결정 (단순화)
        if product_code:
            status = 'success'
        else:
            status = 'not_found'
        
        # 매칭 결과만 포함 (상태 추적 제거)
        result = {
            # 기본 상품 정보
            'iherb_product_name': iherb_product_name or '',
            'coupang_product_name_english': row.get('product_name_english', ''),
            'coupang_product_name': row.get('product_name', ''),
            'coupang_url': coupang_price_info.get('url', ''),
            'iherb_product_url': product_url or '',
            'coupang_product_id': row.get('product_id', ''),
            'iherb_product_code': product_code or '',
            'status': status,
            
            # 매칭 정보
            'similarity_score': similarity_score,
            'matching_reason': matching_reason,
            'gemini_confidence': gemini_confidence,
            
            # 가격 정보
            'coupang_current_price_krw': coupang_price_info.get('current_price', ''),
            'coupang_original_price_krw': coupang_price_info.get('original_price', ''),
            'coupang_discount_rate': coupang_price_info.get('discount_rate', ''),
            'iherb_list_price_krw': iherb_price_info.get('list_price', ''),
            'iherb_discount_price_krw': iherb_price_info.get('discount_price', ''),
            'iherb_discount_percent': iherb_price_info.get('discount_percent', ''),
            'iherb_subscription_discount': iherb_price_info.get('subscription_discount', ''),
            'iherb_price_per_unit': iherb_price_info.get('price_per_unit', ''),
            
            # 재고 정보
            'is_in_stock': iherb_price_info.get('is_in_stock', True),
            'stock_message': iherb_price_info.get('stock_message', ''),
            'back_in_stock_date': iherb_price_info.get('back_in_stock_date', ''),
            
            # 가격 비교
            'cheaper_platform': price_comparison['cheaper_platform'],
            'savings_amount': price_comparison['savings_amount'],
            'savings_percentage': price_comparison['savings_percentage'],
            'price_difference_note': price_comparison['price_difference_note']
        }
        
        return result
    
    def append_result_to_csv(self, result, output_file_path):
        """결과를 CSV에 추가"""
        try:
            result_df = pd.DataFrame([result])
            result_df.to_csv(
                output_file_path, 
                mode='a',
                header=False,
                index=False, 
                encoding='utf-8-sig'
            )
            
        except Exception as e:
            print(f"    CSV 저장 실패: {e}")
    
    def _calculate_price_comparison(self, coupang_price_info, iherb_price_info):
        """가격 비교 계산"""
        comparison = {
            'cheaper_platform': '',
            'savings_amount': '',
            'savings_percentage': '',
            'price_difference_note': ''
        }
        
        try:
            # 쿠팡 가격
            coupang_price = None
            if coupang_price_info.get('current_price'):
                coupang_price = int(coupang_price_info['current_price'])
            
            # 아이허브 가격
            iherb_price = None
            if iherb_price_info.get('discount_price'):
                iherb_price = int(iherb_price_info['discount_price'])
            elif iherb_price_info.get('list_price'):
                iherb_price = int(iherb_price_info['list_price'])
            
            # 비교 계산
            if coupang_price and iherb_price:
                price_diff = coupang_price - iherb_price
                
                if price_diff > 0:
                    comparison['cheaper_platform'] = '아이허브'
                    comparison['savings_amount'] = str(price_diff)
                    savings_pct = round((price_diff / coupang_price) * 100, 1)
                    comparison['savings_percentage'] = str(savings_pct)
                    comparison['price_difference_note'] = f'아이허브가 {price_diff:,}원 더 저렴'
                    
                elif price_diff < 0:
                    abs_diff = abs(price_diff)
                    comparison['cheaper_platform'] = '쿠팡'
                    comparison['savings_amount'] = str(abs_diff)
                    savings_pct = round((abs_diff / iherb_price) * 100, 1)
                    comparison['savings_percentage'] = str(savings_pct)
                    comparison['price_difference_note'] = f'쿠팡이 {abs_diff:,}원 더 저렴'
                    
                else:
                    comparison['cheaper_platform'] = '동일'
                    comparison['savings_amount'] = '0'
                    comparison['savings_percentage'] = '0'
                    comparison['price_difference_note'] = '가격 동일'
            else:
                comparison['price_difference_note'] = '가격 비교 불가'
                
        except Exception as e:
            comparison['price_difference_note'] = f'계산 오류: {str(e)}'
        
        return comparison
    
    def extract_coupang_price_info(self, row):
        """쿠팡 가격 정보 추출"""
        price_info = {}
        
        if 'current_price' in row:
            current_price = str(row['current_price']).replace('원', '').replace(',', '').strip()
            if current_price and current_price != 'nan':
                price_info['current_price'] = current_price
        
        if 'original_price' in row:
            original_price = str(row['original_price']).replace('원', '').replace(',', '').strip()
            if original_price and original_price != 'nan':
                price_info['original_price'] = original_price
        
        if 'discount_rate' in row:
            discount_rate = str(row['discount_rate']).replace('%', '').strip()
            if discount_rate and discount_rate != 'nan':
                price_info['discount_rate'] = discount_rate
        
        if 'product_url' in row:
            price_info['url'] = str(row['product_url'])
        
        return price_info
    
    def print_summary(self, results_df):
        """결과 요약 - iHerb 매칭 전용"""
        total = len(results_df)
        successful = len(results_df[results_df['status'] == 'success'])
        
        print(f"\n=== iHerb 매칭 결과 ===")
        print(f"총 처리: {total}개")
        print(f"매칭 성공: {successful}개 ({successful/total*100:.1f}%)")
        print(f"매칭 실패: {total-successful}개")
    
    def auto_detect_start_point(self, input_csv_path, output_csv_path):
        """자동 시작점 감지 - iHerb 매칭 전용"""
        start_from = 0
        failed_indices = []
        
        try:
            if not os.path.exists(output_csv_path):
                return start_from, failed_indices
            
            existing_df = pd.read_csv(output_csv_path, encoding='utf-8-sig')
            
            if len(existing_df) == 0:
                return start_from, failed_indices
            
            # 처리된 마지막 인덱스 (단순히 행 개수)
            start_from = len(existing_df)
            
            # 매칭 실패한 항목들 찾기
            if 'status' in existing_df.columns:
                failed_mask = existing_df['status'] != 'success'
                failed_indices = existing_df[failed_mask].index.tolist()
            
            if start_from > 0:
                print(f"  기존 처리: {start_from}개")
                if failed_indices:
                    print(f"  재시도 대상: {len(failed_indices)}개")
            
        except Exception as e:
            print(f"  시작점 감지 오류: {e}")
        
        return start_from, failed_indices