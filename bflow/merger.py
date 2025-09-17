import pandas as pd
import re

def merge_iherb_csv_files(file1_path, file2_path, output_path):
    """
    아이허브 상품번호를 기준으로 두 CSV 파일을 병합
    
    Args:
        file1_path (str): 첫 번째 CSV 파일 경로 (iherb_results_20250912.csv)
        file2_path (str): 두 번째 CSV 파일 경로 (iherb_revenue_202508.csv)
        output_path (str): 출력 CSV 파일 경로
    """
    
    # 파일 읽기
    print("파일을 읽는 중...")
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)
    
    # 컬럼명 공백 제거
    df2.columns = df2.columns.str.strip()
    
    print(f"첫 번째 파일: {len(df1)}개 행, {len(df1.columns)}개 컬럼")
    print(f"두 번째 파일: {len(df2)}개 행, {len(df2.columns)}개 컬럼")
    
    # 아이허브 상품번호 정규화 함수
    def normalize_product_code(code):
        if pd.isna(code):
            return None
        return str(code).replace('-', '')
    
    # 정규화된 상품코드 컬럼 추가
    df1['normalized_code'] = df1['iherb_product_code'].apply(normalize_product_code)
    df2['normalized_code'] = df2['판매자상품코드'].apply(normalize_product_code)
    
    # 매칭 통계
    codes1 = set(df1['normalized_code'].dropna())
    codes2 = set(df2['normalized_code'].dropna())
    matched_codes = codes1.intersection(codes2)
    
    print(f"매칭 가능한 상품: {len(matched_codes)}개 (전체 {len(codes1)}개 중)")
    
    # Left Join 수행
    merged = pd.merge(df1, df2, on='normalized_code', how='left', suffixes=('', '_revenue'))
    
    # 컬럼명을 한글로 변경
    column_mapping = {
        'iherb_product_name': '아이허브상품명',
        'coupang_product_name_english': '쿠팡상품명영어',
        'coupang_product_name': '쿠팡상품명',
        'gemini_confidence': '지미니신뢰도',
        'coupang_url': '쿠팡URL',
        'iherb_product_url': '아이허브URL',
        'coupang_product_id': '쿠팡상품ID',
        'iherb_product_code': '아이허브상품코드',
        'status': '상태',
        'coupang_current_price_krw': '쿠팡현재가격',
        'coupang_original_price_krw': '쿠팡정가',
        'coupang_discount_rate': '쿠팡할인율',
        'iherb_list_price_krw': '아이허브정가',
        'iherb_discount_price_krw': '아이허브할인가',
        'iherb_discount_percent': '아이허브할인율',
        'iherb_subscription_discount': '아이허브구독할인',
        'iherb_price_per_unit': '아이허브단가',
        'price_difference_krw': '가격차이',
        'cheaper_platform': '저렴한플랫폼',
        'savings_amount': '절약금액',
        'savings_percentage': '절약비율',
        'price_difference_note': '가격차이메모',
        'processed_at': '처리일시',
        '상품명': '수익상품명',
        '(CA) 셀러 상품번호': 'CA셀러번호',
        '정상가': '수익정상가',  # 기존 정상가와 구분
        '재고': '재고',
        ' 수량': '판매수량',  # 공백 포함 컬럼명 주의
        ' 매출': '매출'  # 공백 포함 컬럼명 주의
    }
    
    # 컬럼명 변경
    merged = merged.rename(columns=column_mapping)
    
    # 불필요한 컬럼 제거 (normalized_code, 판매자상품코드 등)
    columns_to_drop = ['normalized_code', '판매자상품코드']
    merged = merged.drop(columns=[col for col in columns_to_drop if col in merged.columns])
    
    # 결과 저장
    merged.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    # 통계 출력
    matched_products = merged['수익상품명'].notna().sum()
    print(f"\n병합 완료!")
    print(f"총 {len(merged)}개 행")
    print(f"매칭된 상품: {matched_products}개 ({matched_products/len(merged)*100:.1f}%)")
    print(f"출력 파일: {output_path}")
    
    return merged

def main():
    """메인 실행 함수"""
    # 파일 경로 설정
    file1_path = "/Users/brich/Desktop/iherb_price/iherbscraper/output/nowfood_20250915.csv"
    file2_path = "bflow_revenue_202508.csv"
    output_path = "bflow_merged_data.csv"
    
    try:
        # CSV 파일 병합 실행
        result_df = merge_iherb_csv_files(file1_path, file2_path, output_path)
        
        # 샘플 데이터 미리보기
        print("\n=== 병합 결과 미리보기 ===")
        print("매칭된 상품 예시:")
        matched_sample = result_df[result_df['수익상품명'].notna()].iloc[0]
        print(f"- 상품명: {matched_sample['아이허브상품명']}")
        print(f"- 수익 상품명: {matched_sample['수익상품명']}")
        print(f"- 재고: {matched_sample['재고']}")
        print(f"- 매출: {matched_sample['매출']}")
        
    except FileNotFoundError as e:
        print(f"파일을 찾을 수 없습니다: {e}")
    except Exception as e:
        print(f"오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()