"""
신규 상품 상태 직접 확인 스크립트
"""

import pandas as pd
from datetime import datetime

def debug_new_products():
    """신규 상품 상태 직접 확인"""
    csv_file = 'complete_efficient_NOW_Foods_20250916.csv'
    today = datetime.now().strftime("_%Y%m%d")
    
    print(f"오늘 날짜 형식: '{today}' (실제: '_20250916')")
    
    try:
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        print(f"총 상품: {len(df)}개")
        
        # update_status 컬럼의 모든 고유값 확인
        if 'update_status' in df.columns:
            unique_statuses = df['update_status'].value_counts()
            print(f"\nupdate_status 컬럼의 고유값들:")
            for status, count in unique_statuses.items():
                print(f"  '{status}': {count}개")
        
        # 여러 패턴으로 신규 상품 검색
        patterns = [
            f'NEW_PRODUCT{today}',      # NEW_PRODUCT_20250916
            f'NEW_PRODUCT_{today[1:]}', # NEW_PRODUCT_20250916  
            f'NEW_PRODUCT__{today[1:]}', # NEW_PRODUCT__20250916
        ]
        
        print(f"\n다양한 패턴으로 신규 상품 검색:")
        for pattern in patterns:
            matches = df[df['update_status'] == pattern] if 'update_status' in df.columns else pd.DataFrame()
            print(f"  패턴 '{pattern}': {len(matches)}개")
        
        # startswith로도 확인
        new_product_starts = df[df['update_status'].str.startswith('NEW_PRODUCT', na=False)] if 'update_status' in df.columns else pd.DataFrame()
        print(f"\nNEW_PRODUCT로 시작하는 상품: {len(new_product_starts)}개")
        
        if len(new_product_starts) > 0:
            print("실제 NEW_PRODUCT 상품들:")
            sample = new_product_starts.head(3)
            for idx, row in sample.iterrows():
                status_val = row['update_status']
                has_english = bool(row.get('coupang_product_name_english', ''))
                has_iherb_status = bool(row.get('status', ''))
                print(f"  - status: '{status_val}'")
                print(f"    번역: {has_english}, 아이허브매칭: {has_iherb_status}")
                print(f"    상품명: {row['coupang_product_name'][:50]}...")
        
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_new_products()