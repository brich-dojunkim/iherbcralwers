"""
상세 상태 디버깅 스크립트 - 신규 상품 감지 문제 분석
"""

import pandas as pd
from datetime import datetime

def debug_detailed_status():
    csv_file = 'complete_efficient_NOW_Foods_20250916.csv'
    
    print("🔍 상세 상태 디버깅 시작...")
    print("="*60)
    
    df = pd.read_csv(csv_file, encoding='utf-8-sig')
    print(f"총 상품: {len(df)}개")
    
    today = datetime.now().strftime("_%Y%m%d")
    date_suffix = today[1:]  # 20250916
    
    print(f"날짜 형식: '{today}' (suffix: '{date_suffix}')")
    
    # update_status 컬럼 상세 분석
    if 'update_status' in df.columns:
        print(f"\n📊 update_status 컬럼 분석:")
        status_counts = df['update_status'].value_counts()
        for status, count in status_counts.items():
            print(f"  - '{status}': {count}개")
        
        # NEW_PRODUCT 패턴 찾기
        new_product_patterns = df[df['update_status'].str.startswith('NEW_PRODUCT', na=False)]
        print(f"\nNEW_PRODUCT로 시작하는 상품: {len(new_product_patterns)}개")
        
        if len(new_product_patterns) > 0:
            unique_patterns = new_product_patterns['update_status'].unique()
            print(f"NEW_PRODUCT 패턴들:")
            for pattern in unique_patterns:
                count = len(df[df['update_status'] == pattern])
                print(f"  - '{pattern}': {count}개")
        
        # 정확한 패턴 확인
        exact_pattern = f'NEW_PRODUCT__{today}'
        exact_matches = df[df['update_status'] == exact_pattern]
        print(f"\n정확한 패턴 '{exact_pattern}': {len(exact_matches)}개")
        
        if len(exact_matches) == 0:
            print("⚠️ 정확한 패턴 매치 없음!")
            print("다른 패턴들 시도:")
            
            alternative_patterns = [
                f'NEW_PRODUCT{today}',      # NEW_PRODUCT_20250916
                f'NEW_PRODUCT_{today[1:]}', # NEW_PRODUCT_20250916  
                f'NEW_PRODUCT__{today[1:]}', # NEW_PRODUCT__20250916
            ]
            
            for alt_pattern in alternative_patterns:
                alt_matches = df[df['update_status'] == alt_pattern]
                print(f"  - '{alt_pattern}': {len(alt_matches)}개")
                if len(alt_matches) > 0:
                    print(f"    🎯 이 패턴을 사용해야 합니다!")
    
    # 오늘 날짜 아이허브 컬럼 확인
    print(f"\n🌿 오늘 날짜 아이허브 컬럼 확인:")
    today_iherb_columns = [
        f'아이허브매칭상태_{date_suffix}',
        f'아이허브상품명_{date_suffix}',
        f'아이허브정가_{date_suffix}',
        f'아이허브할인가_{date_suffix}',
        f'아이허브매칭일시_{date_suffix}',
    ]
    
    existing_today_columns = [col for col in today_iherb_columns if col in df.columns]
    print(f"기존 오늘 날짜 아이허브 컬럼: {len(existing_today_columns)}개")
    for col in existing_today_columns:
        non_null_count = df[col].notna().sum()
        print(f"  - {col}: {non_null_count}개 데이터")
    
    if not existing_today_columns:
        print("📋 오늘 날짜 아이허브 컬럼이 없음 - 재처리 필요!")
    
    # 기존 아이허브 컬럼들 확인
    print(f"\n🔍 기존 아이허브 컬럼들:")
    existing_iherb_columns = [col for col in df.columns if 'iherb' in col.lower()]
    for col in existing_iherb_columns[:10]:  # 처음 10개만
        non_null_count = df[col].notna().sum()
        print(f"  - {col}: {non_null_count}개 데이터")
    
    # 재시작 매니저 로직과 동일한 검사 시뮬레이션
    print(f"\n🔄 재시작 매니저 로직 시뮬레이션:")
    
    # 1. 기존 상품 vs 처리 완료된 상품
    existing_products = df[~df['update_status'].str.startswith('NEW_PRODUCT', na=False)]
    completed_updates = df[
        (df['update_status'] == 'UPDATED') | 
        (df['update_status'] == 'NOT_FOUND')
    ]
    coupang_complete = len(completed_updates) >= len(existing_products)
    
    print(f"  1. 쿠팡 업데이트:")
    print(f"     - 기존 상품: {len(existing_products)}개")
    print(f"     - 완료된 업데이트: {len(completed_updates)}개")
    print(f"     - 완료 여부: {'✅' if coupang_complete else '❌'}")
    
    # 2. 신규 상품 (실제 코드와 동일한 패턴)
    new_products = df[df['update_status'] == f'NEW_PRODUCT__{today}']
    print(f"  2. 신규 상품 감지:")
    print(f"     - 패턴: 'NEW_PRODUCT__{today}'")
    print(f"     - 결과: {len(new_products)}개")
    
    if len(new_products) == 0:
        print(f"     ⚠️ 문제: 신규 상품이 감지되지 않음!")
        print(f"     원인: restart_manager.py의 패턴이 잘못됨")
        
        # 올바른 패턴 찾기
        print(f"     해결책: 올바른 패턴 찾기...")
        correct_pattern = None
        for pattern_candidate in df['update_status'].unique():
            if isinstance(pattern_candidate, str) and 'NEW_PRODUCT' in pattern_candidate:
                candidate_count = len(df[df['update_status'] == pattern_candidate])
                if candidate_count > 100:  # 187개 정도 예상
                    correct_pattern = pattern_candidate
                    print(f"     🎯 올바른 패턴: '{correct_pattern}' ({candidate_count}개)")
                    break
        
        if correct_pattern:
            print(f"\n💡 수정 방법:")
            print(f"   restart_manager.py의 65번째 줄을:")
            print(f"   new_products = df[df['update_status'] == '{correct_pattern}']")
            print(f"   로 수정하세요.")
    else:
        print(f"     ✅ 신규 상품 정상 감지됨")
    
    print("="*60)
    print("디버깅 완료")

if __name__ == "__main__":
    debug_detailed_status()