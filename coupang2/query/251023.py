#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wide Format CSV - 올바른 버전
1 row = 1 product (카테고리별 구분)
각 날짜 데이터가 정확히 매칭됨
"""

import sqlite3
import pandas as pd
import numpy as np
import os

DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/monitoring.db"
OUTPUT_DIR = "/Users/brich/Desktop/iherb_price/coupang2/outputs"

def generate_wide_format_correct():
    """올바른 Wide Format 생성"""
    
    conn = sqlite3.connect(DB_PATH)
    
    print("\n" + "="*80)
    print("📊 Wide Format CSV 생성 (수정 버전)")
    print("="*80)
    
    # 1. 전체 데이터 로드 (Long Format)
    query = """
    WITH daily_latest_snapshots AS (
        SELECT 
            c.id as category_id,
            c.name as category_name,
            DATE(ps.snapshot_time) as snapshot_date,
            MAX(ps.id) as latest_snapshot_id
        FROM page_snapshots ps
        JOIN categories c ON ps.category_id = c.id
        GROUP BY c.id, c.name, DATE(ps.snapshot_time)
    )
    SELECT 
        dls.category_name,
        dls.snapshot_date,
        prod.coupang_product_id,
        prod.product_name,
        prod.category_rank,
        prod.current_price,
        prod.discount_rate,
        prod.review_count,
        prod.rating_score,
        mr.iherb_upc,
        mr.iherb_part_number
    FROM daily_latest_snapshots dls
    JOIN product_states prod ON dls.latest_snapshot_id = prod.snapshot_id
    INNER JOIN matching_reference mr ON prod.coupang_product_id = mr.coupang_product_id
    WHERE mr.iherb_upc IS NOT NULL OR mr.iherb_part_number IS NOT NULL
    ORDER BY dls.category_name, prod.coupang_product_id, dls.snapshot_date
    """
    
    df_long = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"\n✅ 데이터 로드 완료")
    print(f"  • 총 레코드: {len(df_long):,}개")
    print(f"  • 상품-카테고리 조합: {df_long.groupby(['coupang_product_id', 'category_name']).ngroups:,}개")
    print(f"  • 측정 날짜: {df_long['snapshot_date'].nunique()}일")
    
    dates = sorted(df_long['snapshot_date'].unique())
    print(f"  • 날짜: {', '.join(dates)}")
    
    # 2. 기본 정보 테이블 생성 (상품-카테고리별 1행)
    print(f"\n🔄 Wide Format 변환 중...")
    
    base_info = df_long.groupby(['category_name', 'coupang_product_id']).agg({
        'product_name': 'first',
        'iherb_upc': 'first',
        'iherb_part_number': 'first',
        'review_count': 'last',
        'rating_score': 'last'
    }).reset_index()
    
    print(f"  • 고유 상품-카테고리: {len(base_info):,}개")
    
    # 3. 각 날짜별 데이터를 컬럼으로 추가
    wide_df = base_info.copy()
    
    for i, date in enumerate(dates):
        date_str = date[5:].replace('-', '-')  # '10-20' 형식
        
        # 해당 날짜의 데이터만 필터링
        date_data = df_long[df_long['snapshot_date'] == date][
            ['category_name', 'coupang_product_id', 'category_rank', 'current_price', 'discount_rate']
        ].copy()
        
        # 컬럼명 변경
        date_data.columns = [
            'category_name', 'coupang_product_id',
            f'{date_str}_순위',
            f'{date_str}_가격',
            f'{date_str}_할인율(%)'
        ]
        
        # 병합 (left join으로 모든 상품 유지)
        wide_df = wide_df.merge(
            date_data, 
            on=['category_name', 'coupang_product_id'], 
            how='left'
        )
        
        print(f"  • {date_str} 데이터 추가 완료")
    
    # 4. 변화율 계산
    print(f"\n📊 변화율 계산 중...")
    
    for i in range(len(dates)):
        if i == 0:
            continue  # 첫 날은 변화율 없음
        
        prev_date = dates[i-1][5:].replace('-', '-')
        curr_date = dates[i][5:].replace('-', '-')
        
        # 순위 변화율: (이전순위 - 현재순위) / 이전순위 * 100
        prev_rank_col = f'{prev_date}_순위'
        curr_rank_col = f'{curr_date}_순위'
        change_col = f'{curr_date}_순위변화율(%)'
        
        wide_df[change_col] = np.where(
            (wide_df[prev_rank_col].notna()) & (wide_df[curr_rank_col].notna()) & (wide_df[prev_rank_col] != 0),
            ((wide_df[prev_rank_col] - wide_df[curr_rank_col]) / wide_df[prev_rank_col] * 100).round(1),
            np.nan
        )
        
        # 가격 변화율: (현재가격 - 이전가격) / 이전가격 * 100
        prev_price_col = f'{prev_date}_가격'
        curr_price_col = f'{curr_date}_가격'
        price_change_col = f'{curr_date}_가격변화율(%)'
        
        wide_df[price_change_col] = np.where(
            (wide_df[prev_price_col].notna()) & (wide_df[curr_price_col].notna()) & (wide_df[prev_price_col] != 0),
            ((wide_df[curr_price_col] - wide_df[prev_price_col]) / wide_df[prev_price_col] * 100).round(1),
            np.nan
        )
        
        # 할인율 변화: 현재할인율 - 이전할인율
        prev_disc_col = f'{prev_date}_할인율(%)'
        curr_disc_col = f'{curr_date}_할인율(%)'
        disc_change_col = f'{curr_date}_할인율변화(%)'
        
        wide_df[disc_change_col] = np.where(
            (wide_df[prev_disc_col].notna()) & (wide_df[curr_disc_col].notna()),
            (wide_df[curr_disc_col] - wide_df[prev_disc_col]).round(1),
            np.nan
        )
        
        print(f"  • {prev_date} → {curr_date} 변화율 계산 완료")
    
    # 5. 컬럼 순서 재정렬
    # 기본정보 + 날짜별(순위, 순위변화율, 가격, 가격변화율, 할인율, 할인율변화)
    ordered_cols = ['category_name', 'coupang_product_id', 'product_name', 
                    'iherb_upc', 'iherb_part_number', 'review_count', 'rating_score']
    
    for i, date in enumerate(dates):
        date_str = date[5:].replace('-', '-')
        
        # 순위와 가격, 할인율은 항상 추가
        ordered_cols.extend([
            f'{date_str}_순위',
            f'{date_str}_가격',
            f'{date_str}_할인율(%)'
        ])
        
        # 변화율은 첫 날 제외
        if i > 0:
            # 변화율 컬럼을 순위/가격/할인율 뒤에 바로 추가
            ordered_cols.insert(len(ordered_cols) - 2, f'{date_str}_순위변화율(%)')
            ordered_cols.insert(len(ordered_cols) - 1, f'{date_str}_가격변화율(%)')
            ordered_cols.append(f'{date_str}_할인율변화(%)')
    
    wide_df = wide_df[ordered_cols]
    
    # 6. 컬럼명 한글화
    wide_df.columns = ['카테고리', '쿠팡상품ID', '상품명', 'iHerb_UPC', 'iHerb_품번', 
                       '최신_리뷰수', '최신_평점'] + list(wide_df.columns[7:])
    
    # 7. 데이터 타입 정리 (간단하게)
    # Float로만 유지 (Int64 변환 시 오류 많음)
    for col in wide_df.columns:
        if any(keyword in col for keyword in ['_순위', '_가격', '_할인율', '리뷰수']):
            if '_변화율' not in col and '_변화' not in col:
                wide_df[col] = pd.to_numeric(wide_df[col], errors='coerce')
    
    print(f"\n✅ Wide Format 변환 완료")
    print(f"  • 최종 상품 수: {len(wide_df):,}개")
    print(f"  • 총 컬럼 수: {len(wide_df.columns)}개")
    
    # 8. 검증: 특정 상품 확인
    print(f"\n🔍 데이터 검증:")
    test_product = wide_df.iloc[0]
    print(f"  • 샘플 상품: {test_product['상품명'][:50]}...")
    print(f"  • 카테고리: {test_product['카테고리']}")
    print(f"  • 쿠팡상품ID: {test_product['쿠팡상품ID']}")
    
    for date in dates:
        date_str = date[5:].replace('-', '-')
        rank = test_product[f'{date_str}_순위']
        price = test_product[f'{date_str}_가격']
        print(f"  • {date_str}: 순위 {rank}, 가격 {price:,}원" if pd.notna(rank) else f"  • {date_str}: 데이터 없음")
    
    # 9. CSV 저장
    output_path = os.path.join(OUTPUT_DIR, "wide_format_integrated_v2.csv")
    wide_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\n💾 CSV 저장 완료: {output_path}")
    print(f"  • 파일 크기: {os.path.getsize(output_path) / 1024:.1f} KB")
    
    # 10. 카테고리별 통계
    print(f"\n📊 카테고리별 상품 수:")
    for cat in wide_df['카테고리'].unique():
        count = len(wide_df[wide_df['카테고리'] == cat])
        print(f"  • {cat}: {count:,}개")
    
    return output_path, wide_df


def display_sample_data(wide_df, dates):
    """샘플 데이터 보기 좋게 출력"""
    
    print("\n" + "="*80)
    print("📋 샘플 데이터 (상위 3개 상품)")
    print("="*80)
    
    sample = wide_df.head(3)
    
    # 기본정보
    print("\n[기본정보]")
    print(sample[['카테고리', '상품명', 'iHerb_UPC', '최신_리뷰수']].to_string(index=False))
    
    # 각 날짜별 데이터
    for date in dates:
        date_str = date[5:].replace('-', '-')
        print(f"\n[{date} 데이터]")
        cols = ['상품명', 
                f'{date_str}_순위', f'{date_str}_순위변화율(%)',
                f'{date_str}_가격', f'{date_str}_가격변화율(%)',
                f'{date_str}_할인율(%)', f'{date_str}_할인율변화(%)']
        print(sample[cols].to_string(index=False))


def main():
    """메인 실행"""
    
    print("\n" + "="*80)
    print("🎯 Wide Format CSV 생성 (수정 버전)")
    print("="*80)
    print("목표: 1 row = 1 product, 정확한 시계열 매칭")
    print("="*80)
    
    # Wide Format 생성
    output_path, wide_df = generate_wide_format_correct()
    
    # 날짜 목록
    conn = sqlite3.connect(DB_PATH)
    dates = pd.read_sql_query(
        "SELECT DISTINCT DATE(snapshot_time) as d FROM page_snapshots ORDER BY d", 
        conn
    )['d'].tolist()
    conn.close()
    
    # 샘플 데이터 출력
    display_sample_data(wide_df, dates)
    
    print("\n" + "="*80)
    print("✅ 작업 완료!")
    print("="*80)
    print(f"\n📁 생성된 파일: wide_format_integrated_v2.csv")
    print(f"\n💡 이전 파일(wide_format_integrated.csv)과 비교:")
    print(f"  • v1 (이전): 3,833개 행 - 잘못된 pivot으로 중복/매칭 오류")
    print(f"  • v2 (수정): {len(wide_df):,}개 행 - 정확한 1:1 매칭")
    print("\n" + "="*80)


if __name__ == "__main__":
    main()