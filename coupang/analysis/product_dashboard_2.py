#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
상품 대시보드 (리팩토링)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Analysis Layer: column_map만 정의
Excel Layer: 나머지 자동 처리
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import re

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_manager import DataManager
from src.metrics.core import MetricsManager
from config.settings import Config

from analysis.excel import quick_build, ExcelRenderer


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Analysis Layer: 비즈니스 로직
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_panel_data():
    """패널 데이터 로드 (최신 2개 스냅샷)"""
    dm = DataManager(Config.INTEGRATED_DB_PATH)
    metrics = MetricsManager(dm)
    
    df = metrics.get_view(
        metric_groups=['core', 'action', 'performance_snapshot', 'meta'],
        n_latest=2,
        include_unmatched=True,
        compute_deltas=True,
        delta_metrics=['iherb_sales_quantity', 'iherb_item_winner_ratio']
    )
    
    if df.empty:
        return None, None, None
    
    # 날짜 추출
    date_pattern = r'__(\d{8})$'
    found_dates = set()
    
    for col in df.columns:
        match = re.search(date_pattern, str(col))
        if match:
            found_dates.add(match.group(1))
    
    if len(found_dates) < 2:
        return df, None, None
    
    sorted_dates = sorted(found_dates, reverse=True)
    curr_date = f"{sorted_dates[0][:4]}-{sorted_dates[0][4:6]}-{sorted_dates[0][6:8]}"
    prev_date = f"{sorted_dates[1][:4]}-{sorted_dates[1][4:6]}-{sorted_dates[1][6:8]}"
    
    return df, curr_date, prev_date


def prepare_data(df, curr_date, prev_date):
    """데이터 준비 (동적 계산)"""
    
    curr_d = datetime.strptime(curr_date, "%Y-%m-%d").date()
    prev_d = datetime.strptime(prev_date, "%Y-%m-%d").date()
    curr_s = curr_d.strftime('%Y%m%d')
    prev_s = prev_d.strftime('%Y%m%d')
    
    # 할인율 계산
    def calc_discount(orig_col, sale_col):
        if orig_col in df.columns and sale_col in df.columns:
            orig = pd.to_numeric(df[orig_col], errors='coerce')
            sale = pd.to_numeric(df[sale_col], errors='coerce')
            result = pd.Series(np.nan, index=df.index)
            mask = (orig > 0) & (sale > 0)
            result[mask] = ((orig[mask] - sale[mask]) / orig[mask] * 100).round(1)
            return result
        return pd.Series([np.nan] * len(df))
    
    df['할인율_prev'] = calc_discount(f'iherb_original_price__{prev_s}', f'iherb_price__{prev_s}')
    df['할인율_curr'] = calc_discount(f'iherb_original_price__{curr_s}', f'iherb_price__{curr_s}')
    df['할인율Δ'] = (df['할인율_curr'] - df['할인율_prev']).round(1)
    
    # 순위 계산
    sales_col = f'iherb_sales_quantity__{curr_s}'
    if sales_col in df.columns:
        df['순위_아이허브'] = df[sales_col].rank(method='min', ascending=False).astype('Int64')
    else:
        df['순위_아이허브'] = pd.Series([pd.NA] * len(df))
    
    return df, curr_d, prev_d, curr_s, prev_s


def define_column_map(curr_d, prev_d, curr_s, prev_s):
    """column_map 정의 - 유일한 진실의 원천"""
    
    delta_sales_col = f'iherb_sales_quantity_delta_{curr_s}_{prev_s}'
    delta_winner_col = f'iherb_item_winner_ratio_delta_{curr_s}_{prev_s}'
    
    return {
        # 코어
        '매칭상태': (f'matching_status__{curr_s}',),
        '품번': (f'iherb_part_number__{curr_s}',),
        'Product_ID': (f'product_id__{curr_s}',),
        
        # 할인전략
        '요청할인율': (f'requested_discount_rate__{curr_s}',),
        '추천할인율': (f'recommended_discount_rate__{curr_s}',),
        '손익분기할인율': (f'breakeven_discount_rate__{curr_s}',),
        '유리한곳': (f'cheaper_source__{curr_s}',),
        '가격격차': (f'price_diff__{curr_s}', 'Int64'),
        
        # 변화
        '할인율Δ': ('할인율Δ',),  # 동적 계산
        '판매량Δ': (delta_sales_col, 'Int64'),
        '위너비율Δ': (delta_winner_col,),
        
        # 가격상태
        '정가': (f'iherb_original_price__{curr_s}', 'Int64'),
        f'판매가\n({prev_d})': (f'iherb_price__{prev_s}', 'Int64'),
        f'판매가\n({curr_d})': (f'iherb_price__{curr_s}', 'Int64'),
        f'할인율\n({prev_d})': ('할인율_prev',),  # 동적 계산
        f'할인율\n({curr_d})': ('할인율_curr',),  # 동적 계산
        '로켓_판매가': (f'rocket_price__{curr_s}', 'Int64'),
        
        # 판매/위너
        f'판매량\n({curr_d - timedelta(days=1)})': (f'iherb_sales_quantity__{curr_s}', 'Int64'),
        f'판매량\n({prev_d - timedelta(days=1)})': (f'iherb_sales_quantity__{prev_s}', 'Int64'),
        f'위너비율\n({curr_d - timedelta(days=1)})': (f'iherb_item_winner_ratio__{curr_s}',),
        f'위너비율\n({prev_d - timedelta(days=1)})': (f'iherb_item_winner_ratio__{prev_s}',),
        
        # 메타
        '제품명_아이허브': (f'iherb_product_name__{curr_s}',),
        '제품명_로켓': (f'rocket_product_name__{curr_s}',),
        '카테고리_아이허브': (f'iherb_category__{curr_s}',),
        '카테고리_로켓': (f'rocket_category__{curr_s}',),
        '링크_아이허브': (f'iherb_url__{curr_s}',),
        '링크_로켓': (f'rocket_url__{curr_s}',),
        'Vendor_아이허브': ('iherb_vendor_id',),
        'Vendor_로켓': (f'rocket_vendor_id__{curr_s}',),
        'Item_아이허브': (f'iherb_item_id__{curr_s}',),
        'Item_로켓': (f'rocket_item_id__{curr_s}',),
        '순위_아이허브': ('순위_아이허브',),  # 동적 계산
        '순위_로켓': (f'rocket_rank__{curr_s}', 'Int64'),
        '평점': (f'rocket_rating__{curr_s}',),
        '리뷰수': (f'rocket_reviews__{curr_s}', 'Int64'),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main: 극단적 간소화
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    """메인 워크플로우"""
    
    print(f"\n{'='*80}")
    print(f"📊 상품 대시보드 생성")
    print(f"{'='*80}")
    
    # [1] 데이터 로드
    print(f"\n[1/4] 패널 데이터 로드 중...")
    df, curr_date, prev_date = load_panel_data()
    
    if df is None or curr_date is None or prev_date is None:
        print("❌ 데이터 부족")
        return
    
    print(f"✅ {len(df):,}개 상품 로드")
    print(f"   현재: {curr_date}")
    print(f"   이전: {prev_date}")
    
    # [2] 데이터 준비 (동적 계산)
    print(f"\n[2/4] 동적 계산 중...")
    df, curr_d, prev_d, curr_s, prev_s = prepare_data(df, curr_date, prev_date)
    print(f"✅ 할인율Δ, 순위 계산 완료")
    
    # [3] column_map 정의
    print(f"\n[3/4] column_map 정의 중...")
    column_map = define_column_map(curr_d, prev_d, curr_s, prev_s)
    print(f"✅ {len(column_map)}개 컬럼 정의")
    
    # [4] 🔥 Excel Layer에 모두 위임 (한 줄!)
    print(f"\n[4/4] Excel 생성 중...")
    config, output_df = quick_build(df, column_map)
    
    Config.OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = Config.OUTPUT_DIR / f"product_dashboard_{datetime.now().strftime('%Y%m%d')}.xlsx"
    
    renderer = ExcelRenderer(str(output_path))
    result = renderer.render(output_df, config)
    
    if result['success']:
        print(f"\n{'='*80}")
        print(f"✅ 완료: {result['path']}")
        print(f"   {result['rows']:,}행 × {result['cols']}열")
        print(f"{'='*80}\n")
    else:
        print(f"\n❌ 에러: {result['error']}")


if __name__ == "__main__":
    main()