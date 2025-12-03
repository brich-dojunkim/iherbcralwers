#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
상품 대시보드 (계층적 설계)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Analysis Layer: 비즈니스 로직 (2개 스냅샷 비교)
Excel Layer: 데이터 변환 & 렌더링 위임
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import re

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_manager import DataManager
from src.metrics.core import MetricsManager
from config.settings import Config

from analysis.excel import (
    # DataFrame 변환 (Excel Layer)
    safe_get,
    build_output_dataframe,
    
    # 컬럼 스펙 (Excel Layer)
    get_column_spec,
    get_timestamped_spec,
    get_delta_spec,
    
    # 그룹/규칙 (Excel Layer)
    GroupSpec,
    SubGroup,
    make_delta_rule,
    make_winner_rule,
    make_cheaper_source_rule,
    
    # 렌더링 (Excel Layer)
    ExcelRenderer,
    ExcelConfig,
)


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


def prepare_output_dataframe(df, curr_date, prev_date):
    """Excel 출력용 DataFrame 준비"""
    
    curr_d = datetime.strptime(curr_date, "%Y-%m-%d").date()
    prev_d = datetime.strptime(prev_date, "%Y-%m-%d").date()
    curr_s = curr_d.strftime('%Y%m%d')
    prev_s = prev_d.strftime('%Y%m%d')
    
    # 할인율 계산 함수 (동적 계산 컬럼)
    def calc_discount(orig_col, sale_col):
        if orig_col in df.columns and sale_col in df.columns:
            import pandas as pd
            import numpy as np
            orig = pd.to_numeric(df[orig_col], errors='coerce')
            sale = pd.to_numeric(df[sale_col], errors='coerce')
            result = pd.Series(np.nan, index=df.index)
            mask = (orig > 0) & (sale > 0)
            result[mask] = ((orig[mask] - sale[mask]) / orig[mask] * 100).round(1)
            return result
        import pandas as pd
        import numpy as np
        return pd.Series([np.nan] * len(df))
    
    discount_prev = calc_discount(f'iherb_original_price__{prev_s}', f'iherb_price__{prev_s}')
    discount_curr = calc_discount(f'iherb_original_price__{curr_s}', f'iherb_price__{curr_s}')
    discount_delta = (discount_curr - discount_prev).round(1)
    
    # 순위 계산
    sales_col = f'iherb_sales_quantity__{curr_s}'
    if sales_col in df.columns:
        rank_iherb = df[sales_col].rank(method='min', ascending=False).astype('Int64')
    else:
        import pandas as pd
        rank_iherb = pd.Series([pd.NA] * len(df))
    
    # 선언적 컬럼 매핑
    column_map = {
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
        
        # 가격상태
        '정가': (f'iherb_original_price__{curr_s}', 'Int64'),
        f'판매가\n({prev_d})': (f'iherb_price__{prev_s}', 'Int64'),
        f'판매가\n({curr_d})': (f'iherb_price__{curr_s}', 'Int64'),
        f'할인율\n({prev_d})': (None,),  # 동적 컬럼
        f'할인율\n({curr_d})': (None,),  # 동적 컬럼
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
        '순위_아이허브': (None,),  # 동적 컬럼
        '순위_로켓': (f'rocket_rank__{curr_s}', 'Int64'),
        '평점': (f'rocket_rating__{curr_s}',),
        '리뷰수': (f'rocket_reviews__{curr_s}', 'Int64'),
    }
    
    # 델타 컬럼
    delta_sales_col = f'iherb_sales_quantity_delta_{curr_s}_{prev_s}'
    delta_winner_col = f'iherb_item_winner_ratio_delta_{curr_s}_{prev_s}'
    
    column_map['판매량Δ'] = (delta_sales_col, 'Int64') if delta_sales_col in df.columns else (None,)
    column_map['위너비율Δ'] = (delta_winner_col,) if delta_winner_col in df.columns else (None,)
    column_map['할인율Δ'] = (None,)  # 동적 계산
    
    # DataFrame 구성
    output_df = build_output_dataframe(df, column_map)
    
    # 동적 컬럼 추가
    output_df[f'할인율\n({prev_d})'] = discount_prev
    output_df[f'할인율\n({curr_d})'] = discount_curr
    output_df['할인율Δ'] = discount_delta
    output_df['순위_아이허브'] = rank_iherb
    
    return output_df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Excel Layer: 시각화 스펙 정의
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def define_columns(curr_d, prev_d):
    """컬럼 스펙 정의"""
    return [
        # 코어
        get_column_spec('matching_status', name='매칭상태'),
        get_column_spec('iherb_part_number', name='품번'),
        get_column_spec('rocket_product_id', name='Product_ID'),
        
        # 할인전략
        get_column_spec('requested_discount_rate', name='요청할인율'),
        get_column_spec('recommended_discount_rate', name='추천할인율'),
        get_column_spec('breakeven_discount_rate', name='손익분기할인율'),
        get_column_spec('cheaper_source', name='유리한곳'),
        get_column_spec('price_diff', name='가격격차'),
        
        # 변화
        get_delta_spec('할인율Δ'),
        get_delta_spec('판매량Δ'),
        get_delta_spec('위너비율Δ'),
        
        # 가격상태
        get_column_spec('iherb_original_price', name='정가'),
        get_timestamped_spec('iherb_price', prev_d, name=f'판매가\n({prev_d})'),
        get_timestamped_spec('iherb_price', curr_d, name=f'판매가\n({curr_d})'),
        get_timestamped_spec('할인율', prev_d, name=f'할인율\n({prev_d})', number_format='0.0"%"'),
        get_timestamped_spec('할인율', curr_d, name=f'할인율\n({curr_d})', number_format='0.0"%"'),
        get_column_spec('rocket_price', name='로켓_판매가'),
        
        # 판매/위너
        get_timestamped_spec('iherb_sales_quantity', curr_d - timedelta(days=1)),
        get_timestamped_spec('iherb_sales_quantity', prev_d - timedelta(days=1)),
        get_timestamped_spec('iherb_item_winner_ratio', curr_d - timedelta(days=1)),
        get_timestamped_spec('iherb_item_winner_ratio', prev_d - timedelta(days=1)),
        
        # 메타
        get_column_spec('iherb_product_name', name='제품명_아이허브'),
        get_column_spec('rocket_product_name', name='제품명_로켓'),
        get_column_spec('iherb_category', name='카테고리_아이허브'),
        get_column_spec('rocket_category', name='카테고리_로켓'),
        get_column_spec('iherb_url', name='링크_아이허브'),
        get_column_spec('rocket_url', name='링크_로켓'),
        get_column_spec('iherb_vendor_id', name='Vendor_아이허브'),
        get_column_spec('rocket_vendor_id', name='Vendor_로켓'),
        get_column_spec('iherb_item_id', name='Item_아이허브'),
        get_column_spec('rocket_item_id', name='Item_로켓'),
        get_column_spec('rank', name='순위_아이허브', number_format='0'),
        get_column_spec('rocket_rank', name='순위_로켓'),
        get_column_spec('rocket_rating', name='평점'),
        get_column_spec('rocket_reviews', name='리뷰수'),
    ]


def define_groups(curr_d, prev_d):
    """그룹 스펙 정의"""
    return [
        GroupSpec(
            name="코어",
            color_scheme="info",
            sub_groups=[
                SubGroup(name="", columns=['매칭상태', '품번', 'Product_ID']),
            ]
        ),
        GroupSpec(
            name="할인전략",
            color_scheme="primary",
            sub_groups=[
                SubGroup(name="", columns=['요청할인율', '추천할인율', '손익분기할인율', '유리한곳', '가격격차']),
            ]
        ),
        GroupSpec(
            name="변화",
            color_scheme="secondary",
            sub_groups=[
                SubGroup(name="", columns=['할인율Δ', '판매량Δ', '위너비율Δ']),
            ]
        ),
        GroupSpec(
            name="가격상태",
            color_scheme="tertiary",
            sub_groups=[
                SubGroup(name="", columns=[
                    '정가',
                    f'판매가\n({prev_d})',
                    f'판매가\n({curr_d})',
                    f'할인율\n({prev_d})',
                    f'할인율\n({curr_d})',
                    '로켓_판매가'
                ]),
            ]
        ),
        GroupSpec(
            name="판매/위너",
            color_scheme="success",
            sub_groups=[
                SubGroup(name="", columns=[
                    f'판매량\n({prev_d - timedelta(days=1)})',
                    f'판매량\n({curr_d - timedelta(days=1)})',
                    f'위너비율\n({prev_d - timedelta(days=1)})',
                    f'위너비율\n({curr_d - timedelta(days=1)})',
                ]),
            ]
        ),
        GroupSpec(
            name="제품명",
            color_scheme="info",
            sub_groups=[
                SubGroup(name="", columns=['제품명_아이허브', '제품명_로켓']),
            ]
        ),
        GroupSpec(
            name="카테고리",
            color_scheme="info",
            sub_groups=[
                SubGroup(name="", columns=['카테고리_아이허브', '카테고리_로켓']),
            ]
        ),
        GroupSpec(
            name="링크",
            color_scheme="info",
            sub_groups=[
                SubGroup(name="", columns=['링크_아이허브', '링크_로켓']),
            ]
        ),
        GroupSpec(
            name="ID",
            color_scheme="info",
            sub_groups=[
                SubGroup(name="", columns=['Vendor_아이허브', 'Vendor_로켓', 'Item_아이허브', 'Item_로켓']),
            ]
        ),
        GroupSpec(
            name="순위",
            color_scheme="info",
            sub_groups=[
                SubGroup(name="", columns=['순위_아이허브', '순위_로켓']),
            ]
        ),
        GroupSpec(
            name="평가",
            color_scheme="info",
            sub_groups=[
                SubGroup(name="", columns=['평점', '리뷰수']),
            ]
        ),
    ]


def define_conditional_rules(curr_d):
    """조건부 서식 정의"""
    return [
        *make_delta_rule('할인율Δ'),
        *make_delta_rule('판매량Δ'),
        *make_delta_rule('위너비율Δ'),
        *make_winner_rule(f'위너비율\n({curr_d - timedelta(days=1)})', threshold=100, exact=True),
        *make_cheaper_source_rule('가격격차', '유리한곳'),
    ]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main: 전체 워크플로우
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    """메인 워크플로우"""
    
    print(f"\n{'='*80}")
    print(f"📊 상품 대시보드 생성")
    print(f"{'='*80}")
    
    # [1] Analysis Layer - 데이터 로드
    print(f"\n[1/4] 패널 데이터 로드 중...")
    df, curr_date, prev_date = load_panel_data()
    
    if df is None or curr_date is None or prev_date is None:
        print("❌ 데이터 부족")
        return
    
    print(f"✅ {len(df):,}개 상품 로드")
    print(f"   현재: {curr_date}")
    print(f"   이전: {prev_date}")
    
    # [2] Excel Layer - DataFrame 변환
    print(f"\n[2/4] DataFrame 변환 중...")
    output_df = prepare_output_dataframe(df, curr_date, prev_date)
    print(f"✅ {len(output_df):,}행 × {len(output_df.columns)}열")
    
    # [3] Excel Layer - 시각화 스펙 정의
    print(f"\n[3/4] Excel 스펙 정의 중...")
    curr_d = datetime.strptime(curr_date, "%Y-%m-%d").date()
    prev_d = datetime.strptime(prev_date, "%Y-%m-%d").date()
    
    columns = define_columns(curr_d, prev_d)
    groups = define_groups(curr_d, prev_d)
    rules = define_conditional_rules(curr_d)
    
    config = ExcelConfig(
        groups=groups,
        columns=columns,
        conditional_rules=rules,
        freeze_panes=(4, 4),
        auto_filter=True
    )
    print(f"✅ 스펙 정의 완료")
    
    # [4] Excel Layer - 렌더링
    print(f"\n[4/4] Excel 렌더링 중...")
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