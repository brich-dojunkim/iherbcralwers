#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
가격 비교 리포트 (계층적 설계)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Analysis Layer: 비즈니스 로직에만 집중
Excel Layer: 데이터 변환 & 렌더링 위임
"""

import sys
from pathlib import Path
from datetime import datetime

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
    
    # 그룹/규칙 (Excel Layer)
    GroupSpec,
    SubGroup,
    make_winner_rule,
    make_confidence_rule,
    make_positive_red_rule,
    make_cheaper_source_rule,
    
    # 렌더링 (Excel Layer)
    ExcelRenderer,
    ExcelConfig,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Analysis Layer: 비즈니스 로직
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_data():
    """데이터 로드 (Metrics Layer 사용)"""
    dm = DataManager(Config.INTEGRATED_DB_PATH)
    metrics = MetricsManager(dm)
    
    df = metrics.get_view(
        metric_groups=['all'],
        n_latest=1,
        include_unmatched=True
    )
    
    if df.empty:
        return None
    
    # 시간축 suffix 제거 (단일 스냅샷)
    df.columns = [col.split('__')[0] if '__' in col else col for col in df.columns]
    
    return df


def prepare_output_dataframe(df):
    """Excel 출력용 DataFrame 준비 (Excel Layer 사용)"""
    
    # 선언적 컬럼 매핑 정의
    column_map = {
        # 기본 정보 (8개)
        '매칭상태': ('matching_status',),
        '신뢰도': ('matching_confidence',),
        '로켓': ('rocket_category',),
        '아이허브': ('iherb_category',),
        '로켓_링크': ('rocket_url',),
        '아이허브_링크': ('iherb_url',),
        '품번': ('iherb_part_number',),
        'UPC': ('iherb_upc', 'Int64'),  # Excel Layer가 변환 처리
        
        # 핵심 지표 (9개)
        '순위': ('rocket_rank', 'Int64'),
        '판매량': ('iherb_sales_quantity', 'Int64'),
        '매출(원)': ('iherb_revenue', 'Int64'),
        '아이템위너비율': ('iherb_item_winner_ratio',),
        '가격격차(원)': ('price_diff', 'Int64'),
        '손익분기할인율': ('breakeven_discount_rate',),
        '추천할인율': ('recommended_discount_rate',),
        '요청할인율': ('requested_discount_rate',),
        '유리한곳': ('cheaper_source',),
        
        # 제품 정보 (7개)
        '로켓_제품명': ('rocket_product_name',),
        '아이허브_제품명': ('iherb_product_name',),
        'Product_ID': ('rocket_product_id',),
        '로켓_Vendor': ('rocket_vendor_id',),
        '로켓_Item': ('rocket_item_id',),
        '아이허브_Vendor': ('iherb_vendor_id',),
        '아이허브_Item': ('iherb_item_id',),
        
        # 가격 정보 (8개)
        '정가': ('rocket_original_price', 'Int64'),
        '할인율': ('rocket_discount_rate',),
        '로켓가격': ('rocket_price', 'Int64'),
        '판매가': ('iherb_price', 'Int64'),
        '정가_아이허브': ('iherb_original_price', 'Int64'),
        '쿠팡추천가': ('iherb_recommended_price', 'Int64'),
        '재고': ('iherb_stock', 'Int64'),
        '판매상태': ('iherb_stock_status',),
        
        # 판매 성과 (2개)
        '평점': ('rocket_rating',),
        '리뷰수': ('rocket_reviews', 'Int64'),
    }
    
    # Excel Layer에게 변환 위임
    return build_output_dataframe(df, column_map)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Excel Layer: 시각화 스펙 정의 (선언적)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def define_columns():
    """컬럼 스펙 정의"""
    return [
        # 기본 정보
        get_column_spec('matching_status'),
        get_column_spec('matching_confidence'),
        get_column_spec('rocket_category'),
        get_column_spec('iherb_category'),
        get_column_spec('rocket_url', name='로켓'),
        get_column_spec('iherb_url', name='아이허브'),
        get_column_spec('iherb_part_number'),
        get_column_spec('iherb_upc'),
        
        # 핵심 지표
        get_column_spec('rocket_rank'),
        get_column_spec('iherb_sales_quantity'),
        get_column_spec('iherb_revenue'),
        get_column_spec('iherb_item_winner_ratio'),
        get_column_spec('price_diff'),
        get_column_spec('breakeven_discount_rate'),
        get_column_spec('recommended_discount_rate'),
        get_column_spec('requested_discount_rate'),
        get_column_spec('cheaper_source'),
        
        # 제품 정보
        get_column_spec('rocket_product_name', name='로켓_제품명'),
        get_column_spec('iherb_product_name', name='아이허브_제품명'),
        get_column_spec('rocket_product_id', name='Product_ID'),
        get_column_spec('rocket_vendor_id', name='로켓_Vendor'),
        get_column_spec('rocket_item_id', name='로켓_Item'),
        get_column_spec('iherb_vendor_id', name='아이허브_Vendor'),
        get_column_spec('iherb_item_id', name='아이허브_Item'),
        
        # 가격 정보
        get_column_spec('rocket_original_price', name='정가'),
        get_column_spec('rocket_discount_rate', name='할인율'),
        get_column_spec('rocket_price', name='로켓가격'),
        get_column_spec('iherb_price', name='판매가'),
        get_column_spec('iherb_original_price', name='정가_아이허브'),
        get_column_spec('iherb_recommended_price', name='쿠팡추천가'),
        get_column_spec('iherb_stock', name='재고'),
        get_column_spec('iherb_stock_status', name='판매상태'),
        
        # 판매 성과
        get_column_spec('rocket_rating'),
        get_column_spec('rocket_reviews'),
    ]


def define_groups():
    """그룹 스펙 정의"""
    return [
        GroupSpec(
            name="기본 정보",
            color_scheme="info",
            sub_groups=[
                SubGroup(name="매칭", columns=['매칭상태', '신뢰도']),
                SubGroup(name="카테고리", columns=['로켓', '아이허브']),
                SubGroup(name="링크", columns=['로켓_링크', '아이허브_링크']),
                SubGroup(name="상품번호", columns=['품번', 'UPC']),
            ]
        ),
        GroupSpec(
            name="핵심 지표",
            color_scheme="primary",
            sub_groups=[
                SubGroup(name="로켓", columns=['순위']),
                SubGroup(name="아이허브", columns=['판매량', '매출(원)', '아이템위너비율']),
                SubGroup(name="종합", columns=['가격격차(원)', '손익분기할인율', '추천할인율', '요청할인율', '유리한곳']),
            ]
        ),
        GroupSpec(
            name="제품 정보",
            color_scheme="secondary",
            sub_groups=[
                SubGroup(name="제품명", columns=['로켓_제품명', '아이허브_제품명']),
                SubGroup(name="상품 ID", columns=['Product_ID', '로켓_Vendor', '로켓_Item', '아이허브_Vendor', '아이허브_Item']),
            ]
        ),
        GroupSpec(
            name="가격 정보",
            color_scheme="tertiary",
            sub_groups=[
                SubGroup(name="로켓직구", columns=['정가', '할인율', '로켓가격']),
                SubGroup(name="아이허브", columns=['판매가', '정가_아이허브', '쿠팡추천가', '재고', '판매상태']),
            ]
        ),
        GroupSpec(
            name="판매 성과",
            color_scheme="success",
            sub_groups=[
                SubGroup(name="로켓", columns=['평점', '리뷰수']),
            ]
        ),
    ]


def define_conditional_rules():
    """조건부 서식 정의"""
    return [
        *make_winner_rule('아이템위너비율', threshold=30),
        *make_positive_red_rule('손익분기할인율'),
        *make_positive_red_rule('추천할인율'),
        *make_positive_red_rule('요청할인율'),
        *make_cheaper_source_rule('가격격차(원)'),
        *make_cheaper_source_rule('유리한곳'),        
        *make_confidence_rule('신뢰도'),
    ]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main: 전체 워크플로우 오케스트레이션
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    """메인 워크플로우"""
    
    print(f"\n{'='*80}")
    print(f"📊 가격 비교 리포트 생성")
    print(f"{'='*80}")
    
    # [1] Analysis Layer - 데이터 로드
    print(f"\n[1/4] 데이터 로드 중...")
    df = load_data()
    
    if df is None:
        print("❌ 데이터 없음")
        return
    
    print(f"✅ {len(df):,}개 상품 로드")
    
    # [2] Excel Layer - DataFrame 변환
    print(f"\n[2/4] DataFrame 변환 중...")
    output_df = prepare_output_dataframe(df)
    print(f"✅ {len(output_df):,}행 × {len(output_df.columns)}열")
    
    # [3] Excel Layer - 시각화 스펙 정의
    print(f"\n[3/4] Excel 스펙 정의 중...")
    columns = define_columns()
    groups = define_groups()
    rules = define_conditional_rules()
    
    config = ExcelConfig(
        groups=groups,
        columns=columns,
        conditional_rules=rules,
        freeze_panes=(4, 17),
        auto_filter=True
    )
    print(f"✅ 스펙 정의 완료")
    
    # [4] Excel Layer - 렌더링
    print(f"\n[4/4] Excel 렌더링 중...")
    Config.OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = Config.OUTPUT_DIR / f"price_comparison_{datetime.now().strftime('%Y%m%d')}.xlsx"
    
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