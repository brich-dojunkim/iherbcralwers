#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
가격 비교 리포트 (리팩토링)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Analysis Layer: column_map만 정의
Excel Layer: 나머지 자동 처리
"""

import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_manager import DataManager
from src.metrics.core import MetricsManager
from config.settings import Config

from analysis.excel import quick_build, ExcelRenderer


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Analysis Layer: 비즈니스 로직
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_data():
    """데이터 로드"""
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


def define_column_map():
    """column_map 정의 - 유일한 진실의 원천"""
    return {
        # 기본 정보 (8개)
        '매칭상태': ('matching_status',),
        '신뢰도': ('matching_confidence',),
        '로켓': ('rocket_category',),
        '아이허브': ('iherb_category',),
        '로켓_링크': ('rocket_url',),
        '아이허브_링크': ('iherb_url',),
        '품번': ('iherb_part_number',),
        'UPC': ('iherb_upc', 'Int64'),
        
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main: 극단적 간소화
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    """메인 워크플로우"""
    
    print(f"\n{'='*80}")
    print(f"📊 가격 비교 리포트 생성")
    print(f"{'='*80}")
    
    # [1] 데이터 로드
    print(f"\n[1/3] 데이터 로드 중...")
    df = load_data()
    
    if df is None:
        print("❌ 데이터 없음")
        return
    
    print(f"✅ {len(df):,}개 상품 로드")
    
    # [2] column_map 정의
    print(f"\n[2/3] column_map 정의 중...")
    column_map = define_column_map()
    print(f"✅ {len(column_map)}개 컬럼 정의")
    
    # [3] 🔥 Excel Layer에 모두 위임 (한 줄!)
    print(f"\n[3/3] Excel 생성 중...")
    config, output_df = quick_build(df, column_map)
    
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