#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
가격 비교 리포트 (모듈 조합만 사용)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
새로운 함수 정의 없이 모듈만 조합

🔥 핵심 특징:
  - 함수 정의 0개
  - column_map만 선언
  - 모듈 자동 처리:
    • 'share' → 비중 자동 계산
    • 'rank' → 순위 자동 계산
    • 데이터바 자동 추가
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_manager import DataManager
from src.metrics.core import MetricsManager
from config.settings import Config

from analysis.excel import quick_build, ExcelRenderer


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Analysis Layer: column_map만 정의
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COLUMN_MAP = {
    # ========================================
    # 1️⃣ 기본 정보 (8개)
    # ========================================
    '매칭상태': ('matching_status',),
    '신뢰도': ('matching_confidence',),
    '로켓_카테고리': ('rocket_category',),
    '아이허브_카테고리': ('iherb_category',),
    '로켓_링크': ('rocket_url',),
    '아이허브_링크': ('iherb_url',),
    '품번': ('iherb_part_number',),
    'UPC': ('iherb_upc', 'Int64'),
    
    # ========================================
    # 2️⃣ 핵심 지표 (9개)
    # ========================================
    '순위': ('rocket_rank', 'Int64'),
    '판매량': ('iherb_sales_quantity', 'Int64'),
    '매출(원)': ('iherb_revenue', 'Int64'),
    '아이템위너비율': ('iherb_item_winner_ratio',),
    '가격격차(원)': ('price_diff', 'Int64'),
    '손익분기할인율': ('breakeven_discount_rate',),
    '추천할인율': ('recommended_discount_rate',),
    '요청할인율': ('requested_discount_rate',),
    '유리한곳': ('cheaper_source',),
    
    # ========================================
    # 3️⃣ 제품 정보 (7개)
    # ========================================
    '로켓_제품명': ('rocket_product_name',),
    '아이허브_제품명': ('iherb_product_name',),
    'Product_ID': ('rocket_product_id',),
    '로켓_Vendor': ('rocket_vendor_id',),
    '로켓_Item': ('rocket_item_id',),
    '아이허브_Vendor': ('iherb_vendor_id',),
    '아이허브_Item': ('iherb_item_id',),
    
    # ========================================
    # 4️⃣ 가격 정보 (8개)
    # ========================================
    '정가': ('rocket_original_price', 'Int64'),
    '할인율': ('rocket_discount_rate',),
    '로켓가격': ('rocket_price', 'Int64'),
    '판매가': ('iherb_price', 'Int64'),
    '정가_아이허브': ('iherb_original_price', 'Int64'),
    '쿠팡추천가': ('iherb_recommended_price', 'Int64'),
    '재고': ('iherb_stock', 'Int64'),
    '판매상태': ('iherb_stock_status',),
    
    # ========================================
    # 5️⃣ 판매 성과 (7개) - 🔥 동적 계산 포함
    # ========================================
    '평점': ('rocket_rating',),
    '리뷰수': ('rocket_reviews', 'Int64'),
    '매출비중': ('iherb_revenue', 'share'),           # 🔥 자동 비중 계산
    '판매량비중': ('iherb_sales_quantity', 'share'),  # 🔥 자동 비중 계산
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main: 모듈 조합만 사용 (함수 정의 0개)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    """메인 워크플로우"""
    
    print(f"\n{'='*80}")
    print(f"📊 가격 비교 리포트 생성 (모듈 조합)")
    print(f"{'='*80}")
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # [1] 데이터 로드 (MetricsManager 모듈 사용)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n[1/3] 📥 데이터 로드 중...")
    
    dm = DataManager(Config.INTEGRATED_DB_PATH)
    metrics = MetricsManager(dm)
    
    df = metrics.get_view(
        metric_groups=['all'],
        n_latest=1,
        include_unmatched=True
    )
    
    if df.empty:
        print("❌ 데이터 없음")
        return
    
    # 시간축 suffix 제거 (단일 스냅샷)
    df.columns = [col.split('__')[0] if '__' in col else col for col in df.columns]
    
    print(f"✅ {len(df):,}개 상품 로드")
    print(f"   컬럼: {len(df.columns)}개")
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # [2] Excel 생성 (ExcelConfigBuilder 모듈 사용)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n[2/3] 🔧 Excel Config 생성 중...")
    print(f"   COLUMN_MAP: {len(COLUMN_MAP)}개 컬럼 정의")
    
    config, output_df = quick_build(df, COLUMN_MAP)
    
    print(f"✅ Config 생성 완료")
    print(f"   출력 DataFrame: {len(output_df):,}행 × {len(output_df.columns)}열")
    print(f"   그룹: {len(config.groups)}개")
    print(f"   조건부 서식: {len(config.conditional_rules)}개 규칙")
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # [3] 렌더링 (ExcelRenderer 모듈 사용)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n[3/3] 📝 Excel 렌더링 중...")
    
    Config.OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = Config.OUTPUT_DIR / f"price_comparison_{datetime.now().strftime('%Y%m%d')}.xlsx"
    
    renderer = ExcelRenderer(str(output_path))
    result = renderer.render(output_df, config)
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 결과 출력
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n{'='*80}")
    if result['success']:
        print(f"✅ 완료: {result['path']}")
        print(f"{'='*80}")
        print(f"   📊 {result['rows']:,}행 × {result['cols']}열")
        print(f"   🎨 3단 헤더")
        print(f"   🎯 조건부 서식 {len(config.conditional_rules)}개")
        print(f"   📈 데이터바 자동 추가 (비중 컬럼)")
        print(f"   🔗 하이퍼링크 처리")
        print(f"{'='*80}\n")
    else:
        print(f"❌ 실패: {result['error']}")
        print(f"{'='*80}\n")


if __name__ == "__main__":
    main()