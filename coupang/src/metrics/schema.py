#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
metrics.schema
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
통합 DataFrame 컬럼에 대한 의미 기반 스키마 정의 + 엑셀 출력 메타데이터
"""

from typing import List, Dict
from dataclasses import dataclass
from datetime import datetime, timedelta


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메트릭 그룹 정의
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CORE_METRICS: List[str] = [
    "matching_status",      # '로켓매칭' / '미매칭'
    "matching_method",      # Product ID / 동적필터 등
    "matching_confidence",  # High / Medium / Low / 조건 문자열
    "product_id",           # 공통 Product ID (rocket 우선, fallback iherb)
    "iherb_part_number",    # 아이허브 품번
]

ACTION_METRICS: List[str] = [
    "requested_discount_rate",   # 요청 할인율 (정가 기준)
    "recommended_discount_rate", # 추천 할인율 (판매가 기준)
    "breakeven_discount_rate",   # 손익분기 할인율 (로켓 가격 맞추기)
    "cheaper_source",            # 유리한곳: '아이허브' / '로켓직구' / '동일'
    "price_diff",                # 절대 가격 차이 (아이허브 - 로켓)
    "price_diff_pct",            # 가격 차이 % (로켓 기준)
]

PERFORMANCE_SNAPSHOT: List[str] = [
    # 아이허브 가격
    "iherb_price",              # 오늘 판매가
    "iherb_original_price",     # 정가
    "iherb_recommended_price",  # 추천가 (내부 계산 값)
    # 아이허브 재고/상태
    "iherb_stock",              # 재고 수량
    "iherb_stock_status",       # '판매중', '품절' 등 상태
    # 아이허브 판매/위너 (당일 스냅샷 기준 값)
    "iherb_revenue",            # 매출(해당 스냅샷 구간 기준)
    "iherb_sales_quantity",     # 판매 수량
    "iherb_item_winner_ratio",  # 아이템 위너 비율 (%)
    # 로켓 가격/할인/순위
    "rocket_price",
    "rocket_original_price",
    "rocket_discount_rate",     # 로켓 기준 할인율 (%)
    "rocket_rank",              # 로켓 내 랭킹 (카테고리 내 순위)
]

PERFORMANCE_ROLLING_7D: List[str] = [
    "iherb_sales_quantity_last_7d",  # 최근 7일 누적 판매량
    "iherb_coupang_share_last_7d",   # 최근 7일 쿠팡 비중 (% 혹은 비율)
]

META_METRICS: List[str] = [
    # 제품 이름
    "iherb_product_name",
    "rocket_product_name",
    # 카테고리
    "iherb_category",
    "rocket_category",
    # 링크
    "iherb_url",
    "rocket_url",
    # ID / 식별자
    "iherb_vendor_id",
    "rocket_vendor_id",
    "iherb_item_id",
    "rocket_item_id",
    # 평가/리뷰
    "rocket_rating",
    "rocket_reviews",
]


def _unique(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


ALL_METRICS: List[str] = _unique(
    CORE_METRICS
    + ACTION_METRICS
    + PERFORMANCE_SNAPSHOT
    + PERFORMANCE_ROLLING_7D
    + META_METRICS
)

METRIC_GROUPS: Dict[str, List[str]] = {
    "core": CORE_METRICS,
    "action": ACTION_METRICS,
    "performance_snapshot": PERFORMANCE_SNAPSHOT,
    "performance_rolling_7d": PERFORMANCE_ROLLING_7D,
    "meta": META_METRICS,
    "all": ALL_METRICS,
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 엑셀 출력용 메타데이터
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class ColumnSpec:
    """엑셀 컬럼 사양"""
    metric_key: str          # 메트릭 키 (예: "iherb_price__20251202")
    excel_label: str         # 엑셀 표시명 (예: "판매가\n(2025-12-02)")
    group: str               # 그룹명 (예: "가격상태")
    width: float = 12.0      # 컬럼 너비
    number_format: str = "@" # 숫자 서식
    is_link: bool = False    # 하이퍼링크 여부
    is_delta: bool = False   # Δ 컬럼 여부


# 조건부 서식 규칙
CONDITIONAL_FORMAT_RULES = {
    "delta_positive": {"condition": "positive", "color": "green"},
    "delta_negative": {"condition": "negative", "color": "red"},
    "cheaper_iherb": {"condition": "equals", "value": "아이허브", "color": "green"},
    "cheaper_rocket": {"condition": "equals", "value": "로켓직구", "color": "red"},
    "winner_100": {"condition": "value_equals", "value": 100.0, "color": "green"},
}


def create_panel_column_specs(
    curr_date: str,
    prev_date: str,
) -> List[ColumnSpec]:
    """패널용 동적 컬럼 스펙 생성"""
    
    curr_d = datetime.strptime(curr_date, "%Y-%m-%d").date()
    prev_d = datetime.strptime(prev_date, "%Y-%m-%d").date()
    
    curr_s = curr_d.strftime('%Y%m%d')
    prev_s = prev_d.strftime('%Y%m%d')
    
    sales_curr_d = curr_d - timedelta(days=1)
    sales_prev_d = prev_d - timedelta(days=1)
    
    specs = []
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 1. 코어 (🔥 시간축 suffix 없음)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    specs.append(ColumnSpec(f"matching_status__{curr_s}", "매칭상태", "코어", 10.0, "@"))
    specs.append(ColumnSpec(f"iherb_part_number__{curr_s}", "품번", "코어", 15.0, "@"))
    specs.append(ColumnSpec(f"product_id__{curr_s}", "Product_ID", "코어", 15.0, "@"))
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 2. 할인전략 (최신 스냅샷)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    specs.append(ColumnSpec(f"requested_discount_rate__{curr_s}", "요청할인율", "할인전략", 14.0, '0.0"%"'))
    specs.append(ColumnSpec(f"recommended_discount_rate__{curr_s}", "추천할인율", "할인전략", 14.0, '0.0"%"'))
    specs.append(ColumnSpec(f"breakeven_discount_rate__{curr_s}", "손익분기할인율", "할인전략", 16.0, '0.0"%"'))
    specs.append(ColumnSpec(f"cheaper_source__{curr_s}", "유리한곳", "할인전략", 12.0, "@"))
    specs.append(ColumnSpec(f"price_diff__{curr_s}", "가격격차", "할인전략", 12.0, "#,##0"))
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 3. 변화 (Δ) - 동적 계산
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    specs.append(ColumnSpec("할인율Δ", "할인율Δ", "변화", 12.0, "0.0", is_delta=True))
    specs.append(ColumnSpec(f"iherb_sales_quantity_delta_{curr_s}_{prev_s}", "판매량Δ", "변화", 12.0, "#,##0", is_delta=True))
    specs.append(ColumnSpec(f"iherb_item_winner_ratio_delta_{curr_s}_{prev_s}", "위너비율Δ", "변화", 12.0, "0.0", is_delta=True))
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 4. 가격상태 (시계열)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    specs.append(ColumnSpec(f"iherb_original_price__{curr_s}", "정가", "가격상태", 12.0, "#,##0"))
    specs.append(ColumnSpec(f"iherb_price__{prev_s}", f"판매가\n({prev_d})", "가격상태", 14.0, "#,##0"))
    specs.append(ColumnSpec(f"iherb_price__{curr_s}", f"판매가\n({curr_d})", "가격상태", 14.0, "#,##0"))
    specs.append(ColumnSpec("할인율_prev", f"할인율\n({prev_d})", "가격상태", 14.0, '0.0"%"'))
    specs.append(ColumnSpec("할인율_curr", f"할인율\n({curr_d})", "가격상태", 14.0, '0.0"%"'))
    specs.append(ColumnSpec(f"rocket_price__{curr_s}", "로켓_판매가", "가격상태", 14.0, "#,##0"))
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 5. 판매/위너 (시계열)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    specs.append(ColumnSpec(f"iherb_sales_quantity__{prev_s}", f"판매량\n({sales_prev_d})", "판매/위너", 14.0, "#,##0"))
    specs.append(ColumnSpec(f"iherb_sales_quantity__{curr_s}", f"판매량\n({sales_curr_d})", "판매/위너", 14.0, "#,##0"))
    specs.append(ColumnSpec(f"iherb_item_winner_ratio__{prev_s}", f"위너비율\n({sales_prev_d})", "판매/위너", 16.0, '0.0"%"'))
    specs.append(ColumnSpec(f"iherb_item_winner_ratio__{curr_s}", f"위너비율\n({sales_curr_d})", "판매/위너", 16.0, '0.0"%"'))
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 6. 메타 (🔥 최신 스냅샷 - suffix 있음)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    specs.append(ColumnSpec(f"iherb_product_name__{curr_s}", "제품명_아이허브", "제품명", 60.0, "@"))
    specs.append(ColumnSpec(f"rocket_product_name__{curr_s}", "제품명_로켓", "제품명", 60.0, "@"))
    specs.append(ColumnSpec(f"iherb_category__{curr_s}", "카테고리_아이허브", "카테고리", 16.0, "@"))
    specs.append(ColumnSpec(f"rocket_category__{curr_s}", "카테고리_로켓", "카테고리", 16.0, "@"))
    specs.append(ColumnSpec(f"iherb_url__{curr_s}", "링크_아이허브", "링크", 10.0, "@", is_link=True))
    specs.append(ColumnSpec(f"rocket_url__{curr_s}", "링크_로켓", "링크", 10.0, "@", is_link=True))
    
    # ID는 key이므로 suffix 없음
    specs.append(ColumnSpec("iherb_vendor_id", "Vendor_아이허브", "ID", 18.0, "@"))
    specs.append(ColumnSpec(f"rocket_vendor_id__{curr_s}", "Vendor_로켓", "ID", 18.0, "@"))
    specs.append(ColumnSpec(f"iherb_item_id__{curr_s}", "Item_아이허브", "ID", 16.0, "@"))
    specs.append(ColumnSpec(f"rocket_item_id__{curr_s}", "Item_로켓", "ID", 16.0, "@"))
    
    # 순위 (동적 계산)
    specs.append(ColumnSpec("순위_아이허브", "순위_아이허브", "순위", 14.0, "0"))
    specs.append(ColumnSpec(f"rocket_rank__{curr_s}", "순위_로켓", "순위", 14.0, "0"))
    
    specs.append(ColumnSpec(f"rocket_rating__{curr_s}", "평점", "평가", 10.0, "0.0"))
    specs.append(ColumnSpec(f"rocket_reviews__{curr_s}", "리뷰수", "평가", 12.0, "#,##0"))
    
    return specs