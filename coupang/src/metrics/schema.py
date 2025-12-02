#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
metrics.schema
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
통합 DataFrame 컬럼에 대한 의미 기반 스키마 정의

- core_metrics        : 식별/핵심 상태
- action_metrics      : 할인 전략/의사결정 관련
- performance_snapshot: 현재 스냅샷 기준 퍼포먼스/상태
- performance_rolling_7d: 최근 7일 롤링 퍼포먼스
- meta_metrics        : 이름/카테고리/링크/ID 등 메타 정보

※ 실제 DataFrame에 없는 컬럼을 여기서 정의하지 않는 것이 원칙.
"""

from typing import List, Dict


# ① 코어 메트릭: "이 행이 무엇인가"를 정의하는 최소 단위
CORE_METRICS: List[str] = [
    "matching_status",      # '로켓매칭' / '미매칭'
    "matching_method",      # Product ID / 동적필터 등
    "matching_confidence",  # High / Medium / Low / 조건 문자열
    "product_id",           # 공통 Product ID (rocket 우선, fallback iherb)
    "iherb_part_number",    # 아이허브 품번
]


# ② 액션 메트릭: 오늘 어떤 액션을 취할지를 직접적으로 결정하는 지표
ACTION_METRICS: List[str] = [
    "requested_discount_rate",   # 요청 할인율 (정가 기준)
    "recommended_discount_rate", # 추천 할인율 (판매가 기준)
    "breakeven_discount_rate",   # 손익분기 할인율 (로켓 가격 맞추기)
    "cheaper_source",            # 유리한곳: '아이허브' / '로켓직구' / '동일'
    "price_diff",                # 절대 가격 차이 (아이허브 - 로켓)
    "price_diff_pct",            # 가격 차이 % (로켓 기준)
]


# ③ 퍼포먼스(스냅샷 기준): "오늘 스냅샷에서의 상태/성과"
#    - 가격/재고/판매량/위너비율 등
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


# ④ 퍼포먼스(최근 7일 롤링): 스냅샷에 종속적이지만 기간 누적/비율
#    - DB 실제 컬럼 이름 기준
PERFORMANCE_ROLLING_7D: List[str] = [
    "iherb_sales_quantity_last_7d",  # 최근 7일 누적 판매량
    "iherb_coupang_share_last_7d",   # 최근 7일 쿠팡 비중 (% 혹은 비율)
]


# ⑤ 메타 메트릭: "설명/문맥"에 가까운 정보들
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


# ⑥ 전체 메트릭 카탈로그 (중복 제거 후 순서 유지)
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


# ⑦ 그룹 딕셔너리: 나중에 analysis 단에서 그룹별로 가져다 쓰기 편하게
METRIC_GROUPS: Dict[str, List[str]] = {
    "core": CORE_METRICS,
    "action": ACTION_METRICS,
    "performance_snapshot": PERFORMANCE_SNAPSHOT,
    "performance_rolling_7d": PERFORMANCE_ROLLING_7D,
    "meta": META_METRICS,
    "all": ALL_METRICS,
}
