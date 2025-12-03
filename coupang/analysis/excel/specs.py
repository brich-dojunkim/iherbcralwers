#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Standard Column Specs
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
표준 컬럼 스펙 라이브러리
"""

from typing import Dict, Optional
from .types import ColumnSpec
from .constants import FORMATS


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 표준 컬럼 스펙 정의
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STANDARD_COLUMNS: Dict[str, ColumnSpec] = {
    # 기본 정보
    "matching_status": ColumnSpec(name="매칭상태", width=10.0, number_format=FORMATS["text"]),
    "matching_confidence": ColumnSpec(name="신뢰도", width=9.71, number_format=FORMATS["text"]),
    "iherb_part_number": ColumnSpec(name="품번", width=12.71, number_format=FORMATS["text"]),
    "product_id": ColumnSpec(name="Product_ID", width=14.14, number_format=FORMATS["text"]),
    "iherb_upc": ColumnSpec(name="UPC", width=15.0, number_format=FORMATS["text"]),
    
    # 가격 정보
    "iherb_price": ColumnSpec(name="판매가", width=10.57, number_format=FORMATS["currency"], alignment="right"),
    "iherb_original_price": ColumnSpec(name="정가_아이허브", width=12.29, number_format=FORMATS["currency"], alignment="right"),
    "iherb_recommended_price": ColumnSpec(name="쿠팡추천가", width=12.29, number_format=FORMATS["currency"], alignment="right"),
    "rocket_price": ColumnSpec(name="로켓가격", width=10.57, number_format=FORMATS["currency"], alignment="right"),
    "rocket_original_price": ColumnSpec(name="정가", width=8.86, number_format=FORMATS["currency"], alignment="right"),
    
    # 할인율
    "rocket_discount_rate": ColumnSpec(name="할인율", width=9.0, number_format=FORMATS["percentage"], alignment="right"),
    "breakeven_discount_rate": ColumnSpec(name="손익분기할인율", width=15.57, number_format=FORMATS["percentage"], alignment="right"),
    "recommended_discount_rate": ColumnSpec(name="추천할인율", width=12.29, number_format=FORMATS["percentage"], alignment="right"),
    "requested_discount_rate": ColumnSpec(name="요청할인율", width=12.29, number_format=FORMATS["percentage"], alignment="right"),
    
    # 가격 비교
    "price_diff": ColumnSpec(name="가격격차(원)", width=13.43, number_format=FORMATS["currency"], alignment="right"),
    "cheaper_source": ColumnSpec(name="유리한곳", width=10.57, number_format=FORMATS["text"]),
    
    # 판매 성과
    "iherb_sales_quantity": ColumnSpec(name="판매량", width=9.0, number_format=FORMATS["integer"], alignment="right"),
    "iherb_revenue": ColumnSpec(name="매출(원)", width=10.14, number_format=FORMATS["currency"], alignment="right"),
    "iherb_item_winner_ratio": ColumnSpec(name="아이템위너비율", width=15.57, number_format=FORMATS["percentage"], alignment="right"),
    "iherb_stock": ColumnSpec(name="재고", width=7.29, number_format=FORMATS["integer"], alignment="right"),
    "iherb_stock_status": ColumnSpec(name="판매상태", width=10.57, number_format=FORMATS["text"]),
    
    # 로켓 성과
    "rocket_rank": ColumnSpec(name="순위", width=7.86, number_format=FORMATS["rank"], alignment="right"),
    "rocket_rating": ColumnSpec(name="평점", width=8.86, number_format=FORMATS["float"], alignment="right"),
    "rocket_reviews": ColumnSpec(name="리뷰수", width=9.0, number_format=FORMATS["integer"], alignment="right"),
    
    # 카테고리
    "iherb_category": ColumnSpec(name="아이허브", width=11.0, number_format=FORMATS["text"]),
    "rocket_category": ColumnSpec(name="로켓", width=12.86, number_format=FORMATS["text"]),
    
    # 제품명
    "iherb_product_name": ColumnSpec(name="아이허브", width=60.0, number_format=FORMATS["text"], alignment="left"),
    "rocket_product_name": ColumnSpec(name="로켓", width=60.0, number_format=FORMATS["text"], alignment="left"),
    
    # 링크
    "iherb_url": ColumnSpec(name="아이허브", width=10.57, number_format=FORMATS["text"]),
    "rocket_url": ColumnSpec(name="로켓", width=7.29, number_format=FORMATS["text"]),
    
    # ID
    "iherb_vendor_id": ColumnSpec(name="아이허브_Vendor", width=17.29, number_format=FORMATS["text"]),
    "rocket_vendor_id": ColumnSpec(name="로켓_Vendor", width=17.29, number_format=FORMATS["text"]),
    "iherb_item_id": ColumnSpec(name="아이허브_Item", width=15.14, number_format=FORMATS["text"]),
    "rocket_item_id": ColumnSpec(name="로켓_Item", width=15.14, number_format=FORMATS["text"]),
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 헬퍼 함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_column_spec(key: str, **overrides) -> ColumnSpec:
    """표준 컬럼 스펙 가져오기 + 커스터마이징
    
    Args:
        key: STANDARD_COLUMNS의 키
        **overrides: 덮어쓸 속성 (name, width, number_format 등)
    
    Returns:
        ColumnSpec
    
    Example:
        get_column_spec("iherb_price", name="판매가(오늘)")
    """
    if key not in STANDARD_COLUMNS:
        # 표준에 없으면 기본 스펙 생성
        spec = ColumnSpec(name=key, width=12.0, number_format=FORMATS["text"])
    else:
        spec = STANDARD_COLUMNS[key]
    
    # 오버라이드 적용
    if overrides:
        spec = ColumnSpec(
            name=overrides.get("name", spec.name),
            width=overrides.get("width", spec.width),
            number_format=overrides.get("number_format", spec.number_format),
            alignment=overrides.get("alignment", spec.alignment),
            conditional_rules=overrides.get("conditional_rules", spec.conditional_rules),
        )
    
    return spec


def get_timestamped_spec(
    key: str,
    date: str,
    label: Optional[str] = None,
    **overrides
) -> ColumnSpec:
    """날짜 포함 컬럼 스펙 생성
    
    Args:
        key: 기본 컬럼 키
        date: 날짜 문자열 (YYYY-MM-DD)
        label: 커스텀 라벨 (None이면 자동 생성)
        **overrides: 추가 오버라이드
    
    Returns:
        ColumnSpec
    
    Example:
        get_timestamped_spec("iherb_price", "2025-12-02")
        → ColumnSpec(name="판매가\n(2025-12-02)", ...)
    """
    base_spec = get_column_spec(key)
    
    if label is None:
        label = f"{base_spec.name}\n({date})"
    
    return get_column_spec(key, name=label, **overrides)


def get_delta_spec(base_name: str, width: float = 12.0) -> ColumnSpec:
    """Δ 컬럼 표준 스펙
    
    Args:
        base_name: 기본 이름 (예: "판매량Δ")
        width: 너비
    
    Returns:
        ColumnSpec
    """
    return ColumnSpec(
        name=base_name,
        width=width,
        number_format=FORMATS["float"],
        alignment="right",
    )