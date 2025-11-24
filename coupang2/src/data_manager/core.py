#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DataManager Core
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
통합 데이터 관리 - Product ID 기반 매칭 + 통합 뷰 생성

역할:
- 통합 DB에서 로켓 / 아이허브 데이터를 로드
- Product ID 기반 매칭 수행
- 가격 비교/할인율/손익분기 할인율 계산
- (옵션) 아이허브 미매칭 상품까지 포함한 전체 뷰 생성
"""

import pandas as pd
from typing import Optional

from .db_loader import DataLoader
from .matcher import ProductMatcher
from .calculator import PriceCalculator


class DataManager:
    """통합 데이터 관리 - Product ID 기반 매칭 + 통합 뷰 생성"""

    def __init__(self, db_path: str):
        """
        Args:
            db_path: 통합 DB 경로
        """
        self.db_path = db_path
        self.loader = DataLoader(db_path)
        self.matcher = ProductMatcher()
        self.calculator = PriceCalculator()

    def get_integrated_df(
        self,
        target_date: Optional[str] = None,
        snapshot_id: Optional[int] = None,
        include_unmatched: bool = True,
    ) -> pd.DataFrame:
        """
        통합 데이터프레임 생성

        Args:
            target_date: 특정 날짜 (None이면 최신 snapshot)
            snapshot_id: 특정 snapshot ID (지정 시 target_date보다 우선)
            include_unmatched: 아이허브 미매칭 상품 포함 여부

        Returns:
            DataFrame with columns:

            [매칭 정보]
            - matching_status: '로켓매칭' 또는 '미매칭'
            - matching_method
            - matching_confidence

            [로켓직구]
            - rocket_vendor_id, rocket_product_id, rocket_item_id
            - rocket_product_name
            - rocket_price, rocket_original_price
            - rocket_rank, rocket_rating, rocket_reviews
            - rocket_category
            - rocket_url
            - rocket_discount_rate

            [아이허브 가격/재고]
            - iherb_vendor_id, iherb_product_id, iherb_item_id
            - iherb_product_name
            - iherb_part_number, iherb_upc
            - iherb_price (판매가), iherb_original_price (정가)
            - iherb_recommended_price
            - iherb_stock, iherb_stock_status
            - iherb_category
            - iherb_url

            [아이허브 판매 성과]
            - iherb_revenue
            - iherb_sales_quantity
            - iherb_item_winner_ratio

            [가격 비교/할인 전략]
            - price_diff, price_diff_pct, cheaper_source
            - breakeven_discount_rate
            - recommended_discount_rate (판매가 기준)
            - requested_discount_rate (정가 기준)

            [공통 ID]
            - product_id: rocket_product_id 우선, 없으면 iherb_product_id
        """

        # 1. snapshot ID 결정
        if snapshot_id is None:
            if target_date:
                snapshot_id = self.loader.get_snapshot_by_date(target_date)
                if snapshot_id is None:
                    return pd.DataFrame()
            else:
                snapshot_id = self.loader.get_latest_snapshot_id()
                if snapshot_id is None:
                    return pd.DataFrame()

        # 2. 로켓 / 아이허브 데이터 로드
        df_rocket = self.loader.load_rocket_data(snapshot_id)
        df_iherb = self.loader.load_iherb_data(snapshot_id)

        if df_rocket.empty and df_iherb.empty:
            return pd.DataFrame()
        if df_iherb.empty:
            # 로켓만 있는 스냅샷은 현재 분석 목적상 의미가 없으므로 빈 DF 반환
            return pd.DataFrame()

        # 3. Product ID 기반 매칭 (로켓 기준)
        df_matched = self.matcher.match_products(df_rocket, df_iherb)

        # 매칭된 행 기본 상태값 보정
        if not df_matched.empty:
            if "matching_status" not in df_matched.columns:
                df_matched["matching_status"] = "로켓매칭"
            else:
                df_matched["matching_status"] = df_matched["matching_status"].fillna("로켓매칭")

        # 🔸 여기서는 아직 가격 계산(PriceCalculator)을 태우지 않는다
        #    → 아래에서 미매칭까지 포함한 최종 df_final에 한 번만 적용

        # 4. 미매칭 아이허브 상품 포함 여부
        if include_unmatched:
            # 4-1. 이미 매칭된 아이허브 vendor_id 목록
            matched_iherb_ids = []
            if "iherb_vendor_id" in df_matched.columns and not df_matched.empty:
                matched_iherb_ids = (
                    df_matched["iherb_vendor_id"]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )

            # 4-2. 아이허브 전체에서 아직 안 들어온 것들 = 완전 미매칭
            df_iherb_tmp = df_iherb.copy()
            df_iherb_tmp["iherb_vendor_id"] = df_iherb_tmp["iherb_vendor_id"].astype(str)

            if matched_iherb_ids:
                df_unmatched = df_iherb_tmp[
                    ~df_iherb_tmp["iherb_vendor_id"].isin(matched_iherb_ids)
                ].copy()
            else:
                # 매칭이 하나도 없으면 아이허브 전체가 미매칭
                df_unmatched = df_iherb_tmp.copy()

            if not df_unmatched.empty:
                # 4-3. 로켓 관련 컬럼들(없으면 생성해서 NaN으로 채움)
                rocket_cols = [
                    "rocket_vendor_id",
                    "rocket_product_id",
                    "rocket_item_id",
                    "rocket_product_name",
                    "rocket_price",
                    "rocket_original_price",
                    "rocket_rank",
                    "rocket_rating",
                    "rocket_reviews",
                    "rocket_category",
                    "rocket_url",
                    "rocket_discount_rate",
                ]
                for col in rocket_cols:
                    if col not in df_unmatched.columns:
                        df_unmatched[col] = pd.NA

                # 4-4. 매칭 상태 컬럼 세팅
                df_unmatched["matching_status"] = "미매칭"
                df_unmatched["matching_method"] = "미매칭"
                if "matching_confidence" not in df_unmatched.columns:
                    df_unmatched["matching_confidence"] = ""

                # 4-5. 매칭 결과 + 미매칭 결과를 하나로 합치기
                if df_matched.empty:
                    df_final = df_unmatched.copy()
                else:
                    df_final = pd.concat(
                        [df_matched, df_unmatched],
                        ignore_index=True,
                        sort=False,
                    )
            else:
                # 미매칭이 하나도 없으면 매칭 결과만 사용
                df_final = df_matched.copy()
        else:
            # 미매칭 상품은 포함하지 않고, 로켓 매칭된 상품만 사용
            df_final = df_matched.copy()
            if "matching_status" not in df_final.columns:
                df_final["matching_status"] = "로켓매칭"
            else:
                df_final["matching_status"] = df_final["matching_status"].fillna("로켓매칭")

        if df_final.empty:
            return df_final

        # 5. 가격 비교 / 할인율 계산을 "최종 df_final" 전체에 한 번만 적용
        df_final = self.calculator.calculate_price_comparison(df_final)

        # 6. 공통 product_id 생성 (로켓 우선, 없으면 아이허브)
        if "rocket_product_id" not in df_final.columns:
            df_final["rocket_product_id"] = pd.NA
        if "iherb_product_id" not in df_final.columns:
            df_final["iherb_product_id"] = pd.NA

        df_final["product_id"] = df_final["rocket_product_id"].combine_first(
            df_final["iherb_product_id"]
        )

        # 7. 기본 정렬: 로켓매칭 우선, 그 안에서는 판매량(오늘) 기준 내림차순
        if "matching_status" in df_final.columns:
            sort_key = (
                df_final["matching_status"]
                .map({"로켓매칭": 0, "미매칭": 1})
                .fillna(2)
                .astype(int)
            )
            df_final["_sort_key"] = sort_key

            if "iherb_sales_quantity" in df_final.columns:
                df_final = df_final.sort_values(
                    ["_sort_key", "iherb_sales_quantity"],
                    ascending=[True, False],
                )
            else:
                df_final = df_final.sort_values(["_sort_key"], ascending=[True])

            df_final = df_final.drop(columns=["_sort_key"]).reset_index(drop=True)

        return df_final
