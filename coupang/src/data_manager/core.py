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
- (신규) 여러 스냅샷(panel)을 한 번에 가져오는 기반 제공
"""

import pandas as pd
from typing import Optional, List, Dict, Any

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

    # ------------------------------------------------------------------
    # 0) 내부 공통 유틸
    # ------------------------------------------------------------------
    def _resolve_snapshot_id(
        self,
        target_date: Optional[str],
        snapshot_id: Optional[int],
    ) -> Optional[int]:
        """
        target_date / snapshot_id 둘 중 무엇이 들어오든
        실제 snapshot_id 하나로 정리하는 내부 함수.
        - snapshot_id가 있으면 우선 사용
        - 없으면 target_date로 lookup
        - 둘 다 없으면 최신 snapshot 사용
        """
        if snapshot_id is not None:
            return snapshot_id

        if target_date:
            sid = self.loader.get_snapshot_by_date(target_date)
            return sid

        # 아무 것도 없으면 latest
        sid = self.loader.get_latest_snapshot_id()
        return sid

    # ------------------------------------------------------------------
    # 1) 단일 스냅샷 뷰 (기존 get_integrated_df 로직 → get_snapshot_view로 이동)
    # ------------------------------------------------------------------
    def get_snapshot_view(
        self,
        target_date: Optional[str] = None,
        snapshot_id: Optional[int] = None,
        include_unmatched: bool = True,
    ) -> pd.DataFrame:
        """
        단일 스냅샷 통합 뷰 생성

        Args:
            target_date: 특정 날짜 (YYYY-MM-DD) – None이면 최신 snapshot
            snapshot_id: 특정 snapshot ID (지정 시 target_date보다 우선)
            include_unmatched: 아이허브 미매칭 상품 포함 여부

        Returns:
            통합 DataFrame (기존 get_integrated_df와 동일 구조)

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
            - iherb_revenue                : 누적 매출 (전체 기간 또는 스냅샷 기준)
            - iherb_sales_quantity         : 누적 판매 수량
            - iherb_item_winner_ratio      : 아이템 위너 비율 (%)

            [아이허브 최근 7일 성과]
            - iherb_sales_quantity_last_7d : 최근 7일 판매 수량
            - iherb_coupang_share_last_7d  : 최근 7일 '쿠팡' 채널 비중 (%)

            [가격 비교/할인 전략]
            - price_diff, price_diff_pct, cheaper_source
            - breakeven_discount_rate
            - recommended_discount_rate (판매가 기준)
            - requested_discount_rate (정가 기준)

            [공통 ID]
            - product_id: rocket_product_id 우선, 없으면 iherb_product_id
        """

        # 1. snapshot ID 결정
        sid = self._resolve_snapshot_id(target_date, snapshot_id)
        if sid is None:
            return pd.DataFrame()

        # 2. 로켓 / 아이허브 데이터 로드
        df_rocket = self.loader.load_rocket_data(sid)
        df_iherb = self.loader.load_iherb_data(sid)

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

    def get_integrated_df(
        self,
        target_date: Optional[str] = None,
        snapshot_id: Optional[int] = None,
        include_unmatched: bool = True,
    ) -> pd.DataFrame:
        """
        ✅ 기존 함수 이름 유지용 래퍼
        내부적으로는 get_snapshot_view를 그대로 호출한다.
        """
        return self.get_snapshot_view(
            target_date=target_date,
            snapshot_id=snapshot_id,
            include_unmatched=include_unmatched,
        )

    # ------------------------------------------------------------------
    # 2) 여러 스냅샷(panel) 뷰
    # ------------------------------------------------------------------
    def get_panel_views(
        self,
        snapshot_ids: Optional[List[int]] = None,
        n_latest: int = 3,
        include_unmatched: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        여러 스냅샷을 한 번에 가져오는 panel 기반 뷰.

        Args:
            snapshot_ids:
                - 명시적으로 스냅샷 ID 목록을 지정하고 싶을 때 사용
                - None이면 최신 n_latest개를 자동으로 사용
            n_latest:
                - snapshot_ids가 None일 때만 사용
                - 예: n_latest=3 → 최신 3개 스냅샷
            include_unmatched:
                - 각 snapshot_view에서 미매칭 포함 여부

        Returns:
            panel: 리스트 형태
                [
                    {
                        "snapshot_id": int,
                        "snapshot_date": "YYYY-MM-DD",
                        "df": pd.DataFrame
                    },
                    ...
                ]

            ※ 날짜는 오래된 것 → 최신 순으로 정렬해서 돌려준다.
            (D-2, D-1, D 이런 순서로 metrics에서 쓰기 쉽게)
        """

        # 1) 사용할 snapshot 목록 결정
        if snapshot_ids is None:
            # 최신 n_latest개 스냅샷 ID/날짜 목록 가져오기
            snapshots_df = self.loader.list_snapshots(limit=n_latest)
            if snapshots_df.empty:
                return []

            # 오래된 날짜 → 최신 날짜 순으로 정렬
            snapshots_df = snapshots_df.sort_values(
                ["snapshot_date", "id"], ascending=[True, True]
            )
            id_list = snapshots_df["id"].tolist()
            date_map = {
                row["id"]: row["snapshot_date"] for _, row in snapshots_df.iterrows()
            }
        else:
            # 직접 지정된 ID 목록 사용
            # 날짜는 get_snapshot_info로 개별 조회
            id_list = list(snapshot_ids)
            date_map = {}
            for sid in id_list:
                info = self.loader.get_snapshot_info(sid)
                date_map[sid] = info["snapshot_date"] if info else None

            # 날짜 기준 정렬 (알 수 없는 날짜는 뒤로)
            id_list = sorted(
                id_list,
                key=lambda x: (date_map.get(x) is None, date_map.get(x)),
            )

        # 2) 각 snapshot별 view 생성
        panel: List[Dict[str, Any]] = []
        for sid in id_list:
            df = self.get_snapshot_view(
                snapshot_id=sid,
                include_unmatched=include_unmatched,
            )
            if df is None or df.empty:
                continue

            panel.append(
                {
                    "snapshot_id": sid,
                    "snapshot_date": date_map.get(sid),
                    "df": df,
                }
            )

        return panel
