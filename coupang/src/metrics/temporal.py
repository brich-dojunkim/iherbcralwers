#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
metrics.temporal
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
여러 개의 스냅샷 DataFrame을 "표현/분석용" wide 형태로 다루기 위한 유틸.

핵심 아이디어
- DataManager가 스냅샷 단위로 넘겨주는 df들을
  [최신, -1일, -2일, ...] 순서의 리스트로 받고
  key 컬럼 기준으로 붙여서 wide 패널을 만든다.
- 컬럼 이름은 metric + "__라벨" 형태로 붙인다.
  예: iherb_sales_quantity__t0, iherb_sales_quantity__t1 등
- Δ(증감) 계산도 여기서 공통적으로 처리한다.

※ 실제 엑셀 헤더 텍스트("판매량\n(2025-11-24)" 같은 것)는
   analysis 레벨(product_dashboard, price_agent 등)에서 담당.
"""

from __future__ import annotations

from typing import List, Sequence, Optional, Dict

import pandas as pd
import numpy as np


def _default_labels(n: int) -> List[str]:
    """
    스냅샷 라벨 기본값 생성.

    dfs[0] = 가장 최신 스냅샷 → "t0"
    dfs[1] = 그 이전 → "t1"
    dfs[2] = 그 이전 → "t2"
    ...
    """
    return [f"t{i}" for i in range(n)]


def build_snapshot_panel(
    dfs: Sequence[pd.DataFrame],
    key_cols: Sequence[str],
    metric_cols: Sequence[str],
    labels: Optional[Sequence[str]] = None,
    how: str = "left",
) -> pd.DataFrame:
    """
    여러 스냅샷 DF를 하나의 wide 패널로 합치기.

    Args:
        dfs:
            [df_curr, df_prev, df_prev2, ...] 형태의 리스트.
            각 DF는 최소한 key_cols + metric_cols 를 포함해야 한다고 가정.
        key_cols:
            스냅샷 간 조인 기준이 되는 컬럼들 (예: ["iherb_vendor_id"])
        metric_cols:
            시간축으로 펼치고 싶은 지표들 (예: ["iherb_price", "iherb_sales_quantity"])
        labels:
            각 스냅샷에 붙일 라벨. 길이는 len(dfs)와 같아야 한다.
            None이면 ["t0", "t1", "t2", ...] 을 자동 생성.
        how:
            첫 번째 DF 기준으로 조인 방식:
            - "left": df[0]에 있는 key 만 기준 (현재 price_agent get_data와 같은 형태)
            - "outer": 모든 df의 key를 union

    Returns:
        panel_df:
            key_cols + metric_cols*len(dfs)를 가진 wide DataFrame.

            예를 들어:
            metric_cols = ["iherb_price", "iherb_sales_quantity"]
            labels = ["t0", "t1"]

            → 컬럼:
               key_cols ...
               "iherb_price__t0", "iherb_sales_quantity__t0",
               "iherb_price__t1", "iherb_sales_quantity__t1"
    """
    if not dfs:
        return pd.DataFrame()

    if labels is None:
        labels = _default_labels(len(dfs))

    if len(labels) != len(dfs):
        raise ValueError("labels 길이는 dfs 길이와 같아야 합니다.")

    # key 컬럼 중복 제거: 첫 번째 DF를 기준으로 시작
    base = dfs[0][list(key_cols)].drop_duplicates().copy()

    # 각 스냅샷에서 metric_cols만 뽑아서 suffix 붙인 뒤 merge
    panel = base
    for df, label in zip(dfs, labels):
        # 스냅샷 DF에서 key + metric만 추출
        available_metrics = [m for m in metric_cols if m in df.columns]
        subset_cols = list(key_cols) + available_metrics
        tmp = df[subset_cols].copy()

        # metric 이름에 라벨 suffix 부여
        rename_map = {
            m: f"{m}__{label}"
            for m in available_metrics
        }
        tmp = tmp.rename(columns=rename_map)

        # key 기준으로 병합
        panel = panel.merge(tmp, on=list(key_cols), how=how)

    return panel


def compute_delta(
    panel: pd.DataFrame,
    metric: str,
    newer_label: str,
    older_label: str,
    new_col_name: Optional[str] = None,
    as_pct: bool = False,
) -> pd.DataFrame:
    """
    wide 패널에서 특정 metric의 두 시점 간 Δ 컬럼 추가.

    Args:
        panel:
            build_snapshot_panel 로 생성한 wide DF.
        metric:
            원본 metric 이름 (예: "iherb_sales_quantity" 또는 "iherb_price").
        newer_label:
            최신 쪽 라벨 (예: "t0")
        older_label:
            과거 쪽 라벨 (예: "t1")
        new_col_name:
            생성할 컬럼명. None이면:
            - as_pct=False → f"{metric}_delta_{newer_label}_{older_label}"
            - as_pct=True  → f"{metric}_delta_pct_{newer_label}_{older_label}"
        as_pct:
            True면 (new - old) / old * 100 [%] 로 계산.
            False면 new - old.

    Returns:
        패널 DF (원본에 in-place로 컬럼 추가 후 반환)
    """
    col_new = f"{metric}__{newer_label}"
    col_old = f"{metric}__{older_label}"

    if col_new not in panel.columns or col_old not in panel.columns:
        # 그냥 조용히 지나가고 원본 반환 (analysis 레벨에서 존재 여부 체크해도 됨)
        return panel

    # 숫자 변환
    new_val = pd.to_numeric(panel[col_new], errors="coerce")
    old_val = pd.to_numeric(panel[col_old], errors="coerce")

    if new_col_name is None:
        if as_pct:
            new_col_name = f"{metric}_delta_pct_{newer_label}_{older_label}"
        else:
            new_col_name = f"{metric}_delta_{newer_label}_{older_label}"

    if as_pct:
        diff = (new_val - old_val)
        with np.errstate(divide="ignore", invalid="ignore"):
            delta = (diff / old_val.replace(0, np.nan)) * 100.0
        panel[new_col_name] = delta.round(1)
    else:
        panel[new_col_name] = (new_val - old_val)

    return panel


def compute_multiple_deltas(
    panel: pd.DataFrame,
    metrics: Sequence[str],
    newer_label: str,
    older_label: str,
    as_pct: bool = False,
) -> pd.DataFrame:
    """
    여러 metric에 대해 일괄 Δ 컬럼 생성.

    Args:
        panel:
            build_snapshot_panel 로 생성한 wide DF.
        metrics:
            Δ를 만들 metric 리스트.
        newer_label:
            최신 라벨 (예: "t0")
        older_label:
            과거 라벨 (예: "t1")
        as_pct:
            True면 (new-old)/old*100, False면 new-old

    Returns:
        Δ 컬럼이 여러 개 추가된 panel DF.
    """
    for m in metrics:
        panel = compute_delta(
            panel,
            metric=m,
            newer_label=newer_label,
            older_label=older_label,
            new_col_name=None,
            as_pct=as_pct,
        )
    return panel
