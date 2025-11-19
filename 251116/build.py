#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
바코드 매칭 결과 기반 정답 페어 / 브랜드 매핑 후보 생성 스크립트
- 입력 1: 위해식품목록_매칭결과.xlsx (match_source, product_id 포함)
- 입력 2: iherb_item feed_en.xlsx (product_id, product_partno, product_name, product_brand 포함)

출력:
1) matched_training_pairs.xlsx
   - 위해쪽 이름/제조사 + 아이허브쪽 이름/브랜드 + 정규화 텍스트

2) brand_mapping_candidates.xlsx
   - whee_manufacturer vs iherb_brand 조합별 카운트 및 비중
"""

import pandas as pd
import numpy as np
import re
from typing import Tuple


# ─────────────────────────────────────────
# 텍스트 정규화 유틸 (한글 + 영문 유지)
# ─────────────────────────────────────────

def normalize_text_ko_en(value: object) -> str:
    """
    제품명/브랜드명 정규화용 (한글 + 영문 + 숫자 유지)
    - 소문자 변환
    - 괄호 안 내용 제거
    - 단위/용량 패턴 제거 (mg, g, ml, oz, capsules, tablets 등)
    - 한글/영문/숫자 외 문자 제거
    - 공백 정리
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    s = str(value).lower()

    # 괄호 안 내용 제거 (용량, 부가설명 등)
    s = re.sub(r"\([^)]*\)", " ", s)

    # 흔한 용량/단위 패턴 제거
    s = re.sub(
        r"\b\d+(\.\d+)?\s*(mg|g|gr|gram|ml|mcg|oz|fl\s*oz|lbs?|lb|kg|"
        r"capsules?|tablets?|softgels?|gummies?|vegan)\b",
        " ",
        s,
    )

    # 한글 + 영문 + 숫자만 남기고 나머지 제거
    s = re.sub(r"[^0-9a-z가-힣]+", " ", s)

    # 다중 공백 정리
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ─────────────────────────────────────────
# 정답 페어 + 브랜드 매핑 후보 생성 함수
# ─────────────────────────────────────────

def build_training_pairs(
    matched_result_path: str,
    iherb_path: str,
    output_pairs_path: str = "matched_training_pairs.xlsx",
    output_brandmap_path: str = "brand_mapping_candidates.xlsx",
):
    print("=" * 80)
    print(" [STEP 1] 매칭 결과 / 아이허브 피드 로드")
    print("=" * 80)

    df_res = pd.read_excel(matched_result_path)
    df_iherb = pd.read_excel(iherb_path)

    print(f"  • 매칭결과 행/열: {df_res.shape}")
    print(f"  • 아이허브 행/열: {df_iherb.shape}")

    # 1) 바코드로 확실히 매칭된 행만 사용 (정답 세트)
    print("\n[STEP 2] 매칭된 행 필터링 (정답 세트 만들기)")

    # 1) 우선 product_id가 있는 행 = 어떤 방식으로든 매칭 성공한 행
    if "product_id" not in df_res.columns:
        raise KeyError("매칭 결과 파일에 'product_id' 컬럼이 없습니다.")

    df_gold = df_res[df_res["product_id"].notna()].copy()

    # 참고용으로 match_source 분포도 같이 출력
    if "match_source" in df_res.columns:
        print("  • match_source 분포:")
        print(df_res["match_source"].value_counts(dropna=False))

    print(f"  • product_id가 있는 매칭 행 수 (정답 세트): {len(df_gold)}")

    if len(df_gold) == 0:
        print("  ⚠️ 매칭된 행( product_id not null )이 없습니다. 이후 단계 진행 불가.")
        return

    # 2) 아이허브 쪽에서 필요한 컬럼만 추출
    needed_cols = ["product_id", "product_partno", "product_name", "product_brand"]
    for c in needed_cols:
        if c not in df_iherb.columns:
            raise KeyError(f"아이허브 피드에 '{c}' 컬럼이 없습니다.")

    df_iherb_small = df_iherb[needed_cols].copy()

    print("\n[STEP 3] product_id 기준 1차 JOIN")
    # product_id 기준으로 우선 매칭
    if "product_id" not in df_gold.columns:
        raise KeyError("매칭 결과 파일에 'product_id' 컬럼이 없습니다.")

    pairs = df_gold.merge(
        df_iherb_small,
        on="product_id",
        how="left",
        suffixes=("", "_iherb"),
    )

    # product_name이 비어 있는 경우 → product_partno로 보조 JOIN 시도
    missing_name_mask = pairs["product_name"].isna()
    num_missing_by_id = missing_name_mask.sum()
    print(f"  • product_id 기준 JOIN 후 product_name 비어있는 행 수: {num_missing_by_id}")

    if num_missing_by_id > 0:
        print("  → product_partno 기준 보조 JOIN 시도 중...")

        df_iherb_by_partno = (
            df_iherb_small
            .dropna(subset=["product_partno"])
            .drop_duplicates(subset=["product_partno"])
        )

        # 보조 매칭 대상만 따로
        sub = pairs.loc[missing_name_mask, ["product_partno"]].copy()
        sub = sub.merge(
            df_iherb_by_partno,
            on="product_partno",
            how="left",
            suffixes=("", "_by_partno"),
        )

        # product_name / product_brand 채우기
        pairs.loc[missing_name_mask, "product_name"] = sub["product_name"].values
        pairs.loc[missing_name_mask, "product_brand"] = sub["product_brand"].values

        num_still_missing = pairs["product_name"].isna().sum()
        print(f"  • 보조 JOIN 후에도 product_name 비어있는 행 수: {num_still_missing}")

    print("\n[STEP 4] 정답 페어 테이블 정리 및 정규화 컬럼 생성")

    # 위해쪽 / 아이허브쪽 텍스트 컬럼 이름을 명확히 구분
    if "제품명" not in pairs.columns or "제조사명" not in pairs.columns:
        raise KeyError("매칭 결과 파일에 '제품명' 또는 '제조사명' 컬럼이 없습니다.")

    pairs["whee_name"] = pairs["제품명"]
    pairs["whee_manufacturer"] = pairs["제조사명"]
    pairs["iherb_name"] = pairs["product_name"]
    pairs["iherb_brand"] = pairs["product_brand"]

    # 정규화 버전 (한글+영문 유지)
    pairs["whee_name_norm"] = pairs["whee_name"].apply(normalize_text_ko_en)
    pairs["whee_manufacturer_norm"] = pairs["whee_manufacturer"].apply(normalize_text_ko_en)
    pairs["iherb_name_norm"] = pairs["iherb_name"].apply(normalize_text_ko_en)
    pairs["iherb_brand_norm"] = pairs["iherb_brand"].apply(normalize_text_ko_en)

    # 최소한의 핵심 컬럼만 뽑은 뷰
    core_cols = [
        "product_id",
        "product_partno",
        "whee_name",
        "whee_manufacturer",
        "iherb_name",
        "iherb_brand",
        "whee_name_norm",
        "whee_manufacturer_norm",
        "iherb_name_norm",
        "iherb_brand_norm",
    ]

    pairs_core = pairs[core_cols].copy()
    print(f"  • 정답 페어 행 수: {len(pairs_core)}")

    # 5) 브랜드 매핑 후보 테이블 생성
    print("\n[STEP 5] 브랜드 매핑 후보 통계 생성")
    brand_map = (
        pairs_core
        .groupby(["whee_manufacturer", "iherb_brand"], dropna=False)
        .size()
        .reset_index(name="count")
    )

    # 제조사별 총 횟수 대비 비중 계산
    total_by_manufacturer = brand_map.groupby("whee_manufacturer")["count"].transform("sum")
    brand_map["share_within_manufacturer"] = (
        brand_map["count"] / total_by_manufacturer
    ).round(4)

    # 많이 등장하는 순으로 정렬
    brand_map = brand_map.sort_values(
        ["whee_manufacturer", "count"],
        ascending=[True, False],
    )

    print(f"  • 브랜드 매핑 후보 row 수: {len(brand_map)}")

    # 6) 엑셀 저장
    print("\n[STEP 6] 엑셀로 결과 저장")

    # 정답 페어
    with pd.ExcelWriter(output_pairs_path, engine="openpyxl") as writer:
        pairs_core.to_excel(writer, index=False, sheet_name="pairs_core")

    # 브랜드 매핑 후보
    brand_map.to_excel(output_brandmap_path, index=False)

    print(f"  ✅ 정답 페어 저장: {output_pairs_path}")
    print(f"  ✅ 브랜드 매핑 후보 저장: {output_brandmap_path}")
    print("=" * 80)


# ─────────────────────────────────────────
# 실행부
# ─────────────────────────────────────────

if __name__ == "__main__":
    # 실제 경로에 맞게 수정해서 사용
    build_training_pairs(
        matched_result_path="/Users/brich/Desktop/iherb_price/251116/위해식품목록_매칭결과.xlsx",
        iherb_path="/Users/brich/Desktop/iherb_price/251116/iherb_item feed_en.xlsx",
        output_pairs_path="/Users/brich/Desktop/iherb_price/251116/matched_training_pairs.xlsx",
        output_brandmap_path="/Users/brich/Desktop/iherb_price/251116/brand_mapping_candidates.xlsx",
    )
