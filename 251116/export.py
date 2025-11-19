#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def export_name_brand_matches(
    matched_v2_path: str,
    iherb_path: str,
    output_path: str = "name_brand_matches_review.xlsx",
):
    df_res = pd.read_excel(matched_v2_path)
    df_iherb = pd.read_excel(iherb_path)

    # 1) name_brand_v2만 필터
    df_nb = df_res[df_res["match_source"] == "name_brand_v2"].copy()
    print(f"name_brand_v2 매칭 행 수: {len(df_nb)}")

    # 2) 아이허브에서 필요한 컬럼만
    cols_iherb = ["product_id", "product_partno", "product_name", "product_brand"]
    df_i_small = df_iherb[cols_iherb].drop_duplicates(subset=["product_id"])

    # 3) product_id 기준 JOIN
    review = df_nb.merge(df_i_small, on="product_id", how="left", suffixes=("", "_iherb"))

    # 4) 검수에 필요한 컬럼만 정리
    cols_review = [
        # 위해 쪽
        "제품명",
        "제조사명",
        # 아이허브 쪽
        "product_name",
        "product_brand",
        # 키 정보
        "product_id",
        "product_partno",
        "name_match_score",
    ]
    cols_review = [c for c in cols_review if c in review.columns]

    review = review[cols_review].copy()

    # score 높은 순으로 정렬
    if "name_match_score" in review.columns:
        review = review.sort_values("name_match_score", ascending=False)

    review.to_excel(output_path, index=False)
    print(f"✅ 검수용 엑셀 저장 완료: {output_path}")


if __name__ == "__main__":
    export_name_brand_matches(
        matched_v2_path="/Users/brich/Desktop/iherb_price/251116/위해식품목록_매칭결과_v2.xlsx",
        iherb_path="/Users/brich/Desktop/iherb_price/251116/iherb_item feed_en.xlsx",
        output_path="/Users/brich/Desktop/iherb_price/251116/name_brand_matches_review.xlsx",
    )
