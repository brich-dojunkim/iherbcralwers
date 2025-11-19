#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import re

# 너무 일반적인 단어들 (이름 유사도 계산에서 가중치 줄 예정)
GENERIC_TOKENS = {
    "capsule", "capsules", "tablet", "tablets", "softgels", "softgel",
    "gummies", "gummy", "veggie", "vegetarian", "caps", "tabs",
    "supplement", "supplements", "powder", "liquid", "drops", "drop",
    "formula", "support", "supports", "blend", "complex", "plus", "extra",
    "high", "strength", "super", "max", "ultra", "advanced", "care",
    "vitamin", "vitamins", "mineral", "minerals",
}

# ─────────────────────────────────────────
# 공통 유틸 함수들
# ─────────────────────────────────────────

def normalize_text_ko_en(value: object) -> str:
    """
    제품명/브랜드명 정규화용 (한글 + 영문 + 숫자 유지)
    - 소문자
    - 괄호 내용 제거
    - 용량/단위 제거
    - 한글/영문/숫자만 남기기
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

    # 한글 + 영문 + 숫자만 남기기
    s = re.sub(r"[^0-9a-z가-힣]+", " ", s)

    # 공백 정리
    s = re.sub(r"\s+", " ", s).strip()
    return s


def make_barcode_key(series: pd.Series) -> pd.Series:
    """
    바코드 컬럼을 매칭용 키로 변환
    - float/int/str 섞여 있어도 처리
    """
    num = pd.to_numeric(series, errors="coerce")
    num = num.astype("Int64")
    key = num.astype("string")
    return key


def build_brand_dict(
    brand_map_path: str,
    min_count: int = 2,
    min_share: float = 0.8,
) -> dict:
    """
    brand_mapping_candidates.xlsx 기반으로
    제조사 정규화 → 브랜드 정규화 매핑 딕셔너리 생성

    - 같은 제조사에서 한 브랜드의 점유율이 high(share >= min_share)
    - 등장 횟수도 어느 정도 이상(count >= min_count)일 때만 채택
    """
    df = pd.read_excel(brand_map_path)

    if "whee_manufacturer" not in df.columns or "iherb_brand" not in df.columns:
        raise KeyError("brand_mapping_candidates.xlsx에 'whee_manufacturer' 또는 'iherb_brand'가 없습니다.")

    df["whee_manufacturer_norm"] = df["whee_manufacturer"].apply(normalize_text_ko_en)
    df["iherb_brand_norm"] = df["iherb_brand"].apply(normalize_text_ko_en)
    df = df[df["whee_manufacturer_norm"] != ""].copy()

    # 제조사별로 share 높은 순, count 큰 순으로 정렬 후 1개씩 뽑기
    df = df.sort_values(
        ["whee_manufacturer_norm", "share_within_manufacturer", "count"],
        ascending=[True, False, False],
    )
    grouped = df.groupby("whee_manufacturer_norm", as_index=False).first()

    brand_dict: dict[str, str] = {}
    for _, row in grouped.iterrows():
        if row.get("share_within_manufacturer", 0) >= min_share and row.get("count", 0) >= min_count:
            brand_dict[row["whee_manufacturer_norm"]] = row["iherb_brand_norm"]

    return brand_dict


def compute_name_score(whee_name_norm: str, iherb_name_norm: str, brand_norm: str) -> float:
    """
    브랜드가 이미 맞다고 가정하고,
    위해 제품명 vs 아이허브 product_name의 유사도 스코어 계산

    - 브랜드 토큰 + 너무 일반적인 토큰(GENERIC_TOKENS)은 빼고 비교
    - token overlap + 부분 포함 여부 조합
    """
    if not whee_name_norm or not iherb_name_norm:
        return 0.0

    w_tokens = set(whee_name_norm.split())
    i_tokens = set(iherb_name_norm.split())
    brand_tokens = set(brand_norm.split()) if brand_norm else set()

    w_core = w_tokens - brand_tokens - GENERIC_TOKENS
    i_core = i_tokens - brand_tokens - GENERIC_TOKENS

    # 남은 코어 토큰이 없으면 비교 의미 없음
    if not w_core or not i_core:
        return 0.0

    inter = w_core & i_core
    token_overlap = len(inter) / max(len(w_core), len(i_core))

    # 코어 토큰 수준에서 부분 포함 여부
    s_w = " ".join(sorted(w_core))
    s_i = " ".join(sorted(i_core))
    contains = s_w in s_i or s_i in s_w

    # 가중합 스코어
    score = 0.7 * token_overlap + 0.3 * (1.0 if contains else 0.0)
    return float(score)


# ─────────────────────────────────────────
# 메인 매칭 함수
# ─────────────────────────────────────────

def match_iherb_with_fallback(
    whee_path: str,
    iherb_path: str,
    output_path: str = "위해식품목록_매칭결과.xlsx",
    brand_map_path: str | None = None,
):
    print("\n" + "=" * 80)
    print(" 해외직구 위해식품 ↔ 아이허브 매칭 작업 시작")
    print("=" * 80 + "\n")

    # 1) 데이터 로드
    print("=" * 80)
    print(" [1/5] 엑셀 파일 로드 시작")
    print("=" * 80)
    print(f"  • 위해식품 파일 경로 : {whee_path}")
    print(f"  • 아이허브 파일 경로 : {iherb_path}")

    whee_df = pd.read_excel(whee_path)
    iherb_df = pd.read_excel(iherb_path)

    print("\n  ✅ 엑셀 로드 완료")
    print(f"  • 위해식품 행/열: {whee_df.shape}")
    print(f"  • 아이허브 행/열: {iherb_df.shape}")
    print("=" * 80 + "\n")

    # ── 2) 1차 바코드 매칭 ────────────────────────────────
    print("=" * 80)
    print(" [2/5] 1차 바코드 매칭")
    print("=" * 80)

    if "유통바코드정보" not in whee_df.columns:
        raise KeyError("위해식품 엑셀에 '유통바코드정보' 컬럼이 없습니다.")
    if "product_upc" not in iherb_df.columns:
        raise KeyError("아이허브 엑셀에 'product_upc' 컬럼이 없습니다.")

    whee_df["barcode_key"] = make_barcode_key(whee_df["유통바코드정보"])
    iherb_df["barcode_key"] = make_barcode_key(iherb_df["product_upc"])

    whee_barcode_nonnull = whee_df["barcode_key"].notna().sum()
    iherb_barcode_nonnull = iherb_df["barcode_key"].notna().sum()

    print(f"  • 위해식품 바코드 존재 행 수 : {whee_barcode_nonnull}")
    print(f"  • 아이허브 UPC 존재 행 수   : {iherb_barcode_nonnull}")

    # 아이허브: 바코드 기준 유일 row만 사용
    iherb_barcode = (
        iherb_df[["barcode_key", "product_partno", "product_id"]]
        .dropna(subset=["barcode_key"])
        .drop_duplicates(subset=["barcode_key"])
    )
    print(f"  • 아이허브 고유 바코드 수   : {len(iherb_barcode)}")

    # 위해식품 기준 LEFT JOIN
    print("\n  → 1차 바코드 기준 LEFT JOIN 수행 중...")
    merged = whee_df.merge(iherb_barcode, on="barcode_key", how="left")

    merged["match_source"] = np.where(
        merged["product_id"].notna(), "barcode", pd.NA
    )
    matched_barcode = (merged["match_source"] == "barcode").sum()
    print(f"  ✅ 1차 바코드 매칭 완료 (매칭 행 수: {matched_barcode})")
    print("=" * 80 + "\n")

    # ── 3) 2차: 브랜드 매핑 + 이름 유사도 ─────────────────
    print("=" * 80)
    print(" [3/5] 2차 제품명+제조사 기반 보조 매칭 (브랜드 매핑 + 이름 유사도)")
    print("=" * 80)

    mask_unmatched = merged["product_id"].isna()
    num_unmatched = mask_unmatched.sum()
    print(f"  • 1차 바코드 매칭 후 미매칭 행 수 : {num_unmatched}")

    name_matches = 0

    if num_unmatched and brand_map_path is not None:
        # 3-1. 브랜드 매핑 딕셔너리
        brand_dict = build_brand_dict(brand_map_path)
        print(f"  • 브랜드 매핑 딕셔너리 크기 : {len(brand_dict)}")

        if len(brand_dict) == 0:
            print("  ⚠️ 브랜드 매핑 딕셔너리가 비어 있어 2차 매칭은 건너뜁니다.")
        else:
            # 아이허브 쪽 정규화 컬럼 준비
            if "product_name" not in iherb_df.columns or "product_brand" not in iherb_df.columns:
                raise KeyError("아이허브 엑셀에 'product_name' 또는 'product_brand' 컬럼이 없습니다.")

            iherb_df["brand_norm"] = iherb_df["product_brand"].apply(normalize_text_ko_en)
            iherb_df["name_norm"] = iherb_df["product_name"].apply(normalize_text_ko_en)

            # 위해 미매칭 행 준비
            whee_unmatched = merged.loc[mask_unmatched].copy()

            if "제품명" not in whee_unmatched.columns or "제조사명" not in whee_unmatched.columns:
                raise KeyError("위해식품 엑셀에 '제품명' 또는 '제조사명' 컬럼이 없습니다.")

            whee_unmatched["whee_name_norm"] = whee_unmatched["제품명"].apply(normalize_text_ko_en)
            whee_unmatched["manufacturer_norm"] = whee_unmatched["제조사명"].apply(normalize_text_ko_en)

            # 3-2. 한 행씩 브랜드 → 후보군 좁히고 이름 스코어 계산
            for idx, row in whee_unmatched.iterrows():
                m_norm = row["manufacturer_norm"]
                brand_norm = brand_dict.get(m_norm)
                if not brand_norm:
                    continue  # 브랜드 매핑이 없는 제조사는 스킵

                cand = iherb_df[iherb_df["brand_norm"] == brand_norm]
                if cand.empty:
                    continue

                best_score = 0.0
                second_score = 0.0
                best_pid = None
                best_partno = None

                for _, c_row in cand.iterrows():
                    s = compute_name_score(
                        row["whee_name_norm"],
                        c_row["name_norm"],
                        brand_norm,
                    )
                    if s > best_score:
                        second_score = best_score
                        best_score = s
                        best_pid = c_row["product_id"]
                        best_partno = c_row["product_partno"]
                    elif s > second_score:
                        second_score = s

                # threshold + margin
                if best_score >= 0.45 and (best_score - second_score) >= 0.15:
                    merged.at[idx, "product_id"] = best_pid
                    merged.at[idx, "product_partno"] = best_partno
                    merged.at[idx, "match_source"] = "name_brand_v2"
                    merged.at[idx, "name_match_score"] = best_score
                    name_matches += 1

            print(f"  ✅ 2차 이름+제조사 기반 매칭 완료 (추가 매칭 행 수: {name_matches})")

    else:
        if not num_unmatched:
            print("  • 바코드 매칭에서 이미 전부 매칭되어 2차 매칭은 생략합니다.")
        else:
            print("  ⚠️ brand_map_path가 지정되지 않아 2차 매칭은 건너뜁니다.")

    print("=" * 80 + "\n")

    # ── 4) 요약 리포트 ────────────────────────────────────
    print("=" * 80)
    print(" [4/5] 매칭 요약 리포트")
    print("=" * 80)

    total = len(merged)
    matched_barcode = (merged["match_source"] == "barcode").sum()
    matched_namebrand = (merged["match_source"] == "name_brand_v2").sum()
    matched_any = merged["product_id"].notna().sum()
    unmatched_final = total - matched_any

    print(f"  • 총 행 수                 : {total}")
    print(f"  • 1차 바코드 매칭 성공      : {matched_barcode}")
    print(f"  • 2차 이름+제조사 매칭 성공 : {matched_namebrand}")
    print(f"  • 최종 매칭 성공(중복제외)   : {matched_any}")
    print(f"  • 최종 매칭 실패             : {unmatched_final}")
    print("=" * 80 + "\n")

    # ── 5) 엑셀 저장 ──────────────────────────────────────
    print("=" * 80)
    print(" [5/5] 최종 엑셀 저장")
    print("=" * 80)

    base_cols = [
        c
        for c in merged.columns
        if c not in ["product_partno", "product_id", "match_source", "barcode_key"]
    ]
    # name_match_score가 있으면 같이 붙음
    final_cols = base_cols + [c for c in ["product_partno", "product_id", "match_source", "name_match_score"] if c in merged.columns]
    merged = merged[final_cols]

    merged.to_excel(output_path, index=False)
    print(f"  ✅ 매칭 결과 저장 완료: {output_path}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    match_iherb_with_fallback(
        whee_path="/Users/brich/Desktop/iherb_price/251116/2025.11.18_해외직구+위해식품+목록.xls",
        iherb_path="/Users/brich/Desktop/iherb_price/251116/iherb_item feed_en.xlsx",
        output_path="/Users/brich/Desktop/iherb_price/251116/위해식품목록_매칭결과_v2.xlsx",
        brand_map_path="/Users/brich/Desktop/iherb_price/251116/brand_mapping_candidates.xlsx",
    )
