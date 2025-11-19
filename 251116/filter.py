#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
iHerb 피드에서 비플로우에 실제 등록된 아이허브 상품만 추출 + 중복 제거

- 기준1: iherb_item feed_en.xlsx
- 기준2: 아이허브_전체_상품리스트_20251118_1046.xlsx
- 매칭키:
    * iherb_item feed_en : product_partno
    * 전체상품리스트      : 판매자상품코드
"""

import pandas as pd
from pathlib import Path

# ================== 파일 경로 설정 ==================
BASE_DIR = Path("/Users/brich/Desktop/iherb_price/251116")  # 또는 네 로컬 폴더 경로

FEED_FILE = BASE_DIR / "iherb_item feed_en.xlsx"
ALL_FILE  = BASE_DIR / "아이허브_전체_상품리스트_20251118_1046.xlsx"
OUTPUT_FILE = BASE_DIR / "iherb_item_filtered_by_all_list.xlsx"

def main():
    print("📂 파일 읽는 중...")

    # iHerb feed 전체 파일
    feed_df = pd.read_excel(FEED_FILE)
    # 비플로우 전체 아이허브 상품리스트
    all_df = pd.read_excel(ALL_FILE)

    # 컬럼 이름 확인 (디버깅용)
    print("🔎 feed columns:", list(feed_df.columns))
    print("🔎 all  columns:", list(all_df.columns))

    # 문자열 기준 정리: 공백 제거 + 문자열 캐스팅
    feed_df["product_partno_str"] = (
        feed_df["product_partno"].astype(str).str.strip()
    )
    all_df["판매자상품코드_str"] = (
        all_df["판매자상품코드"].astype(str).str.strip()
    )

    # 전체상품리스트에 있는 판매자상품코드 집합
    valid_partnos = set(all_df["판매자상품코드_str"])

    print(f"✅ 전체상품리스트 기준 유효 PartNo 개수: {len(valid_partnos):,}개")

    # iHerb feed에서 교집합만 필터링
    filtered_df = feed_df[
        feed_df["product_partno_str"].isin(valid_partnos)
    ].copy()

    print(f"✂️ 필터링 후 (교집합) 행 개수: {len(filtered_df):,}개")

    # 중복 제거 (product_partno 기준)
    before_dedup = len(filtered_df)
    filtered_df = filtered_df.drop_duplicates(
        subset=["product_partno_str"], keep="first"
    )
    after_dedup = len(filtered_df)

    print(f"🧹 중복 제거: {before_dedup:,} → {after_dedup:,} (product_partno 기준)")

    # 내부에서만 쓰던 보조 컬럼 제거
    filtered_df = filtered_df.drop(columns=["product_partno_str"])

    # 엑셀로 저장
    filtered_df.to_excel(OUTPUT_FILE, index=False)
    print(f"💾 완료! 결과 파일: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
