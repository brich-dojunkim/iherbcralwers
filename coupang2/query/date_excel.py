#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
옵션 B: 날짜 중심 구조
상위 헤더: 기본정보 | 10/20 | 10/21 | 10/23 | ...
하위 헤더: - | 순위,가격,할인율 | 순위,가격,할인율,순위변화,가격변화,할인율변화 | ...

특징:
- 미니멀 디자인 (색상 최소화)
- 모든 변화량: 절대치
- 변화량 색상: 음수=파랑, 양수=빨강
- 날짜 간 구분선
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/monitoring.db"
OUTPUT_DIR = "/Users/brich/Desktop/iherb_price/coupang2/outputs"

def generate_date_centered_format():
    """iHerb UPC/PartNumber를 공통키로, 소스1/소스2를 날짜별로 비교하는 와이드 포맷 생성"""

    conn = sqlite3.connect(DB_PATH)
    print("\n" + "="*80)
    print("📊 옵션 B: 날짜 중심 구조 (소스1=로켓직구, 소스2=아이허브)")
    print("="*80)

    # 1) (카테고리, 소스, 날짜)별 최신 스냅샷에서 상품 상태 + 매칭키(UPC/품번) 로드
    query = """
    WITH daily_latest_snapshots AS (
        SELECT
            c.id AS category_id,
            c.name AS category_name,
            ps.source_id,
            DATE(ps.snapshot_time) AS snapshot_date,
            MAX(ps.id) AS latest_snapshot_id
        FROM page_snapshots ps
        JOIN categories c ON c.id = ps.category_id
        GROUP BY c.id, c.name, ps.source_id, DATE(ps.snapshot_time)
    ),
    joined AS (
        SELECT
            dls.category_name,
            dls.source_id,
            dls.snapshot_date,
            prod.coupang_product_id,
            prod.product_name,
            prod.category_rank,
            prod.current_price,
            prod.discount_rate,
            prod.review_count,
            prod.rating_score,
            mr.iherb_upc,
            mr.iherb_part_number,
            COALESCE(mr.iherb_upc, mr.iherb_part_number) AS match_key
        FROM daily_latest_snapshots dls
        JOIN product_states prod ON prod.snapshot_id = dls.latest_snapshot_id
        JOIN matching_reference mr ON mr.coupang_product_id = prod.coupang_product_id
        WHERE (mr.iherb_upc IS NOT NULL OR mr.iherb_part_number IS NOT NULL)
          AND dls.source_id IN (1,2)  -- 소스1=1(로켓직구), 소스2=2(아이허브)
    )
    -- 같은 날짜/소스/매칭키에 동상품 중복되면 최상위 랭크 하나만 채택
    SELECT j.*
    FROM joined j
    LEFT JOIN joined k
      ON j.category_name = k.category_name
     AND j.source_id     = k.source_id
     AND j.snapshot_date = k.snapshot_date
     AND j.match_key     = k.match_key
     AND k.category_rank < j.category_rank
    WHERE k.category_rank IS NULL
    ORDER BY j.category_name, j.match_key, j.snapshot_date, j.source_id
    """
    df_long = pd.read_sql_query(query, conn)
    conn.close()

    if df_long.empty:
        print("❌ 매칭된 데이터가 없습니다.")
        return [], pd.DataFrame()

    # 소스 라벨 고정
    source_label_map = {1: "소스1", 2: "소스2"}  # 소스1=로켓직구, 소스2=아이허브
    df_long["source_label"] = df_long["source_id"].map(source_label_map)

    dates = sorted(df_long['snapshot_date'].unique())
    print(f"\n✅ 데이터 로드: {len(df_long):,}개 레코드, {len(dates)}일")

    # 2) 기본정보(카테고리, 매칭키 단위) - 상품명/UPC/품번은 대표값으로
    base_info = (
        df_long
        .sort_values(["snapshot_date"])  # 최근값이 뒤에 오도록
        .groupby(["category_name", "match_key"], as_index=False)
        .agg({
            "product_name": "last",
            "iherb_upc": "last",
            "iherb_part_number": "last",
            "review_count": "last",
            "rating_score": "last"
        })
    )

    wide_df = base_info.copy()

    # 3) 날짜별 + 소스별 지표 머지: 하위헤더 = f"{소스라벨}_지표명"
    print("🔄 데이터 변환 중…")
    for i, date in enumerate(dates):
        date_str = date[5:].replace('-', '/')

        for sid in (1, 2):
            slabel = source_label_map[sid]

            date_src = df_long[(df_long['snapshot_date'] == date) &
                               (df_long['source_id'] == sid)][
                ["category_name", "match_key", "category_rank", "current_price", "discount_rate"]
            ].copy()

            date_src.columns = ["category_name", "match_key",
                                f"{date_str}_{slabel}_순위",
                                f"{date_str}_{slabel}_가격",
                                f"{date_str}_{slabel}_할인율"]

            wide_df = wide_df.merge(date_src, on=["category_name", "match_key"], how="left")

        # 변화량(이전 날짜 대비, 소스별 각각)
        if i > 0:
            prev = dates[i - 1][5:].replace('-', '/')
            cur  = date_str
            for slabel in ("소스1", "소스2"):
                # 순위 변화 = 이전 - 현재 (낮아지면 +)
                wide_df[f"{cur}_{slabel}_순위변화"] = (
                    wide_df[f"{prev}_{slabel}_순위"] - wide_df[f"{cur}_{slabel}_순위"]
                ).round(0)

                # 가격/할인율 변화 = 현재 - 이전
                wide_df[f"{cur}_{slabel}_가격변화"] = (
                    wide_df[f"{cur}_{slabel}_가격"] - wide_df[f"{prev}_{slabel}_가격"]
                ).round(0)

                wide_df[f"{cur}_{slabel}_할인율변화"] = (
                    wide_df[f"{cur}_{slabel}_할인율"] - wide_df[f"{prev}_{slabel}_할인율"]
                ).round(1)

    # 4) 컬럼 재정렬
    base_cols = ["category_name", "match_key", "product_name",
                 "iherb_upc", "iherb_part_number", "review_count", "rating_score"]
    ordered_cols = base_cols.copy()

    for i, date in enumerate(dates):
        d = date[5:].replace('-', '/')
        # 날짜별 소스1 → 소스2 순서로
        ordered_cols.extend([f"{d}_소스1_순위", f"{d}_소스1_가격", f"{d}_소스1_할인율",
                             f"{d}_소스2_순위", f"{d}_소스2_가격", f"{d}_소스2_할인율"])
        if i > 0:
            ordered_cols.extend([f"{d}_소스1_순위변화", f"{d}_소스1_가격변화", f"{d}_소스1_할인율변화",
                                 f"{d}_소스2_순위변화", f"{d}_소스2_가격변화", f"{d}_소스2_할인율변화"])

    # 누락된 열이 있을 수 있으므로 존재하는 컬럼만 취한다
    ordered_cols = [c for c in ordered_cols if c in wide_df.columns]
    wide_df = wide_df[ordered_cols]

    # 5) 두 단계 헤더 구성: (상위=기본정보/날짜, 하위=항목명)
    base_names = {
        "category_name": "카테고리",
        "match_key": "매칭키(UPC/품번)",
        "product_name": "상품명",
        "iherb_upc": "iHerb_UPC",
        "iherb_part_number": "iHerb_품번",
        "review_count": "리뷰수",
        "rating_score": "평점"
    }

    multi_columns = []
    for col in base_cols:
        multi_columns.append(("기본정보", base_names[col]))

    for col in ordered_cols[len(base_cols):]:
        # col 예: "10/20_소스1_순위"
        parts = col.split("_", 2)  # ["10/20","소스1","순위"] or ["10/21","소스2","순위변화"]
        if len(parts) == 3:
            level0, slabel, metric = parts
            multi_columns.append((level0, f"{slabel}_{metric}"))
        else:
            # 방어적 처리
            multi_columns.append(("기본정보", col))

    wide_df.columns = pd.MultiIndex.from_tuples(multi_columns)

    print(f"✅ 변환 완료: {len(wide_df):,}개 상품, {len(wide_df.columns)}개 컬럼")
    return dates, wide_df


def save_to_excel_minimal(dates, wide_df, output_path, option_name):
    """미니멀 디자인 Excel 저장"""
    
    print(f"\n💾 Excel 저장 중...")
    
    wb = Workbook()
    ws = wb.active
    ws.title = '데이터'
    
    # 미니멀 스타일
    header_font = Font(bold=True, size=11)
    subheader_font = Font(bold=True, size=10)
    header_fill = PatternFill(start_color='F0F0F0', end_color='F0F0F0', fill_type='solid')
    
    # 1행: 상위 헤더
    for col_idx, (level0, level1) in enumerate(wide_df.columns, start=1):
        cell = ws.cell(row=1, column=col_idx, value=level0)
        cell.font = header_font
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.fill = header_fill
    
    # 2행: 하위 헤더
    for col_idx, (level0, level1) in enumerate(wide_df.columns, start=1):
        cell = ws.cell(row=2, column=col_idx, value=level1)
        cell.font = subheader_font
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.fill = header_fill
    
    # 3행부터: 데이터 + 변화량 색상
    for row_idx, row_data in enumerate(wide_df.values, start=3):
        for col_idx, value in enumerate(row_data, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            
            # 컬럼 정보
            col_name = wide_df.columns[col_idx - 1][1]  # 하위 헤더
            
            # 숫자 포맷 적용 (세자리수마다 콤마, 소수점 없음)
            if pd.notna(value) and isinstance(value, (int, float)):
                # 변화량 컬럼
                if '변화' in col_name:
                    if value < 0:
                        cell.font = Font(color='0000FF')  # 음수: 파란색
                        cell.number_format = '#,##0'
                    elif value > 0:
                        cell.font = Font(color='FF0000')  # 양수: 빨간색
                        cell.number_format = '+#,##0'
                    else:
                        cell.number_format = '#,##0'
                else:
                    # 측정값 컬럼 (순위, 가격, 할인율)
                    cell.number_format = '#,##0'
    
    # 헤더 병합
    merge_start = 1
    prev_date = None
    for i, (level0, level1) in enumerate(wide_df.columns, start=1):
        if prev_date is not None and prev_date != level0:
            if merge_start < i - 1:
                ws.merge_cells(start_row=1, start_column=merge_start, end_row=1, end_column=i - 1)
            merge_start = i
        prev_date = level0
    
    if merge_start < len(wide_df.columns):
        ws.merge_cells(start_row=1, start_column=merge_start, end_row=1, end_column=len(wide_df.columns))
    
    # 날짜 간 구분선 (회색 중간 두께)
    prev_date = None
    for col_idx, (level0, level1) in enumerate(wide_df.columns, start=1):
        if prev_date is not None and prev_date != level0:
            for row_idx in range(1, len(wide_df) + 3):
                cell = ws.cell(row=row_idx, column=col_idx - 1)
                cell.border = Border(right=Side(style='medium', color='CCCCCC'))
        prev_date = level0
    
    # 컬럼 너비 조정
    for col_idx, (level0, level1) in enumerate(wide_df.columns, start=1):
        max_length = max(len(str(level0)), len(str(level1)), 10)
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_length + 2, 50)
    
    # 행 높이
    ws.row_dimensions[1].height = 25
    ws.row_dimensions[2].height = 20
    
    # 틀 고정
    ws.freeze_panes = 'H3'
    
    wb.save(output_path)
    print(f"✅ 저장 완료: {output_path}")


def main():
    """메인 실행"""
    
    print("\n" + "="*80)
    print("🎯 옵션 B: 날짜 중심 구조")
    print("="*80)
    print("상위 헤더: 기본정보 | 10/20 | 10/21 | 10/23 | ...")
    print("하위 헤더: - | 순위,가격,할인율 | 순위,가격,할인율,순위변화,가격변화,할인율변화 | ...")
    print("\n특징:")
    print("  ✓ 시간순 자연스러운 흐름 → 시계열 분석 용이")
    print("  ✓ 모든 변화량: 절대치")
    print("  ✓ 변화량 색상: 음수=파랑, 양수=빨강")
    print("  ✓ 미니멀 디자인")
    print("="*80)
    
    dates, wide_df = generate_date_centered_format()
    
    output_path = os.path.join(OUTPUT_DIR, "option_B_date_centered.xlsx")
    save_to_excel_minimal(dates, wide_df, output_path, "B")
    
    # CSV
    csv_path = os.path.join(OUTPUT_DIR, "option_B_date_centered.csv")
    flat_df = wide_df.copy()
    flat_df.columns = [f'{level0}_{level1}' if level0 != '기본정보' else level1 
                       for level0, level1 in flat_df.columns]
    flat_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"💾 CSV 저장: {csv_path}")
    
    print("\n" + "="*80)
    print("✅ 옵션 B 완료!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()