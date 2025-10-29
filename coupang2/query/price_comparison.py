#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
실용적인 Price Agent Excel
- 379개 중복 UPC 중심
- 단순하고 직관적인 구조
- 필터링/정렬 완벽 지원
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows

DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/monitoring.db"
OUTPUT_DIR = "/Users/brich/Desktop/iherb_price/coupang2/outputs"


def get_comparison_data():
    """
    Sheet 1: 가격 비교 시트 (최신 스냅샷 기준)
    
    구조:
    - 각 UPC당 최대 2행 (아이허브 1행 + 로켓직구 1행)
    - 간단명료한 컬럼
    """
    
    conn = sqlite3.connect(DB_PATH)
    
    print("\n📊 Sheet 1: 가격 비교 데이터 생성 중...")
    
    query = """
    WITH latest_snapshots AS (
        SELECT 
            s.source_type,
            c.name as category_name,
            MAX(snap.snapshot_time) as latest_time,
            MAX(snap.id) as latest_snapshot_id
        FROM snapshots snap
        JOIN sources s ON snap.source_id = s.id
        JOIN categories c ON snap.category_id = c.id
        GROUP BY s.source_type, c.name
    ),
    overlap_upc AS (
        SELECT 
            mr.iherb_upc
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources s ON snap.source_id = s.id
        INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE mr.iherb_upc IS NOT NULL
        GROUP BY mr.iherb_upc
        HAVING COUNT(DISTINCT s.source_type) = 2
    )
    SELECT 
        mr.iherb_upc as UPC,
        mr.iherb_part_number as 품번,
        ls.category_name as 카테고리,
        CASE ls.source_type
            WHEN 'rocket_direct' THEN '로켓직구'
            WHEN 'iherb_official' THEN '아이허브'
        END as 소스,
        ps.product_name as 상품명,
        ps.current_price as 가격,
        ps.original_price as 정가,
        ps.discount_rate as 할인율,
        ps.category_rank as 순위,
        ps.review_count as 리뷰수,
        ROUND(ps.rating_score, 1) as 평점,
        ps.vendor_item_id as VendorItemID,
        DATE(ls.latest_time) as 수집일자
    FROM overlap_upc ou
    JOIN matching_reference mr ON ou.iherb_upc = mr.iherb_upc
    JOIN product_states ps ON mr.vendor_item_id = ps.vendor_item_id
    JOIN latest_snapshots ls ON ps.snapshot_id = ls.latest_snapshot_id
    ORDER BY mr.iherb_upc, ls.source_type DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"   ✓ {len(df):,}개 레코드")
    print(f"   ✓ 중복 UPC: {df['UPC'].nunique():,}개")
    
    return df


def get_price_diff_summary():
    """
    Sheet 2: 가격 차이 요약
    
    UPC별로 아이허브-로켓직구 가격 차이 계산
    """
    
    conn = sqlite3.connect(DB_PATH)
    
    print("\n💰 Sheet 2: 가격 차이 요약 생성 중...")
    
    query = """
    WITH latest_snapshots AS (
        SELECT 
            s.source_type,
            c.name as category_name,
            MAX(snap.id) as latest_snapshot_id
        FROM snapshots snap
        JOIN sources s ON snap.source_id = s.id
        JOIN categories c ON snap.category_id = c.id
        GROUP BY s.source_type, c.name
    ),
    overlap_upc AS (
        SELECT 
            mr.iherb_upc
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources s ON snap.source_id = s.id
        INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE mr.iherb_upc IS NOT NULL
        GROUP BY mr.iherb_upc
        HAVING COUNT(DISTINCT s.source_type) = 2
    ),
    price_data AS (
        SELECT 
            mr.iherb_upc,
            mr.iherb_part_number,
            ls.category_name,
            ls.source_type,
            ps.product_name,
            ps.current_price,
            ps.category_rank
        FROM overlap_upc ou
        JOIN matching_reference mr ON ou.iherb_upc = mr.iherb_upc
        JOIN product_states ps ON mr.vendor_item_id = ps.vendor_item_id
        JOIN latest_snapshots ls ON ps.snapshot_id = ls.latest_snapshot_id
    )
    SELECT 
        pd.iherb_upc as UPC,
        pd.iherb_part_number as 품번,
        pd.category_name as 카테고리,
        MAX(CASE WHEN pd.source_type = 'iherb_official' THEN pd.product_name END) as 상품명_아이허브,
        MAX(CASE WHEN pd.source_type = 'rocket_direct' THEN pd.product_name END) as 상품명_로켓직구,
        ROUND(AVG(CASE WHEN pd.source_type = 'iherb_official' THEN pd.current_price END), 0) as 평균가_아이허브,
        ROUND(AVG(CASE WHEN pd.source_type = 'rocket_direct' THEN pd.current_price END), 0) as 평균가_로켓직구,
        ROUND(
            AVG(CASE WHEN pd.source_type = 'iherb_official' THEN pd.current_price END) -
            AVG(CASE WHEN pd.source_type = 'rocket_direct' THEN pd.current_price END),
            0
        ) as 가격차이,
        ROUND(
            100.0 * (
                AVG(CASE WHEN pd.source_type = 'iherb_official' THEN pd.current_price END) -
                AVG(CASE WHEN pd.source_type = 'rocket_direct' THEN pd.current_price END)
            ) / AVG(CASE WHEN pd.source_type = 'rocket_direct' THEN pd.current_price END),
            1
        ) as 차이율,
        CASE
            WHEN AVG(CASE WHEN pd.source_type = 'iherb_official' THEN pd.current_price END) <
                 AVG(CASE WHEN pd.source_type = 'rocket_direct' THEN pd.current_price END)
            THEN '아이허브'
            WHEN AVG(CASE WHEN pd.source_type = 'iherb_official' THEN pd.current_price END) >
                 AVG(CASE WHEN pd.source_type = 'rocket_direct' THEN pd.current_price END)
            THEN '로켓직구'
            ELSE '동일'
        END as 저렴한곳,
        ROUND(AVG(CASE WHEN pd.source_type = 'iherb_official' THEN pd.category_rank END), 1) as 평균순위_아이허브,
        ROUND(AVG(CASE WHEN pd.source_type = 'rocket_direct' THEN pd.category_rank END), 1) as 평균순위_로켓직구
    FROM price_data pd
    GROUP BY pd.iherb_upc, pd.iherb_part_number, pd.category_name
    ORDER BY ABS(
        AVG(CASE WHEN pd.source_type = 'iherb_official' THEN pd.current_price END) -
        AVG(CASE WHEN pd.source_type = 'rocket_direct' THEN pd.current_price END)
    ) DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"   ✓ {len(df):,}개 UPC 분석 완료")
    
    return df


def get_category_summary():
    """Sheet 3: 카테고리별 요약"""
    
    conn = sqlite3.connect(DB_PATH)
    
    print("\n📋 Sheet 3: 카테고리별 요약 생성 중...")
    
    query = """
    WITH latest_snapshots AS (
        SELECT 
            s.source_type,
            c.name as category_name,
            MAX(snap.id) as latest_snapshot_id
        FROM snapshots snap
        JOIN sources s ON snap.source_id = s.id
        JOIN categories c ON snap.category_id = c.id
        GROUP BY s.source_type, c.name
    ),
    overlap_upc AS (
        SELECT 
            mr.iherb_upc,
            ls.category_name,
            ls.source_type,
            ps.current_price
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources s ON snap.source_id = s.id
        JOIN categories c ON snap.category_id = c.id
        INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        JOIN latest_snapshots ls ON ps.snapshot_id = ls.latest_snapshot_id
        WHERE mr.iherb_upc IS NOT NULL
        GROUP BY mr.iherb_upc, ls.category_name, ls.source_type, ps.current_price
        HAVING (
            SELECT COUNT(DISTINCT s2.source_type)
            FROM product_states ps2
            JOIN snapshots snap2 ON ps2.snapshot_id = snap2.id
            JOIN sources s2 ON snap2.source_id = s2.id
            INNER JOIN matching_reference mr2 ON ps2.vendor_item_id = mr2.vendor_item_id
            WHERE mr2.iherb_upc = mr.iherb_upc
        ) = 2
    )
    SELECT 
        category_name as 카테고리,
        COUNT(DISTINCT iherb_upc) as 중복UPC개수,
        ROUND(AVG(CASE WHEN source_type = 'iherb_official' THEN current_price END), 0) as 평균가_아이허브,
        ROUND(AVG(CASE WHEN source_type = 'rocket_direct' THEN current_price END), 0) as 평균가_로켓직구,
        ROUND(
            AVG(CASE WHEN source_type = 'iherb_official' THEN current_price END) -
            AVG(CASE WHEN source_type = 'rocket_direct' THEN current_price END),
            0
        ) as 평균가격차이
    FROM overlap_upc
    GROUP BY category_name
    ORDER BY 중복UPC개수 DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"   ✓ {len(df)}개 카테고리 분석 완료")
    
    return df


def save_to_excel(df_comparison, df_diff, df_category, output_path):
    """3개 시트로 Excel 저장"""
    
    print(f"\n💾 Excel 저장 중...")
    
    wb = Workbook()
    wb.remove(wb.active)
    
    # 스타일 정의
    header_font = Font(bold=True, size=11, color='FFFFFF')
    header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
    
    # ===== Sheet 1: 가격 비교 =====
    ws1 = wb.create_sheet("가격 비교")
    
    for r_idx, row in enumerate(dataframe_to_rows(df_comparison, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws1.cell(row=r_idx, column=c_idx, value=value)
            
            if r_idx == 1:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal='center', vertical='center')
            
            # 숫자 포맷
            if r_idx > 1 and isinstance(value, (int, float)) and pd.notna(value):
                if c_idx in [6, 7]:  # 가격, 정가
                    cell.number_format = '#,##0'
                elif c_idx == 8:  # 할인율
                    cell.number_format = '0'
    
    # 컬럼 너비
    widths = {'A': 15, 'B': 15, 'C': 12, 'D': 10, 'E': 50, 'F': 12, 'G': 12, 'H': 8, 'I': 8, 'J': 8, 'K': 8, 'L': 18}
    for col, width in widths.items():
        ws1.column_dimensions[col].width = width
    
    ws1.auto_filter.ref = ws1.dimensions
    ws1.freeze_panes = 'E2'
    
    # ===== Sheet 2: 가격 차이 요약 =====
    ws2 = wb.create_sheet("가격 차이 요약")
    
    for r_idx, row in enumerate(dataframe_to_rows(df_diff, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws2.cell(row=r_idx, column=c_idx, value=value)
            
            if r_idx == 1:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal='center', vertical='center')
            
            # 숫자 포맷
            if r_idx > 1 and isinstance(value, (int, float)) and pd.notna(value):
                if c_idx in [6, 7, 8]:  # 가격 관련
                    cell.number_format = '#,##0'
                    
                    # 가격차이 색상
                    if c_idx == 8:  # 가격차이
                        if value < 0:
                            cell.font = Font(color='0000FF')  # 아이허브 저렴 (파란색)
                        elif value > 0:
                            cell.font = Font(color='FF0000')  # 로켓직구 저렴 (빨간색)
                
                elif c_idx == 9:  # 차이율
                    cell.number_format = '0.0"%"'
    
    # 컬럼 너비
    ws2.column_dimensions['A'].width = 15
    ws2.column_dimensions['B'].width = 15
    ws2.column_dimensions['C'].width = 12
    ws2.column_dimensions['D'].width = 40
    ws2.column_dimensions['E'].width = 40
    
    ws2.auto_filter.ref = ws2.dimensions
    ws2.freeze_panes = 'D2'
    
    # ===== Sheet 3: 카테고리 요약 =====
    ws3 = wb.create_sheet("카테고리 요약")
    
    for r_idx, row in enumerate(dataframe_to_rows(df_category, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws3.cell(row=r_idx, column=c_idx, value=value)
            
            if r_idx == 1:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal='center', vertical='center')
            
            if r_idx > 1 and isinstance(value, (int, float)) and pd.notna(value):
                cell.number_format = '#,##0'
                
                if c_idx == 5:  # 평균가격차이
                    if value < 0:
                        cell.font = Font(color='0000FF')
                    elif value > 0:
                        cell.font = Font(color='FF0000')
    
    ws3.column_dimensions['A'].width = 15
    
    wb.save(output_path)
    print(f"✅ 저장 완료: {output_path}")


def main():
    """메인 실행"""
    
    print("\n" + "="*80)
    print("🎯 실용적인 Price Agent Excel 생성")
    print("="*80)
    print("\n구조:")
    print("  📊 Sheet 1: 가격 비교 (379개 중복 UPC, 소스별 2행)")
    print("  💰 Sheet 2: 가격 차이 요약 (UPC별 집계)")
    print("  📋 Sheet 3: 카테고리별 요약")
    print("="*80)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 데이터 생성
    df_comparison = get_comparison_data()
    df_diff = get_price_diff_summary()
    df_category = get_category_summary()
    
    # Excel 저장
    output_path = os.path.join(OUTPUT_DIR, "price_agent_simple.xlsx")
    save_to_excel(df_comparison, df_diff, df_category, output_path)
    
    # 통계 출력
    print("\n" + "="*80)
    print("📊 데이터 통계")
    print("="*80)
    print(f"\n✅ Sheet 1 (가격 비교)")
    print(f"   - 총 레코드: {len(df_comparison):,}개")
    print(f"   - 중복 UPC: {df_comparison['UPC'].nunique():,}개")
    print(f"   - 소스별 평균 상품 수: {len(df_comparison) / df_comparison['UPC'].nunique():.1f}개/UPC")
    
    print(f"\n✅ Sheet 2 (가격 차이)")
    print(f"   - 분석 UPC: {len(df_diff):,}개")
    
    cheaper_iherb = len(df_diff[df_diff['저렴한곳'] == '아이허브'])
    cheaper_rocket = len(df_diff[df_diff['저렴한곳'] == '로켓직구'])
    same_price = len(df_diff[df_diff['저렴한곳'] == '동일'])
    
    print(f"   - 아이허브가 저렴: {cheaper_iherb}개 ({cheaper_iherb/len(df_diff)*100:.1f}%)")
    print(f"   - 로켓직구가 저렴: {cheaper_rocket}개 ({cheaper_rocket/len(df_diff)*100:.1f}%)")
    print(f"   - 동일 가격: {same_price}개 ({same_price/len(df_diff)*100:.1f}%)")
    
    print(f"\n✅ Sheet 3 (카테고리 요약)")
    print(f"   - 카테고리 수: {len(df_category)}개")
    
    print("\n" + "="*80)
    print("✅ 완료!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()