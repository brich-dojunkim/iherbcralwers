#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
로켓직구 vs 아이허브 가격 비교 Excel 리포트 생성
"""

import pandas as pd
import numpy as np
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime
import os
import sys
import re
from pathlib import Path

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 프로젝트 모듈
from config.settings import Config
from src.data_manager import DataManager


def get_available_dates():
    """사용 가능한 날짜 목록 조회"""
    import sqlite3
    conn = sqlite3.connect(str(Config.DB_PATH))
    query = """
    SELECT DISTINCT DATE(snapshot_time) as date
    FROM snapshots
    ORDER BY date DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['date'].tolist()


def extract_price_comparison_data(target_date=None):
    """가격 비교 데이터 추출"""
    
    print(f"\n📅 처리 날짜: {target_date or '최신'}")
    
    # data_manager를 통해 통합 데이터 로드
    manager = DataManager(
        db_path=str(Config.DB_PATH),
        rocket_csv_path=str(Config.MATCHING_CSV_PATH),
        excel_dir=str(Config.IHERB_EXCEL_DIR)
    )
    df = manager.get_integrated_df(target_date=target_date)
    
    if df.empty:
        print("⚠️ 데이터가 없습니다.")
        return df
    
    print(f"✅ 총 {len(df):,}개 로켓직구 상품")
    print(f"   - 매칭된 아이허브 상품: {df['iherb_vendor_id'].notna().sum():,}개")
    
    return df


def create_excel_report(date_data_dict, output_path):
    """엑셀 리포트 생성"""
    if not date_data_dict:
        print("❌ 데이터가 없어 엑셀 생성을 건너뜁니다.")
        return

    columns_order = [
        ('rocket_category', '카테고리'),
        ('rocket_rank', '로켓_순위'),
        ('upc', 'UPC'),
        ('part_number', '품번'),
        ('rocket_vendor_id', '로켓_상품ID'),
        ('rocket_product_name', '로켓_제품명'),
        ('rocket_price', '로켓_가격'),
        ('rocket_original_price', '로켓_원가'),
        ('rocket_discount_rate', '로켓_할인율(%)'),
        ('rocket_rating', '로켓_평점'),
        ('rocket_reviews', '로켓_리뷰수'),
        ('rocket_url', '로켓_링크'),
        ('iherb_vendor_id', '아이허브_상품ID'),
        ('iherb_product_name', '아이허브_제품명'),
        ('iherb_price', '아이허브_가격'),
        ('iherb_stock', '아이허브_재고'),
        ('iherb_stock_status', '재고상태'),
        ('iherb_part_number', '아이허브_품번'),
        ('iherb_category', '아이허브_카테고리'),
        ('iherb_revenue', '매출(원)'),
        ('iherb_orders', '주문수'),
        ('iherb_sales_quantity', '판매량'),
        ('iherb_visitors', '방문자수'),
        ('iherb_views', '조회수'),
        ('iherb_cart_adds', '장바구니'),
        ('iherb_conversion_rate', '구매전환율(%)'),
        ('iherb_cancel_rate', '취소율(%)'),
        ('price_diff', '가격차이(원)'),
        ('price_diff_pct', '가격차이율(%)'),
        ('cheaper_source', '더_저렴한_곳'),
    ]

    super_headers = [
        ("기본 정보", 1, 4),
        ("로켓직구", 5, 12),
        ("아이허브 기본", 13, 18),
        ("아이허브 판매성과", 19, 27),
        ("가격 비교", 28, 30),
    ]
    group_boundaries = [4, 12, 18, 27]

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for date_str in sorted(date_data_dict.keys(), reverse=True):
            df = date_data_dict[date_str]
            if df.empty:
                continue
            
            export_columns = []
            export_names = []
            for col_key, col_name in columns_order:
                if col_key in df.columns:
                    export_columns.append(col_key)
                    export_names.append(col_name)
            
            df_export = df[export_columns].copy()
            df_export.columns = export_names
            df_export.to_excel(writer, sheet_name=date_str, index=False, startrow=0)

    wb = load_workbook(output_path)

    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=False, shrink_to_fit=True)

    super_fill = PatternFill(start_color="244062", end_color="244062", fill_type="solid")
    super_font = Font(bold=True, color="FFFFFF", size=12)
    super_alignment = Alignment(horizontal='center', vertical='center')

    thin_border = Border(
        left=Side(style='thin', color='DDDDDD'),
        right=Side(style='thin', color='DDDDDD'),
        top=Side(style='thin', color='DDDDDD'),
        bottom=Side(style='thin', color='DDDDDD')
    )

    thick_white_side = Side(style='thick', color='FFFFFF')

    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red_fill   = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]

        ws.insert_rows(1)

        max_col = ws.max_column

        for cell in ws[2]:
            if isinstance(cell.value, str):
                cell.value = cell.value.replace("\n", " ").strip()
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_alignment

        for title, c_start, c_end in super_headers:
            if c_end <= max_col:
                ws.merge_cells(start_row=1, start_column=c_start, end_row=1, end_column=c_end)
                cell = ws.cell(row=1, column=c_start)
                cell.value = title
                cell.fill = super_fill
                cell.font = super_font
                cell.alignment = super_alignment

        for row_idx in range(3, ws.max_row + 1):
            for col_idx in range(1, max_col + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.border = thin_border
                cell.alignment = Alignment(vertical='center', wrap_text=False)

        header_row_values = [cell.value for cell in ws[2]]
        
        def col_idx_of(name):
            try:
                return header_row_values.index(name) + 1
            except ValueError:
                return None

        price_diff_col = col_idx_of('가격차이(원)')
        cheaper_src_col = col_idx_of('더_저렴한_곳')
        
        if price_diff_col and cheaper_src_col:
            for row_idx in range(3, ws.max_row + 1):
                cheaper_value = ws.cell(row=row_idx, column=cheaper_src_col).value
                if cheaper_value == '아이허브':
                    ws.cell(row=row_idx, column=price_diff_col).fill = green_fill
                elif cheaper_value == '로켓직구':
                    ws.cell(row=row_idx, column=price_diff_col).fill = red_fill

        rocket_url_col = col_idx_of('로켓_링크')
        if rocket_url_col:
            for row_idx in range(3, ws.max_row + 1):
                rc = ws.cell(row=row_idx, column=rocket_url_col)
                url_r = rc.value
                if url_r and str(url_r).strip():
                    rc.value = "Link"
                    rc.hyperlink = str(url_r)
                    rc.font = Font(color="0563C1", underline="single")
                    rc.alignment = Alignment(horizontal='center', vertical='center', wrap_text=False)

        last_row = ws.max_row
        for boundary_col in group_boundaries:
            if boundary_col <= max_col:
                for r in range(1, last_row + 1):
                    c = ws.cell(row=r, column=boundary_col)
                    c.border = Border(
                        left=c.border.left,
                        right=thick_white_side,
                        top=c.border.top,
                        bottom=c.border.bottom
                    )

        ws.freeze_panes = 'E3'
        ws.auto_filter.ref = f"A2:{get_column_letter(ws.max_column)}{ws.max_row}"

        def east_asian_factor(s: str) -> float:
            if not s:
                return 1.0
            wide = len(re.findall(r'[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7A3\u3000-\u303F\u3040-\u30FF\u4E00-\u9FFF]', s))
            ratio = wide / max(len(s), 1)
            return 1.0 + 0.25 * ratio

        numeric_like = {'순위','가격','원가','할인','평점','리뷰','차이','율','%','%p','재고','매출','주문','판매','방문','조회','장바구니','전환','취소'}
        max_sample_rows = min(ws.max_row, 500)

        min_widths = {
            '카테고리': 20, '로켓_순위': 16, 'UPC': 20, '품번': 20,
            '로켓_상품ID': 20, '로켓_제품명': 56, '로켓_가격': 18, '로켓_원가': 18,
            '로켓_할인율(%)': 16, '로켓_평점': 16, '로켓_리뷰수': 16, '로켓_링크': 14,
            '아이허브_상품ID': 20, '아이허브_제품명': 56, '아이허브_가격': 18,
            '아이허브_재고': 16, '재고상태': 14, '아이허브_품번': 20,
            '아이허브_카테고리': 24, '매출(원)': 18, '주문수': 14, '판매량': 14,
            '방문자수': 16, '조회수': 14, '장바구니': 14, '구매전환율(%)': 18, '취소율(%)': 14,
            '가격차이(원)': 18, '가격차이율(%)': 16, '더_저렴한_곳': 16,
        }

        for col_idx, col_name in enumerate(header_row_values, start=1):
            col_letter = get_column_letter(col_idx)

            header_base = len(str(col_name))
            header_est  = int((header_base + 3) * east_asian_factor(str(col_name)))

            data_max_len = 0
            for r in range(3, max_sample_rows + 1):
                v = ws.cell(row=r, column=col_idx).value
                if v is None:
                    continue
                s = "Link" if '링크' in col_name else str(v)
                data_max_len = max(data_max_len, int(len(s) * east_asian_factor(s)))

            est = int(max(header_est, data_max_len) * 1.10)

            if col_name in min_widths:
                est = max(est, min_widths[col_name])
            else:
                if '제품명' in col_name:
                    est = max(est, 45)
                    est = min(est, 64)
                elif '링크' in col_name:
                    est = max(est, 10)
                    est = min(est, 14)
                elif any(key in col_name for key in numeric_like):
                    est = max(est, 12)
                    est = min(est, 18)
                else:
                    est = min(max(est, 12), 32)

            ws.column_dimensions[col_letter].width = est

    wb.save(output_path)
    print(f"\n✅ 엑셀 저장 완료: {output_path}")


def generate_summary_stats(date_data_dict):
    """요약 통계"""
    
    if not date_data_dict:
        return
    
    print("\n" + "="*80)
    print("📊 요약 통계")
    print("="*80)
    
    for date_str in sorted(date_data_dict.keys(), reverse=True):
        df = date_data_dict[date_str]
        
        if df.empty:
            continue
        
        print(f"\n📅 {date_str}")
        print(f"   - 총 로켓직구: {len(df):,}개")
        print(f"   - 매칭: {df['iherb_vendor_id'].notna().sum():,}개")
        print(f"   - 매칭률: {df['iherb_vendor_id'].notna().sum() / len(df) * 100:.1f}%")
        
        matched = df[df['iherb_vendor_id'].notna()]
        
        if not matched.empty:
            print(f"\n   💰 가격 경쟁력:")
            for source, count in matched['cheaper_source'].value_counts().items():
                print(f"      • {source}: {count:,}개")


def main():
    """메인 실행"""
    
    print("\n" + "="*80)
    print("🎯 로켓직구 vs 아이허브 가격 비교")
    print("="*80)
    print(f"DB: {Config.DB_PATH}")
    print(f"CSV: {Config.MATCHING_CSV_PATH}")
    print(f"Excel: {Config.IHERB_EXCEL_DIR}")
    print("="*80 + "\n")
    
    # 출력 디렉토리
    Config.ensure_directories()
    
    dates = get_available_dates()
    
    if not dates:
        print("❌ 데이터가 없습니다.")
        return
    
    print(f"📅 사용 가능한 날짜: {len(dates)}개")
    
    date_data_dict = {}
    
    for target_date in dates:
        print(f"\n{'='*80}")
        df = extract_price_comparison_data(target_date=target_date)
        
        if not df.empty:
            date_data_dict[target_date] = df
    
    if not date_data_dict:
        print("\n❌ 처리할 데이터가 없습니다.")
        return
    
    generate_summary_stats(date_data_dict)
    
    today = datetime.now().strftime('%Y%m%d')
    output_path = Config.REPORTS_DIR / f"rocket_vs_iherb_{today}.xlsx"
    create_excel_report(date_data_dict, output_path)
    
    print("\n✅ 완료!")
    print(f"📁 {output_path}")


if __name__ == "__main__":
    main()