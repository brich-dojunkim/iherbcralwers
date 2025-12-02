#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Product Dashboard (Metrics 기반)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Schema 기반 자동화 Excel 대시보드 생성 - 최신 2개 스냅샷 비교
"""

import pandas as pd
import numpy as np
import re
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_manager import DataManager
from src.metrics.core import MetricsManager
from src.metrics.schema import create_panel_column_specs


def get_data(db_path: str):
    """데이터 로드"""
    
    manager = DataManager(db_path)
    metrics = MetricsManager(manager)
    
    # 최신 2개 스냅샷 패널 생성
    df = metrics.get_view(
        metric_groups=['core', 'action', 'performance_snapshot', 'performance_rolling_7d', 'meta'],
        n_latest=2,
        include_unmatched=True,
        compute_deltas=True,
        delta_metrics=['iherb_sales_quantity', 'iherb_item_winner_ratio'],
        as_pct=False
    )
    
    if df.empty:
        return pd.DataFrame(), None, None
    
    # 날짜 추출
    date_pattern = r'__(\d{8})$'
    found_dates = set()
    
    for col in df.columns:
        match = re.search(date_pattern, str(col))
        if match:
            found_dates.add(match.group(1))
    
    if len(found_dates) < 2:
        print(f"⚠️ 날짜를 2개 찾지 못함: {found_dates}")
        return df, None, None
    
    # 날짜 정렬 및 변환
    sorted_dates = sorted(found_dates, reverse=True)
    curr_date = f"{sorted_dates[0][:4]}-{sorted_dates[0][4:6]}-{sorted_dates[0][6:8]}"
    prev_date = f"{sorted_dates[1][:4]}-{sorted_dates[1][4:6]}-{sorted_dates[1][6:8]}"
    
    print(f"\n✅ 데이터 로드 완료:")
    print(f"   현재: {curr_date} ({len(df):,}행)")
    print(f"   이전: {prev_date}")
    
    return df, curr_date, prev_date


def transform_to_excel_data(df: pd.DataFrame, curr_date: str, prev_date: str):
    """메트릭 패널 → Excel 출력용 변환"""
    
    col_specs = create_panel_column_specs(curr_date, prev_date)
    
    curr_d = datetime.strptime(curr_date, "%Y-%m-%d").date()
    prev_d = datetime.strptime(prev_date, "%Y-%m-%d").date()
    curr_s = curr_d.strftime('%Y%m%d')
    prev_s = prev_d.strftime('%Y%m%d')
    
    # 할인율 계산
    def calc_discount(orig_key, sale_key):
        if orig_key in df.columns and sale_key in df.columns:
            orig = pd.to_numeric(df[orig_key], errors='coerce')
            sale = pd.to_numeric(df[sale_key], errors='coerce')
            result = pd.Series(np.nan, index=df.index)
            mask = (orig > 0) & (sale > 0)
            result[mask] = ((orig[mask] - sale[mask]) / orig[mask] * 100).round(1)
            return result
        return pd.Series([pd.NA] * len(df))
    
    # 동적 계산 컬럼 미리 생성
    discount_prev = calc_discount(f'iherb_original_price__{prev_s}', f'iherb_price__{prev_s}')
    discount_curr = calc_discount(f'iherb_original_price__{curr_s}', f'iherb_price__{curr_s}')
    discount_delta = (discount_curr - discount_prev).round(1)
    
    sales_col = f'iherb_sales_quantity__{curr_s}'
    rank_iherb = df[sales_col].rank(method='min', ascending=False).astype('Int64') if sales_col in df.columns else pd.Series([pd.NA] * len(df))
    
    # 출력 DataFrame 구성
    output_data = {}
    matched_count = 0
    
    for spec in col_specs:
        metric_key = spec.metric_key
        
        # 동적 계산 컬럼 처리
        if metric_key == "할인율_prev":
            output_data[spec.excel_label] = discount_prev
        elif metric_key == "할인율_curr":
            output_data[spec.excel_label] = discount_curr
        elif metric_key == "할인율Δ":
            output_data[spec.excel_label] = discount_delta
        elif metric_key == "순위_아이허브":
            output_data[spec.excel_label] = rank_iherb
        elif metric_key in df.columns:
            output_data[spec.excel_label] = df[metric_key]
        else:
            output_data[spec.excel_label] = pd.Series([pd.NA] * len(df))
            continue
        
        matched_count += 1
    
    output_df = pd.DataFrame(output_data)
    
    print(f"\n📊 Excel 변환 완료:")
    print(f"   ✅ {len(output_df):,}행 × {len(output_df.columns)}열")
    print(f"   ✅ 매칭 성공: {matched_count}/{len(col_specs)}개")
    
    return output_df, col_specs


def create_excel(df: pd.DataFrame, output: str, curr_date: str, prev_date: str):
    """Excel 파일 생성"""
    
    if df.empty:
        print("❌ 데이터가 비어있습니다.")
        return
    
    output_df, col_specs = transform_to_excel_data(df, curr_date, prev_date)
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        output_df.to_excel(writer, sheet_name='Price_Agent', index=False, header=False)
    
    apply_excel_styles(output, col_specs)


def apply_excel_styles(path: str, col_specs: list):
    """Excel 스타일 적용"""
    
    wb = load_workbook(path)
    ws = wb["Price_Agent"]
    ws.insert_rows(1, 2)
    
    BG = "F8FAFC"
    BORDER = "E2E8F0"
    GREEN = "C6EFCE"
    RED = "FFC7CE"
    
    # 1행: 그룹 헤더 스타일 먼저 적용
    for i, spec in enumerate(col_specs, 1):
        c = ws.cell(1, i)
        c.fill = PatternFill(start_color=BG, end_color=BG, fill_type="solid")
        c.font = Font(color="1E293B", bold=True, size=10)
        c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        c.border = Border(
            left=Side(style='thin', color=BORDER),
            right=Side(style='thin', color=BORDER),
            top=Side(style='thin', color=BORDER),
            bottom=Side(style='thin', color=BORDER)
        )
    
    # 그룹 병합
    groups = {}
    for i, spec in enumerate(col_specs, 1):
        grp = spec.group
        if grp not in groups:
            groups[grp] = [i, i]
        else:
            groups[grp][1] = i
    
    for grp, (start, end) in groups.items():
        if start < end:
            ws.merge_cells(start_row=1, start_column=start, end_row=1, end_column=end)
        
        # 병합 후 첫 셀에만 값 설정
        first_cell = ws.cell(1, start)
        first_cell.value = grp
        
        # 병합 그룹의 양 끝 테두리 강조
        for i in range(start, end + 1):
            c = ws.cell(1, i)
            if i == start:
                c.border = Border(
                    left=Side(style='medium', color=BORDER),
                    right=Side(style='thin', color=BORDER),
                    top=Side(style='thin', color=BORDER),
                    bottom=Side(style='thin', color=BORDER)
                )
            elif i == end:
                c.border = Border(
                    left=Side(style='thin', color=BORDER),
                    right=Side(style='medium', color=BORDER),
                    top=Side(style='thin', color=BORDER),
                    bottom=Side(style='thin', color=BORDER)
                )
    
    # 2행: 하위 헤더
    for i, spec in enumerate(col_specs, 1):
        c = ws.cell(2, i)
        c.value = spec.excel_label
        c.fill = PatternFill(start_color=BG, end_color=BG, fill_type="solid")
        c.font = Font(color="1E293B", bold=True, size=10)
        c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        c.border = Border(
            left=Side(style='thin', color=BORDER),
            right=Side(style='thin', color=BORDER),
            top=Side(style='thin', color=BORDER),
            bottom=Side(style='thin', color=BORDER)
        )
    
    # 컬럼 너비
    for i, spec in enumerate(col_specs, 1):
        ws.column_dimensions[get_column_letter(i)].width = spec.width
    
    # 데이터 영역
    for row in range(3, ws.max_row + 1):
        for i, spec in enumerate(col_specs, 1):
            c = ws.cell(row, i)
            c.border = Border(
                left=Side(style='thin', color=BORDER),
                right=Side(style='thin', color=BORDER),
                top=Side(style='thin', color=BORDER),
                bottom=Side(style='thin', color=BORDER)
            )
            
            # 정렬
            if spec.group in ['가격상태', '할인전략', '판매/위너', '변화']:
                c.alignment = Alignment(horizontal='right', vertical='center', wrap_text=False)
            else:
                c.alignment = Alignment(vertical='center', wrap_text=False)
            
            # 숫자 서식
            if spec.number_format:
                c.number_format = spec.number_format
            
            # 조건부 서식 (c.value로 수정!)
            if c.value not in (None, ''):
                try:
                    if spec.is_delta or 'Δ' in spec.excel_label:
                        v = float(c.value)
                        if v > 0:
                            c.fill = PatternFill(start_color=GREEN, end_color=GREEN, fill_type="solid")
                        elif v < 0:
                            c.fill = PatternFill(start_color=RED, end_color=RED, fill_type="solid")
                    elif spec.excel_label == '유리한곳':
                        if c.value == '아이허브':
                            c.fill = PatternFill(start_color=GREEN, end_color=GREEN, fill_type="solid")
                        elif c.value == '로켓직구':
                            c.fill = PatternFill(start_color=RED, end_color=RED, fill_type="solid")
                    elif '위너비율' in spec.excel_label:
                        v = float(c.value)
                        if abs(v - 100.0) < 0.0001:
                            c.fill = PatternFill(start_color=GREEN, end_color=GREEN, fill_type="solid")
                except:
                    pass
            
            # 링크
            if spec.is_link and c.value and str(c.value).startswith('http'):
                c.hyperlink = str(c.value)
                c.value = "Link"
                c.font = Font(color="0563C1", underline="single")
                c.alignment = Alignment(horizontal='center', vertical='center')
    
    ws.freeze_panes = 'D3'
    ws.auto_filter.ref = f"A2:{get_column_letter(len(col_specs))}{ws.max_row}"
    
    wb.save(path)
    print(f"\n✅ Excel 파일 저장 완료")


def main():
    from config.settings import Config
    
    df, curr_date, prev_date = get_data(Config.INTEGRATED_DB_PATH)
    
    if not df.empty and curr_date and prev_date:
        Config.OUTPUT_DIR.mkdir(exist_ok=True)
        output = Config.OUTPUT_DIR / f"price_agent_{datetime.now().strftime('%Y%m%d')}.xlsx"
        create_excel(df, str(output), curr_date, prev_date)
        print(f"\n✅ 완료: {output}")
    else:
        print("\n❌ 데이터 없음 또는 스냅샷 부족")


if __name__ == "__main__":
    main()