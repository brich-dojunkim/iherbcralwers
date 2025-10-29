#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
날짜 중심 구조 (소스 컬럼 포함) - vendor_item_id 수정 버전
- coupang_product_id → vendor_item_id로 수정
- matching_reference와 조인 시 vendor_item_id 사용
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

# ⚠️ 실제 경로로 수정 필요
DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/monitoring.db"
OUTPUT_DIR = "/Users/brich/Desktop/iherb_price/coupang2/outputs"


def generate_date_centered_with_source():
    """날짜 중심 구조 생성 (소스 컬럼 포함)"""
    
    conn = sqlite3.connect(DB_PATH)
    
    print("\n" + "="*80)
    print("📊 날짜 중심 구조 (소스 컬럼 포함)")
    print("="*80)
    
    # ✅ 수정된 쿼리: coupang_product_id → vendor_item_id
    query = """
    WITH daily_latest_snapshots AS (
        SELECT 
            c.id as category_id,
            c.name as category_name,
            s.source_type,
            DATE(snap.snapshot_time) as snapshot_date,
            MAX(snap.id) as latest_snapshot_id
        FROM snapshots snap
        JOIN categories c ON c.id = snap.category_id
        JOIN sources s ON s.id = snap.source_id
        GROUP BY c.id, c.name, s.source_type, DATE(snap.snapshot_time)
    )
    SELECT 
        dls.category_name,
        dls.source_type,
        dls.snapshot_date,
        prod.vendor_item_id,
        prod.product_name,
        prod.product_url,
        prod.category_rank,
        prod.current_price,
        prod.discount_rate,
        prod.review_count,
        prod.rating_score,
        mr.iherb_upc,
        mr.iherb_part_number
    FROM daily_latest_snapshots dls
    JOIN product_states prod ON dls.latest_snapshot_id = prod.snapshot_id
    INNER JOIN matching_reference mr ON prod.vendor_item_id = mr.vendor_item_id
    WHERE mr.iherb_upc IS NOT NULL OR mr.iherb_part_number IS NOT NULL
    ORDER BY 
        COALESCE(mr.iherb_upc, mr.iherb_part_number),
        dls.category_name,
        dls.source_type,
        prod.vendor_item_id,
        dls.snapshot_date
    """
    
    df_long = pd.read_sql_query(query, conn)
    conn.close()
    
    if df_long.empty:
        print("❌ 데이터가 없습니다.")
        return [], pd.DataFrame()
    
    # 소스 타입 라벨 매핑
    source_label_map = {
        'rocket_direct': '로켓직구',
        'iherb_official': '아이허브'
    }
    df_long['source_label'] = df_long['source_type'].map(source_label_map).fillna(df_long['source_type'])
    
    dates = sorted(df_long['snapshot_date'].unique())
    print(f"\n✅ 데이터 로드: {len(df_long):,}개 레코드, {len(dates)}일")
    
    # ✅ 수정: vendor_item_id 사용
    base_info = df_long.groupby([
        'iherb_upc',
        'category_name', 
        'source_label', 
        'vendor_item_id'
    ]).agg({
        'product_name': 'first',
        'product_url': 'first',
        'iherb_part_number': 'first',
        'review_count': 'last',
        'rating_score': 'last'
    }).reset_index()
    
    # UPC별 대표 상품 선정
    representative_parts = (
        base_info.groupby('iherb_upc')['iherb_part_number']
        .agg(lambda x: x.mode()[0] if len(x.mode()) > 0 else x.iloc[0])
        .to_dict()
    )
    
    def get_representative_priority(row):
        category_order = {'헬스/건강식품': 1, '출산유아동': 2, '스포츠레저': 3}
        
        source_priority = 0 if row['source_label'] == '아이허브' else 1
        part_match = 0 if row['iherb_part_number'] == representative_parts.get(row['iherb_upc']) else 1
        category_priority = category_order.get(row['category_name'], 99)
        
        return source_priority * 1000 + part_match * 100 + category_priority
    
    base_info['_priority'] = base_info.apply(get_representative_priority, axis=1)
    
    base_info['_is_representative'] = False
    for upc in base_info['iherb_upc'].unique():
        upc_group = base_info[base_info['iherb_upc'] == upc]
        if len(upc_group) > 0:
            rep_idx = upc_group['_priority'].idxmin()
            base_info.loc[rep_idx, '_is_representative'] = True
    
    category_order_map = {'헬스/건강식품': 1, '출산유아동': 2, '스포츠레저': 3}
    base_info['_category_order'] = base_info['category_name'].map(category_order_map).fillna(99)
    
    base_info = base_info.sort_values([
        'iherb_upc',
        '_is_representative',
        '_category_order',
        'source_label',
        'vendor_item_id'
    ], ascending=[True, False, True, True, True]).reset_index(drop=True)
    
    wide_df = base_info.drop(columns=['_priority', '_category_order']).copy()
    
    # 날짜별 데이터 추가
    print(f"🔄 데이터 변환 중...")
    for i, date in enumerate(dates):
        date_str = date[5:].replace('-', '/')
        
        # ✅ vendor_item_id 사용
        date_data = df_long[df_long['snapshot_date'] == date][
            ['iherb_upc', 'category_name', 'source_label', 'vendor_item_id', 
             'category_rank', 'current_price', 'discount_rate']
        ].copy()
        
        date_data.columns = [
            'iherb_upc', 'category_name', 'source_label', 'vendor_item_id',
            f'{date_str}_순위', f'{date_str}_가격', f'{date_str}_할인율'
        ]
        
        wide_df = wide_df.merge(
            date_data, 
            on=['iherb_upc', 'category_name', 'source_label', 'vendor_item_id'], 
            how='left'
        )
        
        if i > 0:
            prev = dates[i-1][5:].replace('-', '/')
            curr = date_str
            
            wide_df[f'{curr}_순위변화'] = (
                wide_df[f'{prev}_순위'] - wide_df[f'{curr}_순위']
            ).round(0)
            
            wide_df[f'{curr}_가격변화'] = (
                wide_df[f'{curr}_가격'] - wide_df[f'{prev}_가격']
            ).round(0)
            
            wide_df[f'{curr}_할인율변화'] = (
                wide_df[f'{curr}_할인율'] - wide_df[f'{prev}_할인율']
            ).round(1)
    
    # 컬럼 재정렬
    base_cols = [
        'category_name', 'source_label',
        'product_name', 'iherb_upc', 'iherb_part_number', 
        'vendor_item_id', 'product_url', 'review_count', 'rating_score',
        '_is_representative'
    ]
    
    ordered_cols = base_cols.copy()
    
    for i, date in enumerate(dates):
        d = date[5:].replace('-', '/')
        ordered_cols.extend([f'{d}_순위', f'{d}_가격', f'{d}_할인율'])
        
        if i > 0:
            ordered_cols.extend([f'{d}_순위변화', f'{d}_가격변화', f'{d}_할인율변화'])
    
    ordered_cols = [c for c in ordered_cols if c in wide_df.columns]
    wide_df = wide_df[ordered_cols]
    
    # 멀티레벨 헤더
    base_names = {
        'category_name': '카테고리',
        'source_label': '소스',
        'product_name': '상품명',
        'iherb_upc': 'UPC',
        'iherb_part_number': '품번',
        'vendor_item_id': 'VendorItemID',
        'product_url': '링크',
        'review_count': '리뷰수',
        'rating_score': '평점',
        '_is_representative': '_is_representative'
    }
    
    multi_columns = []
    
    for col in base_cols:
        multi_columns.append(('기본정보', base_names[col]))
    
    for col in ordered_cols[len(base_cols):]:
        parts = col.rsplit('_', 1)
        if len(parts) == 2:
            date_part, metric = parts
            multi_columns.append((date_part, metric))
        else:
            multi_columns.append(('기타', col))
    
    wide_df.columns = pd.MultiIndex.from_tuples(multi_columns)
    
    print(f"✅ 변환 완료: {len(wide_df):,}개 상품, {len(wide_df.columns)}개 컬럼")
    
    return dates, wide_df


def save_to_excel_with_hyperlinks(dates, wide_df, output_path):
    """하이퍼링크와 UPC 그룹화가 포함된 Excel 저장"""
    
    print(f"\n💾 Excel 저장 중...")
    
    wb = Workbook()
    ws = wb.active
    ws.title = '데이터'
    
    header_font = Font(bold=True, size=11)
    subheader_font = Font(bold=True, size=10)
    header_fill = PatternFill(start_color='F0F0F0', end_color='F0F0F0', fill_type='solid')
    link_font = Font(color='0000FF', underline='single')
    
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
    
    upc_groups = {}
    current_upc = None
    group_start = 3
    representative_products = []
    
    upc_col_idx = None
    is_rep_col_idx = None
    category_col_idx = None
    
    for i, (level0, level1) in enumerate(wide_df.columns):
        if level1 == 'UPC':
            upc_col_idx = i
        elif level1 == '_is_representative':
            is_rep_col_idx = i
        elif level1 == '카테고리':
            category_col_idx = i
    
    # 3행부터: 데이터
    for row_idx, row_data in enumerate(wide_df.values, start=3):
        upc_value = row_data[upc_col_idx] if upc_col_idx is not None else None
        is_representative = row_data[is_rep_col_idx] if is_rep_col_idx is not None else False
        category_value = row_data[category_col_idx] if category_col_idx is not None else ''
        
        if current_upc != upc_value:
            if current_upc is not None and group_start < row_idx:
                upc_groups[current_upc] = (group_start, row_idx - 1)
            
            current_upc = upc_value
            group_start = row_idx
        
        if is_representative:
            representative_products.append({
                'row': row_idx,
                'upc': upc_value,
                'category': category_value,
                'data': row_data
            })
        
        for col_idx, value in enumerate(row_data, start=1):
            if wide_df.columns[col_idx - 1][1] == '_is_representative':
                continue
            
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            
            col_header = wide_df.columns[col_idx - 1]
            col_name = col_header[1]
            col_group = col_header[0]
            
            if col_name == '링크' and pd.notna(value) and str(value).startswith('http'):
                cell.value = 'LINK'
                cell.hyperlink = value
                cell.font = link_font
                cell.alignment = Alignment(horizontal='center', vertical='center')
            
            elif pd.notna(value) and isinstance(value, (int, float)):
                if '변화' in col_name:
                    if value < 0:
                        cell.font = Font(color='0000FF')
                        cell.number_format = '#,##0'
                    elif value > 0:
                        cell.font = Font(color='FF0000')
                        cell.number_format = '+#,##0'
                    else:
                        cell.number_format = '#,##0'
                else:
                    if '할인율' in col_name:
                        cell.number_format = '#,##0'
                    else:
                        cell.number_format = '#,##0'
    
    if current_upc is not None and group_start <= len(wide_df) + 2:
        upc_groups[current_upc] = (group_start, len(wide_df) + 2)
    
    if is_rep_col_idx is not None:
        ws.column_dimensions[get_column_letter(is_rep_col_idx + 1)].hidden = True
    
    print(f"🔗 UPC 그룹화 적용 중... (총 {len(upc_groups)}개 그룹)")
    for upc, (start_row, end_row) in upc_groups.items():
        if end_row > start_row:
            if end_row > start_row:
                ws.row_dimensions.group(start_row + 1, end_row, outline_level=1, hidden=True)
    
    merge_start = 1
    prev_header = None
    
    for i, (level0, level1) in enumerate(wide_df.columns, start=1):
        if level1 == '_is_representative':
            continue
        if prev_header is not None and prev_header != level0:
            if merge_start < i - 1:
                ws.merge_cells(start_row=1, start_column=merge_start, end_row=1, end_column=i - 1)
            merge_start = i
        prev_header = level0
    
    if merge_start < len(wide_df.columns):
        ws.merge_cells(start_row=1, start_column=merge_start, end_row=1, end_column=len(wide_df.columns))
    
    prev_header = None
    for col_idx, (level0, level1) in enumerate(wide_df.columns, start=1):
        if level1 == '_is_representative':
            continue
        if prev_header is not None and prev_header != level0:
            for row_idx in range(1, len(wide_df) + 3):
                cell = ws.cell(row=row_idx, column=col_idx - 1)
                cell.border = Border(right=Side(style='medium', color='CCCCCC'))
        prev_header = level0
    
    for col_idx, (level0, level1) in enumerate(wide_df.columns, start=1):
        if level1 == '_is_representative':
            continue
        if level1 == '링크':
            ws.column_dimensions[get_column_letter(col_idx)].width = 8
        elif level1 in ['카테고리', '소스']:
            ws.column_dimensions[get_column_letter(col_idx)].width = 12
        elif level1 == '상품명':
            ws.column_dimensions[get_column_letter(col_idx)].width = 40
        elif level1 in ['UPC', '품번', 'VendorItemID']:
            ws.column_dimensions[get_column_letter(col_idx)].width = 15
        else:
            ws.column_dimensions[get_column_letter(col_idx)].width = 12
    
    ws.row_dimensions[1].height = 25
    ws.row_dimensions[2].height = 20
    ws.freeze_panes = 'K3'
    ws.sheet_properties.outlinePr.summaryBelow = False
    
    wb.save(output_path)
    print(f"✅ 저장 완료: {output_path}")
    print(f"   💡 Excel에서 좌측 [+] 버튼으로 UPC 그룹을 펼칠 수 있습니다")
    print(f"   💡 초기 상태: 모든 그룹 접힘 (대표 상품만 표시)")
    
    return representative_products


def main():
    """메인 실행"""
    
    print("\n" + "="*80)
    print("🎯 날짜 중심 구조 (소스 컬럼 포함) - vendor_item_id 버전")
    print("="*80)
    print(f"DB 경로: {DB_PATH}")
    print(f"출력 경로: {OUTPUT_DIR}")
    print("="*80)
    
    # 출력 디렉토리 생성
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    dates, wide_df = generate_date_centered_with_source()
    
    if wide_df.empty:
        print("\n❌ 생성할 데이터가 없습니다.")
        return
    
    output_path = os.path.join(OUTPUT_DIR, "date_centered_with_source.xlsx")
    representative_products = save_to_excel_with_hyperlinks(dates, wide_df, output_path)
    
    print(f"\n" + "="*80)
    print(f"📋 대표 상품 리스트 (카테고리별)")
    print(f"="*80)
    
    category_order = ['헬스/건강식품', '출산유아동', '스포츠레저']
    
    for category in category_order:
        category_reps = [p for p in representative_products if p['category'] == category]
        if category_reps:
            print(f"\n[{category}] - {len(category_reps)}개")
            for i, prod in enumerate(category_reps[:10], 1):
                upc = prod['upc']
                print(f"  {i}. UPC: {upc}")
            
            if len(category_reps) > 10:
                print(f"  ... 외 {len(category_reps) - 10}개")
    
    print(f"\n총 대표 상품 수: {len(representative_products)}개")
    print(f"="*80)
    
    csv_path = os.path.join(OUTPUT_DIR, "date_centered_with_source.csv")
    flat_df = wide_df.copy()
    flat_df.columns = [f'{level0}_{level1}' if level0 != '기본정보' else level1 
                       for level0, level1 in flat_df.columns]
    flat_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"💾 CSV 저장: {csv_path}")
    
    print("\n" + "="*80)
    print("✅ 완료!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()