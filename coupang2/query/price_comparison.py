#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
로켓직구 vs 아이허브 가격 비교 Excel 리포트 생성
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
데이터 소스:
- 로켓직구: monitoring.db (크롤링 데이터)
- 아이허브: Excel 파일 (가격/재고 + 판매성과)
- 통합: data_manager.py를 통해 UPC 기반 매칭

특징:
- 로켓직구 전체 상품 기준 (카테고리별 순위순 정렬)
- 매칭되는 아이허브 상품이 있으면 해당 정보 표시
- 일자별 시트 구성
- 링크는 "Link" 텍스트에 하이퍼링크로 표현
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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

# 상위 디렉토리를 sys.path에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# data_manager 임포트
from data_manager import DataManager

# ==================== 설정 ====================
DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/data/rocket/monitoring.db"
EXCEL_DIR = "/Users/brich/Desktop/iherb_price/coupang2/data/iherb"
OUTPUT_DIR = "./output"
# =============================================

def get_available_dates():
    """사용 가능한 날짜 목록 조회"""
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT DISTINCT DATE(snapshot_time) as date
    FROM snapshots
    ORDER BY date DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['date'].tolist()


def extract_price_comparison_data(target_date=None):
    """
    로켓직구 전체 상품 기준 가격 비교 데이터 추출
    매칭되는 아이허브 상품이 있으면 해당 정보 추가
    
    Args:
        target_date: 대상 날짜 (None이면 최신 날짜)
    
    Returns:
        DataFrame: 가격 비교 테이블
    """
    
    print(f"\n📅 처리 날짜: {target_date or '최신'}")
    
    # data_manager를 통해 통합 데이터 로드
    manager = DataManager(db_path=DB_PATH, excel_dir=EXCEL_DIR)
    df = manager.get_integrated_df(target_date=target_date)
    
    if df.empty:
        print("⚠️ 데이터가 없습니다.")
        return df
    
    # 컬럼명 확인 (data_manager에서 이미 모든 처리가 완료됨)
    print(f"✅ 총 {len(df):,}개 로켓직구 상품")
    print(f"   - 매칭된 아이허브 상품: {df['iherb_vendor_id'].notna().sum():,}개")
    
    # 카테고리별 통계
    print(f"   - 카테고리별:")
    for cat, count in df['rocket_category'].value_counts().items():
        matched = df[(df['rocket_category'] == cat) & (df['iherb_vendor_id'].notna())].shape[0]
        print(f"      • {cat}: 전체 {count}개 / 매칭 {matched}개")
    
    return df


def create_excel_report(date_data_dict, output_path):
    """
    엑셀 리포트 생성 (일자별 시트)
    
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    컬럼 구조 (총 30개):
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    [기본 정보] (1~4)
    - 카테고리, 로켓_순위, UPC, 품번
    
    [로켓직구] (5~12)
    - 상품ID, 제품명, 가격, 원가, 할인율, 평점, 리뷰수, 링크
    
    [아이허브 기본] (13~18)
    - 상품ID, 제품명, 가격, 재고, 재고상태, 품번
    
    [아이허브 판매성과] (19~27)
    - 카테고리, 매출, 주문수, 판매량, 방문자수, 조회수, 
      장바구니, 구매전환율, 취소율
    
    [가격 비교] (28~30)
    - 가격차이(원), 가격차이율(%), 더_저렴한_곳
    
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    스타일:
    - 상위 헤더(1행): [기본 정보] [로켓직구] [아이허브 기본] [아이허브 판매성과] [가격 비교]
    - 하위 헤더(2행): 한 줄(shrink-to-fit), 데이터(3행~): 줄바꿈 없음
    - 그룹 경계(4, 12, 18, 27열)에 흰색 두꺼운 세로 구분선
    - 오토필터 적용 후, 열 너비 자동 산정(오토핏 근사)
    """
    if not date_data_dict:
        print("❌ 데이터가 없어 엑셀 생성을 건너뜁니다.")
        return

    # 1) 컬럼 정의 (총 30개)
    columns_order = [
        # 기본 정보 (1~4)
        ('rocket_category', '카테고리'),
        ('rocket_rank', '로켓_순위'),
        ('upc', 'UPC'),
        ('part_number', '품번'),

        # 로켓직구 (5~12)
        ('rocket_vendor_id', '로켓_상품ID'),
        ('rocket_product_name', '로켓_제품명'),
        ('rocket_price', '로켓_가격'),
        ('rocket_original_price', '로켓_원가'),
        ('rocket_discount_rate', '로켓_할인율(%)'),
        ('rocket_rating', '로켓_평점'),
        ('rocket_reviews', '로켓_리뷰수'),
        ('rocket_url', '로켓_링크'),

        # 아이허브 기본 (13~18)
        ('iherb_vendor_id', '아이허브_상품ID'),
        ('iherb_product_name', '아이허브_제품명'),
        ('iherb_price', '아이허브_가격'),
        ('iherb_stock', '아이허브_재고'),
        ('iherb_stock_status', '재고상태'),
        ('iherb_part_number', '아이허브_품번'),

        # 아이허브 판매성과 (19~27)
        ('iherb_category', '아이허브_카테고리'),
        ('iherb_revenue', '매출(원)'),
        ('iherb_orders', '주문수'),
        ('iherb_sales_quantity', '판매량'),
        ('iherb_visitors', '방문자수'),
        ('iherb_views', '조회수'),
        ('iherb_cart_adds', '장바구니'),
        ('iherb_conversion_rate', '구매전환율(%)'),
        ('iherb_cancel_rate', '취소율(%)'),

        # 가격 비교 (28~30)
        ('price_diff', '가격차이(원)'),
        ('price_diff_pct', '가격차이율(%)'),
        ('cheaper_source', '더_저렴한_곳'),
    ]

    # 2) 상위 헤더(병합 범위) & 그룹 경계
    super_headers = [
        ("기본 정보", 1, 4),
        ("로켓직구", 5, 12),
        ("아이허브 기본", 13, 18),
        ("아이허브 판매성과", 19, 27),
        ("가격 비교", 28, 30),
    ]
    group_boundaries = [4, 12, 18, 27]   # 각 그룹의 마지막 열

    # 3) 먼저 시트에 데이터 기록
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for date_str in sorted(date_data_dict.keys(), reverse=True):
            df = date_data_dict[date_str]
            if df.empty:
                continue
            
            # 컬럼 순서대로 추출 (존재하는 컬럼만)
            export_columns = []
            export_names = []
            for col_key, col_name in columns_order:
                if col_key in df.columns:
                    export_columns.append(col_key)
                    export_names.append(col_name)
            
            df_export = df[export_columns].copy()
            df_export.columns = export_names
            df_export.to_excel(writer, sheet_name=date_str, index=False, startrow=0)

    # 4) 스타일링
    wb = load_workbook(output_path)

    # 하위 헤더(2행)
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=False, shrink_to_fit=True)

    # 상위 헤더(1행)
    super_fill = PatternFill(start_color="244062", end_color="244062", fill_type="solid")
    super_font = Font(bold=True, color="FFFFFF", size=12)
    super_alignment = Alignment(horizontal='center', vertical='center')

    # 기본 얇은 테두리
    thin_border = Border(
        left=Side(style='thin', color='DDDDDD'),
        right=Side(style='thin', color='DDDDDD'),
        top=Side(style='thin', color='DDDDDD'),
        bottom=Side(style='thin', color='DDDDDD')
    )

    # 그룹 경계용 흰색 두꺼운 선
    thick_white_side = Side(style='thick', color='FFFFFF')

    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red_fill   = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]

        # (a) 상위 헤더를 위한 1행 삽입
        ws.insert_rows(1)

        max_col = ws.max_column

        # (b) 하위 헤더(2행) 스타일
        for cell in ws[2]:
            if isinstance(cell.value, str):
                cell.value = cell.value.replace("\n", " ").strip()
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_alignment

        # (c) 상위 헤더 병합 및 스타일
        for title, c_start, c_end in super_headers:
            # 실제 존재하는 컬럼만 병합
            if c_end <= max_col:
                ws.merge_cells(start_row=1, start_column=c_start, end_row=1, end_column=c_end)
                cell = ws.cell(row=1, column=c_start)
                cell.value = title
                cell.fill = super_fill
                cell.font = super_font
                cell.alignment = super_alignment

        # (d) 데이터 영역: 줄바꿈 없음 + 테두리
        for row_idx in range(3, ws.max_row + 1):
            for col_idx in range(1, max_col + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.border = thin_border
                cell.alignment = Alignment(vertical='center', wrap_text=False)

        # (e) 조건부 서식 & 링크
        header_row_values = [cell.value for cell in ws[2]]
        
        def col_idx_of(name):
            try:
                return header_row_values.index(name) + 1
            except ValueError:
                return None

        # 가격차이 조건부 서식
        price_diff_col = col_idx_of('가격차이(원)')
        cheaper_src_col = col_idx_of('더_저렴한_곳')
        
        if price_diff_col and cheaper_src_col:
            for row_idx in range(3, ws.max_row + 1):
                cheaper_value = ws.cell(row=row_idx, column=cheaper_src_col).value
                if cheaper_value == '아이허브':
                    ws.cell(row=row_idx, column=price_diff_col).fill = green_fill
                elif cheaper_value == '로켓직구':
                    ws.cell(row=row_idx, column=price_diff_col).fill = red_fill

        # 로켓 링크
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

        # (f) 그룹 경계(흰색 두꺼운 세로줄) 적용: 1행~마지막 행
        last_row = ws.max_row
        for boundary_col in group_boundaries:
            if boundary_col <= max_col:
                for r in range(1, last_row + 1):
                    c = ws.cell(row=r, column=boundary_col)
                    c.border = Border(
                        left=c.border.left,
                        right=thick_white_side,   # 오른쪽 경계선만 두껍게
                        top=c.border.top,
                        bottom=c.border.bottom
                    )

        # (g) 틀 고정 & 필터 (먼저 적용)
        ws.freeze_panes = 'E3'  # 상위(1) + 하위(2) 아래 + 기본정보 4열 고정
        ws.auto_filter.ref = f"A2:{get_column_letter(ws.max_column)}{ws.max_row}"

        # (h) 오토필터 적용 후 '오토핏 근사 v2' 열 너비 설정
        def east_asian_factor(s: str) -> float:
            """한글/全角 문자가 많을수록 가로폭이 더 필요하다는 가중치"""
            if not s:
                return 1.0
            wide = len(re.findall(r'[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7A3\u3000-\u303F\u3040-\u30FF\u4E00-\u9FFF]', s))
            ratio = wide / max(len(s), 1)
            return 1.0 + 0.25 * ratio

        # 숫자형/비율/차이 류 키워드
        numeric_like = {'순위','가격','원가','할인','평점','리뷰','차이','율','%','%p','재고','매출','주문','판매','방문','조회','장바구니','전환','취소'}
        max_sample_rows = min(ws.max_row, 500)

        # 컬럼별 최소 폭
        min_widths = {
            # 기본 정보
            '카테고리': 20,
            '로켓_순위': 16,
            'UPC': 20,
            '품번': 20,

            # 로켓직구
            '로켓_상품ID': 20,
            '로켓_제품명': 56,
            '로켓_가격': 18,
            '로켓_원가': 18,
            '로켓_할인율(%)': 16,
            '로켓_평점': 16,
            '로켓_리뷰수': 16,
            '로켓_링크': 14,

            # 아이허브 기본
            '아이허브_상품ID': 20,
            '아이허브_제품명': 56,
            '아이허브_가격': 18,
            '아이허브_재고': 16,
            '재고상태': 14,
            '아이허브_품번': 20,

            # 아이허브 판매성과
            '아이허브_카테고리': 24,
            '매출(원)': 18,
            '주문수': 14,
            '판매량': 14,
            '방문자수': 16,
            '조회수': 14,
            '장바구니': 14,
            '구매전환율(%)': 18,
            '취소율(%)': 14,

            # 가격 비교
            '가격차이(원)': 18,
            '가격차이율(%)': 16,
            '더_저렴한_곳': 16,
        }

        for col_idx, col_name in enumerate(header_row_values, start=1):
            col_letter = get_column_letter(col_idx)

            # 1) 헤더 길이(필터 아이콘 여백 + 한글 가중치)
            header_base = len(str(col_name))
            header_est  = int((header_base + 3) * east_asian_factor(str(col_name)))

            # 2) 데이터 길이(최대 500행 샘플, 링크열은 "Link"로 평가)
            data_max_len = 0
            for r in range(3, max_sample_rows + 1):
                v = ws.cell(row=r, column=col_idx).value
                if v is None:
                    continue
                s = "Link" if '링크' in col_name else str(v)
                data_max_len = max(data_max_len, int(len(s) * east_asian_factor(s)))

            # 3) 기본 폭(헤더/데이터 중 큰 값에 10% 버퍼)
            est = int(max(header_est, data_max_len) * 1.10)

            # 4) 최소 폭 적용
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
    print(f"   📑 시트 개수: {len(wb.sheetnames)}개")
    print(f"   📅 포함 날짜: {', '.join(wb.sheetnames)}")


def generate_summary_stats(date_data_dict):
    """요약 통계 출력"""
    
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
        print("-" * 80)
        
        print(f"1️⃣ 전체 현황")
        print(f"   - 총 로켓직구 상품: {len(df):,}개")
        print(f"   - 매칭된 아이허브 상품: {df['iherb_vendor_id'].notna().sum():,}개")
        print(f"   - 매칭률: {df['iherb_vendor_id'].notna().sum() / len(df) * 100:.1f}%")
        
        # 매칭된 제품만 필터링
        matched_df = df[df['iherb_vendor_id'].notna()].copy()
        
        if not matched_df.empty:
            print(f"\n2️⃣ 가격 경쟁력 (매칭된 제품 기준)")
            cheaper_counts = matched_df['cheaper_source'].value_counts()
            for source, count in cheaper_counts.items():
                pct = count / len(matched_df) * 100
                print(f"   - {source}: {count:,}개 ({pct:.1f}%)")
            
            print(f"\n3️⃣ 가격 차이 통계 (매칭된 제품 기준)")
            price_diffs = matched_df['price_diff'].dropna()
            if len(price_diffs) > 0:
                print(f"   - 평균 가격차이: {price_diffs.mean():,.0f}원")
                print(f"   - 중앙값: {price_diffs.median():,.0f}원")
                print(f"   - 최대(아이허브가 비쌈): {price_diffs.max():,.0f}원")
                print(f"   - 최소(로켓직구가 비쌈): {price_diffs.min():,.0f}원")
            
            print(f"\n4️⃣ 재고 상태 (매칭된 제품 기준)")
            if 'iherb_stock_status' in matched_df.columns:
                stock_counts = matched_df['iherb_stock_status'].value_counts()
                for status, count in stock_counts.items():
                    pct = count / len(matched_df) * 100
                    print(f"   - {status}: {count:,}개 ({pct:.1f}%)")
            
            print(f"\n5️⃣ 카테고리별 매칭률")
            for category in df['rocket_category'].unique():
                cat_total = (df['rocket_category'] == category).sum()
                cat_matched = ((df['rocket_category'] == category) & (df['iherb_vendor_id'].notna())).sum()
                pct = cat_matched / cat_total * 100 if cat_total > 0 else 0
                print(f"   - {category}: {pct:.1f}% ({cat_matched}/{cat_total})")
            
            print(f"\n6️⃣ 주목할 제품 (로켓직구 상위 20위 내)")
            top_rocket = matched_df[matched_df['rocket_rank'] <= 20].copy()
            if not top_rocket.empty:
                print(f"   - 총 {len(top_rocket)}개")
                top_rocket_iherb_cheaper = (top_rocket['cheaper_source'] == '아이허브').sum()
                print(f"   - 아이허브가 저렴: {top_rocket_iherb_cheaper}개")
                print(f"   - 로켓직구가 저렴: {len(top_rocket) - top_rocket_iherb_cheaper}개")
    
    print("="*80 + "\n")


def main():
    """메인 실행"""
    
    print("\n" + "="*80)
    print("🎯 로켓직구 vs 아이허브 가격 비교 리포트 생성")
    print("="*80)
    print(f"DB 경로: {DB_PATH}")
    print(f"Excel 경로: {EXCEL_DIR}")
    print(f"출력 경로: {OUTPUT_DIR}")
    print("="*80 + "\n")
    
    # 출력 디렉토리 생성
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 사용 가능한 날짜 조회
    available_dates = get_available_dates()
    
    if not available_dates:
        print("❌ 사용 가능한 데이터가 없습니다.")
        return
    
    print(f"📅 사용 가능한 날짜: {len(available_dates)}개")
    for date in available_dates[:5]:  # 최근 5개만 표시
        print(f"   - {date}")
    if len(available_dates) > 5:
        print(f"   ... 외 {len(available_dates) - 5}개")
    
    dates_to_process = available_dates
    print(f"\n✅ 처리할 날짜: {', '.join(dates_to_process)}")
    
    date_data_dict = {}
    
    for target_date in dates_to_process:
        print(f"\n{'='*80}")
        df = extract_price_comparison_data(target_date=target_date)
        
        if not df.empty:
            date_data_dict[target_date] = df
    
    if not date_data_dict:
        print("\n❌ 처리할 데이터가 없습니다.")
        return
    
    # 요약 통계
    generate_summary_stats(date_data_dict)
    
    # 엑셀 리포트 생성
    today = datetime.now().strftime('%Y%m%d')
    output_path = os.path.join(OUTPUT_DIR, f"rocket_vs_iherb_comparison_{today}.xlsx")
    create_excel_report(date_data_dict, output_path)
    
    print("\n✅ 처리 완료!")
    print(f"📁 파일 위치: {output_path}")
    print("\n💡 사용 팁:")
    print("   1. 각 시트는 날짜별로 구성되어 있습니다")
    print("   2. 로켓_순위 컬럼으로 정렬하여 상위 제품 확인")
    print("   3. 카테고리 필터로 특정 카테고리만 보기")
    print("   4. 매칭된 제품만 보려면 '아이허브_상품ID' 필터에서 공백 제외")
    print("   5. 링크는 'Link' 텍스트를 클릭하면 해당 페이지로 이동")
    print("   6. 조건부 서식: 초록=아이허브 저렴, 빨강=로켓직구 저렴")
    print("   7. 아이허브 판매성과: 매출, 주문수, 전환율 등 확인 가능")
    print("   8. 재고상태: 재고있음/품절 확인 가능")


if __name__ == "__main__":
    main()