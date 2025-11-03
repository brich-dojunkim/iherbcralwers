import pandas as pd
import numpy as np
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime
import sqlite3
import sys
from pathlib import Path

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_manager import DataManager


def get_available_dates(db_path):
    """사용 가능한 날짜 목록"""
    conn = sqlite3.connect(db_path)
    query = """
    SELECT DISTINCT DATE(snapshot_time) as date
    FROM snapshots
    ORDER BY date DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['date'].tolist()


def extract_price_comparison_data(db_path, excel_dir, target_date=None):
    """가격 비교 데이터 추출"""
    
    print(f"\n{'='*80}")
    print(f"📅 가격 비교 데이터 추출")
    print(f"{'='*80}")
    print(f"처리 날짜: {target_date or '최신'}")

    manager = DataManager(
        db_path=db_path,
        rocket_csv_path=None,  # 사용 안 함
        excel_dir=excel_dir
    )
    
    df = manager.get_integrated_df(target_date=target_date)
    
    if df.empty:
        print("⚠️ 데이터가 없습니다.")
        return df

    # 비중(%) 계산
    def calculate_share(colname, outname):
        """전체 합계 대비 비중 계산"""
        total = pd.to_numeric(df[colname], errors='coerce').fillna(0).sum()
        if total <= 0:
            df[outname] = np.nan
        else:
            df[outname] = (pd.to_numeric(df[colname], errors='coerce').fillna(0) / total * 100).round(2)

    # 판매 성과 비중
    share_columns = [
        ('iherb_revenue', '매출비중(%)'),
        ('iherb_orders', '주문비중(%)'),
        ('iherb_sales_quantity', '판매량비중(%)'),
        ('iherb_views', '조회비중(%)'),
    ]
    
    for src_col, out_col in share_columns:
        if src_col in df.columns:
            calculate_share(src_col, out_col)

    print(f"\n✅ 총 {len(df):,}개 로켓직구 상품")
    print(f"   - 매칭된 아이허브 상품: {df['iherb_vendor_id'].notna().sum():,}개")
    print(f"   - 매칭률: {df['iherb_vendor_id'].notna().sum() / len(df) * 100:.1f}%")
    
    # 신뢰도 분포
    if df['iherb_vendor_id'].notna().any():
        conf_counts = df[df['iherb_vendor_id'].notna()]['matching_confidence'].value_counts()
        print(f"\n   📊 매칭 신뢰도 분포:")
        for conf, count in conf_counts.items():
            pct = count / df['iherb_vendor_id'].notna().sum() * 100
            print(f"      • {conf}: {count:,}개 ({pct:.1f}%)")
    
    return df


def create_excel_report(date_data_dict, output_path):
    """Excel 리포트 생성"""

    if not date_data_dict:
        print("❌ 데이터가 없어 엑셀 생성을 건너뜁니다.")
        return

    print(f"\n{'='*80}")
    print(f"📊 Excel 리포트 생성")
    print(f"{'='*80}")

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for date_str, df in date_data_dict.items():
            if df.empty:
                continue

            # 1. 컬럼 재구성 및 이름 변경
            output_df = pd.DataFrame()

            # === (1) 성과 지표 ===
            # 로켓 평점/리뷰수/순위, 판매량, 매출(원)
            # 일부 값이 없을 수 있으므로 get() 사용해 NaN 대체
            output_df['로켓_평점'] = df.get('rocket_rating', np.nan)
            output_df['로켓_리뷰수'] = df.get('rocket_reviews', np.nan)
            output_df['로켓_순위'] = df.get('rocket_rank', np.nan)
            output_df['판매량'] = df.get('iherb_sales_quantity', np.nan)
            output_df['매출(원)'] = df.get('iherb_revenue', np.nan)

            # === (2) 제품 정보 ===
            # 카테고리, 제품명, 링크, ID 등
            output_df['카테고리'] = df.get('rocket_category', np.nan)
            output_df['아이허브_카테고리'] = df.get('iherb_category', np.nan)

            output_df['로켓_제품명'] = df.get('rocket_product_name', np.nan)
            output_df['아이허브_제품명'] = df.get('iherb_product_name', np.nan)

            output_df['로켓_링크'] = df.get('rocket_url', np.nan)
            output_df['아이허브_링크'] = df.get('iherb_url', np.nan)

            output_df['로켓_상품ID'] = df.get('rocket_vendor_id', np.nan)
            output_df['로켓_Product_ID'] = df.get('rocket_product_id', np.nan)
            output_df['로켓_Item_ID'] = df.get('rocket_item_id', np.nan)

            output_df['아이허브_상품ID'] = df.get('iherb_vendor_id', np.nan)
            output_df['아이허브_Product_ID'] = df.get('iherb_product_id', np.nan)
            output_df['아이허브_Item_ID'] = df.get('iherb_item_id', np.nan)

            output_df['아이허브_품번'] = df.get('iherb_part_number', np.nan)

            # === (3) 가격 비교 ===
            # 요청한 순서대로 컬럼 재배치
            output_df['로켓_정가'] = df.get('rocket_original_price', np.nan)
            output_df['로켓_할인율(%)'] = df.get('rocket_discount_rate', np.nan)
            output_df['로켓_가격'] = df.get('rocket_price', np.nan)
            output_df['아이허브_가격'] = df.get('iherb_price', np.nan)
            output_df['가격차이(원)'] = df.get('price_diff', np.nan)
            output_df['가격차이(%)'] = df.get('price_diff_pct', np.nan)
            output_df['더_저렴한_곳'] = df.get('cheaper_source', np.nan)

            # === (4) 재고·매칭 및 판매 성과 ===
            output_df['아이허브_재고'] = df.get('iherb_stock', np.nan)
            # 아이허브 판매상태 컬럼명은 기존 코드에서 iherb_stock_status를 사용하고 있어 그에 맞춤
            output_df['아이허브_판매상태'] = df.get('iherb_stock_status', np.nan)
            output_df['매칭_방식'] = df.get('matching_method', np.nan)
            output_df['매칭_신뢰도'] = df.get('matching_confidence', np.nan)

            # 판매 성과 관련 컬럼 (필요 시 포함)
            output_df['매출비중(%)'] = df.get('매출비중(%)', np.nan)
            output_df['주문'] = df.get('iherb_orders', np.nan)
            output_df['주문비중(%)'] = df.get('주문비중(%)', np.nan)
            output_df['판매량비중(%)'] = df.get('판매량비중(%)', np.nan)
            output_df['방문자'] = df.get('iherb_visitors', np.nan)
            output_df['조회'] = df.get('iherb_views', np.nan)
            output_df['조회비중(%)'] = df.get('조회비중(%)', np.nan)
            output_df['장바구니'] = df.get('iherb_cart_adds', np.nan)
            output_df['구매전환율(%)'] = df.get('iherb_conversion_rate', np.nan)
            output_df['총_매출(원)'] = df.get('iherb_total_revenue', np.nan)
            output_df['총_취소금액'] = df.get('iherb_total_cancel_amount', np.nan)
            output_df['총_취소수량'] = df.get('iherb_total_cancel_quantity', np.nan)
            output_df['취소율(%)'] = df.get('iherb_cancel_rate', np.nan)

            # 2. 시트명 생성 후 쓰기
            sheet_name = date_str.replace('-', '')[:10]  # YYYYMMDD
            output_df.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"   ✓ 시트 '{sheet_name}' 작성 완료 ({len(output_df):,}개)")

    # 3. 스타일 적용 (헤더 병합 등은 apply_excel_styles()에서 처리한다고 가정)
    apply_excel_styles(output_path)

    print(f"\n✅ Excel 리포트 생성 완료: {output_path}")



def apply_excel_styles(output_path):
    """Excel 스타일 적용 및 상위 헤더 병합"""

    wb = load_workbook(output_path)

    # 기본 스타일 정의
    group_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True, size=11)

    green_fill  = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red_fill    = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    yellow_fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")

    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    # 그룹별 열 이름 목록 (필요에 따라 수정)
    group_defs = [
        ('성과 지표', [
            '로켓_평점','로켓_리뷰수','로켓_순위','판매량','매출(원)'
        ]),
        ('제품 정보', [
            '카테고리','아이허브_카테고리','로켓_제품명','아이허브_제품명',
            '로켓_링크','아이허브_링크','로켓_상품ID','로켓_Product_ID','로켓_Item_ID',
            '아이허브_상품ID','아이허브_Product_ID','아이허브_Item_ID','아이허브_품번'
        ]),
        ('가격 비교', [
            '로켓_정가','로켓_할인율(%)','로켓_가격','아이허브_가격',
            '가격차이(원)','가격차이(%)','더_저렴한_곳'
        ]),
        ('재고·매칭 정보', [
            '아이허브_재고','아이허브_판매상태','매칭_방식','매칭_신뢰도',
            '매출비중(%)','주문','주문비중(%)','판매량비중(%)',
            '방문자','조회','조회비중(%)','장바구니','구매전환율(%)',
            '총_매출(원)','총_취소금액','총_취소수량','취소율(%)'
        ])
    ]

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]

        # 1. 그룹 헤더를 위한 행 추가
        ws.insert_rows(1)

        # 2. 각 그룹명을 병합하여 작성
        col_pos = 1
        for group_name, cols in group_defs:
            span = len(cols)
            ws.merge_cells(start_row=1, start_column=col_pos,
                           end_row=1, end_column=col_pos + span - 1)
            cell = ws.cell(row=1, column=col_pos)
            cell.value = group_name
            cell.fill = group_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center', vertical='center')
            col_pos += span

        # 3. 2행(기존 헤더)의 스타일 적용
        for cell in ws[2]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.border = thin_border

        # 4. 컬럼 너비 설정 (필요 시 수정)
        column_widths = {
            '카테고리': 15,
            '로켓_순위': 10,
            '로켓_상품ID': 15,
            '로켓_Product_ID': 15,
            '로켓_Item_ID': 15,
            '로켓_제품명': 50,
            '로켓_가격': 12,
            '로켓_정가': 12,
            '로켓_할인율(%)': 12,
            '로켓_평점': 10,
            '로켓_리뷰수': 12,
            '로켓_링크': 12,
            '매칭_방식': 12,
            '매칭_신뢰도': 12,
            '아이허브_상품ID': 15,
            '아이허브_Product_ID': 15,
            '아이허브_Item_ID': 15,
            '아이허브_제품명': 50,
            '아이허브_품번': 15,
            '아이허브_가격': 12,
            '아이허브_재고': 10,
            '아이허브_판매상태': 12,
            '아이허브_링크': 12,
            '가격차이(원)': 12,
            '가격차이(%)': 12,
            '더_저렴한_곳': 12,
        }
        # 헤더 이름 목록 (2행)
        header_names = [cell.value for cell in ws[2]]

        def col_idx_of(name):
            try:
                return header_names.index(name) + 1
            except ValueError:
                return None

        for col_name, width in column_widths.items():
            idx = col_idx_of(col_name)
            if idx:
                ws.column_dimensions[get_column_letter(idx)].width = width

        # 5. 데이터 셀 스타일 (3행 이후)
        max_col = ws.max_column
        for row_idx in range(3, ws.max_row + 1):
            for col_idx in range(1, max_col + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.border = thin_border
                cell.alignment = Alignment(vertical='center', wrap_text=False)

        # 6. 가격 차이 색상
        price_diff_col = col_idx_of('가격차이(원)')
        cheaper_col = col_idx_of('더_저렴한_곳')
        if price_diff_col and cheaper_col:
            for row_idx in range(3, ws.max_row + 1):
                cheaper_value = ws.cell(row=row_idx, column=cheaper_col).value
                if cheaper_value == '아이허브':
                    ws.cell(row=row_idx, column=price_diff_col).fill = green_fill
                elif cheaper_value == '로켓직구':
                    ws.cell(row=row_idx, column=price_diff_col).fill = red_fill

        # 7. 매칭 신뢰도 색상
        conf_col = col_idx_of('매칭_신뢰도')
        if conf_col:
            for row_idx in range(3, ws.max_row + 1):
                conf_value = ws.cell(row=row_idx, column=conf_col).value
                cell = ws.cell(row=row_idx, column=conf_col)
                if conf_value == 'High':
                    cell.fill = green_fill
                elif conf_value == 'Medium':
                    cell.fill = yellow_fill
                elif conf_value == 'Low':
                    cell.fill = red_fill

        # 8. 하이퍼링크 처리 (3행부터)
        rocket_url_col = col_idx_of('로켓_링크')
        if rocket_url_col:
            for row_idx in range(3, ws.max_row + 1):
                cell = ws.cell(row=row_idx, column=rocket_url_col)
                url = cell.value
                if url and str(url).strip():
                    cell.value = "Link"
                    cell.hyperlink = str(url)
                    cell.font = Font(color="0563C1", underline="single")
                    cell.alignment = Alignment(horizontal='center', vertical='center')

        iherb_url_col = col_idx_of('아이허브_링크')
        if iherb_url_col:
            for row_idx in range(3, ws.max_row + 1):
                cell = ws.cell(row=row_idx, column=iherb_url_col)
                url = cell.value
                if url and str(url).strip():
                    cell.value = "Link"
                    cell.hyperlink = str(url)
                    cell.font = Font(color="0563C1", underline="single")
                    cell.alignment = Alignment(horizontal='center', vertical='center')

        # 9. Freeze panes: 두 줄 헤더 뒤로 고정
        freeze_col = col_idx_of('매출(원)')
        if freeze_col:
            # 2행 헤더 아래, 링크 컬럼 다음 칸에서 고정
            ws.freeze_panes = ws.cell(row=3, column=freeze_col + 1)

    wb.save(output_path)


def main():
    """메인 함수"""
    
    # 설정
    DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/data/rocket/monitoring.db"
    EXCEL_DIR = "/Users/brich/Desktop/iherb_price/coupang2/data/iherb"  # 프로젝트 파일 경로
    OUTPUT_DIR = "output"
    
    # 출력 디렉토리 생성
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    
    # 사용 가능한 날짜
    dates = get_available_dates(DB_PATH)
    
    if not dates:
        print("❌ 사용 가능한 날짜가 없습니다.")
        return
    
    print(f"\n사용 가능한 날짜: {len(dates)}개")
    for i, date in enumerate(dates[:5], 1):
        print(f"  {i}. {date}")
    
    # 최신 3개 날짜 처리
    process_dates = dates[:3]
    
    date_data_dict = {}
    for date_str in process_dates:
        df = extract_price_comparison_data(DB_PATH, EXCEL_DIR, target_date=date_str)
        if not df.empty:
            date_data_dict[date_str] = df
    
    # Excel 생성
    if date_data_dict:
        output_file = Path(OUTPUT_DIR) / f"rocket_vs_iherb_{datetime.now().strftime('%Y%m%d')}.xlsx"
        create_excel_report(date_data_dict, str(output_file))
    else:
        print("\n❌ 생성할 데이터가 없습니다.")


if __name__ == "__main__":
    main()