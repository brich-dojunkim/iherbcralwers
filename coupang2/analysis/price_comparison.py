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

            # 컬럼 재구성 및 이름 변경
            output_df = pd.DataFrame()

            # === 기본 정보 ===
            output_df['카테고리'] = df['rocket_category']
            output_df['로켓_순위'] = df['rocket_rank']
            output_df['로켓_상품ID'] = df['rocket_vendor_id']
            output_df['로켓_Product_ID'] = df['rocket_product_id']
            output_df['로켓_Item_ID'] = df['rocket_item_id']

            # === 로켓직구 정보 ===
            output_df['로켓_제품명'] = df['rocket_product_name']
            output_df['로켓_가격'] = df['rocket_price']
            output_df['로켓_정가'] = df['rocket_original_price']
            output_df['로켓_할인율(%)'] = df['rocket_discount_rate']
            output_df['로켓_평점'] = df['rocket_rating']
            output_df['로켓_리뷰수'] = df['rocket_reviews']
            output_df['로켓_링크'] = df['rocket_url']

            # === 매칭 정보 ===
            output_df['매칭_방식'] = df['matching_method']
            output_df['매칭_신뢰도'] = df['matching_confidence']

            # === 아이허브 기본 ===
            output_df['아이허브_상품ID'] = df['iherb_vendor_id']
            output_df['아이허브_Product_ID'] = df['iherb_product_id']
            output_df['아이허브_Item_ID'] = df['iherb_item_id']
            output_df['아이허브_제품명'] = df['iherb_product_name']
            output_df['아이허브_품번'] = df['iherb_part_number']
            output_df['아이허브_가격'] = df['iherb_price']
            output_df['아이허브_재고'] = df['iherb_stock']
            output_df['아이허브_판매상태'] = df['iherb_stock_status']
            output_df['아이허브_링크'] = df['iherb_url']

            # === 가격 비교 ===
            output_df['가격차이(원)'] = df['price_diff']
            output_df['가격차이(%)'] = df['price_diff_pct']
            output_df['더_저렴한_곳'] = df['cheaper_source']

            # === 판매 성과 ===
            output_df['아이허브_카테고리'] = df['iherb_category']
            output_df['매출(원)'] = df['iherb_revenue']
            output_df['매출비중(%)'] = df.get('매출비중(%)', np.nan)
            output_df['주문'] = df['iherb_orders']
            output_df['주문비중(%)'] = df.get('주문비중(%)', np.nan)
            output_df['판매량'] = df['iherb_sales_quantity']
            output_df['판매량비중(%)'] = df.get('판매량비중(%)', np.nan)
            output_df['방문자'] = df['iherb_visitors']
            output_df['조회'] = df['iherb_views']
            output_df['조회비중(%)'] = df.get('조회비중(%)', np.nan)
            output_df['장바구니'] = df['iherb_cart_adds']
            output_df['구매전환율(%)'] = df['iherb_conversion_rate']
            output_df['총_매출(원)'] = df['iherb_total_revenue']
            output_df['총_취소금액'] = df['iherb_total_cancel_amount']
            output_df['총_취소수량'] = df['iherb_total_cancel_quantity']
            output_df['취소율(%)'] = df['iherb_cancel_rate']

            # 시트명
            sheet_name = date_str.replace('-', '')[:10]  # YYYY-MM-DD -> YYYYMMDD
            output_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"   ✓ 시트 '{sheet_name}' 작성 완료 ({len(output_df):,}개)")

    # 스타일 적용
    apply_excel_styles(output_path)
    
    print(f"\n✅ Excel 리포트 생성 완료: {output_path}")


def apply_excel_styles(output_path):
    """Excel 스타일 적용"""
    
    wb = load_workbook(output_path)
    
    # 스타일 정의
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True, size=11)
    
    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    yellow_fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        
        # 헤더 스타일
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.border = thin_border
        
        # 헤더 이름으로 컬럼 인덱스 찾기
        header_values = [cell.value for cell in ws[1]]
        
        def col_idx_of(name):
            try:
                return header_values.index(name) + 1
            except ValueError:
                return None
        
        # 컬럼 너비 조정
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
        
        for col_name, width in column_widths.items():
            col_idx = col_idx_of(col_name)
            if col_idx:
                ws.column_dimensions[get_column_letter(col_idx)].width = width
        
        # 데이터 스타일
        max_col = ws.max_column
        for row_idx in range(2, ws.max_row + 1):
            for col_idx in range(1, max_col + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.border = thin_border
                cell.alignment = Alignment(vertical='center', wrap_text=False)
        
        # 가격 차이 색상
        price_diff_col = col_idx_of('가격차이(원)')
        cheaper_col = col_idx_of('더_저렴한_곳')
        
        if price_diff_col and cheaper_col:
            for row_idx in range(2, ws.max_row + 1):
                cheaper_value = ws.cell(row=row_idx, column=cheaper_col).value
                if cheaper_value == '아이허브':
                    ws.cell(row=row_idx, column=price_diff_col).fill = green_fill
                elif cheaper_value == '로켓직구':
                    ws.cell(row=row_idx, column=price_diff_col).fill = red_fill
        
        # 매칭 신뢰도 색상
        conf_col = col_idx_of('매칭_신뢰도')
        if conf_col:
            for row_idx in range(2, ws.max_row + 1):
                conf_value = ws.cell(row=row_idx, column=conf_col).value
                cell = ws.cell(row=row_idx, column=conf_col)
                if conf_value == 'High':
                    cell.fill = green_fill
                elif conf_value == 'Medium':
                    cell.fill = yellow_fill
                elif conf_value == 'Low':
                    cell.fill = red_fill
        
        # 하이퍼링크
        rocket_url_col = col_idx_of('로켓_링크')
        if rocket_url_col:
            for row_idx in range(2, ws.max_row + 1):
                cell = ws.cell(row=row_idx, column=rocket_url_col)
                url = cell.value
                if url and str(url).strip():
                    cell.value = "Link"
                    cell.hyperlink = str(url)
                    cell.font = Font(color="0563C1", underline="single")
                    cell.alignment = Alignment(horizontal='center', vertical='center')
        
        iherb_url_col = col_idx_of('아이허브_링크')
        if iherb_url_col:
            for row_idx in range(2, ws.max_row + 1):
                cell = ws.cell(row=row_idx, column=iherb_url_col)
                url = cell.value
                if url and str(url).strip():
                    cell.value = "Link"
                    cell.hyperlink = str(url)
                    cell.font = Font(color="0563C1", underline="single")
                    cell.alignment = Alignment(horizontal='center', vertical='center')
        
        # Freeze panes (로켓_링크까지 고정)
        freeze_col = col_idx_of('로켓_링크')
        if freeze_col:
            ws.freeze_panes = ws.cell(row=2, column=freeze_col + 1)
    
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