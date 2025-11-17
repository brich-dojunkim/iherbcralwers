#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
통합 데이터 관리자 - Product ID 기반 매칭 + 동적 필터링
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 주요 변경사항:
- 동적 필터링 기준 계산 추가 (80 백분위수)
- 매칭되지 않은 우수 아이허브 상품 자동 포함
- 단일 데이터프레임으로 통합 (매칭 + 미매칭)
- 할인율기준가(아이허브 정가) 추가
- 요청할인율 추가 (정가 기준)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional
import numpy as np
import re


# ─────────────────────────────────────────────────────────
# 유틸리티 함수
# ─────────────────────────────────────────────────────────

def extract_pack_count(name: str):
    """상품명에서 '... 2개', '... 1개' 등 마지막 등장하는 '~개'의 숫자 부분을 추출."""
    if not isinstance(name, str):
        return np.nan
    matches = re.findall(r'(\d+)\s*개', name)
    if not matches:
        return np.nan
    try:
        return int(matches[-1])
    except Exception:
        return np.nan


def extract_unit_count(name: str):
    """상품명에서 '200정', '100정' 등의 개수를 추출."""
    if not isinstance(name, str):
        return np.nan
    
    # '200정', '100정' 패턴
    matches = re.findall(r'(\d+)\s*정', name)
    if matches:
        try:
            return int(matches[0])
        except:
            pass
    
    # '200베지캡슐', '100캡슐' 패턴
    matches = re.findall(r'(\d+)\s*(?:베지)?캡슐', name)
    if matches:
        try:
            return int(matches[0])
        except:
            pass
    
    return np.nan


def extract_weight(name: str) -> Optional[float]:
    """상품명에서 용량을 추출하여 g 단위로 반환."""
    if not isinstance(name, str):
        return np.nan
    text = name.replace(',', '')
    match = re.search(r'(\d+(?:\.\d+)?)\s*(kg|g|lbs?|lb|oz|파운드)', text, flags=re.I)
    if not match:
        return np.nan

    value = float(match.group(1))
    unit = match.group(2).lower()

    if unit == 'kg':
        return value * 1000
    if unit == 'g':
        return value
    if unit in ('lb', 'lbs', '파운드'):
        return value * 453.59237
    if unit == 'oz':
        return value * 28.3495231

    return np.nan


def normalize_part_number(pn: str):
    """품번 정규화: 대문자, 하이픈/공백 제거"""
    if not isinstance(pn, str):
        return ''
    return re.sub(r'[-\s]', '', str(pn).upper().strip())


def safe_read_excel_header_guess(path, max_try=20):
    """상단 안내문 줄이 있는 엑셀에서 실제 헤더를 자동 탐색."""
    KEY_CANDIDATES = {"업체상품코드", "옵션 ID", "업체상품 ID", "바코드"}
    for header_row in range(max_try):
        df_try = pd.read_excel(path, header=header_row)
        cols = set(map(str, df_try.columns))
        if KEY_CANDIDATES & cols:
            return df_try
    return pd.read_excel(path)


def _pick_col(df: pd.DataFrame, candidates):
    """여러 후보 중 존재하는 첫 번째 컬럼명을 고름."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ─────────────────────────────────────────────────────────


class DataManager:
    """통합 데이터 관리 - Product ID 기반 매칭 + 동적 필터링"""
    
    def __init__(self, 
                 db_path: str = "/Users/brich/Desktop/iherb_price/coupang2/data/rocket/monitoring.db", 
                 excel_dir: str = "/Users/brich/Desktop/iherb_price/coupang2/data/iherb"):
        """
        Args:
            db_path: 로켓직구 모니터링 DB 경로
            excel_dir: 아이허브 Excel 파일 디렉토리
        """
        self.db_path = db_path
        self.excel_dir = Path(excel_dir)
    
    def get_integrated_df(self, target_date: Optional[str] = None, 
                         include_unmatched: bool = True) -> pd.DataFrame:
        """
        통합 데이터프레임 생성 (Product ID 기반 매칭 + 동적 필터링)
        
        Args:
            target_date: 특정 날짜 (None이면 최신)
            include_unmatched: 미매칭 우수 상품 포함 여부
        
        Returns:
            DataFrame with columns:
            
            [매칭 정보]
            - matching_status: '로켓매칭' 또는 '미매칭'
            - matching_method, matching_confidence
            
            [로켓직구]
            - rocket_vendor_id, rocket_product_id, rocket_item_id
            - rocket_product_name, rocket_category
            - rocket_rank, rocket_price, rocket_original_price
            - rocket_discount_rate, rocket_rating, rocket_reviews
            - rocket_url
            
            [아이허브 가격/재고]
            - iherb_vendor_id, iherb_product_id, iherb_item_id
            - iherb_product_name
            - iherb_price (판매가), iherb_original_price (정가)
            - iherb_stock, iherb_stock_status
            - iherb_part_number
            - iherb_recommended_price
            - iherb_upc
            
            [아이허브 판매 성과]
            - iherb_category, iherb_revenue, iherb_orders
            - iherb_sales_quantity, iherb_visitors, iherb_views
            - iherb_cart_adds, iherb_conversion_rate
            - iherb_total_revenue, iherb_total_cancel_amount
            - iherb_total_cancel_quantity, iherb_cancel_rate
            - iherb_item_winner_ratio
            
            [가격 비교]
            - price_diff, price_diff_pct, cheaper_source
            - breakeven_discount_rate
            - recommended_discount_rate (판매가 기준)
            - requested_discount_rate (정가 기준)
        """
        
        print(f"\n{'='*80}")
        print(f"🔗 통합 데이터프레임 생성 (동적 필터링 포함)")
        print(f"{'='*80}\n")
        
        # 1. 로켓직구 데이터 (DB)
        df_rocket = self._load_rocket_df(target_date)

        # 2. 아이허브 가격/재고 (Excel)
        df_price = self._load_price_inventory_df()

        # 3. 아이허브 성과 (Excel)
        df_insights = self._load_seller_insights_df()

        # 4. 쿠팡 추천가 (Excel)
        df_recommended = self._load_coupang_recommended_price_df()

        # 5. UPC 데이터 (Excel)
        df_upc = self._load_upc_df()

        # 6. 아이허브 통합
        df_iherb = self._integrate_iherb(df_price, df_insights, df_recommended, df_upc)

        # 7. 전체 통합 (Product ID 기반)
        df_matched = self._integrate_all_by_product_id(df_rocket, df_iherb)
        
        # 8. 동적 필터링 및 미매칭 상품 추가
        if include_unmatched:
            df_final = self._add_unmatched_products(df_matched, df_iherb)
        else:
            df_final = df_matched
            df_final['matching_status'] = '로켓매칭'
        
        print(f"\n✅ 통합 완료: {len(df_final):,}개 레코드")
        print(f"   - 로켓직구 매칭: {(df_final['matching_status'] == '로켓매칭').sum():,}개")
        
        if include_unmatched:
            print(f"   - 아이허브 미매칭 우수: {(df_final['matching_status'] == '미매칭').sum():,}개")
        
        print(f"   - 최종 매칭률: {(df_final['matching_status'] == '로켓매칭').sum() / len(df_final) * 100:.1f}%\n")
        
        return df_final
    
    def _calculate_dynamic_threshold(self, df_iherb: pd.DataFrame) -> float:
        """
        동적 판매량 필터링 기준 계산 (80 백분위수)
        
        Args:
            df_iherb: 아이허브 통합 데이터
        
        Returns:
            threshold: 필터링 기준값
        """
        # 판매 실적 있는 상품만
        sales_data = df_iherb[
            (df_iherb['iherb_sales_quantity'].notna()) & 
            (df_iherb['iherb_sales_quantity'] > 0)
        ]['iherb_sales_quantity']
        
        if len(sales_data) == 0:
            print("   ⚠️  판매 실적 데이터 없음 - 기본값 5개 사용")
            return 5.0
        
        # 80 백분위수 계산
        threshold = np.percentile(sales_data, 80)
        
        # 통계
        selected_count = (sales_data >= threshold).sum()
        total_sales = sales_data.sum()
        selected_sales = sales_data[sales_data >= threshold].sum()
        coverage_pct = (selected_sales / total_sales * 100) if total_sales > 0 else 0
        
        print(f"\n{'='*80}")
        print(f"📊 동적 필터링 기준 계산 (80 백분위수)")
        print(f"{'='*80}")
        print(f"  수식: PERCENTILE(판매량, 80)")
        print(f"  전체 상품: {len(sales_data):,}개")
        print(f"  평균 판매량: {sales_data.mean():.1f}개")
        print(f"  중앙값: {sales_data.median():.1f}개")
        print(f"")
        print(f"  ✅ 필터링 기준: {threshold:.1f}개 이상")
        print(f"  ✅ 선택 예상: {selected_count:,}개 ({selected_count/len(sales_data)*100:.1f}%)")
        print(f"  ✅ 판매량 커버: {coverage_pct:.1f}%")
        print(f"{'='*80}\n")
        
        return threshold
    
    def _add_unmatched_products(self, df_matched: pd.DataFrame, 
                                df_iherb: pd.DataFrame) -> pd.DataFrame:
        """
        미매칭 우수 상품 추가 (동적 필터링 적용)
        
        Args:
            df_matched: 로켓직구 매칭 상품
            df_iherb: 아이허브 전체 상품
        
        Returns:
            df_final: 매칭 + 미매칭 통합
        """
        print(f"\n📥 미매칭 우수 상품 추가 중...")
        
        # 동적 기준 계산
        threshold = self._calculate_dynamic_threshold(df_iherb)
        
        # 이미 매칭된 vendor_id 목록
        matched_vendor_ids = set(df_matched['iherb_vendor_id'].dropna())
        
        # 미매칭 상품 중 우수 상품 선별
        unmatched = df_iherb[~df_iherb['iherb_vendor_id'].isin(matched_vendor_ids)].copy()
        
        # 필터링 조건
        good_unmatched = unmatched[
            (unmatched['iherb_sales_quantity'].notna()) & 
            (unmatched['iherb_sales_quantity'] >= threshold) &
            (unmatched['iherb_stock_status'] == '판매중') &
            (unmatched['iherb_stock'] > 0)
        ].copy()
        
        print(f"  - 미매칭 전체: {len(unmatched):,}개")
        print(f"  - 기준 적용 후: {len(good_unmatched):,}개")
        print(f"  - 평균 판매량: {good_unmatched['iherb_sales_quantity'].mean():.1f}개")
        
        if len(good_unmatched) == 0:
            print(f"  ⚠️  조건 충족 상품 없음")
            df_matched['matching_status'] = '로켓매칭'
            return df_matched
        
        # 로켓 데이터는 모두 NaN으로
        rocket_cols = [col for col in df_matched.columns if col.startswith('rocket_')]
        for col in rocket_cols:
            if col not in good_unmatched.columns:
                good_unmatched[col] = np.nan
        
        # 매칭 정보 설정
        good_unmatched['matching_status'] = '미매칭'
        good_unmatched['matching_method'] = '동적필터'
        good_unmatched['matching_confidence'] = f'판매량>={threshold:.0f}'
        
        # 가격 비교 (로켓 가격 없으므로 NaN)
        for col in ['price_diff', 'price_diff_pct', 'cheaper_source']:
            if col not in good_unmatched.columns:
                good_unmatched[col] = np.nan
        
        # 할인율 계산
        ip = pd.to_numeric(good_unmatched['iherb_price'], errors='coerce')
        op = pd.to_numeric(good_unmatched['iherb_original_price'], errors='coerce')
        rec_p = pd.to_numeric(good_unmatched.get('iherb_recommended_price', 0), errors='coerce')
        
        good_unmatched['breakeven_discount_rate'] = np.nan
        
        # 추천할인율 (판매가 기준)
        valid_rec = ip.gt(0) & rec_p.gt(0)
        good_unmatched['recommended_discount_rate'] = np.nan
        good_unmatched.loc[valid_rec, 'recommended_discount_rate'] = (
            ((ip - rec_p) / ip * 100).replace([np.inf, -np.inf], np.nan).round(1)
        )
        
        # 요청할인율 (정가 기준)
        valid_req = op.gt(0) & rec_p.gt(0)
        good_unmatched['requested_discount_rate'] = np.nan
        good_unmatched.loc[valid_req, 'requested_discount_rate'] = (
            ((op - rec_p) / op * 100).replace([np.inf, -np.inf], np.nan).round(1)
        )
        
        # 매칭 상품에 status 추가
        df_matched['matching_status'] = '로켓매칭'
        
        # 통합
        df_final = pd.concat([df_matched, good_unmatched], ignore_index=True)
        
        # 정렬: 로켓 매칭 먼저, 그 다음 판매량 높은 순
        df_final['_sort_key'] = df_final['matching_status'].apply(lambda x: 0 if x == '로켓매칭' else 1)
        df_final = df_final.sort_values(
            ['_sort_key', 'iherb_sales_quantity'],
            ascending=[True, False]
        ).drop('_sort_key', axis=1).reset_index(drop=True)
        
        print(f"  ✅ 미매칭 우수 상품 {len(good_unmatched):,}개 추가 완료\n")
        
        return df_final
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 기존 메서드들
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    def _load_rocket_df(self, target_date: Optional[str]) -> pd.DataFrame:
        """로켓직구 데이터 로드 (DB) - 모든 카테고리의 최신 스냅샷을 포함"""

        print(f"📥 1. 로켓직구 데이터 (DB)")

        conn = sqlite3.connect(self.db_path)

        if target_date:
            subquery = f"""
                SELECT category_id, MAX(id) AS latest_id
                FROM snapshots
                WHERE DATE(snapshot_time) = '{target_date}'
                AND source_id = (SELECT id FROM sources WHERE source_type = 'rocket_direct')
                GROUP BY category_id
            """
        else:
            subquery = """
                SELECT category_id, MAX(id) AS latest_id
                FROM snapshots
                WHERE source_id = (SELECT id FROM sources WHERE source_type = 'rocket_direct')
                GROUP BY category_id
            """

        query = f"""
            SELECT 
                ps.vendor_item_id  AS rocket_vendor_id,
                ps.product_name    AS rocket_product_name,
                ps.product_url     AS rocket_url,
                ps.category_rank   AS rocket_rank,
                ps.current_price   AS rocket_price,
                ps.original_price  AS rocket_original_price,
                ps.discount_rate   AS rocket_discount_rate,
                ps.review_count    AS rocket_reviews,
                ps.rating_score    AS rocket_rating,
                c.name             AS rocket_category
            FROM product_states ps
            JOIN snapshots s   ON ps.snapshot_id = s.id
            JOIN categories c  ON s.category_id = c.id
            JOIN ({subquery}) ls ON s.id = ls.latest_id
            WHERE ps.vendor_item_id IS NOT NULL
            ORDER BY c.id, ps.category_rank
        """

        df = pd.read_sql_query(query, conn)
        conn.close()
        
        def extract_ids_from_url(url):
            if not isinstance(url, str):
                return pd.Series({'rocket_product_id': None, 'rocket_item_id': None})
            
            product_id = None
            item_id = None
            
            m_product = re.search(r'/products/(\d+)', url)
            if m_product:
                product_id = m_product.group(1)
            
            m_item = re.search(r'itemId=(\d+)', url)
            if m_item:
                item_id = m_item.group(1)
            
            return pd.Series({'rocket_product_id': product_id, 'rocket_item_id': item_id})
        
        df[['rocket_product_id', 'rocket_item_id']] = df['rocket_url'].apply(extract_ids_from_url)

        print(f"   ✓ {len(df):,}개 상품")
        print(f"   ✓ Product ID 있음: {df['rocket_product_id'].notna().sum():,}개")

        return df
    
    def _load_price_inventory_df(self) -> pd.DataFrame:
        """아이허브 가격/재고 로드 (Excel) - 할인율기준가(정가) 포함"""
        
        print(f"\n📥 2. 아이허브 가격/재고 (Excel)")
        
        files = list(self.excel_dir.glob("*price_inventory*.xlsx"))
        if not files:
            print(f"   ⚠️  price_inventory 파일 없음")
            return pd.DataFrame()
        
        latest = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
        print(f"   파일: {latest.name}")
        
        try:
            df = pd.read_excel(latest, sheet_name='data', skiprows=2)
        except Exception:
            df = safe_read_excel_header_guess(latest, max_try=30)
        
        col_vid = _pick_col(df, ['옵션 ID'])
        col_pid = _pick_col(df, ['Product ID', 'productId', 'PRODUCT_ID'])
        col_iid = _pick_col(df, ['업체상품 ID', 'itemId', 'ITEM_ID'])
        col_pname = _pick_col(df, ['쿠팡 노출 상품명', '상품명'])
        col_pn = _pick_col(df, ['업체상품코드'])
        col_price = _pick_col(df, ['판매가격', '판매가격.1'])
        col_original_price = _pick_col(df, ['할인율기준가'])
        col_stock = _pick_col(df, ['잔여수량(재고)', '잔여수량'])
        col_state = _pick_col(df, ['판매상태', '판매상태.1'])
        
        if col_vid is None:   df['옵션 ID'] = None;              col_vid = '옵션 ID'
        if col_pid is None:   df['Product ID'] = None;           col_pid = 'Product ID'
        if col_iid is None:   df['업체상품 ID'] = None;          col_iid = '업체상품 ID'
        if col_pname is None: df['쿠팡 노출 상품명'] = None;      col_pname = '쿠팡 노출 상품명'
        if col_pn is None:    df['업체상품코드'] = None;          col_pn = '업체상품코드'
        if col_price is None: df['판매가격'] = 0;                 col_price = '판매가격'
        if col_original_price is None: df['할인율기준가'] = 0;    col_original_price = '할인율기준가'
        if col_stock is None: df['잔여수량(재고)'] = 0;           col_stock = '잔여수량(재고)'
        if col_state is None: df['판매상태'] = None;              col_state = '판매상태'
        
        result = pd.DataFrame({
            'iherb_vendor_id': df[col_vid].astype(str).str.split('.').str[0],
            'iherb_product_id': df[col_pid].astype(str).str.replace(r'\.0$', '', regex=True),
            'iherb_item_id': df[col_iid].astype(str).str.replace(r'\.0$', '', regex=True),
            'iherb_product_name': df[col_pname],
            'iherb_part_number': df[col_pn].astype(str).str.strip(),
            'iherb_price': pd.to_numeric(df[col_price], errors='coerce').fillna(0).astype(int),
            'iherb_original_price': pd.to_numeric(df[col_original_price], errors='coerce').fillna(0).astype(int),
            'iherb_stock': pd.to_numeric(df[col_stock], errors='coerce').fillna(0).astype(int),
            'iherb_stock_status': df[col_state],
        })
        
        def _compose_url(p, i, v):
            p = str(p or "").strip()
            i = str(i or "").strip()
            v = str(v or "").strip()
            base = "https://www.coupang.com/vp/products"
            if p and i:
                url = f"{base}/{p}?itemId={i}"
                if v:
                    url += f"&vendorItemId={v}"
                return url
            return ""
        
        result['iherb_url'] = [
            _compose_url(
                result.at[idx, 'iherb_product_id'],
                result.at[idx, 'iherb_item_id'],
                result.at[idx, 'iherb_vendor_id']
            )
            for idx in result.index
        ]
        
        result['iherb_pack'] = result['iherb_product_name'].apply(extract_pack_count)
        result = result[result['iherb_vendor_id'] != '<NA>'].copy()
        
        print(f"   ✓ {len(result):,}개 상품")
        print(f"   ✓ Product ID 있음: {(result['iherb_product_id'] != '').sum():,}개")
        print(f"   ✓ 정가(할인율기준가) 있음: {(result['iherb_original_price'] > 0).sum():,}개")
        
        return result
    
    def _load_seller_insights_df(self) -> pd.DataFrame:
        """아이허브 판매 성과 로드 (Excel)"""
        
        print(f"\n📥 3. 아이허브 판매 성과 (Excel)")
        
        files = list(self.excel_dir.glob("*SELLER_INSIGHTS*.xlsx"))
        if not files:
            print(f"   ⚠️  SELLER_INSIGHTS 파일 없음")
            return pd.DataFrame()
        
        latest = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
        print(f"   파일: {latest.name}")
        
        df = pd.read_excel(latest, sheet_name='vendor item metrics')
        
        def to_int_series(s):
            return pd.to_numeric(s, errors='coerce').fillna(0).astype(int)
        
        def to_percent_series(s):
            return pd.to_numeric(s, errors='coerce').fillna(0.0).round(1)
        
        result = pd.DataFrame({
            'iherb_vendor_id': df['옵션 ID'].astype('Int64').astype(str),
            'iherb_category': df.get('카테고리'),
            'iherb_revenue': to_int_series(df.get('매출(원)', 0)),
            'iherb_orders': to_int_series(df.get('주문', 0)),
            'iherb_sales_quantity': to_int_series(df.get('판매량', 0)),
            'iherb_visitors': to_int_series(df.get('방문자', 0)),
            'iherb_views': to_int_series(df.get('조회', 0)),
            'iherb_cart_adds': to_int_series(df.get('장바구니', 0)),
            'iherb_conversion_rate': to_percent_series(df.get('구매전환율', 0)),
            'iherb_item_winner_ratio': to_percent_series(df.get('아이템위너 비율(%)', 0)),
            'iherb_total_revenue': to_int_series(df.get('총 매출(원)', 0)),
            'iherb_total_cancel_amount': to_int_series(df.get('총 취소 금액', 0)),
            'iherb_total_cancel_quantity': to_int_series(df.get('총 취소된 상품수', 0)),
        })
        
        result = result[result['iherb_vendor_id'] != '<NA>'].copy()
        
        result['iherb_cancel_rate'] = (
            result['iherb_total_cancel_quantity'] / result['iherb_sales_quantity'] * 100
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0).round(1)
        
        print(f"   ✓ {len(result):,}개 상품")
        
        return result
    
    def _load_coupang_recommended_price_df(self) -> pd.DataFrame:
        """쿠팡 추천가 로드 (Excel)"""
        
        print(f"\n📥 4. 쿠팡 추천가 (Excel)")
        
        files = list(self.excel_dir.glob("Coupang_Price_*.xlsx"))
        if not files:
            print(f"   ⚠️  Coupang_Price 파일 없음")
            return pd.DataFrame()
        
        latest = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
        print(f"   파일: {latest.name}")
        
        df = pd.read_excel(latest, header=1)
        
        result = pd.DataFrame({
            'iherb_product_id': df['노출상품ID'].astype(str).str.replace(r'\.0$', '', regex=True),
            'iherb_vendor_id': df['옵션ID'].astype(str).str.replace(r'\.0$', '', regex=True),
            'iherb_recommended_price': pd.to_numeric(df['쿠팡추천가 (원)'], errors='coerce').fillna(0).astype(int)
        })
        
        print(f"   ✓ {len(result):,}개 상품")
        
        return result
    
    def _load_upc_df(self) -> pd.DataFrame:
        """UPC 데이터 로드 (Excel)"""
        
        print(f"\n📥 5. UPC 데이터 (Excel)")
        
        files = list(self.excel_dir.glob("20251024_*.xlsx"))
        if not files:
            print(f"   ⚠️  UPC 파일 없음")
            return pd.DataFrame()
        
        latest = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
        print(f"   파일: {latest.name}")
        
        df = pd.read_excel(latest)
        
        col_item_id = None
        col_upc = None
        
        for col in df.columns:
            if '쿠팡 상품번호' in str(col) or '상품번호' in str(col):
                col_item_id = col
            if 'UPC' in str(col).upper():
                col_upc = col
        
        if col_item_id is None or col_upc is None:
            print(f"   ⚠️  필수 컬럼 없음 (쿠팡 상품번호, UPC)")
            return pd.DataFrame()
        
        result = pd.DataFrame({
            'iherb_item_id': df[col_item_id].astype(str).str.replace(r'\.0$', '', regex=True),
            'iherb_upc': df[col_upc].astype(str).str.strip()
        })
        
        result = result.drop_duplicates(subset=['iherb_item_id'], keep='first')
        
        print(f"   ✓ {len(result):,}개 상품")
        
        return result
    
    def _integrate_iherb(self, df_price: pd.DataFrame, df_insights: pd.DataFrame, 
                         df_recommended: pd.DataFrame, df_upc: pd.DataFrame) -> pd.DataFrame:
        """아이허브 가격 + 성과 + 추천가 + UPC 통합"""
        
        print(f"\n🔗 6. 아이허브 데이터 통합")
        
        if df_insights.empty:
            df = df_price
            print(f"   ⚠️  성과 데이터 없음")
        else:
            df = df_price.merge(df_insights, on='iherb_vendor_id', how='left')
            print(f"   ✓ 성과 데이터 병합: {df['iherb_revenue'].notna().sum():,}개")
        
        if df_recommended.empty:
            print(f"   ⚠️  추천가 데이터 없음")
        else:
            df = df.merge(df_recommended[['iherb_vendor_id', 'iherb_recommended_price']], 
                         on='iherb_vendor_id', how='left')
            print(f"   ✓ 추천가 데이터 병합: {df['iherb_recommended_price'].notna().sum():,}개")
        
        if df_upc.empty:
            print(f"   ⚠️  UPC 데이터 없음")
        else:
            df = df.merge(df_upc[['iherb_item_id', 'iherb_upc']], 
                         on='iherb_item_id', how='left')
            print(f"   ✓ UPC 데이터 병합: {df['iherb_upc'].notna().sum():,}개")
        
        print(f"   ✓ 통합 완료: {len(df):,}개")
        
        return df
    
    def _integrate_all_by_product_id(self, df_rocket: pd.DataFrame, df_iherb: pd.DataFrame) -> pd.DataFrame:
        """전체 통합 (Product ID 기반 매칭 - 1:1 Best Match)"""

        print(f"\n🔗 7. 전체 통합 (Product ID 기반 1:1 매칭)")

        df_rocket['rocket_pack']   = df_rocket['rocket_product_name'].apply(extract_pack_count)
        df_rocket['rocket_unit']   = df_rocket['rocket_product_name'].apply(extract_unit_count)
        df_rocket['rocket_weight'] = df_rocket['rocket_product_name'].apply(extract_weight)

        df_iherb['iherb_pack']   = df_iherb['iherb_product_name'].apply(extract_pack_count)
        df_iherb['iherb_unit']   = df_iherb['iherb_product_name'].apply(extract_unit_count)
        df_iherb['iherb_weight'] = df_iherb['iherb_product_name'].apply(extract_weight)

        matched_pairs = []
        
        for rocket_idx, rocket_row in df_rocket.iterrows():
            rocket_pid = rocket_row['rocket_product_id']
            candidates = df_iherb[df_iherb['iherb_product_id'] == rocket_pid].copy()
            
            if candidates.empty:
                matched_pairs.append({
                    **rocket_row.to_dict(),
                    'matched_iherb_idx': None
                })
                continue
            
            rocket_pack   = rocket_row['rocket_pack']
            rocket_unit   = rocket_row['rocket_unit']
            rocket_weight = rocket_row['rocket_weight']
            
            best_idx = None
            
            # 우선순위별 매칭
            best = candidates[
                (candidates['iherb_pack']   == rocket_pack) &
                (candidates['iherb_unit']   == rocket_unit) &
                (candidates['iherb_weight'] == rocket_weight)
            ]
            if len(best) > 0:
                best_idx = best.index[0]
            
            if best_idx is None:
                best = candidates[
                    (candidates['iherb_pack'] == rocket_pack) &
                    (candidates['iherb_unit'] == rocket_unit)
                ]
                if len(best) > 0:
                    best_idx = best.index[0]
            
            if best_idx is None:
                best = candidates[
                    (candidates['iherb_pack']   == rocket_pack) &
                    (candidates['iherb_weight'] == rocket_weight)
                ]
                if len(best) > 0:
                    best_idx = best.index[0]
            
            if best_idx is None:
                best = candidates[
                    (candidates['iherb_pack'] == rocket_pack)
                ]
                if len(best) > 0:
                    best_idx = best.index[0]
            
            if best_idx is None:
                best = candidates[
                    (candidates['iherb_weight'] == rocket_weight) &
                    (candidates['iherb_pack'].isna() | pd.isna(rocket_pack))
                ]
                if len(best) > 0:
                    best_idx = best.index[0]
            
            if best_idx is None:
                best = candidates[
                    (candidates['iherb_unit'] == rocket_unit) &
                    (candidates['iherb_pack'].isna() | pd.isna(rocket_pack))
                ]
                if len(best) > 0:
                    best_idx = best.index[0]
            
            matched_pairs.append({
                **rocket_row.to_dict(),
                'matched_iherb_idx': best_idx
            })
        
        df_final = pd.DataFrame(matched_pairs)
        
        for idx, row in df_final.iterrows():
            iherb_idx = row['matched_iherb_idx']
            if iherb_idx is not None and not pd.isna(iherb_idx):
                try:
                    iherb_row = df_iherb.loc[iherb_idx]
                    for col in df_iherb.columns:
                        df_final.at[idx, col] = iherb_row[col]
                except KeyError:
                    pass
        
        df_final = df_final.drop(columns=['matched_iherb_idx'])
        
        df_final['matching_method'] = '미매칭'
        df_final['matching_confidence'] = ''
        
        matched_mask = df_final['iherb_vendor_id'].notna()
        df_final.loc[matched_mask, 'matching_method'] = 'Product ID'
        
        high_conf = (
            matched_mask 
            & (df_final['rocket_pack'] == df_final['iherb_pack'])
            & (df_final['rocket_unit'] == df_final['iherb_unit'])
        )
        
        medium_conf = (
            matched_mask 
            & ~high_conf
            & (
                ((df_final['rocket_pack'] == df_final['iherb_pack']) & (df_final['iherb_unit'].isna() | df_final['rocket_unit'].isna())) |
                ((df_final['rocket_unit'] == df_final['iherb_unit']) & (df_final['iherb_pack'].isna() | df_final['rocket_pack'].isna()))
            )
        )
        
        low_conf = matched_mask & ~high_conf & ~medium_conf
        
        df_final.loc[high_conf, 'matching_confidence'] = 'High'
        df_final.loc[medium_conf, 'matching_confidence'] = 'Medium'
        df_final.loc[low_conf, 'matching_confidence'] = 'Low'
        
        df_final = df_final[[c for c in df_final.columns if not c.endswith('_dup')]]
        
        # 가격 비교 계산
        rp = pd.to_numeric(df_final['rocket_price'], errors='coerce')
        ip = pd.to_numeric(df_final['iherb_price'], errors='coerce')
        op = pd.to_numeric(df_final['iherb_original_price'], errors='coerce')
        rec_p = pd.to_numeric(df_final.get('iherb_recommended_price', 0), errors='coerce')
        
        valid = rp.gt(0) & ip.gt(0)
        valid_rec = ip.gt(0) & rec_p.gt(0)
        valid_req = op.gt(0) & rec_p.gt(0)
        
        df_final['price_diff'] = pd.NA
        df_final['price_diff_pct'] = pd.NA
        df_final['cheaper_source'] = pd.NA
        df_final['breakeven_discount_rate'] = pd.NA
        df_final['recommended_discount_rate'] = pd.NA
        df_final['requested_discount_rate'] = pd.NA
        
        diff = (ip - rp).where(valid).astype('float')
        pct = (diff / rp * 100).where(valid).replace([np.inf, -np.inf], np.nan).round(1)
        breakeven = ((ip - rp) / ip * 100).where(valid).replace([np.inf, -np.inf], np.nan).round(1)
        recommended = ((ip - rec_p) / ip * 100).where(valid_rec).replace([np.inf, -np.inf], np.nan).round(1)
        requested = ((op - rec_p) / op * 100).where(valid_req).replace([np.inf, -np.inf], np.nan).round(1)
        
        df_final.loc[valid, 'price_diff'] = diff[valid]
        df_final.loc[valid, 'price_diff_pct'] = pct[valid]
        df_final.loc[valid, 'breakeven_discount_rate'] = breakeven[valid]
        df_final.loc[valid_rec, 'recommended_discount_rate'] = recommended[valid_rec]
        df_final.loc[valid_req, 'requested_discount_rate'] = requested[valid_req]
        df_final.loc[valid, 'cheaper_source'] = np.where(
            df_final.loc[valid, 'price_diff'] > 0, '로켓직구',
            np.where(df_final.loc[valid, 'price_diff'] < 0, '아이허브', '동일')
        )
        
        matched_count = df_final['iherb_vendor_id'].notna().sum()
        print(f"   ✓ 최종 매칭: {matched_count:,}개 ({matched_count/len(df_final)*100:.1f}%)")
        
        if matched_count > 0:
            cheaper_counts = df_final['cheaper_source'].value_counts()
            print(f"\n   💰 가격 경쟁력:")
            for source, count in cheaper_counts.items():
                pct_val = count / matched_count * 100
                print(f"      • {source}: {count:,}개 ({pct_val:.1f}%)")
            
            conf_counts = df_final[matched_mask]['matching_confidence'].value_counts()
            print(f"\n   📊 매칭 신뢰도:")
            for conf, count in conf_counts.items():
                pct_val = count / matched_count * 100
                print(f"      • {conf}: {count:,}개 ({pct_val:.1f}%)")
        
        return df_final


def main():
    """테스트"""
    manager = DataManager()
    
    # 미매칭 상품 포함
    df = manager.get_integrated_df(include_unmatched=True)
    
    print(f"\n최종 결과:")
    print(f"  - 총 상품: {len(df):,}개")
    print(f"  - 로켓 매칭: {(df['matching_status'] == '로켓매칭').sum():,}개")
    print(f"  - 미매칭 우수: {(df['matching_status'] == '미매칭').sum():,}개")


if __name__ == "__main__":
    main()