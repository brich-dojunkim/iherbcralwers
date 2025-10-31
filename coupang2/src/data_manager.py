#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
통합 데이터 관리자 - 수정본 (최소 변경 패치)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 핵심 패치:
- 정규화 유틸 추가(norm_part/norm_upc, 엑셀 헤더 자동탐지)
- 매칭 CSV/아이허브 엑셀에서 part_norm/upc_norm 생성
- _integrate_all()에서 '파트넘버 우선 + UPC 보조' 조인만 보강
  (기존 흐름/출력/함수 이름/인자 유지)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional
import numpy as np

# ─────────────────────────────────────────────────────────
# [PATCH] 키 정규화 & 엑셀 헤더 자동탐지 유틸 (추가)
# ─────────────────────────────────────────────────────────
import re

def norm_part(s):
    """파트넘버 정규화: 영숫자만 남기고 대문자."""
    if s is None:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", str(s)).upper()

def norm_upc(x):
    """UPC 정규화: 숫자만 남기고 12자리 맞춤(좌측 0패딩, 과잉은 자름)."""
    if x is None:
        return ""
    digits = re.sub(r"\D", "", str(x))
    if not digits:
        return ""
    return digits.zfill(12)[:12]

def safe_read_excel_header_guess(path, max_try=20):
    """
    상단 안내문 줄이 있는 엑셀에서 실제 헤더를 자동 탐색.
    '업체상품코드' / '옵션 ID' / '업체상품 ID' / '바코드' 중 하나라도 보이면 헤더로 간주.
    """
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

def extract_pack_count(name: str):
    """
    상품명에서 '... 2개', '... 1개' 등 마지막 등장하는 '~개'의 숫자 부분을 추출.
    - 여러 번 등장하면 '마지막' 항목을 사용
    - 없으면 NaN 반환
    """
    if not isinstance(name, str):
        return np.nan
    matches = re.findall(r'(\d+)\s*개', name)
    if not matches:
        return np.nan
    try:
        return int(matches[-1])
    except Exception:
        return np.nan
# ─────────────────────────────────────────────────────────


class DataManager:
    """통합 데이터 관리"""
    
    def __init__(self, 
                 db_path: str = "monitoring.db", 
                 rocket_csv_path: str = "data/rocket/rocket.csv",
                 excel_dir: str = "data/iherb"):
        """
        Args:
            db_path: 로켓직구 모니터링 DB 경로
            rocket_csv_path: 매칭 CSV 경로 (vendor_item_id 기반)
            excel_dir: 아이허브 Excel 파일 디렉토리
        """
        self.db_path = db_path
        self.rocket_csv_path = rocket_csv_path
        self.excel_dir = Path(excel_dir)
    
    def get_integrated_df(self, target_date: Optional[str] = None) -> pd.DataFrame:
        """
        통합 데이터프레임 생성
        
        Returns:
            DataFrame with columns:
            
            [로켓직구]
            - rocket_vendor_id, rocket_product_name, rocket_category
            - rocket_rank, rocket_price, rocket_original_price
            - rocket_discount_rate, rocket_rating, rocket_reviews
            - rocket_url
            
            [매칭 정보]
            - part_number (아이허브_파트넘버)
            - upc (아이허브_UPC, 참고용)
            
            [아이허브 가격/재고]
            - iherb_vendor_id, iherb_product_name
            - iherb_price, iherb_stock, iherb_stock_status
            - iherb_part_number
            
            [아이허브 판매 성과]
            - iherb_category, iherb_revenue, iherb_orders
            - iherb_sales_quantity, iherb_visitors, iherb_views
            - iherb_cart_adds, iherb_conversion_rate
            - iherb_total_revenue, iherb_total_cancel_amount
            - iherb_total_cancel_quantity, iherb_cancel_rate
            
            [가격 비교]
            - price_diff, price_diff_pct, cheaper_source
        """
        
        print(f"\n{'='*80}")
        print(f"🔗 통합 데이터프레임 생성")
        print(f"{'='*80}\n")
        
        # 1. 로켓직구 데이터 (DB)
        df_rocket = self._load_rocket_df(target_date)
        
        # 2. 매칭 데이터 (CSV)
        df_matching = self._load_matching_df()
        
        # 3. 로켓직구 + 매칭 조인
        df_rocket_matched = self._join_rocket_matching(df_rocket, df_matching)
        
        # 4. 아이허브 가격/재고 (Excel)
        df_price = self._load_price_inventory_df()
        
        # 5. 아이허브 성과 (Excel)
        df_insights = self._load_seller_insights_df()
        
        # 6. 아이허브 통합
        df_iherb = self._integrate_iherb(df_price, df_insights)
        
        # 7. 전체 통합
        df_final = self._integrate_all(df_rocket_matched, df_iherb)
        
        print(f"\n✅ 통합 완료: {len(df_final):,}개 레코드")
        print(f"   - 로켓직구: {len(df_rocket):,}개")
        print(f"   - 매칭 정보 있음: {(df_final['part_number'].notna() & (df_final['part_number'] != '')).sum():,}개")
        print(f"   - 아이허브 매칭: {df_final['iherb_vendor_id'].notna().sum():,}개")
        print(f"   - 최종 매칭률: {df_final['iherb_vendor_id'].notna().sum() / len(df_final) * 100:.1f}%\n")
        
        return df_final
    
    def _load_rocket_df(self, target_date: Optional[str]) -> pd.DataFrame:
        """로켓직구 데이터 로드 (DB) - 각 vendor_item_id의 최신 스냅샷"""
        
        print(f"📥 1. 로켓직구 데이터 (DB)")
        
        conn = sqlite3.connect(self.db_path)
        
        # 날짜 결정
        if target_date is None:
            target_date = conn.execute(
                "SELECT DATE(MAX(snapshot_time)) FROM snapshots"
            ).fetchone()[0]
        
        print(f"   날짜: {target_date}")
        
        # 수정된 쿼리: 각 vendor_item_id의 최신 스냅샷 데이터 가져오기
        query = """
        WITH latest_snapshots AS (
            SELECT 
                ps.vendor_item_id,
                MAX(snap.snapshot_time) as latest_time
            FROM product_states ps
            JOIN snapshots snap ON ps.snapshot_id = snap.id
            JOIN sources src ON snap.source_id = src.id
            WHERE src.source_type = 'rocket_direct'
              AND ps.vendor_item_id IS NOT NULL
              AND ps.vendor_item_id != ''
            GROUP BY ps.vendor_item_id
        )
        SELECT 
            ps.vendor_item_id as rocket_vendor_id,
            ps.product_name as rocket_product_name,
            cat.name as rocket_category,
            ps.category_rank as rocket_rank,
            ps.current_price as rocket_price,
            ps.original_price as rocket_original_price,
            ps.discount_rate as rocket_discount_rate,
            ps.rating_score as rocket_rating,
            ps.review_count as rocket_reviews,
            ps.product_url as rocket_url,
            DATE(snap.snapshot_time) as snapshot_date
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources src ON snap.source_id = src.id
        JOIN categories cat ON snap.category_id = cat.id
        JOIN latest_snapshots ls ON ps.vendor_item_id = ls.vendor_item_id 
                                  AND snap.snapshot_time = ls.latest_time
        WHERE src.source_type = 'rocket_direct'
        ORDER BY cat.name, ps.category_rank
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"   ✓ {len(df):,}개 상품")
        print(f"   ✓ 유니크 vendor_item_id: {df['rocket_vendor_id'].nunique():,}개")
        
        # 날짜별 분포 표시
        date_dist = df['snapshot_date'].value_counts().sort_index(ascending=False)
        print(f"   ✓ 스냅샷 날짜 분포:")
        for date, count in date_dist.head(3).items():
            print(f"      - {date}: {count:,}개")
        
        return df
    
    def _load_matching_df(self) -> pd.DataFrame:
        """매칭 데이터 로드"""
        
        print(f"\n📥 2. 매칭 데이터")
        
        df = pd.read_csv(self.rocket_csv_path)
        
        # vendor_item_id를 문자열로 변환 (DB와 타입 일치)
        result = pd.DataFrame({
            'vendor_item_id': df['vendor_item_id'].astype(str),
            'part_number': df['iherb_part_number'].fillna('').astype(str).str.strip().str.upper(),
            # NOTE: 기존 로직 유지 (Int64 -> str) + 아래 정규화 키 따로 생성
            'upc': df['iherb_upc'].astype('Int64').astype(str)
        })
        
        # UPC의 <NA> 제거 (기존 로직 유지)
        result['upc'] = result['upc'].replace('<NA>', '')
        
        # [PATCH] 정규화 키 추가 (조인에 사용)
        result['part_norm'] = result['part_number'].apply(norm_part)
        result['upc_norm']  = result['upc'].apply(norm_upc)
        
        print(f"   ✓ {len(result):,}개 매칭 정보")
        print(f"   ✓ 파트넘버 있음: {(result['part_number'] != '').sum():,}개")
        print(f"   ✓ UPC 있음: {(result['upc'] != '').sum():,}개")
        
        return result
    
    def _join_rocket_matching(self, df_rocket: pd.DataFrame, df_matching: pd.DataFrame) -> pd.DataFrame:
        """로켓직구 + 매칭 조인 (vendor_item_id 기반)"""
        
        print(f"\n🔗 3. 로켓직구 + 매칭 조인 (vendor_item_id 기반)")
        
        # DB의 vendor_item_id도 문자열로 변환
        df_rocket['rocket_vendor_id'] = df_rocket['rocket_vendor_id'].astype(str)
        
        # vendor_item_id로 조인 (기존 유지)
        df = df_rocket.merge(
            df_matching,
            left_on='rocket_vendor_id',
            right_on='vendor_item_id',
            how='left'
        )
        
        # 중복 컬럼 제거 (기존 유지)
        df = df.drop(columns=['vendor_item_id'])
        
        matched_count = (df['part_number'].notna() & (df['part_number'] != '')).sum()
        print(f"   ✓ 매칭 정보 있음: {matched_count:,}개 ({matched_count/len(df)*100:.1f}%)")
        
        return df
    
    def _load_price_inventory_df(self) -> pd.DataFrame:
        """아이허브 가격/재고 로드 (Excel) + ID 기반 Coupang 상세 URL 생성"""
        print(f"\n📥 4. 아이허브 가격/재고 (Excel)")

        files = list(self.excel_dir.glob("*price_inventory*.xlsx"))
        if not files:
            print(f"   ⚠️  price_inventory 파일 없음")
            return pd.DataFrame()

        latest = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
        print(f"   파일: {latest.name}")

        # 기존: sheet_name='data', skiprows=2 (헤더 위 설명 줄 제거용)
        try:
            df = pd.read_excel(latest, sheet_name='data', skiprows=2)
        except Exception:
            df = safe_read_excel_header_guess(latest, max_try=30)

        # 필수 컬럼 후보
        col_pid   = _pick_col(df, ['옵션 ID'])                       # = vendorItemId
        col_pname = _pick_col(df, ['쿠팡 노출 상품명', '상품명'])
        col_pn    = _pick_col(df, ['업체상품코드'])
        col_price = _pick_col(df, ['판매가격', '판매가격.1'])
        col_stock = _pick_col(df, ['잔여수량(재고)', '잔여수량'])
        col_state = _pick_col(df, ['판매상태', '판매상태.1'])
        col_barcd = _pick_col(df, ['바코드'])

        # URL 재구성용 ID (엑셀에 상세 URL이 없어도 이 3개로 복원)
        col_product_id = _pick_col(df, ['Product ID', 'productId', 'PRODUCT_ID'])   # = productId
        col_item_id    = _pick_col(df, ['업체상품 ID', 'itemId', 'ITEM_ID'])        # = itemId

        # 결측 방어
        if col_pid is None:   df['옵션 ID'] = None;              col_pid = '옵션 ID'
        if col_pname is None: df['쿠팡 노출 상품명'] = None;      col_pname = '쿠팡 노출 상품명'
        if col_pn is None:    df['업체상품코드'] = None;          col_pn = '업체상품코드'
        if col_price is None: df['판매가격'] = 0;                 col_price = '판매가격'
        if col_stock is None: df['잔여수량(재고)'] = 0;           col_stock = '잔여수량(재고)'
        if col_state is None: df['판매상태'] = None;              col_state = '판매상태'
        if col_barcd is None: df['바코드'] = None;                col_barcd = '바코드'

        # 기본 필드 구성
        result = pd.DataFrame({
            'iherb_vendor_id': df[col_pid].astype(str).str.split('.').str[0],   # vendorItemId
            'iherb_product_name': df[col_pname],
            'iherb_part_number': df[col_pn].astype(str).str.strip().str.upper(),
            'iherb_price': pd.to_numeric(df[col_price], errors='coerce').fillna(0).astype(int),
            'iherb_stock': pd.to_numeric(df[col_stock], errors='coerce').fillna(0).astype(int),
            'iherb_stock_status': df[col_state],
        })

        # 정규화 키
        result['part_norm'] = result['iherb_part_number'].apply(norm_part)
        result['upc_norm']  = df[col_barcd].apply(norm_upc)

        # ID 추출 (문자열화하며 소수점 꼬임 제거)
        result['iherb_product_id'] = (
            df[col_product_id].astype(str).str.replace(r'\.0$', '', regex=True)
            if col_product_id else ""
        )
        result['iherb_item_id'] = (
            df[col_item_id].astype(str).str.replace(r'\.0$', '', regex=True)
            if col_item_id else ""
        )

        # 상세 URL 생성 (엑셀에 URL이 없어도 ID 3종으로 복원)
        # 규칙: https://www.coupang.com/vp/products/{productId}?itemId={itemId}&vendorItemId={vendorItemId}
        def _compose_url(p, i, v, pn, upc):
            p = (p or "").strip()
            i = (i or "").strip()
            v = (v or "").strip()
            base = "https://www.coupang.com/vp/products"
            if p and i:
                url = f"{base}/{p}?itemId={i}"
                if v:
                    url += f"&vendorItemId={v}"
                return url
            if p and v:
                return f"{base}/{p}?vendorItemId={v}"
            if v:
                return f"{base}?vendorItemId={v}"
            # 마지막 폴백: 검색
            if pn:
                return f"https://www.coupang.com/np/search?component=&q={pn}"
            if upc:
                return f"https://www.coupang.com/np/search?component=&q={upc}"
            return ""

        result['iherb_url'] = [
            _compose_url(
                result.at[idx, 'iherb_product_id'],
                result.at[idx, 'iherb_item_id'],
                result.at[idx, 'iherb_vendor_id'],
                result.at[idx, 'iherb_part_number'],
                result.at[idx, 'upc_norm']
            )
            for idx in result.index
        ]

        # 팩 수 추출 (아이허브측 명칭에서 '... N개'의 마지막 값)
        result['iherb_pack'] = result['iherb_product_name'].apply(extract_pack_count)

        # '<NA>' vendor 제거
        result = result[result['iherb_vendor_id'] != '<NA>'].copy()

        print(f"   ✓ {len(result):,}개 상품")
        print(f"   ✓ 업체상품코드 있음: {(result['iherb_part_number'] != '').sum():,}개")
        return result
        
    def _load_seller_insights_df(self) -> pd.DataFrame:
        """아이허브 판매 성과 로드 (Excel) — 변수명에 '$' 미사용(최소변경)"""
        print(f"\n📥 5. 아이허브 판매 성과 (Excel)")

        files = list(self.excel_dir.glob("*SELLER_INSIGHTS*.xlsx"))
        if not files:
            print(f"   ⚠️  SELLER_INSIGHTS 파일 없음")
            return pd.DataFrame()

        latest = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
        print(f"   파일: {latest.name}")

        # 기존 시트명 유지
        df = pd.read_excel(latest, sheet_name='vendor item metrics')

        # ✅ 여기부터 변수명에 '$'가 들어가지 않도록 모두 정리
        col_option_id                = '옵션 ID'
        col_category                 = '카테고리'
        col_revenue_won              = '매출(원)'
        col_orders                   = '주문'
        col_sales_quantity           = '판매량'
        col_visitors                 = '방문자'
        col_views                    = '조회'
        col_cart_adds                = '장바구니'
        col_conversion_rate          = '구매전환율'            # 예: "12.3%" 형태일 수 있음
        col_total_revenue_won        = '총 매출(원)'
        col_total_cancel_amount      = '총 취소 금액'          # (이전 코드에서 col_total_can$ 같은 이름 금지)
        col_total_cancel_quantity    = '총 취소된 상품수'

        # 방어적: 만약 헤더가 바뀌었을 때를 위한 후보군 선택
        def _pick(df_, candidates):
            for c in candidates:
                if c in df_.columns:
                    return c
            return None

        col_option_id             = _pick(df, [col_option_id, '옵션id', '옵션Id', '옵션 id', 'option id', 'option_id']) or '옵션 ID'
        col_category              = _pick(df, [col_category, 'category']) or '카테고리'
        col_revenue_won           = _pick(df, [col_revenue_won, '매출', 'revenue', '매출(￦)', '매출(₩)']) or '매출(원)'
        col_orders                = _pick(df, [col_orders, '주문수', 'orders']) or '주문'
        col_sales_quantity        = _pick(df, [col_sales_quantity, '판매 수량', 'sales quantity', 'sales_qty']) or '판매량'
        col_visitors              = _pick(df, [col_visitors, 'visitors']) or '방문자'
        col_views                 = _pick(df, [col_views, 'views']) or '조회'
        col_cart_adds             = _pick(df, [col_cart_adds, 'cart adds', '장바구니수']) or '장바구니'
        col_conversion_rate       = _pick(df, [col_conversion_rate, '구매전환율(%)', 'conversion rate']) or '구매전환율'
        col_total_revenue_won     = _pick(df, [col_total_revenue_won, '총매출(원)', '총매출']) or '총 매출(원)'
        col_total_cancel_amount   = _pick(df, [col_total_cancel_amount, '총 취소금액', '총취소금액', 'total cancel amount']) or '총 취소 금액'
        col_total_cancel_quantity = _pick(df, [col_total_cancel_quantity, '총 취소 상품수', 'total cancel quantity']) or '총 취소된 상품수'

        # 변환 유틸
        def to_int_series(s):
            return pd.to_numeric(s, errors='coerce').fillna(0).astype(int)

        def to_percent_series(s):
            # "12.3%" → 12.3 (숫자)
            return (
                s.astype(str)
                .str.replace('%', '', regex=False)
                .str.replace(',', '', regex=False)
                .str.strip()
                .replace({'nan': None})
            ).astype(float).fillna(0.0)

        # 결과 구성 (기존 키 유지)
        result = pd.DataFrame({
            'iherb_vendor_id': df[col_option_id].astype('Int64').astype(str),
            'iherb_category': df.get(col_category),
            'iherb_revenue': to_int_series(df.get(col_revenue_won, 0)),
            'iherb_orders': to_int_series(df.get(col_orders, 0)),
            'iherb_sales_quantity': to_int_series(df.get(col_sales_quantity, 0)),
            'iherb_visitors': to_int_series(df.get(col_visitors, 0)),
            'iherb_views': to_int_series(df.get(col_views, 0)),
            'iherb_cart_adds': to_int_series(df.get(col_cart_adds, 0)),
            'iherb_conversion_rate': to_percent_series(df.get(col_conversion_rate, 0)),
            'iherb_total_revenue': to_int_series(df.get(col_total_revenue_won, 0)),
            'iherb_total_cancel_amount': to_int_series(df.get(col_total_cancel_amount, 0)),
            'iherb_total_cancel_quantity': to_int_series(df.get(col_total_cancel_quantity, 0)),
        })

        # 옵션 ID '<NA>' 제거
        result = result[result['iherb_vendor_id'] != '<NA>'].copy()

        # 파생: 취소율(%)
        result['iherb_cancel_rate'] = (
            result['iherb_total_cancel_quantity'] / result['iherb_sales_quantity'] * 100
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0).round(1)

        print(f"   ✓ {len(result):,}개 상품")
        return result

    def _integrate_iherb(self, df_price: pd.DataFrame, df_insights: pd.DataFrame) -> pd.DataFrame:
        """아이허브 가격 + 성과 통합"""
        
        print(f"\n🔗 6. 아이허브 데이터 통합")
        
        if df_insights.empty:
            print(f"   ⚠️  성과 데이터 없음")
            return df_price
        
        df = df_price.merge(df_insights, on='iherb_vendor_id', how='left')
        
        print(f"   ✓ 통합 완료: {len(df):,}개")
        print(f"   ✓ 성과 데이터 있음: {df['iherb_revenue'].notna().sum():,}개")
        
        return df
    
    def _integrate_all(self, df_rocket: pd.DataFrame, df_iherb: pd.DataFrame) -> pd.DataFrame:
        """전체 통합 (파트넘버 1차 + UPC 보조, 둘 다 '마지막 ~개' 일치 강제)"""

        print(f"\n🔗 7. 전체 통합 (파트넘버 매칭)")

        # 정규화/팩수 준비
        if 'part_norm' not in df_rocket.columns:
            df_rocket['part_norm'] = df_rocket['part_number'].apply(norm_part)
        if 'upc_norm' not in df_rocket.columns:
            df_rocket['upc_norm'] = df_rocket['upc'].apply(norm_upc)
        if 'rocket_pack' not in df_rocket.columns:
            df_rocket['rocket_pack'] = df_rocket['rocket_product_name'].apply(extract_pack_count)

        if 'part_norm' not in df_iherb.columns:
            df_iherb['part_norm'] = df_iherb['iherb_part_number'].apply(norm_part)
        if 'upc_norm' not in df_iherb.columns:
            df_iherb['upc_norm'] = df_iherb.get('upc_norm', '')
        if 'iherb_pack' not in df_iherb.columns:
            df_iherb['iherb_pack'] = df_iherb['iherb_product_name'].apply(extract_pack_count)

        # 1) 파트넘버 우선 병합
        df = df_rocket.merge(
            df_iherb,
            on='part_norm',
            how='left',
            suffixes=('', '_dup')
        )

        # 파트 매칭 후 '마지막 ~개' 불일치 시 → 아이허브쪽을 전부 미매칭 처리
        iherb_cols = [
            'iherb_vendor_id','iherb_product_name','iherb_price','iherb_stock',
            'iherb_stock_status','iherb_part_number','iherb_category','iherb_revenue',
            'iherb_orders','iherb_sales_quantity','iherb_visitors','iherb_views',
            'iherb_cart_adds','iherb_conversion_rate','iherb_total_revenue',
            'iherb_total_cancel_amount','iherb_total_cancel_quantity','iherb_cancel_rate',
            'iherb_url','iherb_product_id','iherb_item_id','iherb_pack'
        ]
        for c in iherb_cols:
            if c not in df.columns:
                df[c] = pd.NA

        mismatch_mask = (
            df['iherb_vendor_id'].notna()
            & df['rocket_pack'].notna()
            & df['iherb_pack'].notna()
            & (df['rocket_pack'] != df['iherb_pack'])
        )
        if mismatch_mask.any():
            df.loc[mismatch_mask, iherb_cols] = pd.NA

        # 2) UPC 보조 병합: "파트 미매칭 + 로켓 UPC 존재" + 팩수 일치
        need_upc = df['iherb_vendor_id'].isna()
        mask_rocket_has_upc = need_upc & df['upc_norm'].fillna('').ne('')

        df_iherb_upc = df_iherb[df_iherb['upc_norm'].fillna('').ne('')].copy()
        if not df_iherb_upc.empty:
            vc = df_iherb_upc['upc_norm'].value_counts()
            unique_upc = vc[vc == 1].index
            df_iherb_upc_unique = df_iherb_upc[df_iherb_upc['upc_norm'].isin(unique_upc)].copy()
        else:
            df_iherb_upc_unique = df_iherb_upc

        if mask_rocket_has_upc.any() and not df_iherb_upc_unique.empty:
            left_subset = df.loc[mask_rocket_has_upc, ['upc_norm','rocket_pack']].copy()

            upc_join = left_subset.merge(
                df_iherb_upc_unique[[
                    'upc_norm','iherb_vendor_id','iherb_product_name','iherb_price',
                    'iherb_stock','iherb_stock_status','iherb_part_number','iherb_category',
                    'iherb_revenue','iherb_orders','iherb_sales_quantity','iherb_visitors',
                    'iherb_views','iherb_cart_adds','iherb_conversion_rate','iherb_total_revenue',
                    'iherb_total_cancel_amount','iherb_total_cancel_quantity','iherb_cancel_rate',
                    'iherb_url','iherb_product_id','iherb_item_id','iherb_pack'
                ]],
                on='upc_norm', how='left'
            )

            # 팩수 일치 조건 필수
            accept_mask = (
                upc_join['iherb_vendor_id'].notna()
                & (upc_join['iherb_pack'].fillna(-1) == upc_join['rocket_pack'].fillna(-1))
            )
            if accept_mask.any():
                fill_cols = [
                    'iherb_vendor_id','iherb_product_name','iherb_price',
                    'iherb_stock','iherb_stock_status','iherb_part_number','iherb_category',
                    'iherb_revenue','iherb_orders','iherb_sales_quantity','iherb_visitors',
                    'iherb_views','iherb_cart_adds','iherb_conversion_rate','iherb_total_revenue',
                    'iherb_total_cancel_amount','iherb_total_cancel_quantity','iherb_cancel_rate',
                    'iherb_url','iherb_product_id','iherb_item_id','iherb_pack'
                ]
                target_idx = df.index[mask_rocket_has_upc]
                df.loc[target_idx[accept_mask.values], fill_cols] = upc_join.loc[accept_mask, fill_cols].values

        # *_dup 정리
        df = df[[c for c in df.columns if not c.endswith('_dup')]]

        # 가격 비교 계산
        rp = pd.to_numeric(df['rocket_price'], errors='coerce')
        ip = pd.to_numeric(df['iherb_price'],  errors='coerce')
        valid = rp.gt(0) & ip.gt(0)

        df['price_diff'] = pd.NA
        df['price_diff_pct'] = pd.NA
        df['cheaper_source'] = pd.NA

        diff = (ip - rp).where(valid)
        pct  = (diff / rp * 100).where(valid)

        diff = diff.astype('float')
        pct  = pct.astype('float').replace([np.inf, -np.inf], np.nan).round(1)

        df.loc[valid, 'price_diff'] = diff[valid]
        df.loc[valid, 'price_diff_pct'] = pct[valid]
        df.loc[valid, 'cheaper_source'] = np.where(
            df.loc[valid, 'price_diff'] > 0, '로켓직구',
            np.where(df.loc[valid, 'price_diff'] < 0, '아이허브', '동일')
        )

        matched_count = df['iherb_vendor_id'].notna().sum()
        print(f"   ✓ 최종 매칭: {matched_count:,}개 ({matched_count/len(df)*100:.1f}%)")
        if matched_count > 0:
            cheaper_counts = df['cheaper_source'].value_counts()
            print(f"\n   💰 가격 경쟁력:")
            for source, count in cheaper_counts.items():
                pct_val = count / matched_count * 100
                print(f"      • {source}: {count:,}개 ({pct_val:.1f}%)")

        return df


def main():
    """사용 예시"""
    
    print(f"\n{'='*80}")
    print(f"📊 DataManager 사용 예시 (수정본 - 최신 스냅샷 기반)")
    print(f"{'='*80}\n")
    
    manager = DataManager(
        db_path="/Users/brich/Desktop/iherb_price/coupang2/data/rocket/monitoring.db",
        rocket_csv_path="/Users/brich/Desktop/iherb_price/coupang2/data/rocket/rocket.csv",
        excel_dir="/Users/brich/Desktop/iherb_price/coupang2/data/iherb"
    )
    
    df = manager.get_integrated_df(target_date=None)
    
    print(f"\n{'='*80}")
    print(f"📋 매칭된 데이터 샘플 (상위 10개)")
    print(f"{'='*80}\n")
    
    matched = df[df['iherb_vendor_id'].notna()].head(10)
    
    if len(matched) > 0:
        display_cols = [
            'rocket_product_name', 'rocket_rank', 'rocket_price', 'part_number',
            'iherb_product_name', 'iherb_price', 'price_diff', 'cheaper_source'
        ]
        print(matched[display_cols].to_string(index=False))
    else:
        print("매칭된 데이터가 없습니다.")
    
    print(f"\n{'='*80}")
    print(f"✅ 완료!")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
