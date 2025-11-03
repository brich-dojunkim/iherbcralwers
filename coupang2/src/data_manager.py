#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
통합 데이터 관리자 - Product ID 기반 매칭
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 핵심 변경:
- 품번 매칭 → Product ID 기반 매칭으로 전환
- Product ID 중복 시 팩 수 일치 조건으로 필터링
- 매칭 방식 및 신뢰도 기록
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
    """
    상품명에서 '... 2개', '... 1개' 등 마지막 등장하는 '~개'의 숫자 부분을 추출.
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


def extract_unit_count(name: str):
    """
    상품명에서 '200정', '100정' 등의 개수를 추출.
    """
    if not isinstance(name, str):
        return np.nan
    
    # '200정', '100정' 패턴
    matches = re.findall(r'(\d+)\s*정', name)
    if matches:
        try:
            return int(matches[0])  # 첫 번째 매칭 사용
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
    """
    상품명에서 용량을 추출하여 g 단위로 반환.
    예: '1.64kg' → 1640, '907g' → 907, '5lb' → 2267.96
    """
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
    """통합 데이터 관리 - Product ID 기반 매칭"""
    
    def __init__(self, 
                 db_path: str = "monitoring.db", 
                 rocket_csv_path: str = "data/rocket/rocket.csv",
                 excel_dir: str = "data/iherb"):
        """
        Args:
            db_path: 로켓직구 모니터링 DB 경로
            rocket_csv_path: 매칭 CSV 경로 (참고용, 사용 안 함)
            excel_dir: 아이허브 Excel 파일 디렉토리
        """
        self.db_path = db_path
        self.rocket_csv_path = rocket_csv_path
        self.excel_dir = Path(excel_dir)
    
    def get_integrated_df(self, target_date: Optional[str] = None) -> pd.DataFrame:
        """
        통합 데이터프레임 생성 (Product ID 기반 매칭)
        
        Returns:
            DataFrame with columns:
            
            [로켓직구]
            - rocket_vendor_id, rocket_product_id, rocket_item_id
            - rocket_product_name, rocket_category
            - rocket_rank, rocket_price, rocket_original_price
            - rocket_discount_rate, rocket_rating, rocket_reviews
            - rocket_url
            
            [아이허브 가격/재고]
            - iherb_vendor_id, iherb_product_id, iherb_item_id
            - iherb_product_name
            - iherb_price, iherb_stock, iherb_stock_status
            - iherb_part_number
            
            [아이허브 판매 성과]
            - iherb_category, iherb_revenue, iherb_orders
            - iherb_sales_quantity, iherb_visitors, iherb_views
            - iherb_cart_adds, iherb_conversion_rate
            - iherb_total_revenue, iherb_total_cancel_amount
            - iherb_total_cancel_quantity, iherb_cancel_rate
            
            [매칭 정보]
            - matching_method, matching_confidence
            
            [가격 비교]
            - price_diff, price_diff_pct, cheaper_source
        """
        
        print(f"\n{'='*80}")
        print(f"🔗 통합 데이터프레임 생성 (Product ID 기반 매칭)")
        print(f"{'='*80}\n")
        
        # 1. 로켓직구 데이터 (DB)
        df_rocket = self._load_rocket_df(target_date)

        # 2. 아이허브 가격/재고 (Excel)
        df_price = self._load_price_inventory_df()

        # 3. 아이허브 성과 (Excel)
        df_insights = self._load_seller_insights_df()

        # 4. 아이허브 통합
        df_iherb = self._integrate_iherb(df_price, df_insights)

        # 5. 전체 통합 (Product ID 기반)
        df_final = self._integrate_all_by_product_id(df_rocket, df_iherb)
        
        print(f"\n✅ 통합 완료: {len(df_final):,}개 레코드")
        print(f"   - 로켓직구: {len(df_rocket):,}개")
        print(f"   - 아이허브 매칭: {df_final['iherb_vendor_id'].notna().sum():,}개")
        print(f"   - 최종 매칭률: {df_final['iherb_vendor_id'].notna().sum() / len(df_final) * 100:.1f}%\n")
        
        return df_final
    
    def _load_rocket_df(self, target_date: Optional[str]) -> pd.DataFrame:
        """로켓직구 데이터 로드 (DB) - 모든 카테고리의 최신 스냅샷을 포함"""

        print(f"📥 1. 로켓직구 데이터 (DB)")

        conn = sqlite3.connect(self.db_path)

        if target_date:
            # 특정 날짜에 대해 카테고리별 최신 스냅샷 ID를 구하는 서브쿼리
            subquery = f"""
                SELECT category_id, MAX(id) AS latest_id
                FROM snapshots
                WHERE DATE(snapshot_time) = '{target_date}'
                AND source_id = (SELECT id FROM sources WHERE source_type = 'rocket_direct')
                GROUP BY category_id
            """
        else:
            # 전체 기간에 대해 카테고리별 최신 스냅샷 ID를 구하는 서브쿼리
            subquery = """
                SELECT category_id, MAX(id) AS latest_id
                FROM snapshots
                WHERE source_id = (SELECT id FROM sources WHERE source_type = 'rocket_direct')
                GROUP BY category_id
            """

        # 각 카테고리의 최신 스냅샷에 속한 상품 상태를 조회
        query = f"""
            SELECT 
                ps.vendor_item_id  AS rocket_vendor_id,
                ps.product_id      AS rocket_product_id,
                ps.item_id         AS rocket_item_id,
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

        print(f"   ✓ {len(df):,}개 상품")
        print(f"   ✓ Product ID 있음: {df['rocket_product_id'].notna().sum():,}개")

        return df
    
    def _load_price_inventory_df(self) -> pd.DataFrame:
        """아이허브 가격/재고 로드 (Excel)"""
        
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
        
        # 컬럼 매핑
        col_vid = _pick_col(df, ['옵션 ID'])
        col_pid = _pick_col(df, ['Product ID', 'productId', 'PRODUCT_ID'])
        col_iid = _pick_col(df, ['업체상품 ID', 'itemId', 'ITEM_ID'])
        col_pname = _pick_col(df, ['쿠팡 노출 상품명', '상품명'])
        col_pn = _pick_col(df, ['업체상품코드'])
        col_price = _pick_col(df, ['판매가격', '판매가격.1'])
        col_stock = _pick_col(df, ['잔여수량(재고)', '잔여수량'])
        col_state = _pick_col(df, ['판매상태', '판매상태.1'])
        
        # 결측 방어
        if col_vid is None:   df['옵션 ID'] = None;              col_vid = '옵션 ID'
        if col_pid is None:   df['Product ID'] = None;           col_pid = 'Product ID'
        if col_iid is None:   df['업체상품 ID'] = None;          col_iid = '업체상품 ID'
        if col_pname is None: df['쿠팡 노출 상품명'] = None;      col_pname = '쿠팡 노출 상품명'
        if col_pn is None:    df['업체상품코드'] = None;          col_pn = '업체상품코드'
        if col_price is None: df['판매가격'] = 0;                 col_price = '판매가격'
        if col_stock is None: df['잔여수량(재고)'] = 0;           col_stock = '잔여수량(재고)'
        if col_state is None: df['판매상태'] = None;              col_state = '판매상태'
        
        # 기본 필드 구성
        result = pd.DataFrame({
            'iherb_vendor_id': df[col_vid].astype(str).str.split('.').str[0],
            'iherb_product_id': df[col_pid].astype(str).str.replace(r'\.0$', '', regex=True),
            'iherb_item_id': df[col_iid].astype(str).str.replace(r'\.0$', '', regex=True),
            'iherb_product_name': df[col_pname],
            'iherb_part_number': df[col_pn].astype(str).str.strip(),
            'iherb_price': pd.to_numeric(df[col_price], errors='coerce').fillna(0).astype(int),
            'iherb_stock': pd.to_numeric(df[col_stock], errors='coerce').fillna(0).astype(int),
            'iherb_stock_status': df[col_state],
        })
        
        # URL 생성
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
        
        # 팩 수 추출
        result['iherb_pack'] = result['iherb_product_name'].apply(extract_pack_count)
        
        # '<NA>' vendor 제거
        result = result[result['iherb_vendor_id'] != '<NA>'].copy()
        
        print(f"   ✓ {len(result):,}개 상품")
        print(f"   ✓ Product ID 있음: {(result['iherb_product_id'] != '').sum():,}개")
        
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
            'iherb_total_revenue': to_int_series(df.get('총 매출(원)', 0)),
            'iherb_total_cancel_amount': to_int_series(df.get('총 취소 금액', 0)),
            'iherb_total_cancel_quantity': to_int_series(df.get('총 취소된 상품수', 0)),
        })
        
        result = result[result['iherb_vendor_id'] != '<NA>'].copy()
        
        # 취소율(%)
        result['iherb_cancel_rate'] = (
            result['iherb_total_cancel_quantity'] / result['iherb_sales_quantity'] * 100
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0).round(1)
        
        print(f"   ✓ {len(result):,}개 상품")
        
        return result
    
    def _integrate_iherb(self, df_price: pd.DataFrame, df_insights: pd.DataFrame) -> pd.DataFrame:
        """아이허브 가격 + 성과 통합"""
        
        print(f"\n🔗 4. 아이허브 데이터 통합")
        
        if df_insights.empty:
            print(f"   ⚠️  성과 데이터 없음")
            return df_price
        
        df = df_price.merge(df_insights, on='iherb_vendor_id', how='left')
        
        print(f"   ✓ 통합 완료: {len(df):,}개")
        print(f"   ✓ 성과 데이터 있음: {df['iherb_revenue'].notna().sum():,}개")
        
        return df
    
    def _integrate_all_by_product_id(self, df_rocket: pd.DataFrame, df_iherb: pd.DataFrame) -> pd.DataFrame:
        """
        전체 통합 (Product ID 기반 매칭 - Best Match 선택)
        팩 수, 단위 수, 용량(무게)까지 비교하여 일치하는 후보를 우선순위로 매칭.
        """

        print(f"\n🔗 5. 전체 통합 (Product ID 기반 매칭)")

        # 팩 수, 단위 수, 용량 계산
        df_rocket['rocket_pack']   = df_rocket['rocket_product_name'].apply(extract_pack_count)
        df_rocket['rocket_unit']   = df_rocket['rocket_product_name'].apply(extract_unit_count)
        df_rocket['rocket_weight'] = df_rocket['rocket_product_name'].apply(extract_weight)

        df_iherb['iherb_pack']   = df_iherb['iherb_product_name'].apply(extract_pack_count)
        df_iherb['iherb_unit']   = df_iherb['iherb_product_name'].apply(extract_unit_count)
        df_iherb['iherb_weight'] = df_iherb['iherb_product_name'].apply(extract_weight)

        # Product ID로 조인
        df = df_rocket.merge(
            df_iherb,
            left_on='rocket_product_id',
            right_on='iherb_product_id',
            how='left',
            suffixes=('', '_dup')
        )

        print(f"   ✓ Product ID 조인: {df['iherb_vendor_id'].notna().sum():,}개 원시 매칭")

        # Best Match 선택 로직
        matched_rows = []
        unmatched_rows = []

        for (rocket_vid, rocket_cat), group in df.groupby(['rocket_vendor_id', 'rocket_category'], dropna=False):
            # 매칭 없는 경우
            if group['iherb_vendor_id'].isna().all():
                unmatched_rows.append(group.iloc[0])
                continue

            candidates = group[group['iherb_vendor_id'].notna()].copy()

            rocket_pack   = group.iloc[0]['rocket_pack']
            rocket_unit   = group.iloc[0]['rocket_unit']
            rocket_weight = group.iloc[0]['rocket_weight']

            # 1순위: 팩 수, 단위 수, 용량 모두 일치
            best = candidates[
                (candidates['iherb_pack']   == rocket_pack) &
                (candidates['iherb_unit']   == rocket_unit) &
                (candidates['iherb_weight'] == rocket_weight)
            ]
            if len(best) > 0:
                matched_rows.append(best.iloc[0])
                continue

            # 2순위: 팩 수와 용량만 일치 (단위 수 정보 없음)
            best = candidates[
                (candidates['iherb_pack']   == rocket_pack) &
                (candidates['iherb_weight'] == rocket_weight) &
                (candidates['iherb_unit'].isna() | pd.isna(rocket_unit))
            ]
            if len(best) > 0:
                matched_rows.append(best.iloc[0])
                continue

            # 3순위: 용량만 일치
            best = candidates[
                (candidates['iherb_weight'] == rocket_weight)
            ]
            if len(best) > 0:
                matched_rows.append(best.iloc[0])
                continue

            # 기존 로직: 팩 수 + 단위 수 일치
            best = candidates[
                (candidates['iherb_pack'] == rocket_pack) &
                (candidates['iherb_unit'] == rocket_unit)
            ]
            if len(best) > 0:
                matched_rows.append(best.iloc[0])
                continue

            # 기존 로직: 팩 수만 일치
            best = candidates[
                (candidates['iherb_pack'] == rocket_pack) &
                (candidates['iherb_unit'].isna() | pd.isna(rocket_unit))
            ]
            if len(best) > 0:
                matched_rows.append(best.iloc[0])
                continue

            # 기존 로직: 단위 수만 일치
            best = candidates[
                (candidates['iherb_unit'] == rocket_unit) &
                (candidates['iherb_pack'].isna() | pd.isna(rocket_pack))
            ]
            if len(best) > 0:
                matched_rows.append(best.iloc[0])
                continue

            # 매칭 실패 - 후보는 있지만 조건 불일치
            row = group.iloc[0].copy()
            iherb_cols = [col for col in row.index if col.startswith('iherb_')]
            row[iherb_cols] = pd.NA
            unmatched_rows.append(row)

        # 재구성
        df_final = pd.concat(matched_rows + unmatched_rows, axis=1).T.reset_index(drop=True)
        
        removed_count = len(df) - len(df_final)
        if removed_count > 0:
            print(f"   ⚠️  중복 제거: {removed_count:,}개")
        
        # 매칭 방식 및 신뢰도 기록
        df_final['matching_method'] = '미매칭'
        df_final['matching_confidence'] = ''
        
        matched_mask = df_final['iherb_vendor_id'].notna()
        df_final.loc[matched_mask, 'matching_method'] = 'Product ID'
        
        # 신뢰도 계산
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
        
        # _dup 정리
        df_final = df_final[[c for c in df_final.columns if not c.endswith('_dup')]]
        
        # 가격 비교 계산
        rp = pd.to_numeric(df_final['rocket_price'], errors='coerce')
        ip = pd.to_numeric(df_final['iherb_price'], errors='coerce')
        valid = rp.gt(0) & ip.gt(0)
        
        df_final['price_diff'] = pd.NA
        df_final['price_diff_pct'] = pd.NA
        df_final['cheaper_source'] = pd.NA
        
        diff = (ip - rp).where(valid).astype('float')
        pct = (diff / rp * 100).where(valid).replace([np.inf, -np.inf], np.nan).round(1)
        
        df_final.loc[valid, 'price_diff'] = diff[valid]
        df_final.loc[valid, 'price_diff_pct'] = pct[valid]
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
            
            # 신뢰도 분포
            conf_counts = df_final[matched_mask]['matching_confidence'].value_counts()
            print(f"\n   📊 매칭 신뢰도:")
            for conf, count in conf_counts.items():
                pct_val = count / matched_count * 100
                print(f"      • {conf}: {count:,}개 ({pct_val:.1f}%)")
        
        return df_final