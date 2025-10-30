#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
통합 데이터 관리자
- DB (로켓직구) + Excel (아이허브) → 통합 DataFrame
- 모든 쿼리가 이 DF를 사용
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional


class DataManager:
    """통합 데이터 관리"""
    
    def __init__(self, db_path: str = "data/monitoring.db", data_dir: str = "data/excel"):
        self.db_path = db_path
        self.data_dir = Path(data_dir)
    
    def get_integrated_df(self, target_date: Optional[str] = None) -> pd.DataFrame:
        """
        통합 데이터프레임 생성
        
        Args:
            target_date: 로켓직구 날짜 (YYYY-MM-DD), None이면 최신
        
        Returns:
            DataFrame: 로켓직구 + 아이허브 통합
        """
        
        print(f"\n{'='*80}")
        print(f"🔗 통합 데이터프레임 생성")
        print(f"{'='*80}\n")
        
        # 1. 로켓직구 데이터 (DB)
        df_rocket = self._load_rocket_df(target_date)
        
        # 2. 아이허브 가격/재고 (Excel)
        df_price = self._load_price_inventory_df()
        
        # 3. 아이허브 성과 (Excel)
        df_insights = self._load_seller_insights_df()
        
        # 4. 아이허브 통합 (가격 + 성과)
        df_iherb = self._integrate_iherb(df_price, df_insights)
        
        # 5. 로켓직구 + 아이허브 통합 (UPC 매칭)
        df_final = self._integrate_all(df_rocket, df_iherb)
        
        print(f"\n✅ 통합 완료: {len(df_final):,}개 레코드")
        print(f"   - 로켓직구: {len(df_rocket):,}개")
        print(f"   - 아이허브 매칭: {df_final['iherb_vendor_id'].notna().sum():,}개\n")
        
        return df_final
    
    def _load_rocket_df(self, target_date: Optional[str]) -> pd.DataFrame:
        """로켓직구 데이터 로드 (DB)"""
        
        print(f"📥 1. 로켓직구 데이터 (DB)")
        
        conn = sqlite3.connect(self.db_path)
        
        # 날짜 결정
        if target_date is None:
            target_date = conn.execute(
                "SELECT DATE(MAX(snapshot_time)) FROM snapshots"
            ).fetchone()[0]
        
        print(f"   날짜: {target_date}")
        
        # 쿼리
        query = """
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
            mr.iherb_upc as upc,
            mr.iherb_part_number as part_number
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources src ON snap.source_id = src.id
        JOIN categories cat ON snap.category_id = cat.id
        LEFT JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE src.source_type = 'rocket_direct'
          AND DATE(snap.snapshot_time) = ?
        ORDER BY cat.name, ps.category_rank
        """
        
        df = pd.read_sql_query(query, conn, params=(target_date,))
        conn.close()
        
        print(f"   ✓ {len(df):,}개 상품")
        return df
    
    def _load_price_inventory_df(self) -> pd.DataFrame:
        """price_inventory 로드"""
        
        print(f"📥 2. 아이허브 가격/재고 (Excel)")
        
        # 최신 파일 찾기
        files = list(self.data_dir.glob("price_inventory_*.xlsx"))
        if not files:
            raise FileNotFoundError(f"price_inventory 파일 없음: {self.data_dir}")
        
        latest = sorted(files, key=lambda x: x.stem)[-1]
        print(f"   파일: {latest.name}")
        
        # 로드
        df = pd.read_excel(latest, header=1, skiprows=[0])
        
        # 컬럼 정리
        result = pd.DataFrame({
            'iherb_vendor_id': df['옵션 ID'].astype('Int64').astype(str),
            'iherb_product_name': df['쿠팡 노출 상품명'],
            'iherb_price': pd.to_numeric(df['판매가격'], errors='coerce').fillna(0).astype(int),
            'iherb_stock': pd.to_numeric(df['잔여수량(재고)'], errors='coerce').fillna(0).astype(int),
            'iherb_part_number': df.get('업체상품코드', '').fillna('').astype(str),
            'upc': df.get('바코드', '').fillna('').astype(str)
        })
        
        # 유효한 vendor_id만
        result = result[result['iherb_vendor_id'] != '<NA>'].copy()
        
        # 재고 상태
        result['iherb_stock_status'] = result['iherb_stock'].apply(
            lambda x: '재고있음' if x > 0 else '품절'
        )
        
        print(f"   ✓ {len(result):,}개 상품")
        return result
    
    def _load_seller_insights_df(self) -> pd.DataFrame:
        """SELLER_INSIGHTS 로드"""
        
        print(f"📥 3. 아이허브 판매 성과 (Excel)")
        
        # 최신 파일 찾기
        files = list(self.data_dir.glob("*SELLER_INSIGHTS*.xlsx"))
        if not files:
            print(f"   ⚠️  SELLER_INSIGHTS 파일 없음, 성과 데이터 없이 진행")
            return pd.DataFrame()
        
        latest = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
        print(f"   파일: {latest.name}")
        
        # 로드
        df = pd.read_excel(latest, sheet_name='vendor item metrics')
        
        # 컬럼 정리
        result = pd.DataFrame({
            'iherb_vendor_id': df['옵션 ID'].astype('Int64').astype(str),
            'iherb_category': df['카테고리'],
            'iherb_revenue': pd.to_numeric(df['매출(원)'], errors='coerce').fillna(0).astype(int),
            'iherb_orders': pd.to_numeric(df['주문'], errors='coerce').fillna(0).astype(int),
            'iherb_sales_quantity': pd.to_numeric(df['판매량'], errors='coerce').fillna(0).astype(int),
            'iherb_visitors': pd.to_numeric(df['방문자'], errors='coerce').fillna(0).astype(int),
            'iherb_views': pd.to_numeric(df['조회'], errors='coerce').fillna(0).astype(int),
            'iherb_cart_adds': pd.to_numeric(df['장바구니'], errors='coerce').fillna(0).astype(int),
            'iherb_conversion_rate': self._parse_percentage(df['구매전환율']),
            'iherb_total_revenue': pd.to_numeric(df['총 매출(원)'], errors='coerce').fillna(0).astype(int),
            'iherb_total_cancel_amount': pd.to_numeric(df['총 취소 금액'], errors='coerce').fillna(0).astype(int),
            'iherb_total_cancel_quantity': pd.to_numeric(df['총 취소된 상품수'], errors='coerce').fillna(0).astype(int)
        })
        
        # 유효한 vendor_id만
        result = result[result['iherb_vendor_id'] != '<NA>'].copy()
        
        # 취소율 계산
        result['iherb_cancel_rate'] = (
            result['iherb_total_cancel_quantity'] / result['iherb_sales_quantity'] * 100
        ).fillna(0).round(1)
        
        print(f"   ✓ {len(result):,}개 상품")
        return result
    
    def _integrate_iherb(self, df_price: pd.DataFrame, df_insights: pd.DataFrame) -> pd.DataFrame:
        """아이허브 가격 + 성과 통합"""
        
        print(f"\n🔗 4. 아이허브 데이터 통합")
        
        if df_insights.empty:
            print(f"   ⚠️  성과 데이터 없음, 가격/재고만 사용")
            return df_price
        
        # vendor_id 기준 조인
        df = df_price.merge(
            df_insights,
            on='iherb_vendor_id',
            how='left'
        )
        
        print(f"   ✓ 통합 완료: {len(df):,}개")
        print(f"   ✓ 성과 데이터 있음: {df['iherb_revenue'].notna().sum():,}개")
        
        return df
    
    def _integrate_all(self, df_rocket: pd.DataFrame, df_iherb: pd.DataFrame) -> pd.DataFrame:
        """로켓직구 + 아이허브 통합 (UPC 매칭)"""
        
        print(f"\n🔗 5. 전체 통합 (UPC 매칭)")
        
        # UPC 기준 조인
        df = df_rocket.merge(
            df_iherb,
            on='upc',
            how='left',
            suffixes=('', '_dup')
        )
        
        # 중복 컬럼 제거
        df = df[[c for c in df.columns if not c.endswith('_dup')]]
        
        # 가격 비교 계산
        matched_mask = (
            df['rocket_price'].notna() & 
            df['iherb_price'].notna() & 
            (df['rocket_price'] > 0) &
            (df['iherb_price'] > 0)
        )
        
        df['price_diff'] = None
        df['price_diff_pct'] = None
        df['cheaper_source'] = None
        
        if matched_mask.any():
            df.loc[matched_mask, 'price_diff'] = (
                df.loc[matched_mask, 'iherb_price'] - df.loc[matched_mask, 'rocket_price']
            )
            df.loc[matched_mask, 'price_diff_pct'] = (
                df.loc[matched_mask, 'price_diff'] / df.loc[matched_mask, 'rocket_price'] * 100
            ).round(1)
            df.loc[matched_mask, 'cheaper_source'] = df.loc[matched_mask, 'price_diff'].apply(
                lambda x: '로켓직구' if x > 0 else ('아이허브' if x < 0 else '동일')
            )
        
        matched_count = df['iherb_vendor_id'].notna().sum()
        print(f"   ✓ UPC 매칭: {matched_count:,}개 ({matched_count/len(df)*100:.1f}%)")
        
        if matched_count > 0:
            cheaper_counts = df['cheaper_source'].value_counts()
            print(f"\n   💰 가격 경쟁력:")
            for source, count in cheaper_counts.items():
                pct = count / matched_count * 100
                print(f"      • {source}: {count:,}개 ({pct:.1f}%)")
        
        return df
    
    @staticmethod
    def _parse_percentage(series) -> pd.Series:
        """퍼센트 문자열 → 숫자"""
        return series.astype(str).str.replace('%', '').str.strip().apply(
            lambda x: float(x) if x and x != 'nan' else 0.0
        )