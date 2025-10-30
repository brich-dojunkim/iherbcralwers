#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
통합 데이터 관리자 (rocket_cleaned.csv 기반)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 역할: DB(로켓직구) + CSV(매칭) + Excel(아이허브) → 통합 DataFrame

데이터 흐름:
1. 로켓직구 DB (product_states) - 크롤링 데이터
2. rocket_cleaned.csv - 매칭 테이블 (vendor_item_id ↔ 아이허브_파트넘버)
3. price_inventory.xlsx - 아이허브 가격/재고 (업체상품코드)
4. SELLER_INSIGHTS.xlsx - 아이허브 판매 성과

개선사항:
- vendor_item_id 기반 매칭 (URL에서 추출한 정확한 ID)
- 불필요한 컬럼 제거
- 깔끔한 데이터 구조
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional


class DataManager:
    """통합 데이터 관리 (rocket_cleaned.csv 기반)"""
    
    def __init__(self, 
                 db_path: str = "monitoring.db", 
                 rocket_csv_path: str = "data/rocket/rocket_cleaned.csv",
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
        
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        📊 데이터 흐름:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        1. DB에서 로켓직구 데이터 로드 (vendor_item_id)
        2. rocket_cleaned.csv에서 매칭 정보 로드 (vendor_item_id → 파트넘버)
        3. Excel에서 아이허브 데이터 로드 (업체상품코드)
        4. vendor_item_id로 DB + CSV 조인
        5. 파트넘버 = 업체상품코드로 아이허브 조인
        
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
        
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """
        
        print(f"\n{'='*80}")
        print(f"🔗 통합 데이터프레임 생성 (rocket_cleaned.csv 기반)")
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
            ps.product_url as rocket_url
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources src ON snap.source_id = src.id
        JOIN categories cat ON snap.category_id = cat.id
        WHERE src.source_type = 'rocket_direct'
          AND DATE(snap.snapshot_time) = ?
        ORDER BY cat.name, ps.category_rank
        """
        
        df = pd.read_sql_query(query, conn, params=(target_date,))
        conn.close()
        
        print(f"   ✓ {len(df):,}개 상품")
        return df
    
    def _load_matching_df(self) -> pd.DataFrame:
        """매칭 데이터 로드 (rocket_cleaned.csv)"""
        
        print(f"📥 2. 매칭 데이터 (rocket_cleaned.csv)")
        
        df = pd.read_csv(self.rocket_csv_path)
        
        # vendor_item_id를 문자열로 변환 (DB와 타입 일치)
        result = pd.DataFrame({
            'vendor_item_id': df['vendor_item_id'].astype(str),
            'part_number': df['iherb_part_number'].fillna('').astype(str).str.strip().str.upper(),
            'upc': df['iherb_upc'].astype('Int64').astype(str)
        })
        
        # UPC의 <NA> 제거
        result['upc'] = result['upc'].replace('<NA>', '')
        
        print(f"   ✓ {len(result):,}개 매칭 정보")
        print(f"   ✓ 파트넘버 있음: {(result['part_number'] != '').sum():,}개")
        print(f"   ✓ UPC 있음: {(result['upc'] != '').sum():,}개")
        
        return result
    
    def _join_rocket_matching(self, df_rocket: pd.DataFrame, df_matching: pd.DataFrame) -> pd.DataFrame:
        """로켓직구 + 매칭 조인 (vendor_item_id 기반)"""
        
        print(f"\n🔗 3. 로켓직구 + 매칭 조인 (vendor_item_id 기반)")
        
        # DB의 vendor_item_id도 문자열로 변환
        df_rocket['rocket_vendor_id'] = df_rocket['rocket_vendor_id'].astype(str)
        
        # vendor_item_id로 조인
        df = df_rocket.merge(
            df_matching,
            left_on='rocket_vendor_id',
            right_on='vendor_item_id',
            how='left'
        )
        
        # 중복 컬럼 제거
        df = df.drop(columns=['vendor_item_id'])
        
        matched_count = (df['part_number'].notna() & (df['part_number'] != '')).sum()
        print(f"   ✓ 매칭 정보 있음: {matched_count:,}개 ({matched_count/len(df)*100:.1f}%)")
        
        return df
    
    def _load_price_inventory_df(self) -> pd.DataFrame:
        """아이허브 가격/재고 로드 (Excel)"""
        
        print(f"\n📥 4. 아이허브 가격/재고 (Excel)")
        
        files = list(self.excel_dir.glob("price_inventory_*.xlsx"))
        if not files:
            raise FileNotFoundError(f"price_inventory 파일 없음: {self.excel_dir}")
        
        latest = sorted(files, key=lambda x: x.stem)[-1]
        print(f"   파일: {latest.name}")
        
        df = pd.read_excel(latest, header=1, skiprows=[0])
        
        result = pd.DataFrame({
            'iherb_vendor_id': df['옵션 ID'].astype('Int64').astype(str),
            'iherb_product_name': df['쿠팡 노출 상품명'],
            'iherb_price': pd.to_numeric(df['판매가격'], errors='coerce').fillna(0).astype(int),
            'iherb_stock': pd.to_numeric(df['잔여수량(재고)'], errors='coerce').fillna(0).astype(int),
            'iherb_part_number': df['업체상품코드'].fillna('').astype(str).str.strip().str.upper()
        })
        
        result = result[result['iherb_vendor_id'] != '<NA>'].copy()
        result['iherb_stock_status'] = result['iherb_stock'].apply(
            lambda x: '재고있음' if x > 0 else '품절'
        )
        
        print(f"   ✓ {len(result):,}개 상품")
        print(f"   ✓ 업체상품코드 있음: {(result['iherb_part_number'] != '').sum():,}개")
        
        return result
    
    def _load_seller_insights_df(self) -> pd.DataFrame:
        """아이허브 판매 성과 로드 (Excel)"""
        
        print(f"📥 5. 아이허브 판매 성과 (Excel)")
        
        files = list(self.excel_dir.glob("*SELLER_INSIGHTS*.xlsx"))
        if not files:
            print(f"   ⚠️  SELLER_INSIGHTS 파일 없음")
            return pd.DataFrame()
        
        latest = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
        print(f"   파일: {latest.name}")
        
        df = pd.read_excel(latest, sheet_name='vendor item metrics')
        
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
        
        result = result[result['iherb_vendor_id'] != '<NA>'].copy()
        result['iherb_cancel_rate'] = (
            result['iherb_total_cancel_quantity'] / result['iherb_sales_quantity'] * 100
        ).fillna(0).round(1)
        
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
        """전체 통합 (파트넘버 매칭)"""
        
        print(f"\n🔗 7. 전체 통합 (파트넘버 매칭)")
        
        # part_number = iherb_part_number로 조인
        df = df_rocket.merge(
            df_iherb,
            left_on='part_number',
            right_on='iherb_part_number',
            how='left',
            suffixes=('', '_dup')
        )
        
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
        print(f"   ✓ 최종 매칭: {matched_count:,}개 ({matched_count/len(df)*100:.1f}%)")
        
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


def main():
    """사용 예시"""
    
    print(f"\n{'='*80}")
    print(f"📊 DataManager 사용 예시 (rocket")
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