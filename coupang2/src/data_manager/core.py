#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DataManager Core
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
통합 데이터 관리 - 모듈화된 메인 클래스
"""

import pandas as pd
from typing import Optional

from .db_loader import DataLoader
from .matcher import ProductMatcher
from .filter import DynamicFilter
from .calculator import PriceCalculator


class DataManager:
    """통합 데이터 관리 - Product ID 기반 매칭 + 동적 필터링"""
    
    def __init__(self, db_path: str):
        """
        Args:
            db_path: 통합 DB 경로
        """
        self.db_path = db_path
        self.loader = DataLoader(db_path)
        self.matcher = ProductMatcher()
        self.filter = DynamicFilter()
        self.calculator = PriceCalculator()
    
    def get_integrated_df(self, 
                         target_date: Optional[str] = None,
                         snapshot_id: Optional[int] = None,
                         include_unmatched: bool = True) -> pd.DataFrame:
        """통합 데이터프레임 생성 (Product ID 기반 매칭 + 동적 필터링)
        
        Args:
            target_date: 특정 날짜 (None이면 최신)
            snapshot_id: 특정 snapshot ID (우선순위 높음)
            include_unmatched: 미매칭 우수 상품 포함 여부
        
        Returns:
            DataFrame with columns:
            
            [매칭 정보]
            - matching_status: '로켓매칭' 또는 '미매칭'
            - matching_method, matching_confidence
            
            [로켓직구]
            - rocket_vendor_id, rocket_product_id, rocket_item_id
            - rocket_product_name
            - rocket_rank, rocket_price, rocket_original_price
            - rocket_rating, rocket_reviews
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
            - iherb_revenue
            - iherb_sales_quantity
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
        
        # 1. Snapshot ID 결정
        if snapshot_id is None:
            if target_date:
                snapshot_id = self.loader.get_snapshot_by_date(target_date)
                if snapshot_id is None:
                    print(f"❌ 날짜 {target_date}에 해당하는 snapshot이 없습니다.")
                    return pd.DataFrame()
            else:
                snapshot_id = self.loader.get_latest_snapshot_id()
                if snapshot_id is None:
                    print(f"❌ DB에 snapshot이 없습니다.")
                    return pd.DataFrame()
        
        print(f"📌 Snapshot ID: {snapshot_id}\n")
        
        # 2. 데이터 로드
        print(f"📥 데이터 로드 중...")
        df_rocket = self.loader.load_rocket_data(snapshot_id)
        df_iherb = self.loader.load_iherb_data(snapshot_id)
        
        if df_rocket.empty:
            print(f"⚠️  로켓직구 데이터가 없습니다.")
            return pd.DataFrame()
        
        if df_iherb.empty:
            print(f"⚠️  아이허브 데이터가 없습니다.")
            return pd.DataFrame()
        
        # 3. Product ID 매칭
        df_matched = self.matcher.match_products(df_rocket, df_iherb)
        
        # 4. 가격 비교 계산
        df_matched = self.calculator.calculate_price_comparison(df_matched)
        
        # 5. 동적 필터링 및 미매칭 상품 추가
        if include_unmatched:
            df_final = self.filter.add_unmatched_products(df_matched, df_iherb)
            
            # 미매칭 상품도 가격 계산
            unmatched_mask = df_final['matching_status'] == '미매칭'
            if unmatched_mask.sum() > 0:
                df_final = self.calculator.calculate_price_comparison(df_final)
        else:
            df_final = df_matched
            df_final['matching_status'] = '로켓매칭'
        
        print(f"\n✅ 통합 완료: {len(df_final):,}개 레코드")
        print(f"   - 로켓직구 매칭: {(df_final['matching_status'] == '로켓매칭').sum():,}개")
        
        if include_unmatched:
            print(f"   - 아이허브 미매칭 우수: {(df_final['matching_status'] == '미매칭').sum():,}개")
        
        matching_rate = (df_final['matching_status'] == '로켓매칭').sum() / len(df_final) * 100
        print(f"   - 최종 매칭률: {matching_rate:.1f}%\n")
        
        return df_final


def main():
    """테스트"""
    db_path = "/Users/brich/Desktop/iherb_price/coupang2/data/integrated/rocket_iherb.db"
    
    manager = DataManager(db_path)
    
    # 미매칭 상품 포함
    df = manager.get_integrated_df(include_unmatched=True)
    
    print(f"\n최종 결과:")
    print(f"  - 총 상품: {len(df):,}개")
    print(f"  - 로켓 매칭: {(df['matching_status'] == '로켓매칭').sum():,}개")
    print(f"  - 미매칭 우수: {(df['matching_status'] == '미매칭').sum():,}개")


if __name__ == "__main__":
    main()