"""
데이터 처리 모듈
"""

import pandas as pd
from datetime import datetime
from typing import Tuple


class DataProcessor:
    """데이터 분류 및 업데이트 처리"""
    
    def __init__(self):
        self.timestamp = datetime.now().isoformat()
    
    def classify_products(self, base_df: pd.DataFrame, crawled_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        기존 vs 신규 상품 분류
        
        Args:
            base_df: 기존 매칭 결과 데이터
            crawled_df: 크롤링된 최신 쿠팡 데이터
            
        Returns:
            (기존 상품 DataFrame, 신규 상품 DataFrame)
        """
        # 크롤링 데이터가 비어있는 경우 처리
        if len(crawled_df) == 0 or 'product_id' not in crawled_df.columns:
            print("   크롤링 데이터가 없어서 분류를 건너뜁니다")
            empty_df = pd.DataFrame(columns=['product_id', 'product_name', 'current_price', 'original_price', 'discount_rate'])
            return empty_df, empty_df
        
        # 기존 상품 ID 집합 생성
        if 'coupang_product_id' in base_df.columns:
            existing_ids = set(base_df['coupang_product_id'].dropna().astype(str))
        else:
            existing_ids = set()
        
        # 크롤링된 상품들 분류
        crawled_ids = crawled_df['product_id'].astype(str)
        
        # 기존 상품 (가격 업데이트 대상)
        existing_mask = crawled_ids.isin(existing_ids)
        existing_products_df = crawled_df[existing_mask].copy()
        
        # 신규 상품 (매칭 필요)
        new_products_df = crawled_df[~existing_mask].copy()
        
        return existing_products_df, new_products_df
    
    def update_existing_prices(self, base_df: pd.DataFrame, existing_products_df: pd.DataFrame) -> pd.DataFrame:
        """
        기존 상품의 쿠팡 가격 정보 업데이트
        
        Args:
            base_df: 기존 데이터
            existing_products_df: 최신 쿠팡 가격 데이터
            
        Returns:
            가격이 업데이트된 DataFrame
        """
        updated_df = base_df.copy()
        
        if len(existing_products_df) == 0:
            return updated_df
        
        # 중복된 product_id 제거 (첫 번째 것만 유지)
        existing_products_df_clean = existing_products_df.drop_duplicates(subset=['product_id'], keep='first')
        
        print(f"   중복 제거: {len(existing_products_df)}개 → {len(existing_products_df_clean)}개")
        
        # 상품 ID를 키로 하는 딕셔너리 생성
        price_updates = existing_products_df_clean.set_index('product_id').to_dict('index')
        
        update_count = 0
        
        for idx, row in updated_df.iterrows():
            coupang_id = str(row.get('coupang_product_id', ''))
            
            if coupang_id in price_updates:
                new_data = price_updates[coupang_id]
                
                # 쿠팡 가격 정보 업데이트
                price_fields = {
                    'coupang_current_price_krw': 'current_price',
                    'coupang_original_price_krw': 'original_price',
                    'coupang_discount_rate': 'discount_rate'
                }
                
                for base_field, crawled_field in price_fields.items():
                    if crawled_field in new_data and new_data[crawled_field]:
                        updated_df.at[idx, base_field] = new_data[crawled_field]
                
                # 업데이트 시간 기록
                updated_df.at[idx, 'price_updated_at'] = self.timestamp
                update_count += 1
        
        print(f"   업데이트된 상품: {update_count}개")
        return updated_df
    
    def integrate_final_data(self, updated_base_df: pd.DataFrame, matched_new_df: pd.DataFrame) -> pd.DataFrame:
        """
        기존 데이터와 신규 매칭 데이터 통합
        
        Args:
            updated_base_df: 가격이 업데이트된 기존 데이터
            matched_new_df: 매칭된 신규 데이터
            
        Returns:
            통합된 최종 DataFrame
        """
        if len(matched_new_df) > 0:
            # 기존 + 신규 통합
            final_df = pd.concat([updated_base_df, matched_new_df], ignore_index=True)
        else:
            # 기존 데이터만
            final_df = updated_base_df.copy()
        
        # 메타데이터 추가
        final_df['last_updated'] = self.timestamp
        
        print(f"   통합 완료: {len(final_df)}개 상품")
        return final_df