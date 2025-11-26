#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Product Matcher
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Product ID 기반 1:1 Best Match 로직 (기존 data_manager 로직 100% 보존)
"""

import pandas as pd
import numpy as np
from .utils import extract_pack_count, extract_unit_count, extract_weight


class ProductMatcher:
    """Product ID 기반 매칭"""
    
    @staticmethod
    def match_products(df_rocket: pd.DataFrame, df_iherb: pd.DataFrame) -> pd.DataFrame:
        """전체 통합 (Product ID 기반 1:1 매칭)
        
        Args:
            df_rocket: 로켓직구 데이터
            df_iherb: 아이허브 데이터
        
        Returns:
            매칭된 DataFrame (기존 data_manager._integrate_all_by_product_id 로직)
        """
        
        print(f"\n🔗 Product ID 기반 1:1 매칭")
        
        # pack/unit/weight 추출
        df_rocket = df_rocket.copy()
        df_iherb = df_iherb.copy()
        
        df_rocket['rocket_pack'] = df_rocket['rocket_product_name'].apply(extract_pack_count)
        df_rocket['rocket_unit'] = df_rocket['rocket_product_name'].apply(extract_unit_count)
        df_rocket['rocket_weight'] = df_rocket['rocket_product_name'].apply(extract_weight)
        
        df_iherb['iherb_pack'] = df_iherb['iherb_product_name'].apply(extract_pack_count)
        df_iherb['iherb_unit'] = df_iherb['iherb_product_name'].apply(extract_unit_count)
        df_iherb['iherb_weight'] = df_iherb['iherb_product_name'].apply(extract_weight)
        
        matched_pairs = []
        
        # 각 로켓 상품에 대해 Best Match 찾기
        for rocket_idx, rocket_row in df_rocket.iterrows():
            rocket_pid = rocket_row['rocket_product_id']
            candidates = df_iherb[df_iherb['iherb_product_id'] == rocket_pid].copy()
            
            if candidates.empty:
                matched_pairs.append({
                    **rocket_row.to_dict(),
                    'matched_iherb_idx': None
                })
                continue
            
            rocket_pack = rocket_row['rocket_pack']
            rocket_unit = rocket_row['rocket_unit']
            rocket_weight = rocket_row['rocket_weight']
            
            best_idx = None
            
            # 1순위: pack + unit + weight 모두 일치
            best = candidates[
                (candidates['iherb_pack'] == rocket_pack) &
                (candidates['iherb_unit'] == rocket_unit) &
                (candidates['iherb_weight'] == rocket_weight)
            ]
            if len(best) > 0:
                best_idx = best.index[0]
            
            # 2순위: pack + unit 일치
            if best_idx is None:
                best = candidates[
                    (candidates['iherb_pack'] == rocket_pack) &
                    (candidates['iherb_unit'] == rocket_unit)
                ]
                if len(best) > 0:
                    best_idx = best.index[0]
            
            # 3순위: pack + weight 일치
            if best_idx is None:
                best = candidates[
                    (candidates['iherb_pack'] == rocket_pack) &
                    (candidates['iherb_weight'] == rocket_weight)
                ]
                if len(best) > 0:
                    best_idx = best.index[0]
            
            # 4순위: unit + weight 일치
            if best_idx is None:
                best = candidates[
                    (candidates['iherb_unit'] == rocket_unit) &
                    (candidates['iherb_weight'] == rocket_weight)
                ]
                if len(best) > 0:
                    best_idx = best.index[0]
            
            # 5순위: weight 일치 (pack이 둘 다 없는 경우)
            if best_idx is None:
                best = candidates[
                    (candidates['iherb_weight'] == rocket_weight) &
                    (candidates['iherb_pack'].isna() | pd.isna(rocket_pack))
                ]
                if len(best) > 0:
                    best_idx = best.index[0]
            
            # 6순위: unit 일치 (pack이 둘 다 없는 경우)
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
        
        # DataFrame 생성
        df_final = pd.DataFrame(matched_pairs)
        
        # 🔥 핵심 수정: 로켓직구 고유 컬럼 보호 목록
        ROCKET_PROTECTED_COLUMNS = [
            'rocket_category',      # 카테고리
            'rocket_rank',          # 순위
            'rocket_rating',        # 평점
            'rocket_reviews',       # 리뷰수
            'rocket_vendor_id',     # Vendor ID
            'rocket_product_id',    # Product ID
            'rocket_item_id',       # Item ID
            'rocket_product_name',  # 제품명
            'rocket_url',           # URL
            'rocket_price',         # 가격
            'rocket_original_price', # 정가
            'rocket_discount_rate'  # 할인율
        ]
        
        # 아이허브 데이터 병합 (dtype 경고 해결)
        for idx, row in df_final.iterrows():
            iherb_idx = row['matched_iherb_idx']
            if iherb_idx is not None and not pd.isna(iherb_idx):
                try:
                    iherb_row = df_iherb.loc[iherb_idx]
                    
                    # 아이허브 컬럼만 병합 (로켓 컬럼 보호)
                    for col in df_iherb.columns:
                        if col not in ROCKET_PROTECTED_COLUMNS:
                            # dtype 불일치 해결: 컬럼이 없으면 object 타입으로 생성
                            if col not in df_final.columns:
                                df_final[col] = pd.Series(dtype='object')
                            
                            # 값 할당
                            df_final.at[idx, col] = iherb_row[col]
                            
                except KeyError:
                    pass
        
        df_final = df_final.drop(columns=['matched_iherb_idx'])
        
        # 매칭 신뢰도 계산 (기존 로직)
        df_final['matching_method'] = '미매칭'
        df_final['matching_confidence'] = ''
        
        matched_mask = df_final['iherb_vendor_id'].notna()
        df_final.loc[matched_mask, 'matching_method'] = 'Product ID'
        
        # High: pack + unit 일치
        high_conf = (
            matched_mask 
            & (df_final['rocket_pack'] == df_final['iherb_pack'])
            & (df_final['rocket_unit'] == df_final['iherb_unit'])
        )
        
        # Medium: pack 또는 unit 중 하나만
        medium_conf = (
            matched_mask 
            & ~high_conf
            & (
                ((df_final['rocket_pack'] == df_final['iherb_pack']) & 
                 (df_final['iherb_unit'].isna() | df_final['rocket_unit'].isna())) |
                ((df_final['rocket_unit'] == df_final['iherb_unit']) & 
                 (df_final['iherb_pack'].isna() | df_final['rocket_pack'].isna()))
            )
        )
        
        # Low: 나머지
        low_conf = matched_mask & ~high_conf & ~medium_conf
        
        df_final.loc[high_conf, 'matching_confidence'] = 'High'
        df_final.loc[medium_conf, 'matching_confidence'] = 'Medium'
        df_final.loc[low_conf, 'matching_confidence'] = 'Low'
        
        # 중복 컬럼 제거
        df_final = df_final[[c for c in df_final.columns if not c.endswith('_dup')]]
        
        # 통계
        matched_count = df_final['iherb_vendor_id'].notna().sum()
        print(f"   ✓ 최종 매칭: {matched_count:,}개 ({matched_count/len(df_final)*100:.1f}%)")
        
        if matched_count > 0:
            conf_counts = df_final[matched_mask]['matching_confidence'].value_counts()
            print(f"\n   📊 매칭 신뢰도:")
            for conf, count in conf_counts.items():
                pct_val = count / matched_count * 100
                print(f"      • {conf}: {count:,}개 ({pct_val:.1f}%)")
        
        return df_final