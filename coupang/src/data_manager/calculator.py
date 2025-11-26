#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Price Calculator
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
가격 비교 계산 (손익분기/추천/요청 할인율) - 기존 로직 100% 보존
"""

import pandas as pd
import numpy as np


class PriceCalculator:
    """가격 비교 계산"""
    
    @staticmethod
    def calculate_price_comparison(df: pd.DataFrame) -> pd.DataFrame:
        """가격 비교 계산 (기존 data_manager 로직)
        
        Args:
            df: 통합 DataFrame
        
        Returns:
            가격 비교 컬럼이 추가된 DataFrame
        """
        
        df = df.copy()
        
        # 가격 변수
        rp = pd.to_numeric(df.get('rocket_price', 0), errors='coerce')
        ip = pd.to_numeric(df.get('iherb_price', 0), errors='coerce')
        op = pd.to_numeric(df.get('iherb_original_price', 0), errors='coerce')
        rec_p = pd.to_numeric(df.get('iherb_recommended_price', 0), errors='coerce')
        
        # 유효성 체크
        valid = rp.gt(0) & ip.gt(0)
        valid_rec = ip.gt(0) & rec_p.gt(0)
        valid_req = op.gt(0) & rec_p.gt(0)
        
        # 초기화
        df['price_diff'] = pd.NA
        df['price_diff_pct'] = pd.NA
        df['cheaper_source'] = pd.NA
        df['breakeven_discount_rate'] = pd.NA
        df['recommended_discount_rate'] = pd.NA
        df['requested_discount_rate'] = pd.NA
        
        # 가격 차이
        diff = (ip - rp).where(valid).astype('float')
        pct = (diff / rp * 100).where(valid).replace([np.inf, -np.inf], np.nan).round(1)
        
        # 손익분기 할인율 (로켓 가격에 맞추려면)
        breakeven = ((ip - rp) / ip * 100).where(valid).replace([np.inf, -np.inf], np.nan).round(1)
        
        # 추천 할인율 (판매가 기준)
        recommended = ((ip - rec_p) / ip * 100).where(valid_rec).replace([np.inf, -np.inf], np.nan).round(1)
        
        # 요청 할인율 (정가 기준)
        requested = ((op - rec_p) / op * 100).where(valid_req).replace([np.inf, -np.inf], np.nan).round(1)
        
        # 값 할당
        df.loc[valid, 'price_diff'] = diff[valid]
        df.loc[valid, 'price_diff_pct'] = pct[valid]
        df.loc[valid, 'breakeven_discount_rate'] = breakeven[valid]
        df.loc[valid_rec, 'recommended_discount_rate'] = recommended[valid_rec]
        df.loc[valid_req, 'requested_discount_rate'] = requested[valid_req]
        
        # 유리한 곳
        df.loc[valid, 'cheaper_source'] = np.where(
            df.loc[valid, 'price_diff'] > 0, '로켓직구',
            np.where(df.loc[valid, 'price_diff'] < 0, '아이허브', '동일')
        )
        
        # 통계
        if valid.sum() > 0:
            cheaper_counts = df.loc[valid, 'cheaper_source'].value_counts()
            print(f"\n   💰 가격 경쟁력:")
            for source, count in cheaper_counts.items():
                pct_val = count / valid.sum() * 100
                print(f"      • {source}: {count:,}개 ({pct_val:.1f}%)")
        
        return df