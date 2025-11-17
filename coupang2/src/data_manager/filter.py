#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dynamic Filter
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
동적 필터링 (80 백분위수) - 미매칭 우수 상품 선별 (기존 로직 100% 보존)
"""

import pandas as pd
import numpy as np


class DynamicFilter:
    """동적 필터링 (미매칭 우수 상품)"""
    
    @staticmethod
    def calculate_threshold(df_iherb: pd.DataFrame) -> float:
        """동적 판매량 필터링 기준 계산 (80 백분위수)
        
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
    
    @staticmethod
    def add_unmatched_products(df_matched: pd.DataFrame, 
                               df_iherb: pd.DataFrame) -> pd.DataFrame:
        """미매칭 우수 상품 추가 (동적 필터링 적용)
        
        Args:
            df_matched: 로켓직구 매칭 상품
            df_iherb: 아이허브 전체 상품
        
        Returns:
            df_final: 매칭 + 미매칭 통합
        """
        print(f"\n📥 미매칭 우수 상품 추가 중...")
        
        # 동적 기준 계산
        threshold = DynamicFilter.calculate_threshold(df_iherb)
        
        # 이미 매칭된 vendor_id 목록
        matched_vendor_ids = set(df_matched['iherb_vendor_id'].dropna())
        
        # 미매칭 상품 중 우수 상품 선별
        unmatched = df_iherb[~df_iherb['iherb_vendor_id'].isin(matched_vendor_ids)].copy()
        
        # 필터링 조건 (기존 로직)
        good_unmatched = unmatched[
            (unmatched['iherb_sales_quantity'].notna()) & 
            (unmatched['iherb_sales_quantity'] >= threshold) &
            (unmatched['iherb_stock_status'] == '판매중') &
            (unmatched['iherb_stock'] > 0)
        ].copy()
        
        print(f"  - 미매칭 전체: {len(unmatched):,}개")
        print(f"  - 기준 적용 후: {len(good_unmatched):,}개")
        
        if len(good_unmatched) > 0:
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
        
        # 매칭 상품에 status 추가
        df_matched['matching_status'] = '로켓매칭'
        
        # 통합
        df_final = pd.concat([df_matched, good_unmatched], ignore_index=True)
        
        # 정렬: 로켓 매칭 먼저, 그 다음 판매량 높은 순
        df_final['_sort_key'] = df_final['matching_status'].apply(
            lambda x: 0 if x == '로켓매칭' else 1
        )
        df_final = df_final.sort_values(
            ['_sort_key', 'iherb_sales_quantity'],
            ascending=[True, False]
        ).drop('_sort_key', axis=1).reset_index(drop=True)
        
        print(f"  ✅ 미매칭 우수 상품 {len(good_unmatched):,}개 추가 완료\n")
        
        return df_final