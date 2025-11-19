#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data Loaders
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
통합 DB에서 데이터 읽기
"""

import sqlite3
import pandas as pd
import re
from typing import Optional


class DataLoader:
    """통합 DB에서 데이터 로드"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def load_rocket_data(self, snapshot_id: int) -> pd.DataFrame:
        """로켓직구 데이터 로드
        
        Returns:
            DataFrame with columns:
            - rocket_vendor_id, rocket_product_id, rocket_item_id
            - rocket_product_name, rocket_url, rocket_category
            - rocket_rank, rocket_price, rocket_original_price, rocket_discount_rate
            - rocket_rating, rocket_reviews
        """
        conn = sqlite3.connect(self.db_path)
        
        # 먼저 snapshot의 카테고리 URL 조회
        snapshot_query = """
            SELECT 
                rocket_category_url_1,
                rocket_category_url_2,
                rocket_category_url_3
            FROM snapshots
            WHERE id = ?
        """
        snapshot_row = conn.execute(snapshot_query, (snapshot_id,)).fetchone()
        
        # URL → 카테고리명 매핑
        category_map = {}
        if snapshot_row:
            if snapshot_row[0]:  # url_1
                category_map[snapshot_row[0]] = '헬스/건강식품'
            if snapshot_row[1]:  # url_2
                category_map[snapshot_row[1]] = '출산유아동'
            if snapshot_row[2]:  # url_3
                category_map[snapshot_row[2]] = '스포츠레저'
        
        # 상품 데이터 조회
        query = """
            SELECT 
                p.vendor_item_id AS rocket_vendor_id,
                p.product_id AS rocket_product_id,
                p.item_id AS rocket_item_id,
                p.name AS rocket_product_name,
                pr.rocket_price,
                pr.rocket_original_price,
                f.rocket_rank,
                f.rocket_rating,
                f.rocket_reviews
            FROM products p
            LEFT JOIN product_price pr ON p.vendor_item_id = pr.vendor_item_id 
                AND pr.snapshot_id = ?
            LEFT JOIN product_features f ON p.vendor_item_id = f.vendor_item_id 
                AND f.snapshot_id = ?
            WHERE pr.rocket_price IS NOT NULL
            ORDER BY f.rocket_rank
        """
        
        df = pd.read_sql_query(query, conn, params=(snapshot_id, snapshot_id))
        conn.close()
        
        # URL 재구성
        def compose_url(product_id, item_id, vendor_id):
            if pd.notna(product_id) and pd.notna(item_id):
                url = f"https://www.coupang.com/vp/products/{product_id}?itemId={item_id}"
                if pd.notna(vendor_id):
                    url += f"&vendorItemId={vendor_id}"
                return url
            return None
        
        df['rocket_url'] = df.apply(
            lambda row: compose_url(
                row['rocket_product_id'],
                row['rocket_item_id'],
                row['rocket_vendor_id']
            ),
            axis=1
        )
        
        # 🔥 카테고리 판별 (URL 기반)
        # URL에서 카테고리 추출
        def extract_category_from_url(url):
            """URL에 category 파라미터가 있으면 카테고리명 반환"""
            if not isinstance(url, str):
                return None
            
            # category=305433 형태 추출
            match = re.search(r'category=(\d+)', url)
            if match:
                cat_id = match.group(1)
                if cat_id == '305433':
                    return '헬스/건강식품'
                elif cat_id == '219079':
                    return '출산유아동'
                elif cat_id == '317675':
                    return '스포츠레저'
            return None
        
        # 카테고리 컬럼 생성 (우선 None으로 초기화)
        df['rocket_category'] = None
        
        # 방법 1: URL 기반으로 카테고리 판별 (더 정확)
        df['rocket_category'] = df['rocket_url'].apply(extract_category_from_url)
        
        # 방법 2: URL이 없으면 rank 기반으로 추정
        # rank 1~50: url_1, 51~100: url_2, 101~: url_3 (예시)
        if df['rocket_category'].isna().any():
            # 이 방법은 부정확할 수 있으므로 추천하지 않음
            # 대신 크롤링 시 URL을 제대로 저장하는 것이 중요
            pass
        
        # 🔥 할인율 계산
        df['rocket_discount_rate'] = 0.0
        valid_price = (df['rocket_price'] > 0) & (df['rocket_original_price'] > 0)
        df.loc[valid_price, 'rocket_discount_rate'] = (
            (1 - df.loc[valid_price, 'rocket_price'] / df.loc[valid_price, 'rocket_original_price']) * 100
        ).round(1)
        
        print(f"   ✓ 로켓직구: {len(df):,}개 상품")
        print(f"   ✓ Product ID 있음: {df['rocket_product_id'].notna().sum():,}개")
        
        # 카테고리 분포 확인
        if 'rocket_category' in df.columns:
            category_counts = df['rocket_category'].value_counts()
            if len(category_counts) > 0:
                print(f"   ✓ 카테고리 분포:")
                for cat, count in category_counts.items():
                    print(f"      • {cat}: {count:,}개")
        
        return df
    
    def load_iherb_data(self, snapshot_id: int) -> pd.DataFrame:
        """아이허브 데이터 로드 (가격 + 성과 + UPC)
        
        Returns:
            DataFrame with columns:
            - iherb_vendor_id, iherb_product_id, iherb_item_id
            - iherb_product_name, iherb_part_number, iherb_upc
            - iherb_price, iherb_original_price, iherb_recommended_price
            - iherb_stock, iherb_stock_status
            - iherb_revenue, iherb_sales_quantity, iherb_item_winner_ratio
        """
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT 
                p.vendor_item_id AS iherb_vendor_id,
                p.product_id AS iherb_product_id,
                p.item_id AS iherb_item_id,
                p.name AS iherb_product_name,
                p.part_number AS iherb_part_number,
                p.upc AS iherb_upc,
                pr.iherb_price,
                pr.iherb_original_price,
                pr.iherb_recommended_price,
                f.iherb_stock,
                f.iherb_stock_status,
                f.iherb_revenue,
                f.iherb_sales_quantity,
                f.iherb_item_winner_ratio
            FROM products p
            LEFT JOIN product_price pr ON p.vendor_item_id = pr.vendor_item_id 
                AND pr.snapshot_id = ?
            LEFT JOIN product_features f ON p.vendor_item_id = f.vendor_item_id 
                AND f.snapshot_id = ?
            WHERE pr.iherb_price IS NOT NULL OR f.iherb_revenue IS NOT NULL
        """
        
        df = pd.read_sql_query(query, conn, params=(snapshot_id, snapshot_id))
        conn.close()
        
        # URL 재구성
        def compose_url(product_id, item_id, vendor_id):
            if pd.notna(product_id) and pd.notna(item_id):
                url = f"https://www.coupang.com/vp/products/{product_id}?itemId={item_id}"
                if pd.notna(vendor_id):
                    url += f"&vendorItemId={vendor_id}"
                return url
            return None
        
        df['iherb_url'] = df.apply(
            lambda row: compose_url(
                row['iherb_product_id'],
                row['iherb_item_id'],
                row['iherb_vendor_id']
            ),
            axis=1
        )
        
        print(f"   ✓ 아이허브: {len(df):,}개 상품")
        print(f"   ✓ Product ID 있음: {(df['iherb_product_id'].notna()).sum():,}개")
        print(f"   ✓ 정가 있음: {(df['iherb_original_price'] > 0).sum():,}개")
        
        return df
    
    def get_latest_snapshot_id(self) -> Optional[int]:
        """최신 snapshot ID 조회"""
        conn = sqlite3.connect(self.db_path)
        result = conn.execute(
            "SELECT id FROM snapshots ORDER BY snapshot_date DESC, id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        
        return result[0] if result else None
    
    def get_snapshot_by_date(self, target_date: str) -> Optional[int]:
        """특정 날짜의 snapshot ID 조회"""
        conn = sqlite3.connect(self.db_path)
        result = conn.execute(
            "SELECT id FROM snapshots WHERE snapshot_date = ? ORDER BY id DESC LIMIT 1",
            (target_date,)
        ).fetchone()
        conn.close()
        
        return result[0] if result else None