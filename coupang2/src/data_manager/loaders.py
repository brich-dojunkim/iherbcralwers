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
            - rocket_product_name, rocket_url
            - rocket_rank, rocket_price, rocket_original_price
            - rocket_rating, rocket_reviews
        """
        conn = sqlite3.connect(self.db_path)
        
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
        
        # URL 재구성 (필요시)
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
        
        print(f"   ✓ 로켓직구: {len(df):,}개 상품")
        print(f"   ✓ Product ID 있음: {df['rocket_product_id'].notna().sum():,}개")
        
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