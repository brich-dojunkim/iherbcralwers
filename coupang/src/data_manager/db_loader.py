#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data Loaders
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
통합 DB에서 데이터 로드 - 현재 스키마에 최적화

🔥 수정 사항:
  - load_iherb_data()에 아이허브 할인율 계산 추가
  - UPC/할인율 데이터 유무 진단 통계 추가
"""

import sqlite3
import pandas as pd
from typing import Optional


class DataLoader:
    """통합 DB에서 데이터 로드"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def load_rocket_data(self, snapshot_id: int) -> pd.DataFrame:
        """로켓직구 데이터 로드"""
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
                f.rocket_reviews,
                f.rocket_category
            FROM products p
            INNER JOIN product_price pr 
                ON p.vendor_item_id = pr.vendor_item_id 
                AND pr.snapshot_id = ?
            LEFT JOIN product_features f 
                ON p.vendor_item_id = f.vendor_item_id 
                AND f.snapshot_id = ?
            WHERE pr.rocket_price IS NOT NULL
            ORDER BY f.rocket_rank NULLS LAST
        """
        
        df = pd.read_sql_query(query, conn, params=(snapshot_id, snapshot_id))
        conn.close()
        
        # URL 재구성
        df['rocket_url'] = df.apply(
            lambda row: self._compose_url(
                row['rocket_product_id'],
                row['rocket_item_id'],
                row['rocket_vendor_id']
            ) if pd.notna(row['rocket_product_id']) else None,
            axis=1
        )
        
        # 할인율 계산
        df['rocket_discount_rate'] = 0.0
        valid_price = (df['rocket_price'] > 0) & (df['rocket_original_price'] > 0)
        df.loc[valid_price, 'rocket_discount_rate'] = (
            (1 - df.loc[valid_price, 'rocket_price'] / 
             df.loc[valid_price, 'rocket_original_price']) * 100
        ).round(1)
        
        # 통계
        print(f"   ✓ 로켓직구: {len(df):,}개 상품")
        print(f"   ✓ Product ID 있음: {df['rocket_product_id'].notna().sum():,}개")
        
        if 'rocket_category' in df.columns:
            category_counts = df['rocket_category'].value_counts()
            if len(category_counts) > 0:
                print(f"   ✓ 카테고리 분포:")
                for cat, count in category_counts.items():
                    if pd.notna(cat):
                        print(f"      • {cat}: {count:,}개")
        
        return df
    
    def load_iherb_data(self, snapshot_id: int) -> pd.DataFrame:
        """아이허브 데이터 로드
        
        🔥 핵심 수정:
        - iherb_discount_rate 계산 추가
        - UPC/할인율 데이터 진단 통계 추가
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
                f.iherb_item_winner_ratio,
                f.iherb_category,
                f.iherb_sales_quantity_last_7d,
                f.iherb_coupang_share_last_7d
            FROM products p
            LEFT JOIN product_price pr 
                ON p.vendor_item_id = pr.vendor_item_id 
                AND pr.snapshot_id = ?
            LEFT JOIN product_features f 
                ON p.vendor_item_id = f.vendor_item_id 
                AND f.snapshot_id = ?
            WHERE pr.iherb_price IS NOT NULL 
               OR f.iherb_revenue IS NOT NULL
        """
        
        df = pd.read_sql_query(query, conn, params=(snapshot_id, snapshot_id))
        conn.close()
        
        # URL 재구성
        df['iherb_url'] = df.apply(
            lambda row: self._compose_url(
                row['iherb_product_id'],
                row['iherb_item_id'],
                row['iherb_vendor_id']
            ) if pd.notna(row['iherb_product_id']) else None,
            axis=1
        )
        
        # 🔥 아이허브 할인율 계산 추가
        df['iherb_discount_rate'] = 0.0
        valid_price = (df['iherb_price'] > 0) & (df['iherb_original_price'] > 0)
        
        if valid_price.sum() > 0:
            df.loc[valid_price, 'iherb_discount_rate'] = (
                (1 - df.loc[valid_price, 'iherb_price'] / 
                 df.loc[valid_price, 'iherb_original_price']) * 100
            ).round(1)
        
        # 🔥 진단 통계 출력
        print(f"   ✓ 아이허브: {len(df):,}개 상품")
        print(f"   ✓ Product ID 있음: {df['iherb_product_id'].notna().sum():,}개")
        print(f"   ✓ 정가 있음: {(df['iherb_original_price'] > 0).sum():,}개")
        
        # 할인율 계산 통계
        discount_calculated = (df['iherb_discount_rate'] > 0).sum()
        if discount_calculated > 0:
            print(f"   ✓ 할인율 계산됨: {discount_calculated:,}개 ({discount_calculated/len(df)*100:.1f}%)")
            avg_discount = df[df['iherb_discount_rate'] > 0]['iherb_discount_rate'].mean()
            print(f"   ✓ 평균 할인율: {avg_discount:.1f}%")
        else:
            print(f"   ⚠️  할인율 계산 불가: iherb_original_price 데이터가 없습니다")
            print(f"   💡 해결: price_inventory 엑셀의 '할인율기준가' 컬럼 확인 필요")
        
        # UPC 통계
        upc_valid = df['iherb_upc'].notna().sum()
        if upc_valid > 0:
            print(f"   ✓ UPC 있음: {upc_valid:,}개 ({upc_valid/len(df)*100:.1f}%)")
        else:
            print(f"   ⚠️  UPC 데이터 없음")
            print(f"   💡 해결: UPC 엑셀 파일(20251024_*.xlsx)을 로드해야 합니다")
        
        # 카테고리 분포
        if 'iherb_category' in df.columns:
            category_counts = df['iherb_category'].value_counts()
            if len(category_counts) > 0:
                print(f"   ✓ 카테고리 분포:")
                for cat, count in list(category_counts.items())[:5]:
                    if pd.notna(cat):
                        print(f"      • {cat}: {count:,}개")
                if len(category_counts) > 5:
                    print(f"      ... 외 {len(category_counts) - 5}개")
        
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
    
    def get_snapshot_info(self, snapshot_id: int) -> Optional[dict]:
        """Snapshot 상세 정보 조회"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            """SELECT 
                snapshot_date,
                rocket_category_url_1,
                rocket_category_url_2,
                rocket_category_url_3,
                price_file_name,
                insights_file_name,
                reco_file_name
            FROM snapshots
            WHERE id = ?""",
            (snapshot_id,)
        )
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'snapshot_date': result[0],
                'rocket_urls': {
                    'url_1': result[1],
                    'url_2': result[2],
                    'url_3': result[3]
                },
                'file_names': {
                    'price': result[4],
                    'insights': result[5],
                    'reco': result[6]
                }
            }
        return None
    
    def list_snapshots(self, limit: int = 10) -> pd.DataFrame:
        """Snapshot 목록 조회"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT 
                s.id,
                s.snapshot_date,
                COUNT(DISTINCT CASE WHEN pr.rocket_price IS NOT NULL 
                      THEN pr.vendor_item_id END) as rocket_count,
                COUNT(DISTINCT CASE WHEN pr.iherb_price IS NOT NULL 
                      THEN pr.vendor_item_id END) as iherb_count,
                s.price_file_name,
                s.insights_file_name
            FROM snapshots s
            LEFT JOIN product_price pr ON s.id = pr.snapshot_id
            GROUP BY s.id
            ORDER BY s.snapshot_date DESC, s.id DESC
            LIMIT ?
        """
        
        df = pd.read_sql_query(query, conn, params=(limit,))
        conn.close()
        
        return df
    
    @staticmethod
    def _compose_url(product_id, item_id, vendor_id) -> Optional[str]:
        """쿠팡 URL 생성"""
        import pandas as pd
        
        if pd.notna(product_id) and pd.notna(item_id):
            url = f"https://www.coupang.com/vp/products/{product_id}?itemId={item_id}"
            if pd.notna(vendor_id):
                url += f"&vendorItemId={vendor_id}"
            return url
        return None