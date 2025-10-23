#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
쿼리 기반 분석 라이브러리 (change_events 대체)
- 모든 변화 분석을 실시간 쿼리로 수행
- 유연하고 확장 가능
"""

import sqlite3
import pandas as pd
from typing import List, Tuple, Optional


class QueryAnalyzer:
    """쿼리 기반 분석기"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_rank_changes(self, snapshot_id_prev: int, snapshot_id_curr: int, 
                         category_id: Optional[int] = None, 
                         min_change: int = 10) -> pd.DataFrame:
        """
        두 스냅샷 간 순위 변화 조회
        
        Args:
            snapshot_id_prev: 이전 스냅샷 ID
            snapshot_id_curr: 현재 스냅샷 ID
            category_id: 카테고리 필터 (선택)
            min_change: 최소 순위 변화 (절대값)
        
        Returns:
            순위 변화 DataFrame
        """
        conn = sqlite3.connect(self.db_path)
        
        query = """
        WITH prev AS (
            SELECT 
                ps.coupang_product_id,
                ps.product_name,
                ps.category_rank,
                ps.current_price,
                ps.review_count,
                snap.category_id
            FROM product_states ps
            JOIN page_snapshots snap ON ps.snapshot_id = snap.id
            WHERE ps.snapshot_id = ?
        ),
        curr AS (
            SELECT 
                ps.coupang_product_id,
                ps.product_name,
                ps.category_rank,
                ps.current_price,
                ps.review_count,
                snap.category_id
            FROM product_states ps
            JOIN page_snapshots snap ON ps.snapshot_id = snap.id
            WHERE ps.snapshot_id = ?
        )
        SELECT 
            curr.coupang_product_id,
            curr.product_name,
            prev.category_rank as old_rank,
            curr.category_rank as new_rank,
            (prev.category_rank - curr.category_rank) as rank_change,
            prev.current_price as old_price,
            curr.current_price as new_price,
            (curr.current_price - prev.current_price) as price_change,
            curr.review_count
        FROM prev
        JOIN curr ON prev.coupang_product_id = curr.coupang_product_id
        WHERE ABS(prev.category_rank - curr.category_rank) >= ?
        """
        
        params = [snapshot_id_prev, snapshot_id_curr, min_change]
        
        if category_id:
            query += " AND curr.category_id = ?"
            params.append(category_id)
        
        query += " ORDER BY ABS(prev.category_rank - curr.category_rank) DESC"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df
    
    def get_new_products(self, snapshot_id_prev: int, snapshot_id_curr: int) -> pd.DataFrame:
        """신규 진입 상품 조회"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            curr.coupang_product_id,
            curr.product_name,
            curr.category_rank,
            curr.current_price,
            curr.review_count
        FROM product_states curr
        WHERE curr.snapshot_id = ?
          AND curr.coupang_product_id NOT IN (
              SELECT coupang_product_id 
              FROM product_states 
              WHERE snapshot_id = ?
          )
        ORDER BY curr.category_rank
        """
        
        df = pd.read_sql_query(query, conn, params=(snapshot_id_curr, snapshot_id_prev))
        conn.close()
        
        return df
    
    def get_removed_products(self, snapshot_id_prev: int, snapshot_id_curr: int) -> pd.DataFrame:
        """이탈 상품 조회"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            prev.coupang_product_id,
            prev.product_name,
            prev.category_rank,
            prev.current_price,
            prev.review_count
        FROM product_states prev
        WHERE prev.snapshot_id = ?
          AND prev.coupang_product_id NOT IN (
              SELECT coupang_product_id 
              FROM product_states 
              WHERE snapshot_id = ?
          )
        ORDER BY prev.category_rank
        """
        
        df = pd.read_sql_query(query, conn, params=(snapshot_id_prev, snapshot_id_curr))
        conn.close()
        
        return df
    
    def get_price_changes(self, snapshot_id_prev: int, snapshot_id_curr: int,
                         min_change_amount: int = 1000) -> pd.DataFrame:
        """가격 변화 조회"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        WITH prev AS (
            SELECT coupang_product_id, product_name, current_price, category_rank
            FROM product_states WHERE snapshot_id = ?
        ),
        curr AS (
            SELECT coupang_product_id, product_name, current_price, category_rank
            FROM product_states WHERE snapshot_id = ?
        )
        SELECT 
            curr.coupang_product_id,
            curr.product_name,
            curr.category_rank,
            prev.current_price as old_price,
            curr.current_price as new_price,
            (curr.current_price - prev.current_price) as price_change,
            ROUND(100.0 * (curr.current_price - prev.current_price) / prev.current_price, 2) as price_change_pct
        FROM prev
        JOIN curr ON prev.coupang_product_id = curr.coupang_product_id
        WHERE ABS(curr.current_price - prev.current_price) >= ?
        ORDER BY ABS(curr.current_price - prev.current_price) DESC
        """
        
        df = pd.read_sql_query(query, conn, params=(snapshot_id_prev, snapshot_id_curr, min_change_amount))
        conn.close()
        
        return df
    
    def get_product_history(self, product_id: str, category_id: Optional[int] = None) -> pd.DataFrame:
        """특정 상품의 전체 이력 조회"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            snap.id as snapshot_id,
            snap.snapshot_time,
            ps.category_rank,
            ps.current_price,
            ps.review_count,
            ps.rating_score
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        WHERE ps.coupang_product_id = ?
        """
        
        params = [product_id]
        
        if category_id:
            query += " AND snap.category_id = ?"
            params.append(category_id)
        
        query += " ORDER BY snap.snapshot_time"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df
    
    def get_continuous_changes(self, snapshot_ids: List[int], product_id: Optional[str] = None) -> pd.DataFrame:
        """
        연속 스냅샷 간 변화 추적 (10 → 13 → 16 등)
        
        Args:
            snapshot_ids: 추적할 스냅샷 ID 리스트 (예: [10, 13, 16])
            product_id: 특정 상품만 조회 (선택)
        
        Returns:
            연속 변화 DataFrame
        """
        if len(snapshot_ids) < 2:
            raise ValueError("최소 2개 이상의 스냅샷 ID가 필요합니다")
        
        conn = sqlite3.connect(self.db_path)
        
        # 동적으로 쿼리 생성
        snapshot_placeholders = ','.join(['?'] * len(snapshot_ids))
        
        query = f"""
        WITH product_ranks AS (
            SELECT 
                ps.coupang_product_id,
                ps.product_name,
                ps.snapshot_id,
                ps.category_rank,
                ps.current_price
            FROM product_states ps
            WHERE ps.snapshot_id IN ({snapshot_placeholders})
        )
        SELECT 
            pr.coupang_product_id,
            pr.product_name
        """
        
        # 각 스냅샷에 대한 컬럼 추가
        for i, snap_id in enumerate(snapshot_ids):
            query += f",\n            snap{i}.category_rank as rank_snap{snap_id}"
            query += f",\n            snap{i}.current_price as price_snap{snap_id}"
        
        query += "\n        FROM product_ranks pr"
        
        # JOIN 추가
        for i, snap_id in enumerate(snapshot_ids):
            if i == 0:
                query += f"\n        LEFT JOIN product_ranks snap{i} ON pr.coupang_product_id = snap{i}.coupang_product_id AND snap{i}.snapshot_id = {snap_id}"
            else:
                query += f"\n        LEFT JOIN product_ranks snap{i} ON pr.coupang_product_id = snap{i}.coupang_product_id AND snap{i}.snapshot_id = {snap_id}"
        
        query += f"\n        WHERE pr.snapshot_id = {snapshot_ids[0]}"
        
        params = snapshot_ids
        
        if product_id:
            query += "\n          AND pr.coupang_product_id = ?"
            params.append(product_id)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # 순위 변화량 계산
        for i in range(len(snapshot_ids) - 1):
            curr_snap = snapshot_ids[i]
            next_snap = snapshot_ids[i + 1]
            df[f'change_{curr_snap}_to_{next_snap}'] = df[f'rank_snap{curr_snap}'] - df[f'rank_snap{next_snap}']
        
        return df
    
    def get_category_summary(self, snapshot_id: int) -> pd.DataFrame:
        """스냅샷의 카테고리 요약 정보"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            cat.name as category,
            COUNT(*) as total_products,
            AVG(ps.current_price) as avg_price,
            AVG(ps.review_count) as avg_reviews,
            AVG(ps.rating_score) as avg_rating,
            MIN(ps.current_price) as min_price,
            MAX(ps.current_price) as max_price
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        JOIN categories cat ON snap.category_id = cat.id
        WHERE ps.snapshot_id = ?
        GROUP BY cat.name
        """
        
        df = pd.read_sql_query(query, conn, params=(snapshot_id,))
        conn.close()
        
        return df
    
    def get_top_products(self, snapshot_id: int, category_id: Optional[int] = None, 
                        limit: int = 20) -> pd.DataFrame:
        """상위 상품 조회"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            ps.category_rank,
            ps.coupang_product_id,
            ps.product_name,
            ps.current_price,
            ps.review_count,
            ps.rating_score,
            mr.iherb_upc
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        LEFT JOIN matching_reference mr ON ps.coupang_product_id = mr.coupang_product_id
        WHERE ps.snapshot_id = ?
        """
        
        params = [snapshot_id]
        
        if category_id:
            query += " AND snap.category_id = ?"
            params.append(category_id)
        
        query += " ORDER BY ps.category_rank LIMIT ?"
        params.append(limit)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df
    
    def compare_snapshots_summary(self, snapshot_id_prev: int, snapshot_id_curr: int) -> dict:
        """두 스냅샷 간 변화 요약"""
        
        # 순위 변화
        rank_changes = self.get_rank_changes(snapshot_id_prev, snapshot_id_curr, min_change=1)
        
        # 신규/제거
        new_products = self.get_new_products(snapshot_id_prev, snapshot_id_curr)
        removed_products = self.get_removed_products(snapshot_id_prev, snapshot_id_curr)
        
        # 가격 변화
        price_changes = self.get_price_changes(snapshot_id_prev, snapshot_id_curr, min_change_amount=100)
        
        return {
            'rank_changes_count': len(rank_changes),
            'new_products_count': len(new_products),
            'removed_products_count': len(removed_products),
            'price_changes_count': len(price_changes),
            'top_rank_up': rank_changes.nlargest(5, 'rank_change') if not rank_changes.empty else pd.DataFrame(),
            'top_rank_down': rank_changes.nsmallest(5, 'rank_change') if not rank_changes.empty else pd.DataFrame(),
            'new_products': new_products,
            'removed_products': removed_products
        }


def demo_analysis():
    """분석 데모"""
    
    db_path = "monitoring.db"
    analyzer = QueryAnalyzer(db_path)
    
    print("="*80)
    print("쿼리 기반 분석 데모")
    print("="*80)
    
    # 스냅샷 목록 조회
    conn = sqlite3.connect(db_path)
    snapshots = pd.read_sql_query("""
        SELECT snap.id, snap.snapshot_time, cat.name as category
        FROM page_snapshots snap
        LEFT JOIN categories cat ON snap.category_id = cat.id
        ORDER BY snap.snapshot_time DESC
        LIMIT 5
    """, conn)
    conn.close()
    
    print("\n최근 스냅샷:")
    print(snapshots.to_string(index=False))
    
    if len(snapshots) >= 2:
        prev_id = snapshots.iloc[1]['id']
        curr_id = snapshots.iloc[0]['id']
        
        print(f"\n스냅샷 {prev_id} → {curr_id} 비교:")
        
        # 순위 변화
        rank_changes = analyzer.get_rank_changes(prev_id, curr_id, min_change=10)
        print(f"\n주요 순위 변화 (10위 이상): {len(rank_changes)}개")
        if not rank_changes.empty:
            print(rank_changes.head().to_string(index=False))
        
        # 신규 상품
        new_products = analyzer.get_new_products(prev_id, curr_id)
        print(f"\n신규 상품: {len(new_products)}개")
        
        # 제거 상품
        removed_products = analyzer.get_removed_products(prev_id, curr_id)
        print(f"제거 상품: {len(removed_products)}개")


if __name__ == "__main__":
    demo_analysis()
