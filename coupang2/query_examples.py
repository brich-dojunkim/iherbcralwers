#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
분석 쿼리 예제 - 개선된 DB를 활용한 실전 분석
"""

from database import MonitoringDatabase
import pandas as pd
import sqlite3
from datetime import datetime, timedelta


class MonitoringAnalyzer:
    """모니터링 데이터 분석 클래스"""
    
    def __init__(self, db_path="improved_monitoring.db"):
        self.db = MonitoringDatabase(db_path)
    
    def get_matched_products_report(self) -> pd.DataFrame:
        """매칭 제품 현황 리포트"""
        print("\n📊 매칭 제품 현황 리포트\n")
        
        df = self.db.get_matched_products_summary()
        
        if df.empty:
            print("⚠️ 매칭된 제품이 없습니다")
            return df
        
        print(f"총 매칭 제품: {len(df)}개\n")
        print("카테고리별 분포:")
        print(df['category'].value_counts())
        print("\n상위 10개 제품:")
        print(df.head(10)[['category', 'product_name', 'category_rank', 'iherb_part_number']])
        
        return df
    
    def get_trending_products_report(self, days=7, min_improvement=10) -> pd.DataFrame:
        """급상승 제품 리포트 - 중복 제거"""
        print(f"\n🔥 최근 {days}일 급상승 제품 (순위 {min_improvement}단계 이상 상승)\n")
        
        df = self.db.get_trending_products(days=days, min_improvement=min_improvement)
        
        if df.empty:
            print(f"⚠️ 최근 {days}일간 급상승 제품이 없습니다")
            return df
        
        # 중복 제거 (같은 상품의 여러 이벤트 중 최대 상승만)
        df = df.sort_values('rank_improvement', ascending=False).drop_duplicates('coupang_product_id', keep='first')
        
        print(f"총 {len(df)}개 제품이 급상승했습니다\n")
        print("TOP 10:")
        print(df.head(10)[['product_name', 'old_rank', 'new_rank', 
                           'rank_improvement', 'iherb_part_number', 'category']])
        
        return df
    
    def get_product_lifecycle(self, product_id: str):
        """제품 생명주기 분석"""
        print(f"\n📈 제품 생명주기 분석 (ID: {product_id})\n")
        
        df = self.db.get_rank_history(product_id)
        
        if df.empty:
            print(f"⚠️ 제품 {product_id}의 히스토리가 없습니다")
            return
        
        print(f"총 {len(df)}개 스냅샷")
        print(f"첫 발견: {df.iloc[0]['snapshot_time']}")
        print(f"최종 업데이트: {df.iloc[-1]['snapshot_time']}")
        print(f"최고 순위: {df['category_rank'].min()}위")
        print(f"최저 순위: {df['category_rank'].max()}위")
        print(f"현재 순위: {df.iloc[-1]['category_rank']}위")
        print(f"\n순위 변화 추이:")
        print(df[['snapshot_time', 'category_rank', 'current_price', 'review_count']])
        
        return df
    
    def get_category_performance(self) -> pd.DataFrame:
        """카테고리별 매칭률 리포트"""
        print("\n📊 카테고리별 성과 분석\n")
        
        conn = sqlite3.connect(self.db.db_path)
        
        query = """
        SELECT 
            c.name as category,
            COUNT(DISTINCT pst.coupang_product_id) as total_products,
            COUNT(DISTINCT CASE WHEN mr.iherb_upc IS NOT NULL THEN pst.coupang_product_id END) as matched_products,
            ROUND(
                100.0 * COUNT(DISTINCT CASE WHEN mr.iherb_upc IS NOT NULL THEN pst.coupang_product_id END) 
                / COUNT(DISTINCT pst.coupang_product_id), 
                2
            ) as matching_rate,
            AVG(pst.current_price) as avg_price,
            AVG(pst.review_count) as avg_reviews
        FROM categories c
        JOIN page_snapshots ps ON c.id = ps.category_id
        JOIN product_states pst ON ps.id = pst.snapshot_id
        LEFT JOIN matching_reference mr ON pst.coupang_product_id = mr.coupang_product_id
        WHERE ps.id IN (
            SELECT MAX(id) FROM page_snapshots GROUP BY category_id
        )
        GROUP BY c.name
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print("카테고리별 통계:")
        print(df)
        
        return df
    
    def get_price_rank_correlation(self, category_name: str = None) -> pd.DataFrame:
        """가격-순위 상관관계 분석"""
        print(f"\n💰 가격-순위 상관관계 분석")
        if category_name:
            print(f"   (카테고리: {category_name})\n")
        else:
            print(f"   (전체 카테고리)\n")
        
        conn = sqlite3.connect(self.db.db_path)
        
        query = """
        SELECT 
            ce_price.coupang_product_id,
            pst.product_name,
            ce_price.old_value as old_price,
            ce_price.new_value as new_price,
            ce_price.change_magnitude as price_change,
            ce_rank.change_magnitude as rank_change,
            c.name as category
        FROM change_events ce_price
        LEFT JOIN change_events ce_rank 
            ON ce_price.coupang_product_id = ce_rank.coupang_product_id
            AND ce_rank.event_type = 'rank_change'
            AND ABS(julianday(ce_rank.event_time) - julianday(ce_price.event_time)) < 1
        JOIN product_states pst ON ce_price.coupang_product_id = pst.coupang_product_id
        JOIN page_snapshots ps ON pst.snapshot_id = ps.id
        JOIN categories c ON ps.category_id = c.id
        WHERE ce_price.event_type = 'price_change'
        AND ce_price.change_magnitude < 0
        AND ce_rank.change_magnitude > 0
        AND ce_price.event_time > datetime('now', '-30 days')
        """
        
        if category_name:
            query += f" AND c.name = '{category_name}'"
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("⚠️ 가격 인하 → 순위 상승 사례가 없습니다")
            return df
        
        print(f"가격 인하 후 순위 상승 사례: {len(df)}건")
        print(f"평균 가격 인하: {df['price_change'].mean():.0f}원")
        print(f"평균 순위 상승: {df['rank_change'].mean():.1f}단계")
        print("\n주요 사례:")
        print(df.head(10)[['product_name', 'price_change', 'rank_change', 'category']])
        
        return df
    
    def get_new_products_last_week(self) -> pd.DataFrame:
        """최근 1주일 신규 진입 제품 - 상품ID 기준 중복 제거"""
        print("\n🆕 최근 1주일 신규 진입 제품\n")
        
        conn = sqlite3.connect(self.db.db_path)
        
        query = """
        WITH latest_snapshots AS (
            SELECT MAX(id) as snapshot_id, category_id
            FROM page_snapshots
            GROUP BY category_id
        ),
        new_products_with_rank AS (
            SELECT 
                ce.coupang_product_id,
                ps.product_name,
                ps.category_rank,
                mr.iherb_part_number,
                c.name as category,
                ce.event_time,
                ROW_NUMBER() OVER (PARTITION BY ce.coupang_product_id ORDER BY ps.category_rank) as rn
            FROM change_events ce
            JOIN latest_snapshots ls ON ce.snapshot_id = ls.snapshot_id
            JOIN product_states ps ON ce.coupang_product_id = ps.coupang_product_id AND ps.snapshot_id = ls.snapshot_id
            JOIN categories c ON ce.category_id = c.id
            LEFT JOIN matching_reference mr ON ce.coupang_product_id = mr.coupang_product_id
            WHERE ce.event_type = 'new_product'
            AND ce.event_time > datetime('now', '-7 days')
        )
        SELECT 
            coupang_product_id,
            product_name,
            category_rank,
            iherb_part_number,
            category,
            event_time
        FROM new_products_with_rank
        WHERE rn = 1
        ORDER BY category_rank
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("⚠️ 최근 1주일간 신규 진입 제품이 없습니다")
            return df
        
        print(f"총 {len(df)}개 신규 제품 (중복 제거됨)")
        print(f"매칭된 제품: {df['iherb_part_number'].notna().sum()}개")
        print("\n상위 순위 신규 제품 (TOP 10):")
        print(df.head(10)[['product_name', 'category_rank', 'iherb_part_number', 'category']])
        
        return df
    
    def export_full_report(self, output_path="monitoring_report.xlsx"):
        """전체 리포트 Excel 출력"""
        print(f"\n📄 전체 리포트 생성 중...\n")
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # 1. 매칭 제품 현황
            df_matched = self.get_matched_products_report()
            if not df_matched.empty:
                df_matched.to_excel(writer, sheet_name='매칭제품현황', index=False)
                print(f"  ✅ 매칭 제품: {len(df_matched)}개")
            
            # 2. 급상승 제품
            df_trending = self.get_trending_products_report(days=7, min_improvement=10)
            if not df_trending.empty:
                df_trending.to_excel(writer, sheet_name='급상승제품', index=False)
                print(f"  ✅ 급상승 제품: {len(df_trending)}개")
            
            # 3. 카테고리별 성과
            df_category = self.get_category_performance()
            if not df_category.empty:
                df_category.to_excel(writer, sheet_name='카테고리성과', index=False)
                print(f"  ✅ 카테고리 성과: {len(df_category)}개")
            
            # 4. 가격-순위 상관관계
            df_price_rank = self.get_price_rank_correlation()
            if not df_price_rank.empty:
                df_price_rank.to_excel(writer, sheet_name='가격순위상관', index=False)
                print(f"  ✅ 가격-순위 상관: {len(df_price_rank)}개")
            
            # 5. 신규 진입 제품
            df_new = self.get_new_products_last_week()
            if not df_new.empty:
                df_new.to_excel(writer, sheet_name='신규진입제품', index=False)
                print(f"  ✅ 신규 진입: {len(df_new)}개")
        
        print(f"\n✅ 리포트 생성 완료: {output_path}")


def main():
    """분석 예제 실행"""
    
    analyzer = MonitoringAnalyzer("improved_monitoring.db")
    
    print("="*70)
    print("🎯 모니터링 데이터 분석 시작")
    print("="*70)
    
    # 1. 매칭 제품 현황
    analyzer.get_matched_products_report()
    
    # 2. 급상승 제품
    analyzer.get_trending_products_report(days=7, min_improvement=10)
    
    # 3. 카테고리별 성과
    analyzer.get_category_performance()
    
    # 4. 가격-순위 상관관계
    analyzer.get_price_rank_correlation()
    
    # 5. 신규 진입 제품
    analyzer.get_new_products_last_week()
    
    # 6. 전체 리포트 Excel 출력
    analyzer.export_full_report("monitoring_report.xlsx")
    
    print("\n" + "="*70)
    print("✅ 분석 완료")
    print("="*70)


if __name__ == "__main__":
    main()