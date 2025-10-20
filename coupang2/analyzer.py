#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
모니터링 데이터 분석 및 CSV 생성
DB 구조 검증 및 데이터 품질 확인용
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime


class MonitoringAnalyzer:
    """모니터링 데이터 분석 및 CSV 생성 클래스"""
    
    def __init__(self, db_path="improved_monitoring.db"):
        self.db_path = db_path
        
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {db_path}")
        
        print(f"✅ DB 연결: {db_path}")
    
    def export_snapshots_csv(self, output_path=None):
        """
        1. 스냅샷 원본 CSV 생성
        page_snapshots + product_states + matching_reference 조인
        """
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"snapshots_{timestamp}.csv"
        
        print(f"\n[1/3] 스냅샷 CSV 생성 중...")
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            snap.id as snapshot_id,
            snap.snapshot_time,
            cat.name as category,
            ps.coupang_product_id as product_id,
            ps.category_rank as rank,
            ps.product_name,
            ps.product_url,
            ps.current_price,
            ps.original_price,
            ps.discount_rate,
            ps.review_count,
            ps.rating_score,
            ps.is_rocket_delivery,
            ps.is_free_shipping,
            mr.iherb_upc,
            mr.iherb_part_number,
            CASE 
                WHEN mr.iherb_upc IS NOT NULL THEN 'matched'
                ELSE 'unmatched'
            END as matching_status
        FROM page_snapshots snap
        JOIN categories cat ON snap.category_id = cat.id
        JOIN product_states ps ON snap.id = ps.snapshot_id
        LEFT JOIN matching_reference mr ON ps.coupang_product_id = mr.coupang_product_id
        ORDER BY snap.snapshot_time DESC, cat.name, ps.category_rank
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("  ⚠️ 스냅샷 데이터가 없습니다")
            return None
        
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"  ✅ 스냅샷 CSV 생성 완료: {output_path}")
        print(f"     - 총 {len(df)}개 레코드")
        print(f"     - 스냅샷 수: {df['snapshot_id'].nunique()}개")
        print(f"     - 카테고리: {df['category'].nunique()}개")
        print(f"     - 매칭된 상품: {len(df[df['matching_status']=='matched'])}개")
        
        return output_path
    
    def export_change_events_csv(self, output_path=None):
        """
        2. 변화 이벤트 원본 CSV 생성
        change_events 테이블 그대로
        """
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"change_events_{timestamp}.csv"
        
        print(f"\n[2/3] 변화 이벤트 CSV 생성 중...")
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            ce.id as event_id,
            ce.event_time,
            ce.snapshot_id,
            cat.name as category,
            ce.coupang_product_id as product_id,
            ps.product_name,
            ce.event_type,
            ce.old_value,
            ce.new_value,
            ce.change_magnitude,
            ce.description
        FROM change_events ce
        JOIN categories cat ON ce.category_id = cat.id
        LEFT JOIN (
            SELECT DISTINCT coupang_product_id, product_name
            FROM product_states
        ) ps ON ce.coupang_product_id = ps.coupang_product_id
        ORDER BY ce.event_time DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("  ⚠️ 변화 이벤트 데이터가 없습니다")
            return None
        
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"  ✅ 변화 이벤트 CSV 생성 완료: {output_path}")
        print(f"     - 총 {len(df)}개 이벤트")
        
        # 이벤트 타입별 통계
        event_counts = df['event_type'].value_counts()
        for event_type, count in event_counts.items():
            print(f"     - {event_type}: {count}개")
        
        return output_path
    
    def export_categories_summary_csv(self, output_path=None):
        """
        3. 카테고리 요약 CSV 생성
        카테고리별 수집 현황 및 매칭률
        """
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"categories_summary_{timestamp}.csv"
        
        print(f"\n[3/3] 카테고리 요약 CSV 생성 중...")
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            cat.id as category_id,
            cat.name as category_name,
            cat.url as category_url,
            MAX(snap.snapshot_time) as latest_snapshot_time,
            COUNT(DISTINCT snap.id) as total_snapshots,
            (
                SELECT COUNT(*)
                FROM product_states ps2
                WHERE ps2.snapshot_id = (
                    SELECT MAX(id) 
                    FROM page_snapshots 
                    WHERE category_id = cat.id
                )
            ) as total_products_in_latest,
            (
                SELECT COUNT(DISTINCT ps3.coupang_product_id)
                FROM product_states ps3
                JOIN matching_reference mr ON ps3.coupang_product_id = mr.coupang_product_id
                WHERE ps3.snapshot_id = (
                    SELECT MAX(id) 
                    FROM page_snapshots 
                    WHERE category_id = cat.id
                )
                AND mr.iherb_upc IS NOT NULL
            ) as matched_products,
            ROUND(
                100.0 * (
                    SELECT COUNT(DISTINCT ps3.coupang_product_id)
                    FROM product_states ps3
                    JOIN matching_reference mr ON ps3.coupang_product_id = mr.coupang_product_id
                    WHERE ps3.snapshot_id = (
                        SELECT MAX(id) 
                        FROM page_snapshots 
                        WHERE category_id = cat.id
                    )
                    AND mr.iherb_upc IS NOT NULL
                ) / NULLIF((
                    SELECT COUNT(*)
                    FROM product_states ps2
                    WHERE ps2.snapshot_id = (
                        SELECT MAX(id) 
                        FROM page_snapshots 
                        WHERE category_id = cat.id
                    )
                ), 0),
                2
            ) as matching_rate_percent,
            (
                SELECT AVG(ps4.current_price)
                FROM product_states ps4
                WHERE ps4.snapshot_id = (
                    SELECT MAX(id) 
                    FROM page_snapshots 
                    WHERE category_id = cat.id
                )
            ) as avg_price,
            (
                SELECT AVG(ps5.review_count)
                FROM product_states ps5
                WHERE ps5.snapshot_id = (
                    SELECT MAX(id) 
                    FROM page_snapshots 
                    WHERE category_id = cat.id
                )
            ) as avg_review_count
        FROM categories cat
        LEFT JOIN page_snapshots snap ON cat.id = snap.category_id
        GROUP BY cat.id, cat.name, cat.url
        ORDER BY cat.name
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("  ⚠️ 카테고리 데이터가 없습니다")
            return None
        
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"  ✅ 카테고리 요약 CSV 생성 완료: {output_path}")
        print(f"     - 총 {len(df)}개 카테고리")
        
        for _, row in df.iterrows():
            print(f"     - {row['category_name']}: {row['total_products_in_latest']}개 상품, "
                  f"매칭률 {row['matching_rate_percent']:.1f}%")
        
        return output_path
    
    def export_all_csvs(self, output_dir="csv_reports"):
        """모든 CSV 파일 일괄 생성"""
        print(f"\n{'='*70}")
        print(f"📊 모니터링 데이터 CSV 생성 시작")
        print(f"{'='*70}")
        
        # 출력 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. 스냅샷
        snapshot_path = os.path.join(output_dir, f"snapshots_{timestamp}.csv")
        self.export_snapshots_csv(snapshot_path)
        
        # 2. 변화 이벤트
        events_path = os.path.join(output_dir, f"change_events_{timestamp}.csv")
        self.export_change_events_csv(events_path)
        
        # 3. 카테고리 요약
        summary_path = os.path.join(output_dir, f"categories_summary_{timestamp}.csv")
        self.export_categories_summary_csv(summary_path)
        
        print(f"\n{'='*70}")
        print(f"✅ 모든 CSV 생성 완료!")
        print(f"{'='*70}")
        print(f"출력 디렉토리: {output_dir}")
        print(f"타임스탬프: {timestamp}")
        print(f"\n생성된 파일:")
        print(f"  1. {snapshot_path}")
        print(f"  2. {events_path}")
        print(f"  3. {summary_path}")
        print(f"{'='*70}")
    
    def get_db_statistics(self):
        """DB 전체 통계 조회"""
        print(f"\n{'='*70}")
        print(f"📊 DB 전체 통계")
        print(f"{'='*70}")
        
        conn = sqlite3.connect(self.db_path)
        
        # 카테고리 수
        cat_count = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        print(f"카테고리: {cat_count}개")
        
        # 스냅샷 수
        snap_count = conn.execute("SELECT COUNT(*) FROM page_snapshots").fetchone()[0]
        print(f"스냅샷: {snap_count}개")
        
        # 상품 상태 수
        state_count = conn.execute("SELECT COUNT(*) FROM product_states").fetchone()[0]
        print(f"상품 상태 레코드: {state_count}개")
        
        # 변화 이벤트 수
        event_count = conn.execute("SELECT COUNT(*) FROM change_events").fetchone()[0]
        print(f"변화 이벤트: {event_count}개")
        
        # 매칭 참조 수
        match_count = conn.execute(
            "SELECT COUNT(*) FROM matching_reference WHERE iherb_upc IS NOT NULL"
        ).fetchone()[0]
        print(f"매칭 참조: {match_count}개")
        
        # 최신 스냅샷 시간
        latest = conn.execute(
            "SELECT MAX(snapshot_time) FROM page_snapshots"
        ).fetchone()[0]
        print(f"최신 스냅샷: {latest}")
        
        conn.close()
        print(f"{'='*70}")


def main():
    """메인 실행 함수"""
    
    # DB 경로 (improved_monitoring.db 또는 page_monitoring.db)
    db_path = "improved_monitoring.db"
    
    if not os.path.exists(db_path):
        db_path = "page_monitoring.db"
    
    if not os.path.exists(db_path):
        print("❌ DB 파일을 찾을 수 없습니다")
        print("   - improved_monitoring.db 또는")
        print("   - page_monitoring.db")
        return
    
    try:
        analyzer = MonitoringAnalyzer(db_path)
        
        # DB 통계 출력
        analyzer.get_db_statistics()
        
        # 모든 CSV 생성
        analyzer.export_all_csvs("csv_reports")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()