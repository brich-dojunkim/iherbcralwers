#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DB 검증 스크립트
모니터링 시스템이 DB에 데이터를 제대로 쌓고 있는지 검증
"""

import sqlite3
import os
from datetime import datetime


class DBValidator:
    """DB 검증 클래스"""
    
    def __init__(self, db_path="improved_monitoring.db"):
        self.db_path = db_path
        
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {db_path}")
        
        self.conn = sqlite3.connect(db_path)
        print(f"✅ DB 연결: {db_path}\n")
    
    def print_section(self, title):
        """섹션 헤더 출력"""
        print(f"\n{'='*70}")
        print(f"[{title}]")
        print(f"{'='*70}")
    
    def validate_snapshot_collection(self):
        """1. 스냅샷 수집 현황 검증"""
        self.print_section("스냅샷 수집 현황")
        
        query = """
        SELECT 
            COUNT(DISTINCT id) as total_snapshots,
            MIN(snapshot_time) as first_snapshot,
            MAX(snapshot_time) as latest_snapshot,
            COUNT(DISTINCT category_id) as categories_monitored
        FROM page_snapshots
        """
        
        result = self.conn.execute(query).fetchone()
        
        print(f"총 스냅샷: {result[0]}개")
        print(f"첫 수집: {result[1]}")
        print(f"최근 수집: {result[2]}")
        print(f"모니터링 중인 카테고리: {result[3]}개")
    
    def validate_category_snapshots(self):
        """2. 카테고리별 스냅샷 수"""
        self.print_section("카테고리별 스냅샷")
        
        query = """
        SELECT 
            c.name,
            COUNT(ps.id) as snapshot_count,
            MIN(ps.snapshot_time) as first_snapshot,
            MAX(ps.snapshot_time) as latest_snapshot
        FROM categories c
        LEFT JOIN page_snapshots ps ON c.id = ps.category_id
        GROUP BY c.name
        ORDER BY c.name
        """
        
        results = self.conn.execute(query).fetchall()
        
        for row in results:
            print(f"{row[0]}: {row[1]}개 (첫 수집: {row[2]}, 최근: {row[3]})")
    
    def validate_snapshot_products(self):
        """3. 최근 스냅샷별 상품 수"""
        self.print_section("최근 10개 스냅샷 상품 수")
        
        query = """
        SELECT 
            ps.id as snapshot_id,
            ps.snapshot_time,
            c.name as category,
            COUNT(pst.id) as product_count,
            MIN(pst.category_rank) as min_rank,
            MAX(pst.category_rank) as max_rank
        FROM page_snapshots ps
        JOIN categories c ON ps.category_id = c.id
        LEFT JOIN product_states pst ON ps.id = pst.snapshot_id
        GROUP BY ps.id
        ORDER BY ps.snapshot_time DESC
        LIMIT 10
        """
        
        results = self.conn.execute(query).fetchall()
        
        print(f"{'ID':<5} {'시간':<20} {'카테고리':<15} {'상품수':<8} {'순위범위':<15}")
        print(f"{'-'*70}")
        
        for row in results:
            rank_range = f"{row[4]}~{row[5]}" if row[4] and row[5] else "N/A"
            print(f"{row[0]:<5} {row[1]:<20} {row[2]:<15} {row[3]:<8} {rank_range:<15}")
    
    def validate_rank_continuity(self):
        """4. 순위 연속성 검증"""
        self.print_section("순위 누락 체크")
        
        query = """
        SELECT 
            snapshot_id,
            COUNT(*) as product_count,
            MAX(category_rank) as max_rank,
            MAX(category_rank) - COUNT(*) as missing_ranks
        FROM product_states
        GROUP BY snapshot_id
        HAVING missing_ranks > 0
        """
        
        results = self.conn.execute(query).fetchall()
        
        if not results:
            print("✅ 순위 누락 없음 (모든 스냅샷 정상)")
        else:
            print(f"⚠️  순위 누락 발견:")
            print(f"{'스냅샷ID':<10} {'상품수':<10} {'최대순위':<10} {'누락수':<10}")
            print(f"{'-'*70}")
            for row in results:
                print(f"{row[0]:<10} {row[1]:<10} {row[2]:<10} {row[3]:<10}")
    
    def validate_change_events(self):
        """5. 변화 이벤트 발생 현황"""
        self.print_section("변화 이벤트 타입별 발생")
        
        query = """
        SELECT 
            event_type,
            COUNT(*) as count,
            MIN(event_time) as first_event,
            MAX(event_time) as latest_event
        FROM change_events
        GROUP BY event_type
        ORDER BY count DESC
        """
        
        results = self.conn.execute(query).fetchall()
        
        if not results:
            print("⚠️  변화 이벤트 없음")
        else:
            total = sum(row[1] for row in results)
            print(f"총 이벤트: {total}개\n")
            print(f"{'이벤트 타입':<20} {'발생 수':<10} {'첫 발생':<20} {'최근 발생':<20}")
            print(f"{'-'*70}")
            for row in results:
                print(f"{row[0]:<20} {row[1]:<10} {row[2]:<20} {row[3]:<20}")
    
    def validate_recent_events(self):
        """6. 최근 이벤트 샘플"""
        self.print_section("최근 20개 변화 이벤트")
        
        query = """
        SELECT 
            ce.event_time,
            c.name as category,
            ce.event_type,
            ce.description
        FROM change_events ce
        JOIN categories c ON ce.category_id = c.id
        ORDER BY ce.event_time DESC
        LIMIT 20
        """
        
        results = self.conn.execute(query).fetchall()
        
        if not results:
            print("⚠️  이벤트 없음")
        else:
            for row in results:
                print(f"{row[0]} | {row[1]:<15} | {row[2]:<15} | {row[3]}")
    
    def validate_data_quality(self):
        """7. 데이터 품질 검증 (최신 스냅샷 기준)"""
        self.print_section("데이터 품질 (최신 스냅샷)")
        
        query = """
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN product_name IS NULL OR product_name = '' THEN 1 ELSE 0 END) as null_product_name,
            SUM(CASE WHEN current_price = 0 THEN 1 ELSE 0 END) as zero_price,
            SUM(CASE WHEN category_rank IS NULL THEN 1 ELSE 0 END) as null_rank,
            SUM(CASE WHEN product_url IS NULL OR product_url = '' THEN 1 ELSE 0 END) as null_url
        FROM product_states
        WHERE snapshot_id = (SELECT MAX(id) FROM page_snapshots)
        """
        
        result = self.conn.execute(query).fetchone()
        
        if result[0] == 0:
            print("⚠️  최신 스냅샷에 상품 없음")
        else:
            print(f"총 레코드: {result[0]}개")
            print(f"상품명 NULL/빈값: {result[1]}개")
            print(f"가격 0원: {result[2]}개")
            print(f"순위 NULL: {result[3]}개")
            print(f"URL NULL/빈값: {result[4]}개")
    
    def validate_matching_info(self):
        """8. 매칭 정보 연결 검증"""
        self.print_section("매칭 정보")
        
        query = """
        SELECT 
            COUNT(DISTINCT ps.coupang_product_id) as total_unique_products,
            COUNT(DISTINCT CASE WHEN mr.iherb_upc IS NOT NULL THEN ps.coupang_product_id END) as matched_products,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN mr.iherb_upc IS NOT NULL THEN ps.coupang_product_id END) / 
                  NULLIF(COUNT(DISTINCT ps.coupang_product_id), 0), 2) as matching_rate
        FROM product_states ps
        LEFT JOIN matching_reference mr ON ps.coupang_product_id = mr.coupang_product_id
        """
        
        result = self.conn.execute(query).fetchone()
        
        print(f"전체 unique 상품: {result[0]}개")
        print(f"매칭된 상품: {result[1]}개")
        print(f"매칭률: {result[2]}%")
    
    def validate_category_matching(self):
        """9. 카테고리별 매칭률"""
        self.print_section("카테고리별 매칭률")
        
        query = """
        SELECT 
            c.name as category,
            COUNT(DISTINCT ps.coupang_product_id) as total_products,
            COUNT(DISTINCT CASE WHEN mr.iherb_upc IS NOT NULL THEN ps.coupang_product_id END) as matched_products,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN mr.iherb_upc IS NOT NULL THEN ps.coupang_product_id END) / 
                  NULLIF(COUNT(DISTINCT ps.coupang_product_id), 0), 2) as matching_rate
        FROM categories c
        JOIN page_snapshots snap ON c.id = snap.category_id
        JOIN product_states ps ON snap.id = ps.snapshot_id
        LEFT JOIN matching_reference mr ON ps.coupang_product_id = mr.coupang_product_id
        GROUP BY c.name
        ORDER BY c.name
        """
        
        results = self.conn.execute(query).fetchall()
        
        print(f"{'카테고리':<20} {'전체 상품':<12} {'매칭 상품':<12} {'매칭률':<10}")
        print(f"{'-'*70}")
        
        for row in results:
            print(f"{row[0]:<20} {row[1]:<12} {row[2]:<12} {row[3]}%")
    
    def validate_time_series_tracking(self):
        """10. 시계열 추적 검증"""
        self.print_section("시계열 추적 (상위 10개 상품)")
        
        query = """
        SELECT 
            coupang_product_id,
            product_name,
            COUNT(DISTINCT snapshot_id) as tracked_snapshots,
            MIN(category_rank) as best_rank,
            MAX(category_rank) as worst_rank,
            GROUP_CONCAT(DISTINCT snapshot_id) as snapshot_ids
        FROM product_states
        GROUP BY coupang_product_id, product_name
        HAVING tracked_snapshots > 1
        ORDER BY tracked_snapshots DESC
        LIMIT 10
        """
        
        results = self.conn.execute(query).fetchall()
        
        if not results:
            print("⚠️  시계열 추적 데이터 없음 (스냅샷이 1개뿐이거나 상품 변동이 심함)")
        else:
            print(f"{'상품ID':<12} {'상품명':<30} {'추적횟수':<10} {'최고순위':<10} {'최저순위':<10} {'스냅샷ID':<20}")
            print(f"{'-'*100}")
            for row in results:
                name = row[1][:27] + '...' if len(row[1]) > 30 else row[1]
                snapshot_ids = row[5][:17] + '...' if len(row[5]) > 20 else row[5]
                print(f"{row[0]:<12} {name:<30} {row[2]:<10} {row[3]:<10} {row[4]:<10} {snapshot_ids:<20}")
    
    def validate_snapshot_intervals(self):
        """11. 스냅샷 수집 간격"""
        self.print_section("스냅샷 수집 간격")
        
        query = """
        SELECT 
            c.name as category,
            COUNT(DISTINCT ps.id) as snapshot_count,
            MIN(ps.snapshot_time) as first_snapshot,
            MAX(ps.snapshot_time) as last_snapshot,
            ROUND((julianday(MAX(ps.snapshot_time)) - julianday(MIN(ps.snapshot_time))) * 24 / 
                  NULLIF(COUNT(DISTINCT ps.id) - 1, 0), 2) as avg_interval_hours
        FROM categories c
        LEFT JOIN page_snapshots ps ON c.id = ps.category_id
        GROUP BY c.name
        HAVING snapshot_count > 0
        ORDER BY c.name
        """
        
        results = self.conn.execute(query).fetchall()
        
        if not results:
            print("⚠️  스냅샷 데이터 없음")
        else:
            print(f"{'카테고리':<20} {'스냅샷 수':<12} {'평균 간격(시간)':<20} {'수집 기간':<40}")
            print(f"{'-'*100}")
            for row in results:
                if row[1] == 1:
                    interval_str = "N/A (1개)"
                    period = f"{row[2]}"
                else:
                    interval_str = f"{row[4]:.1f}" if row[4] else "N/A"
                    period = f"{row[2]} ~ {row[3]}"
                print(f"{row[0]:<20} {row[1]:<12} {interval_str:<20} {period:<40}")
    
    def validate_change_magnitude(self):
        """12. change_magnitude 검증"""
        self.print_section("change_magnitude 통계")
        
        # 순위 변화
        query_rank = """
        SELECT 
            COUNT(*) as total_rank_changes,
            AVG(change_magnitude) as avg_magnitude,
            MIN(change_magnitude) as min_magnitude,
            MAX(change_magnitude) as max_magnitude,
            SUM(CASE WHEN ABS(change_magnitude) > 50 THEN 1 ELSE 0 END) as extreme_changes
        FROM change_events
        WHERE event_type = 'rank_change'
        """
        
        result_rank = self.conn.execute(query_rank).fetchone()
        
        if result_rank[0] > 0:
            print("순위 변화 (rank_change):")
            print(f"  총 이벤트: {result_rank[0]}개")
            print(f"  평균 변화: {result_rank[1]:.2f}단계")
            print(f"  최소 변화: {result_rank[2]}단계")
            print(f"  최대 변화: {result_rank[3]}단계")
            print(f"  극단적 변화 (±50 이상): {result_rank[4]}개")
        else:
            print("순위 변화 이벤트 없음")
        
        # 가격 변화
        query_price = """
        SELECT 
            COUNT(*) as total_price_changes,
            AVG(change_magnitude) as avg_magnitude,
            MIN(change_magnitude) as min_magnitude,
            MAX(change_magnitude) as max_magnitude,
            SUM(CASE WHEN ABS(change_magnitude) > 10000 THEN 1 ELSE 0 END) as large_changes
        FROM change_events
        WHERE event_type = 'price_change'
        """
        
        result_price = self.conn.execute(query_price).fetchone()
        
        if result_price[0] > 0:
            print("\n가격 변화 (price_change):")
            print(f"  총 이벤트: {result_price[0]}개")
            print(f"  평균 변화: {result_price[1]:.0f}원")
            print(f"  최소 변화: {result_price[2]}원")
            print(f"  최대 변화: {result_price[3]}원")
            print(f"  큰 변화 (±10,000원 이상): {result_price[4]}개")
        else:
            print("\n가격 변화 이벤트 없음")
    
    def validate_category_details(self):
        """13. 카테고리 상세 현황"""
        self.print_section("카테고리 상세 정보")
        
        query = """
        SELECT 
            c.id,
            c.name,
            c.url,
            c.created_at,
            COUNT(ps.id) as snapshot_count
        FROM categories c
        LEFT JOIN page_snapshots ps ON c.id = ps.category_id
        GROUP BY c.id, c.name, c.url, c.created_at
        ORDER BY c.id
        """
        
        results = self.conn.execute(query).fetchall()
        
        print(f"총 등록된 카테고리: {len(results)}개\n")
        print(f"{'ID':<5} {'이름':<25} {'스냅샷':<10} {'생성일':<20}")
        print(f"{'-'*70}")
        
        for row in results:
            name = row[1][:22] + '...' if len(row[1]) > 25 else row[1]
            print(f"{row[0]:<5} {name:<25} {row[4]:<10} {row[3]:<20}")
        
        # 스냅샷이 없는 카테고리
        empty_categories = [row for row in results if row[4] == 0]
        if empty_categories:
            print(f"\n⚠️  스냅샷이 없는 카테고리: {len(empty_categories)}개")
            for row in empty_categories:
                print(f"   - ID {row[0]}: {row[1]}")
    
    def validate_snapshot_timeline(self):
        """14. 스냅샷 타임라인"""
        self.print_section("스냅샷 시간순 조회")
        
        query = """
        SELECT 
            ps.id,
            ps.snapshot_time,
            ps.category_id,
            c.name as category_name,
            ps.total_products,
            COUNT(pst.id) as actual_product_count
        FROM page_snapshots ps
        LEFT JOIN categories c ON ps.category_id = c.id
        LEFT JOIN product_states pst ON ps.id = pst.snapshot_id
        GROUP BY ps.id, ps.snapshot_time, ps.category_id, c.name, ps.total_products
        ORDER BY ps.id
        """
        
        results = self.conn.execute(query).fetchall()
        
        print(f"전체 스냅샷: {len(results)}개\n")
        print(f"{'ID':<5} {'시간':<20} {'카테고리ID':<12} {'카테고리명':<25} {'상품수':<10}")
        print(f"{'-'*80}")
        
        orphaned_snapshots = []
        for row in results:
            category_name = row[3] if row[3] else "⚠️ 카테고리 없음"
            category_id = row[2] if row[2] else "NULL"
            
            if not row[3]:  # 카테고리가 연결되지 않은 스냅샷
                orphaned_snapshots.append(row[0])
            
            print(f"{row[0]:<5} {row[1]:<20} {category_id:<12} {category_name:<25} {row[5]:<10}")
        
        # 시간 간격 분석
        if len(results) > 1:
            print(f"\n시간 간격 분석:")
            for i in range(1, len(results)):
                prev_time = datetime.strptime(results[i-1][1], '%Y-%m-%d %H:%M:%S')
                curr_time = datetime.strptime(results[i][1], '%Y-%m-%d %H:%M:%S')
                diff = curr_time - prev_time
                hours = diff.total_seconds() / 3600
                print(f"  스냅샷 {results[i-1][0]} → {results[i][0]}: {hours:.1f}시간 간격")
        
        # 고아 스냅샷 경고
        if orphaned_snapshots:
            print(f"\n⚠️  카테고리 연결이 끊긴 스냅샷: {len(orphaned_snapshots)}개")
            print(f"   스냅샷 ID: {', '.join(map(str, orphaned_snapshots))}")
            print(f"   → 과거 테스트 데이터이거나 삭제된 카테고리의 스냅샷")
    
    def validate_duplicate_products(self):
        """15. 중복 상품 체크 (같은 스냅샷에 동일 product_id)"""
        self.print_section("중복 상품 체크")
        
        query = """
        SELECT 
            snapshot_id,
            coupang_product_id,
            COUNT(*) as duplicate_count
        FROM product_states
        GROUP BY snapshot_id, coupang_product_id
        HAVING duplicate_count > 1
        """
        
        results = self.conn.execute(query).fetchall()
        
        if not results:
            print("✅ 중복 상품 없음")
        else:
            print(f"⚠️  중복 상품 발견: {len(results)}건")
            print(f"{'스냅샷ID':<10} {'상품ID':<15} {'중복수':<10}")
            print(f"{'-'*70}")
            for row in results[:20]:  # 최대 20건만
                print(f"{row[0]:<10} {row[1]:<15} {row[2]:<10}")
    
    def run_all_validations(self):
        """모든 검증 실행"""
        print(f"\n{'#'*70}")
        print(f"# DB 검증 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"# DB: {self.db_path}")
        print(f"{'#'*70}")
        
        self.validate_snapshot_collection()
        self.validate_category_snapshots()
        self.validate_snapshot_products()
        self.validate_rank_continuity()
        self.validate_change_events()
        self.validate_recent_events()
        self.validate_data_quality()
        self.validate_matching_info()
        self.validate_category_matching()
        self.validate_time_series_tracking()
        self.validate_snapshot_intervals()
        self.validate_change_magnitude()
        self.validate_category_details()
        self.validate_snapshot_timeline()
        self.validate_duplicate_products()
        
        print(f"\n{'#'*70}")
        print(f"# DB 검증 완료")
        print(f"{'#'*70}\n")
    
    def close(self):
        """DB 연결 종료"""
        if self.conn:
            self.conn.close()


def main():
    """메인 실행"""
    
    # DB 경로 찾기
    db_path = "improved_monitoring.db"
    
    if not os.path.exists(db_path):
        db_path = "page_monitoring.db"
    
    if not os.path.exists(db_path):
        print("❌ DB 파일을 찾을 수 없습니다")
        print("   - improved_monitoring.db 또는")
        print("   - page_monitoring.db")
        return
    
    try:
        validator = DBValidator(db_path)
        validator.run_all_validations()
        validator.close()
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()