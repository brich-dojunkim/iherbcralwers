#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
모니터링 데이터 종합 분석 모듈
coupang_monitoring.py에서 수집한 데이터를 분석하고 리포트 생성
"""

import sqlite3
import pandas as pd
import json
from datetime import datetime
import os


class MonitoringAnalyzer:
    """모니터링 데이터 종합 분석기"""
    
    def __init__(self, db_path="page_monitoring.db"):
        self.db_path = db_path
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {db_path}")
    
    # ========== 기본 통계 ==========
    
    def get_db_statistics(self):
        """DB 통계 정보 조회"""
        conn = sqlite3.connect(self.db_path)
        
        stats = {}
        tables = ['categories', 'matching_reference', 'page_snapshots', 
                  'product_states', 'change_events']
        
        for table in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                stats[table] = count
            except sqlite3.OperationalError:
                stats[table] = 0
        
        # 매칭 통계
        try:
            matched_count = conn.execute("""
                SELECT COUNT(*) FROM matching_reference 
                WHERE iherb_upc IS NOT NULL
            """).fetchone()[0]
            stats['matched_products'] = matched_count
            stats['unmatched_products'] = stats.get('matching_reference', 0) - matched_count
        except:
            stats['matched_products'] = 0
            stats['unmatched_products'] = 0
        
        # 최신 스냅샷 정보
        try:
            latest = conn.execute("""
                SELECT snapshot_time, total_products 
                FROM page_snapshots 
                ORDER BY id DESC LIMIT 1
            """).fetchone()
            if latest:
                stats['latest_snapshot_time'] = latest[0]
                stats['latest_total_products'] = latest[1]
        except:
            pass
        
        conn.close()
        return stats
    
    def print_db_statistics(self):
        """DB 통계 출력"""
        stats = self.get_db_statistics()
        
        print(f"\n{'='*70}")
        print(f"📊 모니터링 DB 통계")
        print(f"{'='*70}")
        print(f"DB 파일: {self.db_path}")
        print(f"\n[테이블별 데이터]")
        print(f"  카테고리: {stats.get('categories', 0)}개")
        print(f"  매칭 참조: {stats.get('matching_reference', 0)}개 상품")
        print(f"    - 매칭 완료: {stats.get('matched_products', 0)}개")
        print(f"    - 미매칭: {stats.get('unmatched_products', 0)}개")
        print(f"  페이지 스냅샷: {stats.get('page_snapshots', 0)}회 수집")
        print(f"  상품 상태: {stats.get('product_states', 0)}개 레코드")
        print(f"  변화 이벤트: {stats.get('change_events', 0)}개")
        
        if stats.get('latest_snapshot_time'):
            print(f"\n[최근 수집]")
            print(f"  시간: {stats['latest_snapshot_time']}")
            print(f"  상품 수: {stats.get('latest_total_products', 0)}개")
        
        return stats
    
    # ========== 변화 분석 ==========
    
    def analyze_changes(self, days=7, top_n=10):
        """최근 변화 분석"""
        print(f"\n{'='*70}")
        print(f"📈 최근 {days}일 변화 분석")
        print(f"{'='*70}")
        
        conn = sqlite3.connect(self.db_path)
        
        # 변화 유형별 통계
        query = """
        SELECT 
            event_type,
            category_name,
            COUNT(*) as count
        FROM change_events
        WHERE event_time >= datetime('now', '-{} days')
        GROUP BY event_type, category_name
        ORDER BY count DESC
        """.format(days)
        
        change_stats = pd.read_sql_query(query, conn)
        
        if not change_stats.empty:
            print(f"\n[카테고리별 변화 통계]")
            for _, row in change_stats.iterrows():
                event_type_korean = {
                    'rank_change': '순위 변화',
                    'price_change': '가격 변화',
                    'new_product': '신규 상품'
                }.get(row['event_type'], row['event_type'])
                print(f"  [{row['category_name']}] {event_type_korean}: {row['count']}건")
        
        # 주요 순위 상승
        rank_up_query = """
        SELECT 
            ce.coupang_product_id,
            (SELECT product_name FROM product_states 
             WHERE coupang_product_id = ce.coupang_product_id 
             ORDER BY id DESC LIMIT 1) as product_name,
            ce.category_name,
            ce.old_value as old_rank,
            ce.new_value as new_rank,
            CAST(ce.old_value AS INTEGER) - CAST(ce.new_value AS INTEGER) as rank_improvement
        FROM change_events ce
        WHERE ce.event_type = 'rank_change'
        AND ce.event_time >= datetime('now', '-{} days')
        AND CAST(ce.old_value AS INTEGER) > CAST(ce.new_value AS INTEGER)
        ORDER BY rank_improvement DESC
        LIMIT ?
        """.format(days)
        
        rank_ups = pd.read_sql_query(rank_up_query, conn, params=[top_n])
        
        if not rank_ups.empty:
            print(f"\n[주요 순위 상승 TOP {top_n}]")
            for idx, row in rank_ups.iterrows():
                print(f"  {row['product_name'][:40]}...")
                print(f"    [{row['category_name']}] {row['old_rank']}위 → {row['new_rank']}위 (↑{row['rank_improvement']})")
        
        # 주요 가격 인하
        price_down_query = """
        SELECT 
            ce.coupang_product_id,
            (SELECT product_name FROM product_states 
             WHERE coupang_product_id = ce.coupang_product_id 
             ORDER BY id DESC LIMIT 1) as product_name,
            ce.category_name,
            ce.old_value as old_price,
            ce.new_value as new_price,
            CAST(ce.old_value AS INTEGER) - CAST(ce.new_value AS INTEGER) as price_drop
        FROM change_events ce
        WHERE ce.event_type = 'price_change'
        AND ce.event_time >= datetime('now', '-{} days')
        AND CAST(ce.old_value AS INTEGER) > CAST(ce.new_value AS INTEGER)
        ORDER BY price_drop DESC
        LIMIT ?
        """.format(days)
        
        price_downs = pd.read_sql_query(price_down_query, conn, params=[top_n])
        
        if not price_downs.empty:
            print(f"\n[주요 가격 인하 TOP {top_n}]")
            for idx, row in price_downs.iterrows():
                print(f"  {row['product_name'][:40]}...")
                print(f"    [{row['category_name']}] {int(row['old_price']):,}원 → {int(row['new_price']):,}원 (↓{int(row['price_drop']):,}원)")
        
        conn.close()
    
    # ========== 카테고리별 분석 ==========
    
    def analyze_by_category(self):
        """카테고리별 통계 분석"""
        print(f"\n{'='*70}")
        print(f"📂 카테고리별 분석")
        print(f"{'='*70}")
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            snap.category_name,
            COUNT(DISTINCT ps.coupang_product_id) as total_products,
            AVG(ps.current_price) as avg_price,
            AVG(ps.discount_rate) as avg_discount,
            AVG(ps.review_count) as avg_reviews,
            MAX(snap.snapshot_time) as last_updated
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        WHERE snap.id IN (
            SELECT MAX(id) FROM page_snapshots GROUP BY category_name
        )
        GROUP BY snap.category_name
        ORDER BY total_products DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("카테고리 데이터가 없습니다.")
            return None
        
        print(f"\n총 {len(df)}개 카테고리\n")
        
        for idx, row in df.iterrows():
            print(f"{idx+1}. {row['category_name']}")
            print(f"   상품 수: {row['total_products']}개")
            print(f"   평균 가격: {row['avg_price']:,.0f}원")
            print(f"   평균 할인율: {row['avg_discount']:.1f}%")
            print(f"   평균 리뷰수: {row['avg_reviews']:.0f}개")
            print(f"   최근 업데이트: {row['last_updated']}")
            print()
        
        return df
    
    # ========== 중복 상품 분석 ==========
    
    def analyze_cross_category_products(self, top_n=20):
        """여러 카테고리에 동시 등장하는 상품 분석"""
        print(f"\n{'='*70}")
        print(f"🔍 중복 상품 분석 (여러 카테고리 동시 등장)")
        print(f"{'='*70}")
        
        conn = sqlite3.connect(self.db_path)
        
        # 여러 카테고리에 나타나는 상품 찾기
        query = """
        WITH latest_snapshots AS (
            SELECT category_name, MAX(id) as snapshot_id
            FROM page_snapshots
            GROUP BY category_name
        ),
        product_categories AS (
            SELECT 
                ps.coupang_product_id,
                ps.product_name,
                snap.category_name,
                ps.category_rank,
                ps.current_price
            FROM product_states ps
            JOIN page_snapshots snap ON ps.snapshot_id = snap.id
            JOIN latest_snapshots ls ON snap.id = ls.snapshot_id
        )
        SELECT 
            coupang_product_id,
            product_name,
            COUNT(DISTINCT category_name) as category_count,
            GROUP_CONCAT(category_name, ', ') as categories
        FROM product_categories
        GROUP BY coupang_product_id, product_name
        HAVING category_count > 1
        ORDER BY category_count DESC, product_name
        LIMIT ?
        """
        
        duplicate_products = pd.read_sql_query(query, conn, params=[top_n])
        
        if duplicate_products.empty:
            print("중복 상품이 발견되지 않았습니다.")
            conn.close()
            return None
        
        print(f"\n총 {len(duplicate_products)}개 중복 상품 발견")
        print(f"상위 {min(top_n, len(duplicate_products))}개 상품:\n")
        
        for idx, row in duplicate_products.iterrows():
            print(f"{idx+1}. {row['product_name'][:45]}...")
            print(f"   등장 카테고리: {row['categories']} ({row['category_count']}개)")
            
            # 카테고리별 순위 조회
            rank_query = """
            WITH latest_snapshots AS (
                SELECT category_name, MAX(id) as snapshot_id
                FROM page_snapshots
                GROUP BY category_name
            )
            SELECT 
                snap.category_name,
                ps.category_rank,
                ps.current_price
            FROM product_states ps
            JOIN page_snapshots snap ON ps.snapshot_id = snap.id
            JOIN latest_snapshots ls ON snap.id = ls.snapshot_id
            WHERE ps.coupang_product_id = ?
            ORDER BY ps.category_rank
            """
            
            ranks = pd.read_sql_query(rank_query, conn, params=[row['coupang_product_id']])
            
            if not ranks.empty:
                rank_strs = []
                for _, rank_info in ranks.iterrows():
                    rank_strs.append(f"{rank_info['category_name']}: {rank_info['category_rank']}위")
                print(f"   순위: {', '.join(rank_strs)}")
            print()
        
        conn.close()
        return duplicate_products
    
    # ========== CSV 출력 ==========
    
    def export_latest_snapshot(self, output_path=None):
        """최신 스냅샷을 CSV로 출력"""
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"latest_snapshot_{timestamp}.csv"
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            ps.category_rank as '순위',
            snap.category_name as '카테고리',
            ps.coupang_product_id as '쿠팡_상품ID',
            ps.product_name as '쿠팡_상품명',
            ps.product_url as '쿠팡_상품URL',
            ps.current_price as '현재가격',
            ps.original_price as '정가',
            ps.discount_rate as '할인율(%)',
            ps.review_count as '리뷰수',
            ps.rating_score as '평점',
            CASE WHEN ps.is_rocket_delivery THEN '로켓배송' ELSE '일반배송' END as '배송타입',
            ps.cashback_amount as '적립금',
            ps.iherb_upc as '아이허브_UPC',
            ps.iherb_part_number as '아이허브_파트넘버',
            ps.matching_status as '매칭상태',
            snap.snapshot_time as '수집시간'
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        WHERE snap.id IN (
            SELECT MAX(id) FROM page_snapshots GROUP BY category_name
        )
        ORDER BY snap.category_name, ps.category_rank
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("출력할 데이터가 없습니다.")
            return None
        
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"✅ 최신 스냅샷 CSV 출력: {output_path} ({len(df)}개 상품)")
        return output_path
    
    def export_change_summary(self, days=7, output_path=None):
        """최근 변화 요약 CSV 출력"""
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"change_summary_{days}days_{timestamp}.csv"
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            ce.coupang_product_id as '쿠팡_상품ID',
            (SELECT product_name FROM product_states 
             WHERE coupang_product_id = ce.coupang_product_id 
             ORDER BY id DESC LIMIT 1) as '상품명',
            ce.category_name as '카테고리',
            ce.event_type as '변화유형',
            ce.old_value as '이전값',
            ce.new_value as '현재값',
            ce.description as '설명',
            ce.event_time as '변화시간'
        FROM change_events ce
        WHERE ce.event_time >= datetime('now', '-{} days')
        ORDER BY ce.event_time DESC
        """.format(days)
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("출력할 변화 데이터가 없습니다.")
            return None
        
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"✅ 변화 요약 CSV 출력: {output_path} ({len(df)}개 이벤트)")
        return output_path
    
    def export_category_ranking(self, category_name, top_n=50, output_path=None):
        """특정 카테고리의 최신 순위 CSV 출력"""
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            safe_category = category_name.replace('/', '_')
            output_path = f"ranking_{safe_category}_top{top_n}_{timestamp}.csv"
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            ps.category_rank as '순위',
            ps.product_name as '상품명',
            ps.product_url as '상품URL',
            ps.current_price as '현재가격',
            ps.original_price as '정가',
            ps.discount_rate as '할인율(%)',
            ps.review_count as '리뷰수',
            ps.rating_score as '평점',
            ps.iherb_upc as '아이허브_UPC',
            ps.iherb_part_number as '아이허브_파트넘버'
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        WHERE snap.category_name = ?
        AND snap.id = (
            SELECT MAX(id) FROM page_snapshots WHERE category_name = ?
        )
        AND ps.category_rank <= ?
        ORDER BY ps.category_rank
        """
        
        df = pd.read_sql_query(query, conn, params=[category_name, category_name, top_n])
        conn.close()
        
        if df.empty:
            print(f"카테고리 '{category_name}'의 데이터가 없습니다.")
            return None
        
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"✅ {category_name} 순위 CSV 출력: {output_path} ({len(df)}개 상품)")
        return output_path
    
    # ========== 종합 리포트 ==========
    
    def generate_full_report(self, days=7):
        """종합 분석 리포트 생성"""
        print(f"\n{'='*70}")
        print(f"📊 모니터링 데이터 종합 분석 리포트")
        print(f"{'='*70}")
        print(f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. DB 통계
        self.print_db_statistics()
        
        # 2. 카테고리별 분석
        self.analyze_by_category()
        
        # 3. 최근 변화 분석
        self.analyze_changes(days=days)
        
        # 4. 중복 상품 분석
        self.analyze_cross_category_products(top_n=10)
        
        print(f"\n{'='*70}")
        print(f"✅ 리포트 생성 완료")
        print(f"{'='*70}")


def main():
    """메인 함수 - 분석 실행"""
    
    try:
        analyzer = MonitoringAnalyzer(db_path="page_monitoring.db")
        
        # 종합 리포트 생성
        analyzer.generate_full_report(days=7)
        
        print(f"\n{'='*70}")
        print(f"📄 CSV 파일 출력")
        print(f"{'='*70}")
        
        # CSV 출력
        analyzer.export_latest_snapshot()
        analyzer.export_change_summary(days=7)
        
        # 카테고리별 순위 출력 (예시)
        categories = ['헬스/건강식품', '출산유아동', '스포츠레저']
        for category in categories:
            try:
                analyzer.export_category_ranking(category, top_n=30)
            except:
                print(f"⚠️ {category} 카테고리 데이터 없음")
        
        print(f"\n✅ 모든 분석 및 출력 완료!")
        
    except FileNotFoundError as e:
        print(f"❌ 오류: {e}")
        print("먼저 coupang_monitoring.py를 실행하여 DB를 생성하세요.")
    except Exception as e:
        print(f"❌ 분석 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()