"""
CSV 출력 전용 모듈
page_monitor.py에서 생성된 DB 데이터를 다양한 형태의 CSV로 출력
"""

import sqlite3
import pandas as pd
from datetime import datetime
import os


class CSVExporter:
    """DB 데이터를 CSV로 출력"""
    
    def __init__(self, db_path="page_monitoring.db"):
        self.db_path = db_path
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {db_path}")
    
    def export_latest_snapshot(self, output_path=None):
        """최신 스냅샷을 CSV로 출력"""
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"latest_snapshot_{timestamp}.csv"
        
        conn = sqlite3.connect(self.db_path)
        
        # 최신 스냅샷 데이터 조회
        query = """
        SELECT 
            ps.rank_position as '순위',
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
        WHERE snap.id = (
            SELECT MAX(id) FROM page_snapshots
        )
        ORDER BY ps.rank_position
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            print("출력할 데이터가 없습니다.")
            return None
        
        # CSV 저장
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"최신 스냅샷 CSV 출력 완료: {output_path}")
        print(f"총 {len(df)}개 상품 데이터")
        
        return output_path
    
    def export_change_summary(self, days=7, output_path=None):
        """최근 N일간의 변화 요약을 CSV로 출력"""
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
            ce.event_type as '변화유형',
            ce.old_value as '이전값',
            ce.new_value as '현재값',
            ce.change_magnitude as '변화크기',
            ce.description as '설명',
            ce.change_time as '변화시간'
        FROM change_events ce
        WHERE ce.change_time >= datetime('now', '-{} days')
        ORDER BY ce.change_time DESC
        """.format(days)
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # CSV 저장
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"최근 {days}일 변화 요약 CSV 출력 완료: {output_path}")
        print(f"총 {len(df)}개 변화 이벤트")
        
        return output_path
    
    def export_matched_products_only(self, output_path=None):
        """매칭된 상품만 CSV로 출력 (기존 CSV와 유사한 형태)"""
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"matched_products_{timestamp}.csv"
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            ps.rank_position as '수집순서',
            '통합' as '카테고리',
            ps.product_url as '쿠팡_상품URL',
            ps.product_name as '쿠팡_제품명',
            ps.current_price || '원' as '쿠팡_비회원가격',
            ps.iherb_upc as '아이허브_UPC',
            ps.iherb_part_number as '아이허브_파트넘버',
            snap.snapshot_time as '수집시간'
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        WHERE snap.id = (SELECT MAX(id) FROM page_snapshots)
        AND ps.iherb_upc IS NOT NULL
        ORDER BY ps.rank_position
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # CSV 저장
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"매칭된 상품만 CSV 출력 완료: {output_path}")
        print(f"총 {len(df)}개 매칭 상품")
        
        return output_path
    
    def export_product_history(self, product_id, output_path=None):
        """특정 상품의 히스토리를 CSV로 출력"""
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"product_history_{product_id}_{timestamp}.csv"
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            snap.snapshot_time as '수집시간',
            ps.rank_position as '순위',
            ps.current_price as '가격',
            ps.original_price as '정가',
            ps.discount_rate as '할인율(%)',
            ps.review_count as '리뷰수',
            ps.rating_score as '평점'
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        WHERE ps.coupang_product_id = ?
        ORDER BY snap.snapshot_time
        """
        
        df = pd.read_sql_query(query, conn, params=[product_id])
        conn.close()
        
        if df.empty:
            print(f"상품 ID {product_id}의 데이터를 찾을 수 없습니다.")
            return None
        
        # CSV 저장
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"상품 {product_id} 히스토리 CSV 출력 완료: {output_path}")
        print(f"총 {len(df)}개 시점 데이터")
        
        return output_path
    
    def export_ranking_trends(self, top_n=50, output_path=None):
        """상위 N개 상품의 순위 변화 추세를 CSV로 출력"""
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"ranking_trends_top{top_n}_{timestamp}.csv"
        
        conn = sqlite3.connect(self.db_path)
        
        # 최신 스냅샷에서 상위 N개 상품 선정
        top_products_query = """
        SELECT coupang_product_id 
        FROM product_states ps
        JOIN page_snapshots snap ON ps.snapshot_id = snap.id
        WHERE snap.id = (SELECT MAX(id) FROM page_snapshots)
        AND ps.rank_position <= ?
        ORDER BY ps.rank_position
        """
        
        top_products = pd.read_sql_query(top_products_query, conn, params=[top_n])
        
        if top_products.empty:
            print("상위 상품 데이터를 찾을 수 없습니다.")
            conn.close()
            return None
        
        # 각 상품의 순위 변화 조회
        trend_data = []
        for product_id in top_products['coupang_product_id']:
            query = """
            SELECT 
                ps.coupang_product_id,
                ps.product_name,
                snap.snapshot_time,
                ps.rank_position,
                ps.current_price
            FROM product_states ps
            JOIN page_snapshots snap ON ps.snapshot_id = snap.id
            WHERE ps.coupang_product_id = ?
            ORDER BY snap.snapshot_time
            """
            
            product_data = pd.read_sql_query(query, conn, params=[product_id])
            trend_data.append(product_data)
        
        conn.close()
        
        if trend_data:
            all_trends = pd.concat(trend_data, ignore_index=True)
            
            # 피벗 테이블로 변환 (상품별 시간대별 순위)
            pivot_table = all_trends.pivot_table(
                index=['coupang_product_id', 'product_name'],
                columns='snapshot_time',
                values='rank_position',
                aggfunc='first'
            )
            
            # CSV 저장
            pivot_table.to_csv(output_path, encoding='utf-8-sig')
            
            print(f"상위 {top_n}개 상품 순위 추세 CSV 출력 완료: {output_path}")
            print(f"총 {len(pivot_table)}개 상품의 순위 변화")
            
            return output_path
        
        return None
    
    def get_db_statistics(self):
        """DB 통계 정보 출력"""
        conn = sqlite3.connect(self.db_path)
        
        # 각 테이블의 레코드 수
        stats = {}
        
        tables = ['matching_reference', 'page_snapshots', 'product_states', 'change_events']
        
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
        except sqlite3.OperationalError:
            matched_count = 0
        
        # 최신 스냅샷 정보
        try:
            latest_snapshot = conn.execute("""
                SELECT snapshot_time, total_products 
                FROM page_snapshots 
                ORDER BY id DESC LIMIT 1
            """).fetchone()
        except sqlite3.OperationalError:
            latest_snapshot = None
        
        conn.close()
        
        print(f"\n=== DB 통계 정보 ===")
        print(f"DB 파일: {self.db_path}")
        print(f"매칭 참조: {stats.get('matching_reference', 0)}개 상품")
        print(f"  - 매칭 완료: {matched_count}개")
        print(f"  - 미매칭: {stats.get('matching_reference', 0) - matched_count}개")
        print(f"페이지 스냅샷: {stats.get('page_snapshots', 0)}회 수집")
        print(f"상품 상태: {stats.get('product_states', 0)}개 레코드")
        print(f"변화 이벤트: {stats.get('change_events', 0)}개")
        
        if latest_snapshot:
            print(f"최근 수집: {latest_snapshot[0]} ({latest_snapshot[1]}개 상품)")
        else:
            print("수집된 데이터가 없습니다.")
        
        return stats


def export_all_data(db_path="page_monitoring.db"):
    """모든 데이터를 CSV로 출력"""
    try:
        exporter = CSVExporter(db_path)
        
        print("DB 통계 정보:")
        stats = exporter.get_db_statistics()
        
        if stats.get('page_snapshots', 0) == 0:
            print("출력할 데이터가 없습니다. 먼저 모니터링을 실행하세요.")
            return
        
        print(f"\n=== CSV 출력 시작 ===")
        
        # 1. 최신 스냅샷
        exporter.export_latest_snapshot()
        
        # 2. 최근 7일 변화 요약
        exporter.export_change_summary(days=7)
        
        # 3. 매칭된 상품만 (기존 CSV 형태)
        exporter.export_matched_products_only()
        
        # 4. 상위 30개 상품 순위 추세
        exporter.export_ranking_trends(top_n=30)
        
        print(f"=== 모든 CSV 출력 완료 ===")
        
    except FileNotFoundError as e:
        print(f"오류: {e}")
        print("먼저 page_monitor.py를 실행하여 DB를 생성하세요.")
    except Exception as e:
        print(f"CSV 출력 중 오류 발생: {e}")


if __name__ == "__main__":
    # 사용 예시
    export_all_data()