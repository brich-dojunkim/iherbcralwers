#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DB 상태 종합 요약 리포트
- 모든 테이블 통계
- 데이터 무결성 체크
- 매칭 상태 분석
"""

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path


DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/monitoring.db"


class DatabaseSummary:
    """데이터베이스 상태 요약 리포트"""
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """DB 연결"""
        self.conn = sqlite3.connect(self.db_path)
        print(f"✅ DB 연결: {self.db_path}")
    
    def close(self):
        """DB 연결 종료"""
        if self.conn:
            self.conn.close()
            print(f"✅ DB 연결 종료")
    
    def print_section(self, title: str):
        """섹션 헤더 출력"""
        print(f"\n{'='*80}")
        print(f"{title}")
        print(f"{'='*80}\n")
    
    def section_1_basic_info(self):
        """1. DB 기본 정보"""
        self.print_section("1. DB 기본 정보")
        
        # DB 파일 크기
        db_size_mb = Path(self.db_path).stat().st_size / 1024 / 1024
        
        # 페이지 정보
        page_count = self.conn.execute("PRAGMA page_count").fetchone()[0]
        page_size = self.conn.execute("PRAGMA page_size").fetchone()[0]
        db_size_from_pragma = page_count * page_size / 1024 / 1024
        
        print(f"DB 경로: {self.db_path}")
        print(f"파일 크기: {db_size_mb:.2f} MB")
        print(f"DB 크기 (PRAGMA): {db_size_from_pragma:.2f} MB")
        print(f"페이지 수: {page_count:,}")
        print(f"페이지 크기: {page_size:,} bytes")
        print(f"현재 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def section_2_table_counts(self):
        """2. 테이블별 레코드 수"""
        self.print_section("2. 테이블별 레코드 수")
        
        tables = ['sources', 'categories', 'snapshots', 'product_states', 'matching_reference']
        
        data = []
        for table in tables:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            data.append({'테이블': table, '레코드 수': f"{count:,}"})
        
        df = pd.DataFrame(data)
        print(df.to_string(index=False))
    
    def section_3_source_stats(self):
        """3. 소스별 통계"""
        self.print_section("3. 소스별 통계")
        
        query = """
        SELECT 
            s.source_type as '소스타입',
            s.display_name as '표시명',
            COUNT(DISTINCT snap.id) as '스냅샷수',
            COUNT(DISTINCT ps.vendor_item_id) as '고유제품수',
            MIN(DATE(snap.snapshot_time)) as '첫스냅샷',
            MAX(DATE(snap.snapshot_time)) as '최근스냅샷'
        FROM sources s
        LEFT JOIN snapshots snap ON s.id = snap.source_id
        LEFT JOIN product_states ps ON snap.id = ps.snapshot_id
        GROUP BY s.source_type, s.display_name
        ORDER BY COUNT(DISTINCT snap.id) DESC
        """
        
        df = pd.read_sql_query(query, self.conn)
        print(df.to_string(index=False))
    
    def section_4_category_stats(self):
        """4. 카테고리별 통계"""
        self.print_section("4. 카테고리별 통계")
        
        query = """
        SELECT 
            c.name as '카테고리',
            COUNT(DISTINCT snap.id) as '스냅샷수',
            COUNT(DISTINCT ps.vendor_item_id) as '고유제품수',
            MIN(DATE(snap.snapshot_time)) as '첫스냅샷',
            MAX(DATE(snap.snapshot_time)) as '최근스냅샷'
        FROM categories c
        LEFT JOIN snapshots snap ON c.id = snap.category_id
        LEFT JOIN product_states ps ON snap.id = ps.snapshot_id
        GROUP BY c.name
        ORDER BY COUNT(DISTINCT snap.id) DESC
        """
        
        df = pd.read_sql_query(query, self.conn)
        print(df.to_string(index=False))
    
    def section_5_recent_snapshots(self):
        """5. 최근 스냅샷"""
        self.print_section("5. 최근 스냅샷 (최근 10개)")
        
        query = """
        SELECT 
            snap.id as 'ID',
            src.source_type as '소스',
            cat.name as '카테고리',
            DATE(snap.snapshot_time) as '날짜',
            TIME(snap.snapshot_time) as '시각',
            snap.total_products as '제품수',
            ROUND(snap.crawl_duration_seconds, 1) as '소요시간(초)',
            snap.error_message as '에러'
        FROM snapshots snap
        JOIN sources src ON snap.source_id = src.id
        JOIN categories cat ON snap.category_id = cat.id
        ORDER BY snap.id DESC
        LIMIT 10
        """
        
        df = pd.read_sql_query(query, self.conn)
        print(df.to_string(index=False))
    
    def section_6_daily_snapshot_counts(self):
        """6. 날짜별 스냅샷 수"""
        self.print_section("6. 날짜별 스냅샷 수")
        
        query = """
        SELECT 
            DATE(snapshot_time) as '날짜',
            COUNT(*) as '스냅샷수',
            SUM(total_products) as '총제품수'
        FROM snapshots
        GROUP BY DATE(snapshot_time)
        ORDER BY DATE(snapshot_time) DESC
        LIMIT 15
        """
        
        df = pd.read_sql_query(query, self.conn)
        print(df.to_string(index=False))
    
    def section_7_matching_stats(self):
        """7. 매칭 통계"""
        self.print_section("7. 매칭 통계")
        
        queries = [
            ("전체 매칭", "SELECT COUNT(*) FROM matching_reference"),
            ("UPC 있음", "SELECT COUNT(*) FROM matching_reference WHERE iherb_upc IS NOT NULL"),
            ("품번 있음", "SELECT COUNT(*) FROM matching_reference WHERE iherb_part_number IS NOT NULL AND iherb_part_number != ''"),
            ("UPC + 품번 모두", "SELECT COUNT(*) FROM matching_reference WHERE iherb_upc IS NOT NULL AND iherb_part_number IS NOT NULL AND iherb_part_number != ''")
        ]
        
        data = []
        for metric, query in queries:
            count = self.conn.execute(query).fetchone()[0]
            data.append({'지표': metric, '개수': f"{count:,}"})
        
        df = pd.DataFrame(data)
        print(df.to_string(index=False))
    
    def section_8_matching_by_source(self):
        """8. 소스별 매칭 통계"""
        self.print_section("8. 소스별 매칭 통계")
        
        query = """
        SELECT 
            matching_source as '매칭소스',
            COUNT(*) as '총개수',
            SUM(CASE WHEN iherb_upc IS NOT NULL THEN 1 ELSE 0 END) as 'UPC있음',
            ROUND(100.0 * SUM(CASE WHEN iherb_upc IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as 'UPC비율(%)'
        FROM matching_reference
        GROUP BY matching_source
        ORDER BY COUNT(*) DESC
        """
        
        df = pd.read_sql_query(query, self.conn)
        print(df.to_string(index=False))
    
    def section_9_db_product_matching(self):
        """9. DB 내 제품 매칭률"""
        self.print_section("9. DB 내 제품 매칭률")
        
        total_products = self.conn.execute("""
            SELECT COUNT(DISTINCT vendor_item_id) 
            FROM product_states 
            WHERE vendor_item_id IS NOT NULL
        """).fetchone()[0]
        
        matched_products = self.conn.execute("""
            SELECT COUNT(DISTINCT ps.vendor_item_id)
            FROM product_states ps
            INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        """).fetchone()[0]
        
        unmatched_products = total_products - matched_products
        match_rate = (matched_products / total_products * 100) if total_products > 0 else 0
        
        data = [
            {'지표': 'DB 전체 제품', '개수': f"{total_products:,}"},
            {'지표': '매칭된 제품', '개수': f"{matched_products:,}"},
            {'지표': '미매칭 제품', '개수': f"{unmatched_products:,}"},
            {'지표': '매칭률', '개수': f"{match_rate:.1f}%"}
        ]
        
        df = pd.DataFrame(data)
        print(df.to_string(index=False))
    
    def section_10_source_matching_rate(self):
        """10. 소스별 DB 매칭률"""
        self.print_section("10. 소스별 DB 매칭률")
        
        query = """
        SELECT 
            src.source_type as '소스',
            COUNT(DISTINCT ps.vendor_item_id) as '총제품수',
            COUNT(DISTINCT CASE WHEN mr.vendor_item_id IS NOT NULL THEN ps.vendor_item_id END) as '매칭제품수',
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN mr.vendor_item_id IS NOT NULL THEN ps.vendor_item_id END) / COUNT(DISTINCT ps.vendor_item_id), 1) as '매칭률(%)'
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources src ON snap.source_id = src.id
        LEFT JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE ps.vendor_item_id IS NOT NULL
        GROUP BY src.source_type
        ORDER BY COUNT(DISTINCT ps.vendor_item_id) DESC
        """
        
        df = pd.read_sql_query(query, self.conn)
        print(df.to_string(index=False))
    
    def section_11_vendor_item_id_check(self):
        """11. vendor_item_id NULL 체크"""
        self.print_section("11. vendor_item_id NULL 체크")
        
        null_count = self.conn.execute("""
            SELECT COUNT(*) FROM product_states WHERE vendor_item_id IS NULL
        """).fetchone()[0]
        
        empty_count = self.conn.execute("""
            SELECT COUNT(*) FROM product_states WHERE vendor_item_id = ''
        """).fetchone()[0]
        
        data = [
            {'이슈': 'NULL vendor_item_id', '개수': f"{null_count:,}"},
            {'이슈': '빈 문자열 vendor_item_id', '개수': f"{empty_count:,}"}
        ]
        
        df = pd.DataFrame(data)
        print(df.to_string(index=False))
    
    def section_12_schema_validation(self):
        """12. 스키마 검증"""
        self.print_section("12. 스키마 검증")
        
        # vendor_item_id 컬럼 존재 확인
        vendor_col = self.conn.execute("""
            SELECT COUNT(*) FROM pragma_table_info('product_states') 
            WHERE name = 'vendor_item_id'
        """).fetchone()[0]
        
        # coupang_product_id 컬럼 존재 확인
        coupang_col = self.conn.execute("""
            SELECT COUNT(*) FROM pragma_table_info('product_states') 
            WHERE name = 'coupang_product_id'
        """).fetchone()[0]
        
        data = [
            {
                '체크항목': 'product_states에 vendor_item_id 컬럼',
                '상태': '✅ 존재' if vendor_col > 0 else '❌ 없음'
            },
            {
                '체크항목': 'product_states에 coupang_product_id 컬럼',
                '상태': '⚠️ 존재 (마이그레이션 필요)' if coupang_col > 0 else '✅ 없음 (정리됨)'
            }
        ]
        
        df = pd.DataFrame(data)
        print(df.to_string(index=False))
    
    def section_13_recent_product_sample(self):
        """13. 최근 제품 샘플"""
        self.print_section("13. 최근 제품 샘플 (상위 5개)")
        
        query = """
        SELECT 
            ps.vendor_item_id as 'VendorItemID',
            SUBSTR(ps.product_name, 1, 40) || '...' as '상품명',
            ps.category_rank as '순위',
            ps.current_price as '가격',
            mr.iherb_upc as 'UPC',
            SUBSTR(mr.iherb_part_number, 1, 15) as '품번',
            mr.matching_source as '매칭소스'
        FROM product_states ps
        LEFT JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE ps.snapshot_id = (SELECT MAX(id) FROM snapshots)
        ORDER BY ps.category_rank
        LIMIT 5
        """
        
        df = pd.read_sql_query(query, self.conn)
        print(df.to_string(index=False))
    
    def section_14_data_integrity(self):
        """14. 데이터 무결성 체크"""
        self.print_section("14. 데이터 무결성 체크")
        
        # 중복 vendor_item_id (같은 스냅샷 내)
        dup_vendor = self.conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT snapshot_id, vendor_item_id, COUNT(*) as cnt
                FROM product_states
                WHERE vendor_item_id IS NOT NULL
                GROUP BY snapshot_id, vendor_item_id
                HAVING cnt > 1
            )
        """).fetchone()[0]
        
        # 순위 중복 (같은 스냅샷 내)
        dup_rank = self.conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT snapshot_id, category_rank, COUNT(*) as cnt
                FROM product_states
                GROUP BY snapshot_id, category_rank
                HAVING cnt > 1
            )
        """).fetchone()[0]
        
        # 순위 1부터 시작하지 않는 스냅샷
        no_rank_1 = self.conn.execute("""
            SELECT COUNT(DISTINCT snapshot_id)
            FROM product_states ps
            WHERE ps.snapshot_id NOT IN (
                SELECT snapshot_id 
                FROM product_states 
                WHERE category_rank = 1
            )
        """).fetchone()[0]
        
        data = [
            {'이슈': '중복 vendor_item_id (같은 스냅샷)', '개수': f"{dup_vendor:,}"},
            {'이슈': '순위 중복 (같은 스냅샷)', '개수': f"{dup_rank:,}"},
            {'이슈': '순위 1부터 시작하지 않는 스냅샷', '개수': f"{no_rank_1:,}"}
        ]
        
        df = pd.DataFrame(data)
        print(df.to_string(index=False))
    
    def generate_full_report(self):
        """전체 리포트 생성"""
        print("\n" + "="*80)
        print("📊 DB 상태 종합 요약 리포트")
        print("="*80)
        
        try:
            self.connect()
            
            self.section_1_basic_info()
            self.section_2_table_counts()
            self.section_3_source_stats()
            self.section_4_category_stats()
            self.section_5_recent_snapshots()
            self.section_6_daily_snapshot_counts()
            self.section_7_matching_stats()
            self.section_8_matching_by_source()
            self.section_9_db_product_matching()
            self.section_10_source_matching_rate()
            self.section_11_vendor_item_id_check()
            self.section_12_schema_validation()
            self.section_13_recent_product_sample()
            self.section_14_data_integrity()
            
            print("\n" + "="*80)
            print("✅ 리포트 생성 완료")
            print("="*80 + "\n")
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.close()


def main():
    """메인 함수"""
    
    # DB 파일 존재 확인
    if not Path(DB_PATH).exists():
        print(f"❌ DB 파일이 없습니다: {DB_PATH}")
        return
    
    # 리포트 생성
    summary = DatabaseSummary(DB_PATH)
    summary.generate_full_report()


if __name__ == "__main__":
    main()