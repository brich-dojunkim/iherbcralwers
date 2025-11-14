#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data Migrator - 기존 monitoring.db → 통합 DB
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
로켓직구 시계열 데이터를 날짜별 snapshot으로 변환
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List


class DataMigrator:
    """기존 DB → 통합 DB 마이그레이션"""
    
    def __init__(self, legacy_db_path: str, integrated_db):
        """
        Args:
            legacy_db_path: 기존 monitoring.db 경로
            integrated_db: IntegratedDatabase 인스턴스
        """
        self.legacy_db_path = legacy_db_path
        self.integrated_db = integrated_db
    
    def migrate_all(self):
        """전체 마이그레이션 실행"""
        
        print(f"\n{'='*80}")
        print(f"🔄 데이터 마이그레이션 시작")
        print(f"{'='*80}")
        print(f"Legacy DB: {self.legacy_db_path}")
        print(f"Integrated DB: {self.integrated_db.db_path}")
        print(f"{'='*80}\n")
        
        # 1. 날짜별 snapshot 목록 조회
        snapshot_dates = self._get_snapshot_dates()
        
        if not snapshot_dates:
            print("⚠️  마이그레이션할 데이터가 없습니다.")
            return
        
        print(f"📅 발견된 날짜: {len(snapshot_dates)}개")
        for date_info in snapshot_dates[:5]:
            print(f"   • {date_info['date']}: {date_info['count']}개 스냅샷")
        if len(snapshot_dates) > 5:
            print(f"   ... 외 {len(snapshot_dates) - 5}개")
        print()
        
        # 2. 날짜별 마이그레이션
        total_products = 0
        total_snapshots = 0
        
        for date_info in snapshot_dates:
            date_str = date_info['date']
            print(f"\n📆 {date_str} 처리 중...")
            
            # 해당 날짜의 카테고리별 최신 snapshot 조회
            snapshots = self._get_snapshots_by_date(date_str)
            
            if not snapshots:
                continue
            
            # 통합 DB에 snapshot 생성
            snapshot_id = self._create_integrated_snapshot(date_str, snapshots)
            
            # 각 카테고리의 데이터 마이그레이션
            for snap in snapshots:
                products = self._get_products_from_snapshot(snap['id'])
                
                if products:
                    self._migrate_products(snapshot_id, products, snap['category'])
                    total_products += len(products)
                    print(f"   ✓ {snap['category']}: {len(products)}개 상품")
            
            total_snapshots += 1
        
        print(f"\n{'='*80}")
        print(f"✅ 마이그레이션 완료")
        print(f"{'='*80}")
        print(f"총 Snapshot: {total_snapshots}개")
        print(f"총 상품 레코드: {total_products:,}개")
        print(f"{'='*80}\n")
    
    def _get_snapshot_dates(self) -> List[Dict]:
        """날짜별 snapshot 목록 조회"""
        conn = sqlite3.connect(self.legacy_db_path)
        
        query = """
            SELECT DATE(snapshot_time) as date, COUNT(*) as count
            FROM snapshots
            WHERE source_id = (SELECT id FROM sources WHERE source_type = 'rocket_direct')
            GROUP BY DATE(snapshot_time)
            ORDER BY date DESC
        """
        
        cursor = conn.execute(query)
        results = [{'date': row[0], 'count': row[1]} for row in cursor.fetchall()]
        
        conn.close()
        return results
    
    def _get_snapshots_by_date(self, target_date: str) -> List[Dict]:
        """특정 날짜의 카테고리별 최신 snapshot 조회"""
        conn = sqlite3.connect(self.legacy_db_path)
        
        # 카테고리별 최신 snapshot
        query = """
            SELECT s.id, s.page_url, s.snapshot_time, c.name as category, c.coupang_category_id
            FROM snapshots s
            JOIN categories c ON s.category_id = c.id
            WHERE DATE(s.snapshot_time) = ?
            AND s.source_id = (SELECT id FROM sources WHERE source_type = 'rocket_direct')
            AND s.id IN (
                SELECT MAX(id)
                FROM snapshots
                WHERE DATE(snapshot_time) = ?
                AND source_id = (SELECT id FROM sources WHERE source_type = 'rocket_direct')
                GROUP BY category_id
            )
            ORDER BY c.id
        """
        
        cursor = conn.execute(query, (target_date, target_date))
        
        snapshots = []
        for row in cursor.fetchall():
            snapshots.append({
                'id': row[0],
                'page_url': row[1],
                'snapshot_time': row[2],
                'category': row[3],
                'category_id': row[4]
            })
        
        conn.close()
        return snapshots
    
    def _create_integrated_snapshot(self, date_str: str, snapshots: List[Dict]) -> int:
        """통합 DB에 snapshot 생성"""
        
        # URL을 카테고리별로 매핑
        rocket_urls = {}
        
        for snap in snapshots:
            cat_id = snap['category_id']
            page_url = snap['page_url']
            
            # category_id로 url_column 결정
            if cat_id == '305433':  # 헬스/건강식품
                rocket_urls['url_1'] = page_url
            elif cat_id == '219079':  # 출산유아동
                rocket_urls['url_2'] = page_url
            elif cat_id == '317675':  # 스포츠레저
                rocket_urls['url_3'] = page_url
        
        snapshot_id = self.integrated_db.create_snapshot(
            snapshot_date=date_str,
            rocket_urls=rocket_urls,
            file_names=None  # 엑셀은 나중에 추가
        )
        
        return snapshot_id
    
    def _get_products_from_snapshot(self, snapshot_id: int) -> List[Dict]:
        """특정 snapshot의 상품 데이터 조회"""
        conn = sqlite3.connect(self.legacy_db_path)
        
        query = """
            SELECT 
                vendor_item_id,
                category_rank,
                product_name,
                product_url,
                current_price,
                original_price,
                discount_rate,
                rating_score,
                review_count
            FROM product_states
            WHERE snapshot_id = ?
            ORDER BY category_rank
        """
        
        cursor = conn.execute(query, (snapshot_id,))
        
        products = []
        for row in cursor.fetchall():
            products.append({
                'vendor_item_id': row[0],
                'rank': row[1],
                'name': row[2],
                'url': row[3],
                'current_price': row[4],
                'original_price': row[5],
                'discount_rate': row[6],
                'rating': row[7],
                'reviews': row[8]
            })
        
        conn.close()
        return products
    
    def _migrate_products(self, snapshot_id: int, products: List[Dict], category: str):
        """상품 데이터를 통합 DB로 마이그레이션"""
        
        # URL에서 product_id, item_id 추출
        import re
        
        products_data = []
        prices_data = []
        features_data = []
        
        for p in products:
            vendor_id = p['vendor_item_id']
            url = p['url']
            
            # product_id, item_id 추출
            product_id = None
            item_id = None
            
            if url:
                m_product = re.search(r'/products/(\d+)', url)
                if m_product:
                    product_id = m_product.group(1)
                
                m_item = re.search(r'itemId=(\d+)', url)
                if m_item:
                    item_id = m_item.group(1)
            
            # products 테이블
            products_data.append({
                'vendor_item_id': vendor_id,
                'product_id': product_id,
                'item_id': item_id,
                'part_number': None,
                'upc': None,
                'name': p['name']
            })
            
            # product_price 테이블
            prices_data.append({
                'vendor_item_id': vendor_id,
                'rocket_price': p['current_price'],
                'rocket_original_price': p['original_price'],
                'iherb_price': None,
                'iherb_original_price': None,
                'iherb_recommended_price': None
            })
            
            # product_features 테이블
            features_data.append({
                'vendor_item_id': vendor_id,
                'rocket_rank': p['rank'],
                'rocket_rating': p['rating'],
                'rocket_reviews': p['reviews'],
                'iherb_stock': None,
                'iherb_stock_status': None,
                'iherb_revenue': None,
                'iherb_sales_quantity': None,
                'iherb_item_winner_ratio': None
            })
        
        # 일괄 저장
        if products_data:
            self.integrated_db.batch_upsert_products(products_data)
        
        if prices_data:
            self.integrated_db.batch_save_product_prices(snapshot_id, prices_data)
        
        if features_data:
            self.integrated_db.batch_save_product_features(snapshot_id, features_data)


def main():
    """마이그레이션 실행"""
    from database import IntegratedDatabase
    
    # 경로 설정 (실제 경로로 변경 필요)
    legacy_db_path = "/Users/brich/Desktop/iherb_price/coupang2/data/rocket/monitoring.db"
    integrated_db_path = "/Users/brich/Desktop/iherb_price/coupang2/data/integrated/rocket_iherb.db"
    
    # 통합 DB 초기화
    integrated_db = IntegratedDatabase(integrated_db_path)
    integrated_db.init_database()
    
    # 마이그레이션 실행
    migrator = DataMigrator(legacy_db_path, integrated_db)
    
    print("\n⚠️  경고: 기존 통합 DB의 데이터가 있다면 중복될 수 있습니다.")
    confirm = input("계속하시겠습니까? (yes/no): ").strip().lower()
    
    if confirm == 'yes':
        migrator.migrate_all()
    else:
        print("❌ 취소되었습니다.")


if __name__ == "__main__":
    main()