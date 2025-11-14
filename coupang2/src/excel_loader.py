#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Loader - 아이허브 엑셀 파일을 통합 DB에 적재
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
price_inventory, SELLER_INSIGHTS, Coupang_Price 3종 처리
"""

import pandas as pd
import re
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime


def safe_read_excel_header_guess(path, max_try=20):
    """상단 안내문 줄이 있는 엑셀에서 실제 헤더를 자동 탐색"""
    KEY_CANDIDATES = {"업체상품코드", "옵션 ID", "업체상품 ID", "바코드"}
    for header_row in range(max_try):
        df_try = pd.read_excel(path, header=header_row)
        cols = set(map(str, df_try.columns))
        if KEY_CANDIDATES & cols:
            return df_try
    return pd.read_excel(path)


def _pick_col(df: pd.DataFrame, candidates):
    """여러 후보 중 존재하는 첫 번째 컬럼명을 고름"""
    for c in candidates:
        if c in df.columns:
            return c
    return None


class ExcelLoader:
    """엑셀 파일을 통합 DB에 적재"""
    
    def __init__(self, db):
        """
        Args:
            db: IntegratedDatabase 인스턴스
        """
        self.db = db
    
    def load_all_excel_files(self, snapshot_id: int, 
                            excel_dir: Path,
                            price_file: Optional[Path] = None,
                            insights_file: Optional[Path] = None,
                            reco_file: Optional[Path] = None,
                            upc_file: Optional[Path] = None) -> Dict[str, int]:
        """모든 엑셀 파일 적재
        
        Args:
            snapshot_id: 대상 snapshot ID
            excel_dir: 엑셀 디렉토리
            price_file: price_inventory 파일 (None이면 자동 탐색)
            insights_file: SELLER_INSIGHTS 파일 (None이면 자동 탐색)
            reco_file: Coupang_Price 파일 (None이면 자동 탐색)
            upc_file: UPC 파일 (None이면 자동 탐색)
        
        Returns:
            {'products': count, 'prices': count, 'features': count}
        """
        
        print(f"\n{'='*80}")
        print(f"📥 엑셀 파일 적재 시작 (Snapshot ID: {snapshot_id})")
        print(f"{'='*80}\n")
        
        # 파일 자동 탐색
        if price_file is None:
            price_files = list(excel_dir.glob("*price_inventory*.xlsx"))
            price_file = sorted(price_files, key=lambda x: x.stat().st_mtime, reverse=True)[0] if price_files else None
        
        if insights_file is None:
            insights_files = list(excel_dir.glob("*SELLER_INSIGHTS*.xlsx"))
            insights_file = sorted(insights_files, key=lambda x: x.stat().st_mtime, reverse=True)[0] if insights_files else None
        
        if reco_file is None:
            reco_files = list(excel_dir.glob("Coupang_Price_*.xlsx"))
            reco_file = sorted(reco_files, key=lambda x: x.stat().st_mtime, reverse=True)[0] if reco_files else None
        
        if upc_file is None:
            upc_files = list(excel_dir.glob("20251024_*.xlsx"))
            upc_file = sorted(upc_files, key=lambda x: x.stat().st_mtime, reverse=True)[0] if upc_files else None
        
        # 1. price_inventory 처리
        products_data = []
        prices_data = []
        
        if price_file and price_file.exists():
            print(f"📄 1. Price Inventory: {price_file.name}")
            prod, price = self._load_price_inventory(price_file)
            products_data.extend(prod)
            prices_data.extend(price)
            print(f"   ✓ 상품: {len(prod):,}개, 가격: {len(price):,}개\n")
        else:
            print(f"⚠️  1. Price Inventory: 파일 없음\n")
        
        # 2. UPC 처리 (products에 병합)
        if upc_file and upc_file.exists():
            print(f"📄 2. UPC: {upc_file.name}")
            upc_data = self._load_upc(upc_file)
            self._merge_upc_to_products(products_data, upc_data)
            print(f"   ✓ UPC: {len(upc_data):,}개\n")
        else:
            print(f"⚠️  2. UPC: 파일 없음\n")
        
        # 3. Coupang_Price 처리 (prices에 병합)
        if reco_file and reco_file.exists():
            print(f"📄 3. Coupang Price: {reco_file.name}")
            reco_data = self._load_coupang_recommended_price(reco_file)
            self._merge_reco_to_prices(prices_data, reco_data)
            print(f"   ✓ 추천가: {len(reco_data):,}개\n")
        else:
            print(f"⚠️  3. Coupang Price: 파일 없음\n")
        
        # 4. SELLER_INSIGHTS 처리
        features_data = []
        if insights_file and insights_file.exists():
            print(f"📄 4. Seller Insights: {insights_file.name}")
            features_data = self._load_seller_insights(insights_file)
            print(f"   ✓ 성과: {len(features_data):,}개\n")
        else:
            print(f"⚠️  4. Seller Insights: 파일 없음\n")
        
        # 5. DB 저장
        print(f"💾 DB 저장 중...")
        
        if products_data:
            self.db.batch_upsert_products(products_data)
            print(f"   ✓ Products: {len(products_data):,}개")
        
        if prices_data:
            # snapshot_id 추가
            for p in prices_data:
                p['snapshot_id'] = snapshot_id
            self.db.batch_save_product_prices(snapshot_id, prices_data)
            print(f"   ✓ Prices: {len(prices_data):,}개")
        
        if features_data:
            # snapshot_id 추가
            for f in features_data:
                f['snapshot_id'] = snapshot_id
            self.db.batch_save_product_features(snapshot_id, features_data)
            print(f"   ✓ Features: {len(features_data):,}개")
        
        print(f"\n{'='*80}")
        print(f"✅ 엑셀 적재 완료")
        print(f"{'='*80}\n")
        
        return {
            'products': len(products_data),
            'prices': len(prices_data),
            'features': len(features_data)
        }
    
    def _load_price_inventory(self, file_path: Path) -> tuple:
        """price_inventory 엑셀 읽기
        
        Returns:
            (products_data, prices_data)
        """
        try:
            df = pd.read_excel(file_path, sheet_name='data', skiprows=2)
        except Exception:
            df = safe_read_excel_header_guess(file_path, max_try=30)
        
        # 컬럼 매핑
        col_vid = _pick_col(df, ['옵션 ID'])
        col_pid = _pick_col(df, ['Product ID', 'productId', 'PRODUCT_ID'])
        col_iid = _pick_col(df, ['업체상품 ID', 'itemId', 'ITEM_ID'])
        col_pname = _pick_col(df, ['쿠팡 노출 상품명', '상품명'])
        col_pn = _pick_col(df, ['업체상품코드'])
        col_price = _pick_col(df, ['판매가격', '판매가격.1'])
        col_original = _pick_col(df, ['할인율기준가'])
        col_stock = _pick_col(df, ['잔여수량(재고)', '잔여수량'])
        col_state = _pick_col(df, ['판매상태', '판매상태.1'])
        
        # 기본값 설정
        if col_vid is None:   df['옵션 ID'] = None;            col_vid = '옵션 ID'
        if col_pid is None:   df['Product ID'] = None;         col_pid = 'Product ID'
        if col_iid is None:   df['업체상품 ID'] = None;        col_iid = '업체상품 ID'
        if col_pname is None: df['쿠팡 노출 상품명'] = None;    col_pname = '쿠팡 노출 상품명'
        if col_pn is None:    df['업체상품코드'] = None;        col_pn = '업체상품코드'
        if col_price is None: df['판매가격'] = 0;               col_price = '판매가격'
        if col_original is None: df['할인율기준가'] = 0;        col_original = '할인율기준가'
        if col_stock is None: df['잔여수량(재고)'] = 0;         col_stock = '잔여수량(재고)'
        if col_state is None: df['판매상태'] = None;            col_state = '판매상태'
        
        # products 데이터
        products_data = []
        prices_data = []
        
        for _, row in df.iterrows():
            vendor_id = str(row[col_vid]).split('.')[0]
            if vendor_id == '<NA>' or vendor_id == 'nan':
                continue
            
            product_id = str(row[col_pid]).replace('.0', '')
            item_id = str(row[col_iid]).replace('.0', '')
            name = row[col_pname]
            part_number = str(row[col_pn]).strip()
            
            products_data.append({
                'vendor_item_id': vendor_id,
                'product_id': product_id if product_id != 'nan' else None,
                'item_id': item_id if item_id != 'nan' else None,
                'part_number': part_number if part_number != 'nan' else None,
                'upc': None,  # UPC는 별도 파일에서
                'name': name if pd.notna(name) else None
            })
            
            # prices 데이터 (snapshot_id는 나중에 추가)
            iherb_price = pd.to_numeric(row[col_price], errors='coerce')
            iherb_original = pd.to_numeric(row[col_original], errors='coerce')
            
            prices_data.append({
                'vendor_item_id': vendor_id,
                'rocket_price': None,
                'rocket_original_price': None,
                'iherb_price': int(iherb_price) if pd.notna(iherb_price) else None,
                'iherb_original_price': int(iherb_original) if pd.notna(iherb_original) else None,
                'iherb_recommended_price': None  # 추천가는 별도 파일에서
            })
        
        return products_data, prices_data
    
    def _load_upc(self, file_path: Path) -> List[Dict]:
        """UPC 엑셀 읽기"""
        df = pd.read_excel(file_path)
        
        col_item_id = None
        col_upc = None
        
        for col in df.columns:
            if '쿠팡 상품번호' in str(col) or '상품번호' in str(col):
                col_item_id = col
            if 'UPC' in str(col).upper():
                col_upc = col
        
        if col_item_id is None or col_upc is None:
            return []
        
        upc_data = []
        for _, row in df.iterrows():
            item_id = str(row[col_item_id]).replace('.0', '')
            upc = str(row[col_upc]).strip()
            
            if item_id != 'nan' and upc != 'nan':
                upc_data.append({
                    'item_id': item_id,
                    'upc': upc
                })
        
        return upc_data
    
    def _merge_upc_to_products(self, products_data: List[Dict], upc_data: List[Dict]):
        """UPC를 products에 병합"""
        upc_map = {u['item_id']: u['upc'] for u in upc_data}
        
        for p in products_data:
            item_id = p.get('item_id')
            if item_id and item_id in upc_map:
                p['upc'] = upc_map[item_id]
    
    def _load_coupang_recommended_price(self, file_path: Path) -> List[Dict]:
        """Coupang_Price 엑셀 읽기"""
        df = pd.read_excel(file_path, header=1)
        
        reco_data = []
        for _, row in df.iterrows():
            vendor_id = str(row['옵션ID']).replace('.0', '')
            reco_price = pd.to_numeric(row['쿠팡추천가 (원)'], errors='coerce')
            
            if vendor_id != 'nan' and pd.notna(reco_price):
                reco_data.append({
                    'vendor_item_id': vendor_id,
                    'iherb_recommended_price': int(reco_price)
                })
        
        return reco_data
    
    def _merge_reco_to_prices(self, prices_data: List[Dict], reco_data: List[Dict]):
        """추천가를 prices에 병합"""
        reco_map = {r['vendor_item_id']: r['iherb_recommended_price'] for r in reco_data}
        
        for p in prices_data:
            vendor_id = p.get('vendor_item_id')
            if vendor_id and vendor_id in reco_map:
                p['iherb_recommended_price'] = reco_map[vendor_id]
    
    def _load_seller_insights(self, file_path: Path) -> List[Dict]:
        """SELLER_INSIGHTS 엑셀 읽기"""
        df = pd.read_excel(file_path, sheet_name='vendor item metrics')
        
        def to_int(s):
            return pd.to_numeric(s, errors='coerce').fillna(0).astype(int)
        
        def to_float(s):
            return pd.to_numeric(s, errors='coerce').fillna(0.0).round(1)
        
        features_data = []
        for _, row in df.iterrows():
            vendor_id = str(row['옵션 ID'])
            if vendor_id == 'nan' or vendor_id == '<NA>':
                continue
            
            # product_features 스키마에 맞춤
            features_data.append({
                'vendor_item_id': vendor_id,
                'rocket_rank': None,  # 로켓은 별도
                'rocket_rating': None,
                'rocket_reviews': None,
                'iherb_stock': None,  # price_inventory에서
                'iherb_stock_status': None,
                'iherb_revenue': to_int(row.get('매출(원)', 0)),
                'iherb_sales_quantity': to_int(row.get('판매량', 0)),
                'iherb_item_winner_ratio': to_float(row.get('아이템위너 비율(%)', 0))
            })
        
        return features_data


def main():
    """테스트"""
    from database import IntegratedDatabase
    
    db_path = "/home/claude/test_integrated.db"
    excel_dir = Path("/home/claude/test_excel")
    
    db = IntegratedDatabase(db_path)
    db.init_database()
    
    # Snapshot 생성
    snapshot_id = db.create_snapshot(
        snapshot_date='2025-01-15',
        file_names={
            'price': 'price_20250115.xlsx',
            'insights': 'insights_20250115.xlsx',
            'reco': 'reco_20250115.xlsx'
        }
    )
    
    # Excel 적재
    loader = ExcelLoader(db)
    
    # 실제 파일 경로를 지정하거나 자동 탐색
    # result = loader.load_all_excel_files(snapshot_id, excel_dir)
    
    print(f"✅ 테스트 완료")


if __name__ == "__main__":
    main()