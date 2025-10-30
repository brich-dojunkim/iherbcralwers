#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
로켓직구 ↔ 아이허브 매칭 데이터 준비 스크립트
- 로켓직구: DB에서 로드
- 아이허브: Excel 파일에서 로드
- UPC 기반 매칭

데이터 소스:
1. 로켓직구 (rocket_direct):
   - vendor_item_id: DB의 product_states.vendor_item_id
   - 매칭 정보는 rocket.csv 또는 자동 매칭

2. 아이허브 (Excel):
   - price_inventory: 가격/재고 정보
   - 20251024_1444: UPC 정보
   - UPC를 키로 매칭
"""

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path


# ==================== 설정 ====================
PRICE_INVENTORY_PATH = "/Users/brich/Desktop/iherb_price/coupang2/data/iherb/price_inventory_251028.xlsx"
OFFICIAL_EXCEL_PATH = "/Users/brich/Desktop/iherb_price/coupang2/data/iherb/20251024_1444.xlsx"
ROCKET_CSV_PATH = "/Users/brich/Desktop/iherb_price/coupang2/data/rocket/rocket.csv"
DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/data/rocket/monitoring.db"
# =============================================


class RocketIherbMatcher:
    """로켓직구 ↔ 아이허브 매칭"""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
    
    def load_rocket_csv_matching(self, csv_path: str):
        """
        로켓직구 CSV 매칭 데이터 로드
        - rocket.csv에 이미 UPC/품번이 매핑되어 있음
        """
        
        print(f"\n{'='*80}")
        print(f"📥 로켓직구 CSV 매칭 데이터 로드")
        print(f"{'='*80}\n")
        
        # CSV 로드
        print(f"1. rocket.csv 로드...")
        df = pd.read_csv(csv_path)
        print(f"   ✓ {len(df):,}개 레코드")
        
        # 데이터 정제
        df_valid = df[df['아이허브_UPC'].notna()].copy()
        print(f"   ✓ UPC 있는 제품: {len(df_valid):,}개")
        
        # 타입 변환
        df_valid['vendor_item_id'] = df_valid['product_id'].astype(str)
        df_valid['iherb_upc'] = df_valid['아이허브_UPC'].astype('Int64').astype(str)
        df_valid['iherb_part_number'] = df_valid['아이허브_파트넘버'].fillna('').astype(str)
        df_valid['product_name'] = df_valid['쿠팡_제품명'].fillna('')
        
        # DB 저장
        print(f"\n2. matching_reference 테이블 업데이트...")
        
        conn = sqlite3.connect(self.db_path)
        
        inserted = 0
        updated = 0
        
        for idx, row in df_valid.iterrows():
            vendor_id = row['vendor_item_id']
            
            # 기존 레코드 확인
            existing = conn.execute("""
                SELECT vendor_item_id FROM matching_reference 
                WHERE vendor_item_id = ?
            """, (vendor_id,)).fetchone()
            
            if existing:
                # 업데이트
                conn.execute("""
                    UPDATE matching_reference 
                    SET iherb_upc = ?,
                        iherb_part_number = ?,
                        matching_source = 'rocket_csv',
                        matching_confidence = 1.0,
                        product_name = ?
                    WHERE vendor_item_id = ?
                """, (row['iherb_upc'], row['iherb_part_number'], 
                      row['product_name'], vendor_id))
                updated += 1
            else:
                # 신규 생성
                conn.execute("""
                    INSERT INTO matching_reference 
                    (vendor_item_id, iherb_upc, iherb_part_number, 
                     matching_source, matching_confidence, product_name)
                    VALUES (?, ?, ?, 'rocket_csv', 1.0, ?)
                """, (vendor_id, row['iherb_upc'], row['iherb_part_number'], 
                      row['product_name']))
                inserted += 1
            
            if (idx + 1) % 500 == 0:
                conn.commit()
                print(f"   ... {idx + 1:,}개 처리 중")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ 로켓직구 CSV 매칭 완료")
        print(f"   • 신규 생성: {inserted:,}개")
        print(f"   • 업데이트: {updated:,}개")
        print(f"   • 총 처리: {inserted + updated:,}개")
    
    def verify_iherb_excel_data(self, price_inventory_path: str, official_excel_path: str):
        """
        아이허브 Excel 데이터 검증
        - price_inventory: 가격/재고 정보 확인
        - 20251024_1444: UPC 정보 확인
        """
        
        print(f"\n{'='*80}")
        print(f"📥 아이허브 Excel 데이터 검증")
        print(f"{'='*80}\n")
        
        # 1. price_inventory 확인
        print(f"1. price_inventory 로드...")
        df_price = pd.read_excel(price_inventory_path, header=1, skiprows=[0])
        print(f"   ✓ {len(df_price):,}개 레코드")
        
        # 주요 컬럼 확인
        required_cols = ['옵션 ID', '쿠팡 노출 상품명', '판매가격', '잔여수량(재고)']
        missing_cols = [col for col in required_cols if col not in df_price.columns]
        
        if missing_cols:
            print(f"   ⚠️  누락된 컬럼: {missing_cols}")
        else:
            print(f"   ✅ 필수 컬럼 모두 존재")
        
        # 유효 데이터 확인
        valid_count = df_price['옵션 ID'].notna().sum()
        print(f"   ✓ 유효한 옵션 ID: {valid_count:,}개")
        
        # 2. 20251024_1444 확인
        print(f"\n2. 20251024_1444 (UPC 소스) 로드...")
        df_official = pd.read_excel(official_excel_path)
        print(f"   ✓ {len(df_official):,}개 레코드")
        
        # UPC 컬럼 확인
        if 'UPC' in df_official.columns:
            upc_count = df_official['UPC'].notna().sum()
            print(f"   ✓ UPC 있는 레코드: {upc_count:,}개")
        else:
            print(f"   ⚠️  UPC 컬럼 없음")
        
        print(f"\n💡 아이허브 데이터는 Excel 파일로 관리됩니다")
        print(f"   - data_manager.py를 통해 로켓직구 DB와 통합됩니다")
        print(f"   - 매칭은 UPC를 기준으로 자동 수행됩니다")
    
    @staticmethod
    def _process_barcode(barcode) -> str:
        """바코드를 UPC로 변환 (13자리 EAN-13 → 12자리 UPC)"""
        if pd.isna(barcode):
            return None
        
        barcode_str = str(int(barcode)) if isinstance(barcode, float) else str(barcode)
        
        # 13자리 EAN-13인 경우, 맨 앞 0 제거
        if len(barcode_str) == 13 and barcode_str.startswith('0'):
            return barcode_str[1:]
        
        # 12자리 UPC
        if len(barcode_str) == 12:
            return barcode_str
        
        return barcode_str
    
    def show_statistics(self):
        """매칭 통계 출력"""
        
        print(f"\n{'='*80}")
        print(f"📊 매칭 통계")
        print(f"{'='*80}\n")
        
        conn = sqlite3.connect(self.db_path)
        
        # 전체 통계
        total = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
        print(f"전체 매칭: {total:,}개")
        
        # 출처별 통계
        print(f"\n출처별 분포:")
        cursor = conn.execute("""
            SELECT matching_source, COUNT(*) as cnt
            FROM matching_reference
            GROUP BY matching_source
            ORDER BY cnt DESC
        """)
        for source, cnt in cursor.fetchall():
            print(f"  • {source:20s}: {cnt:,}개 ({cnt/total*100:.1f}%)")
        
        # UPC/품번 통계
        with_upc = conn.execute("""
            SELECT COUNT(*) FROM matching_reference WHERE iherb_upc IS NOT NULL
        """).fetchone()[0]
        with_part = conn.execute("""
            SELECT COUNT(*) FROM matching_reference 
            WHERE iherb_part_number IS NOT NULL AND iherb_part_number != ''
        """).fetchone()[0]
        
        print(f"\nUPC/품번 통계:")
        print(f"  • UPC 있음: {with_upc:,}개 ({with_upc/total*100:.1f}%)")
        print(f"  • 품번 있음: {with_part:,}개 ({with_part/total*100:.1f}%)")
        
        # product_states와 매칭률
        in_db = conn.execute("""
            SELECT COUNT(DISTINCT ps.vendor_item_id)
            FROM product_states ps
            INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        """).fetchone()[0]
        
        total_products = conn.execute("""
            SELECT COUNT(DISTINCT vendor_item_id) FROM product_states
        """).fetchone()[0]
        
        print(f"\nDB 제품 매칭률:")
        print(f"  • 전체 제품: {total_products:,}개")
        print(f"  • 매칭됨: {in_db:,}개 ({in_db/total_products*100:.1f}%)")
        print(f"  • 미매칭: {total_products - in_db:,}개")
        
        conn.close()


def main():
    """메인 함수"""
    
    print(f"\n{'='*80}")
    print(f"🎯 로켓직구 ↔ 아이허브 매칭 데이터 준비")
    print(f"{'='*80}\n")
    print(f"rocket.csv:      {ROCKET_CSV_PATH}")
    print(f"price_inventory: {PRICE_INVENTORY_PATH}")
    print(f"official_excel:  {OFFICIAL_EXCEL_PATH}")
    print(f"DB:              {DB_PATH}")
    
    # 파일 존재 확인
    if not Path(DB_PATH).exists():
        print(f"\n❌ DB 파일이 없습니다: {DB_PATH}")
        return
    
    matcher = RocketIherbMatcher(DB_PATH)
    
    try:
        # 1. 로켓직구 CSV 매칭
        if Path(ROCKET_CSV_PATH).exists():
            matcher.load_rocket_csv_matching(ROCKET_CSV_PATH)
        else:
            print(f"\n⚠️  rocket.csv 파일이 없습니다")
        
        # 2. 아이허브 Excel 데이터 검증
        if Path(PRICE_INVENTORY_PATH).exists() and Path(OFFICIAL_EXCEL_PATH).exists():
            matcher.verify_iherb_excel_data(PRICE_INVENTORY_PATH, OFFICIAL_EXCEL_PATH)
        else:
            print(f"\n⚠️  아이허브 Excel 파일이 누락되었습니다")
        
        # 3. 통계 출력
        matcher.show_statistics()
        
        print(f"\n{'='*80}")
        print(f"✅ 매칭 데이터 준비 완료!")
        print(f"{'='*80}")
        print(f"\n💡 다음 단계:")
        print(f"   1. data_manager.py로 통합 데이터프레임 생성")
        print(f"   2. price_comparison.py로 가격 비교 리포트 생성")
        print(f"   3. price_comparison_app.py로 대시보드 실행")
        print()
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()