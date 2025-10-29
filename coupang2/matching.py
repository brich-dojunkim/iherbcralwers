#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
매칭 데이터 준비 스크립트 (완전한 워크플로우)

데이터 소스:
1. iHerb 공식 스토어 (iherb_official):
   - vendor_item_id: price_inventory의 '옵션 ID'
   - UPC: 20251024_1444의 'UPC' (Product ID로 조인)
   - Part Number: price_inventory의 '업체상품코드' 우선, 없으면 20251024_1444의 '판매자상품코드'

2. 로켓직구 (rocket_direct):
   - vendor_item_id: rocket.csv의 'product_id'
   - UPC: rocket.csv의 '아이허브_UPC'
   - Part Number: rocket.csv의 '아이허브_파트넘버'
"""

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path


# ==================== 설정 ====================
PRICE_INVENTORY_PATH = "/mnt/user-data/uploads/price_inventory_251028.xlsx"
OFFICIAL_EXCEL_PATH = "/mnt/user-data/uploads/20251024_1444.xlsx"
ROCKET_CSV_PATH = "/mnt/user-data/uploads/rocket.csv"
DB_PATH = "/home/claude/monitoring.db"
# =============================================


class ComprehensiveMatchingLoader:
    """완전한 워크플로우 매칭 데이터 로더"""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
    
    def load_iherb_official_matching(self, price_inventory_path: str, official_excel_path: str):
        """
        iHerb 공식 스토어 매칭 (두 파일 조인)
        
        전략:
        1. price_inventory에서 옵션 ID (vendor_item_id) 로드
        2. 20251024_1444에서 UPC 로드
        3. Product ID를 키로 조인하여 통합
        """
        
        print(f"\n{'='*80}")
        print(f"📥 iHerb 공식 스토어 매칭 (price_inventory + 20251024_1444)")
        print(f"{'='*80}\n")
        
        # 1. price_inventory 로드
        print(f"1. price_inventory 로드...")
        df_price = pd.read_excel(price_inventory_path, header=1, skiprows=[0])
        print(f"   ✓ {len(df_price):,}개 레코드")
        
        # 타입 변환
        df_price['옵션 ID'] = df_price['옵션 ID'].astype('Int64').astype(str)
        df_price['Product ID'] = df_price['Product ID'].astype('Int64').astype(str)
        df_price['업체상품코드'] = df_price['업체상품코드'].fillna('').astype(str)
        
        # 옵션 ID가 있는 것만
        df_price = df_price[df_price['옵션 ID'] != '<NA>'].copy()
        print(f"   ✓ 유효한 옵션 ID: {len(df_price):,}개")
        
        # 2. 20251024_1444 로드
        print(f"\n2. 20251024_1444 (UPC 소스) 로드...")
        df_official = pd.read_excel(official_excel_path)
        print(f"   ✓ {len(df_official):,}개 레코드")
        
        # 타입 변환
        df_official['쿠팡 상품번호'] = df_official['쿠팡 상품번호'].astype('Int64').astype(str)
        
        # UPC 처리 (EAN-13 → UPC 변환)
        df_official['iherb_upc'] = df_official['UPC'].apply(self._process_barcode)
        
        # 유효한 쿠팡 상품번호만
        df_official = df_official[df_official['쿠팡 상품번호'] != '<NA>'].copy()
        print(f"   ✓ 유효한 쿠팡 상품번호: {len(df_official):,}개")
        print(f"   ✓ UPC 있는 레코드: {df_official['iherb_upc'].notna().sum():,}개")
        
        # 3. 업체상품코드 기준 조인
        print(f"\n3. 업체상품코드 ↔ 판매자상품코드 조인...")
        merged = df_price.merge(
            df_official[['판매자상품코드', 'iherb_upc']],
            left_on='업체상품코드',
            right_on='판매자상품코드',
            how='left'
        )
        print(f"   ✓ 조인 결과: {len(merged):,}개 레코드")
        print(f"   ✓ UPC 매칭된 레코드: {merged['iherb_upc'].notna().sum():,}개")
        
        # 4. 옵션 ID별 그룹화 (중복 제거)
        print(f"\n4. 옵션 ID별 데이터 통합...")
        
        # 그룹별 첫 번째 값 선택
        grouped = merged.groupby('옵션 ID').agg({
            'iherb_upc': 'first',
            '업체상품코드': 'first',
            '쿠팡 노출 상품명': 'first'
        }).reset_index()
        
        print(f"   ✓ 고유 옵션 ID: {len(grouped):,}개")
        print(f"   ✓ UPC 있는 레코드: {grouped['iherb_upc'].notna().sum():,}개")
        print(f"   ✓ Part Number 있는 레코드: {grouped['업체상품코드'].notna().sum():,}개")
        
        # 5. DB 저장
        print(f"\n5. matching_reference 테이블 업데이트...")
        
        conn = sqlite3.connect(self.db_path)
        
        inserted = 0
        updated = 0
        
        for idx, row in grouped.iterrows():
            vendor_id = row['옵션 ID']
            
            # 기존 레코드 확인
            existing = conn.execute("""
                SELECT vendor_item_id FROM matching_reference 
                WHERE vendor_item_id = ?
            """, (vendor_id,)).fetchone()
            
            upc_value = row['iherb_upc'] if pd.notna(row['iherb_upc']) else None
            part_value = row['업체상품코드'] if pd.notna(row['업체상품코드']) else None
            product_name = row['쿠팡 노출 상품명'] if pd.notna(row['쿠팡 노출 상품명']) else None
            
            if existing:
                # 업데이트
                conn.execute("""
                    UPDATE matching_reference 
                    SET iherb_upc = ?,
                        iherb_part_number = ?,
                        matching_source = 'iherb_official',
                        matching_confidence = 1.0,
                        product_name = ?
                    WHERE vendor_item_id = ?
                """, (upc_value, part_value, product_name, vendor_id))
                updated += 1
            else:
                # 신규 생성
                conn.execute("""
                    INSERT INTO matching_reference 
                    (vendor_item_id, iherb_upc, iherb_part_number, 
                     matching_source, matching_confidence, product_name)
                    VALUES (?, ?, ?, 'iherb_official', 1.0, ?)
                """, (vendor_id, upc_value, part_value, product_name))
                inserted += 1
            
            if (idx + 1) % 1000 == 0:
                conn.commit()
                print(f"   ... {idx + 1:,}개 처리 중")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ iHerb 공식 매칭 완료")
        print(f"   • 신규 생성: {inserted:,}개")
        print(f"   • 업데이트: {updated:,}개")
        print(f"   • 총 처리: {inserted + updated:,}개")
    
    def load_rocket_direct_matching(self, csv_path: str):
        """로켓직구 매칭 데이터 로드"""
        
        print(f"\n{'='*80}")
        print(f"📥 로켓직구 매칭 데이터 로드")
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
        skipped = 0
        
        for idx, row in df_valid.iterrows():
            vendor_id = row['vendor_item_id']
            
            # 기존 레코드 확인
            existing = conn.execute("""
                SELECT vendor_item_id, matching_source FROM matching_reference 
                WHERE vendor_item_id = ?
            """, (vendor_id,)).fetchone()
            
            if existing:
                # iherb_official이 아닌 경우에만 업데이트
                if existing[1] != 'iherb_official':
                    conn.execute("""
                        UPDATE matching_reference 
                        SET iherb_upc = ?,
                            iherb_part_number = ?,
                            matching_source = 'rocket_direct',
                            matching_confidence = 1.0,
                            product_name = ?
                        WHERE vendor_item_id = ?
                    """, (row['iherb_upc'], row['iherb_part_number'], 
                          row['product_name'], vendor_id))
                    updated += 1
                else:
                    skipped += 1
            else:
                # 신규 생성
                conn.execute("""
                    INSERT INTO matching_reference 
                    (vendor_item_id, iherb_upc, iherb_part_number, 
                     matching_source, matching_confidence, product_name)
                    VALUES (?, ?, ?, 'rocket_direct', 1.0, ?)
                """, (vendor_id, row['iherb_upc'], row['iherb_part_number'], 
                      row['product_name']))
                inserted += 1
            
            if (idx + 1) % 500 == 0:
                conn.commit()
                print(f"   ... {idx + 1:,}개 처리 중")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ 로켓직구 매칭 완료")
        print(f"   • 신규 생성: {inserted:,}개")
        print(f"   • 업데이트: {updated:,}개")
        print(f"   • 건너뜀 (iHerb 공식 우선): {skipped:,}개")
    
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
        
        # 소스별 UPC 통계
        print(f"\n소스별 UPC 통계:")
        cursor = conn.execute("""
            SELECT 
                matching_source,
                COUNT(*) as total,
                SUM(CASE WHEN iherb_upc IS NOT NULL THEN 1 ELSE 0 END) as with_upc
            FROM matching_reference
            GROUP BY matching_source
        """)
        for source, total_cnt, upc_cnt in cursor.fetchall():
            print(f"  • {source:20s}: {upc_cnt:,}/{total_cnt:,} ({upc_cnt/total_cnt*100:.1f}%)")
        
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
        
        # 소스별 DB 매칭률
        print(f"\n소스별 DB 매칭률:")
        cursor = conn.execute("""
            SELECT 
                src.source_type,
                COUNT(DISTINCT ps.vendor_item_id) as total,
                COUNT(DISTINCT CASE WHEN mr.vendor_item_id IS NOT NULL THEN ps.vendor_item_id END) as matched
            FROM product_states ps
            JOIN snapshots s ON ps.snapshot_id = s.id
            JOIN sources src ON s.source_id = src.id
            LEFT JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
            GROUP BY src.source_type
        """)
        for source, total_cnt, matched_cnt in cursor.fetchall():
            rate = matched_cnt / total_cnt * 100 if total_cnt > 0 else 0
            print(f"  • {source:20s}: {matched_cnt:,}/{total_cnt:,} ({rate:.1f}%)")
        
        conn.close()


def main():
    """메인 함수"""
    
    print(f"\n{'='*80}")
    print(f"🎯 완전한 매칭 데이터 준비")
    print(f"{'='*80}\n")
    print(f"price_inventory: {PRICE_INVENTORY_PATH}")
    print(f"official_excel:  {OFFICIAL_EXCEL_PATH}")
    print(f"rocket.csv:      {ROCKET_CSV_PATH}")
    print(f"DB:              {DB_PATH}")
    
    # 파일 존재 확인
    if not Path(PRICE_INVENTORY_PATH).exists():
        print(f"\n❌ price_inventory 파일이 없습니다")
        return
    
    if not Path(OFFICIAL_EXCEL_PATH).exists():
        print(f"\n❌ 20251024_1444 파일이 없습니다")
        return
    
    if not Path(ROCKET_CSV_PATH).exists():
        print(f"\n⚠️  rocket.csv 파일이 없습니다")
    
    if not Path(DB_PATH).exists():
        print(f"\n❌ DB 파일이 없습니다")
        return
    
    loader = ComprehensiveMatchingLoader(DB_PATH)
    
    try:
        # 1. iHerb 공식 매칭 (두 파일 조인)
        loader.load_iherb_official_matching(PRICE_INVENTORY_PATH, OFFICIAL_EXCEL_PATH)
        
        # 2. 로켓직구 매칭
        if Path(ROCKET_CSV_PATH).exists():
            loader.load_rocket_direct_matching(ROCKET_CSV_PATH)
        
        # 3. 통계 출력
        loader.show_statistics()
        
        print(f"\n{'='*80}")
        print(f"✅ 완전한 매칭 데이터 준비 완료!")
        print(f"{'='*80}\n")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()