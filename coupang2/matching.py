#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
매칭 데이터 준비 스크립트 (개선 버전)
- CSV/Excel에서 UPC 정보를 읽어 matching_reference 테이블 생성
- 크롤링 전에 1회 실행
"""

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path


# ==================== 설정 ====================
# 파일 경로 하드코딩
ROCKET_CSV_PATH = "/Users/brich/Desktop/iherb_price/coupang2/coupang_iherb_products_updated_with_upc.csv"
OFFICIAL_EXCEL_PATH = "/Users/brich/Desktop/iherb_price/coupang2/20251024_1444.xlsx"
DB_PATH = "monitoring.db"
# =============================================


class MatchingDataLoader:
    """매칭 데이터 로더"""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
    
    def load_rocket_matching(self, csv_path: str):
        """로켓직구 매칭 데이터 로드 (CSV)"""
        
        print(f"\n{'='*80}")
        print(f"📥 로켓직구 매칭 데이터 로드")
        print(f"{'='*80}\n")
        
        # CSV 로드
        print(f"1. CSV 파일 읽기...")
        df = pd.read_csv(csv_path)
        
        print(f"   ✓ 총 {len(df):,}개 레코드")
        
        # 필수 컬럼 확인
        required_cols = ['product_id', '아이허브_UPC', '아이허브_파트넘버']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"필수 컬럼 누락: {missing_cols}")
        
        # 데이터 정제
        print(f"\n2. 데이터 정제")
        
        # UPC가 있는 것만
        df_valid = df[df['아이허브_UPC'].notna()].copy()
        print(f"   ✓ UPC 있는 제품: {len(df_valid):,}개")
        
        # UPC 형식 통일 (float → int → str)
        df_valid['iherb_upc'] = df_valid['아이허브_UPC'].astype('Int64').astype(str)
        df_valid['iherb_part_number'] = df_valid['아이허브_파트넘버'].fillna('')
        df_valid['coupang_product_id'] = df_valid['product_id'].astype(str)
        
        # DB 저장
        print(f"\n3. matching_reference 테이블 업데이트")
        
        conn = sqlite3.connect(self.db_path)
        
        inserted = 0
        updated = 0
        
        for idx, row in df_valid.iterrows():
            # 기존 레코드 확인
            existing = conn.execute("""
                SELECT coupang_product_id, iherb_upc, iherb_part_number 
                FROM matching_reference 
                WHERE coupang_product_id = ?
            """, (row['coupang_product_id'],)).fetchone()
            
            if existing:
                # 기존 UPC와 비교
                if existing[1] != row['iherb_upc'] or existing[2] != row['iherb_part_number']:
                    # 값이 다르면 업데이트
                    conn.execute("""
                        UPDATE matching_reference 
                        SET iherb_upc = ?,
                            iherb_part_number = ?
                        WHERE coupang_product_id = ?
                    """, (row['iherb_upc'], row['iherb_part_number'], 
                          row['coupang_product_id']))
                    updated += 1
            else:
                # 신규 생성
                conn.execute("""
                    INSERT INTO matching_reference 
                    (coupang_product_id, iherb_upc, iherb_part_number)
                    VALUES (?, ?, ?)
                """, (row['coupang_product_id'], row['iherb_upc'], 
                      row['iherb_part_number']))
                inserted += 1
            
            if (idx + 1) % 500 == 0:
                conn.commit()
                print(f"   ... {idx + 1:,}개 처리 중")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ 로켓직구 매칭 완료")
        print(f"   • 신규 생성: {inserted:,}개")
        print(f"   • 업데이트: {updated:,}개")
        print(f"   • 총 처리: {inserted + updated:,}개")
    
    def load_official_matching(self, excel_path: str):
        """iHerb 공식 매칭 데이터 로드 (Excel)"""
        
        print(f"\n{'='*80}")
        print(f"📥 iHerb 공식 매칭 데이터 로드")
        print(f"{'='*80}\n")
        
        # Excel 로드
        print(f"1. Excel 파일 읽기...")
        df = pd.read_excel(excel_path)
        
        print(f"   ✓ 총 {len(df):,}개 레코드")
        
        # 필수 컬럼 확인
        required_cols = ['쿠팡 상품번호', 'UPC', '판매자상품코드']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"필수 컬럼 누락: {missing_cols}")
        
        # 데이터 정제
        print(f"\n2. 데이터 정제")
        
        # UPC가 있는 것만
        df_valid = df[df['UPC'].notna()].copy()
        print(f"   ✓ UPC 있는 제품: {len(df_valid):,}개")
        
        # UPC 형식 통일
        df_valid['iherb_upc'] = df_valid['UPC'].astype('Int64').astype(str)
        df_valid['coupang_product_id'] = df_valid['쿠팡 상품번호'].astype('Int64').astype(str)
        df_valid['iherb_part_number'] = df_valid['판매자상품코드'].fillna('').astype(str)
        
        # DB 저장
        print(f"\n3. matching_reference 테이블 업데이트")
        
        conn = sqlite3.connect(self.db_path)
        
        inserted = 0
        updated = 0
        
        for idx, row in df_valid.iterrows():
            # 기존 레코드 확인
            existing = conn.execute("""
                SELECT coupang_product_id, iherb_upc, iherb_part_number 
                FROM matching_reference 
                WHERE coupang_product_id = ?
            """, (row['coupang_product_id'],)).fetchone()
            
            if existing:
                # 기존 UPC와 비교
                if existing[1] != row['iherb_upc'] or existing[2] != row['iherb_part_number']:
                    # 값이 다르면 업데이트
                    conn.execute("""
                        UPDATE matching_reference 
                        SET iherb_upc = ?,
                            iherb_part_number = ?
                        WHERE coupang_product_id = ?
                    """, (row['iherb_upc'], row['iherb_part_number'],
                          row['coupang_product_id']))
                    updated += 1
            else:
                # 신규 생성
                conn.execute("""
                    INSERT INTO matching_reference 
                    (coupang_product_id, iherb_upc, iherb_part_number)
                    VALUES (?, ?, ?)
                """, (row['coupang_product_id'], row['iherb_upc'],
                      row['iherb_part_number']))
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
    
    def show_statistics(self):
        """매칭 통계 출력"""
        
        print(f"\n{'='*80}")
        print(f"📊 매칭 통계")
        print(f"{'='*80}\n")
        
        conn = sqlite3.connect(self.db_path)
        
        # 전체 통계
        total = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
        with_upc = conn.execute("SELECT COUNT(*) FROM matching_reference WHERE iherb_upc IS NOT NULL").fetchone()[0]
        
        print(f"전체 제품 수: {total:,}개")
        print(f"UPC 있는 제품: {with_upc:,}개 ({with_upc/total*100:.1f}%)")
        
        # Part Number 통계
        with_part = conn.execute("SELECT COUNT(*) FROM matching_reference WHERE iherb_part_number IS NOT NULL AND iherb_part_number != ''").fetchone()[0]
        print(f"Part Number 있는 제품: {with_part:,}개 ({with_part/total*100:.1f}%)")
        
        # UPC 중복 확인
        duplicates = conn.execute("""
            SELECT iherb_upc, COUNT(*) as cnt
            FROM matching_reference
            WHERE iherb_upc IS NOT NULL
            GROUP BY iherb_upc
            HAVING cnt > 1
            ORDER BY cnt DESC
            LIMIT 5
        """).fetchall()
        
        if duplicates:
            print(f"\n⚠️  UPC 중복 (상위 5개):")
            for upc, cnt in duplicates:
                print(f"  • UPC {upc}: {cnt}개 제품")
        else:
            print(f"\n✅ UPC 중복 없음")
        
        conn.close()


def main():
    """메인 함수"""
    
    print(f"\n{'='*80}")
    print(f"🎯 매칭 데이터 준비")
    print(f"{'='*80}\n")
    print(f"로켓직구 CSV: {ROCKET_CSV_PATH}")
    print(f"iHerb 공식 Excel: {OFFICIAL_EXCEL_PATH}")
    print(f"DB: {DB_PATH}")
    
    # 파일 존재 확인
    if not Path(ROCKET_CSV_PATH).exists():
        print(f"\n❌ 로켓직구 CSV 파일이 없습니다: {ROCKET_CSV_PATH}")
        return
    
    if not Path(OFFICIAL_EXCEL_PATH).exists():
        print(f"\n❌ iHerb 공식 Excel 파일이 없습니다: {OFFICIAL_EXCEL_PATH}")
        return
    
    # DB 존재 확인
    if not Path(DB_PATH).exists():
        print(f"\n❌ DB 파일이 없습니다: {DB_PATH}")
        print(f"   먼저 database.py를 실행하여 DB를 초기화하세요.")
        return
    
    loader = MatchingDataLoader(DB_PATH)
    
    try:
        # 로켓직구 매칭
        loader.load_rocket_matching(ROCKET_CSV_PATH)
        
        # iHerb 공식 매칭
        loader.load_official_matching(OFFICIAL_EXCEL_PATH)
        
        # 통계 출력
        loader.show_statistics()
        
        print(f"\n{'='*80}")
        print(f"✅ 매칭 데이터 준비 완료!")
        print(f"{'='*80}\n")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

    """매칭 데이터 로더"""
    
    def __init__(self, db_path="monitoring.db"):
        self.db_path = db_path
    
    def load_rocket_matching(self, csv_path: str):
        """로켓직구 매칭 데이터 로드 (CSV)"""
        
        print(f"\n{'='*80}")
        print(f"📥 로켓직구 매칭 데이터 로드")
        print(f"{'='*80}\n")
        
        # CSV 로드
        print(f"1. CSV 파일 읽기: {csv_path}")
        df = pd.read_csv(csv_path)
        
        print(f"   ✓ 총 {len(df):,}개 레코드")
        
        # 필수 컬럼 확인
        required_cols = ['product_id', '아이허브_UPC', '아이허브_파트넘버', '쿠팡_제품명', '카테고리']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"필수 컬럼 누락: {missing_cols}")
        
        # 데이터 정제
        print(f"\n2. 데이터 정제")
        
        # UPC가 있는 것만
        df_valid = df[df['아이허브_UPC'].notna()].copy()
        print(f"   ✓ UPC 있는 제품: {len(df_valid):,}개")
        
        # UPC 형식 통일 (float → int → str)
        df_valid['iherb_upc'] = df_valid['아이허브_UPC'].astype('Int64').astype(str)
        df_valid['iherb_part_number'] = df_valid['아이허브_파트넘버'].fillna('')
        df_valid['coupang_product_id'] = df_valid['product_id'].astype(str)
        
        # DB 저장
        print(f"\n3. matching_reference 테이블 업데이트")
        
        conn = sqlite3.connect(self.db_path)
        
        inserted = 0
        updated = 0
        
        for idx, row in df_valid.iterrows():
            # 기존 레코드 확인
            existing = conn.execute("""
                SELECT coupang_product_id FROM matching_reference 
                WHERE coupang_product_id = ?
            """, (row['coupang_product_id'],)).fetchone()
            
            if existing:
                # 업데이트 (UPC 정보만)
                conn.execute("""
                    UPDATE matching_reference 
                    SET iherb_upc = ?,
                        iherb_part_number = ?,
                        updated_at = ?
                    WHERE coupang_product_id = ?
                """, (row['iherb_upc'], row['iherb_part_number'], 
                      datetime.now(), row['coupang_product_id']))
                updated += 1
            else:
                # 신규 생성
                conn.execute("""
                    INSERT INTO matching_reference 
                    (coupang_product_id, iherb_upc, iherb_part_number,
                     first_discovered_source, first_discovered_category, 
                     first_discovered_name, first_discovered_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (row['coupang_product_id'], row['iherb_upc'], 
                      row['iherb_part_number'], 'rocket_direct',
                      row['카테고리'], row['쿠팡_제품명'], datetime.now()))
                inserted += 1
            
            if (idx + 1) % 500 == 0:
                conn.commit()
                print(f"   ... {idx + 1:,}개 처리 중")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ 로켓직구 매칭 완료")
        print(f"   • 신규 생성: {inserted:,}개")
        print(f"   • 업데이트: {updated:,}개")
        print(f"   • 총 처리: {inserted + updated:,}개")
    
    def load_official_matching(self, excel_path: str):
        """iHerb 공식 매칭 데이터 로드 (Excel)"""
        
        print(f"\n{'='*80}")
        print(f"📥 iHerb 공식 매칭 데이터 로드")
        print(f"{'='*80}\n")
        
        # Excel 로드
        print(f"1. Excel 파일 읽기: {excel_path}")
        df = pd.read_excel(excel_path)
        
        print(f"   ✓ 총 {len(df):,}개 레코드")
        
        # 필수 컬럼 확인
        required_cols = ['쿠팡 상품번호', 'UPC', '상품명']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"필수 컬럼 누락: {missing_cols}")
        
        # 데이터 정제
        print(f"\n2. 데이터 정제")
        
        # UPC가 있는 것만
        df_valid = df[df['UPC'].notna()].copy()
        print(f"   ✓ UPC 있는 제품: {len(df_valid):,}개")
        
        # UPC 형식 통일
        df_valid['iherb_upc'] = df_valid['UPC'].astype('Int64').astype(str)
        df_valid['coupang_product_id'] = df_valid['쿠팡 상품번호'].astype('Int64').astype(str)
        
        # 카테고리 추출 (옵션)
        if '카테고리명' in df_valid.columns:
            df_valid['category'] = df_valid['카테고리명']
        else:
            df_valid['category'] = ''
        
        # DB 저장
        print(f"\n3. matching_reference 테이블 업데이트")
        
        conn = sqlite3.connect(self.db_path)
        
        inserted = 0
        updated = 0
        
        for idx, row in df_valid.iterrows():
            # 기존 레코드 확인
            existing = conn.execute("""
                SELECT coupang_product_id FROM matching_reference 
                WHERE coupang_product_id = ?
            """, (row['coupang_product_id'],)).fetchone()
            
            if existing:
                # 업데이트 (UPC 정보만)
                conn.execute("""
                    UPDATE matching_reference 
                    SET iherb_upc = ?,
                        updated_at = ?
                    WHERE coupang_product_id = ?
                """, (row['iherb_upc'], datetime.now(), row['coupang_product_id']))
                updated += 1
            else:
                # 신규 생성
                conn.execute("""
                    INSERT INTO matching_reference 
                    (coupang_product_id, iherb_upc, iherb_part_number,
                     first_discovered_source, first_discovered_category, 
                     first_discovered_name, first_discovered_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (row['coupang_product_id'], row['iherb_upc'], '',
                      'iherb_official', row['category'], 
                      row['상품명'], datetime.now()))
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
    
    
    def show_statistics(self):
        """매칭 통계 출력"""
        
        print(f"\n{'='*80}")
        print(f"📊 매칭 통계")
        print(f"{'='*80}\n")
        
        conn = sqlite3.connect(self.db_path)
        
        # 전체 통계
        total = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
        with_upc = conn.execute("SELECT COUNT(*) FROM matching_reference WHERE iherb_upc IS NOT NULL").fetchone()[0]
        
        print(f"전체 제품 수: {total:,}개")
        print(f"UPC 있는 제품: {with_upc:,}개 ({with_upc/total*100:.1f}%)")
        
        # Part Number 통계
        with_part = conn.execute("SELECT COUNT(*) FROM matching_reference WHERE iherb_part_number IS NOT NULL AND iherb_part_number != ''").fetchone()[0]
        print(f"Part Number 있는 제품: {with_part:,}개 ({with_part/total*100:.1f}%)")
        
        # UPC 중복 확인
        duplicates = conn.execute("""
            SELECT iherb_upc, COUNT(*) as cnt
            FROM matching_reference
            WHERE iherb_upc IS NOT NULL
            GROUP BY iherb_upc
            HAVING cnt > 1
            ORDER BY cnt DESC
            LIMIT 5
        """).fetchall()
        
        if duplicates:
            print(f"\n⚠️  UPC 중복 (상위 5개):")
            for upc, cnt in duplicates:
                print(f"  • UPC {upc}: {cnt}개 제품")
        else:
            print(f"\n✅ UPC 중복 없음")
        
        conn.close()


def main():
    """메인 함수"""
    
    print(f"\n{'='*80}")
    print(f"🎯 매칭 데이터 준비")
    print(f"{'='*80}\n")
    print(f"로켓직구 CSV: {ROCKET_CSV_PATH}")
    print(f"iHerb 공식 Excel: {OFFICIAL_EXCEL_PATH}")
    print(f"DB: {DB_PATH}")
    
    # 파일 존재 확인
    if not Path(ROCKET_CSV_PATH).exists():
        print(f"\n❌ 로켓직구 CSV 파일이 없습니다: {ROCKET_CSV_PATH}")
        return
    
    if not Path(OFFICIAL_EXCEL_PATH).exists():
        print(f"\n❌ iHerb 공식 Excel 파일이 없습니다: {OFFICIAL_EXCEL_PATH}")
        return
    
    # DB 존재 확인
    if not Path(DB_PATH).exists():
        print(f"\n❌ DB 파일이 없습니다: {DB_PATH}")
        print(f"   먼저 database.py를 실행하여 DB를 초기화하세요.")
        return
    
    loader = MatchingDataLoader(DB_PATH)
    
    try:
        # 로켓직구 매칭
        loader.load_rocket_matching(ROCKET_CSV_PATH)
        
        # iHerb 공식 매칭
        loader.load_official_matching(OFFICIAL_EXCEL_PATH)
        
        # 통계 출력
        loader.show_statistics()
        
        print(f"\n{'='*80}")
        print(f"✅ 매칭 데이터 준비 완료!")
        print(f"{'='*80}\n")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()