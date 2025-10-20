#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
최종 매칭 데이터 통합 스크립트
1. 패턴 기반 정규화
2. updated.csv의 개선된 매칭 정보를 기존 CSV에 통합
3. DB 업데이트
"""

import pandas as pd
import sqlite3
import re
from datetime import datetime
import os


class FinalMatchingMerger:
    """최종 매칭 데이터 통합 클래스"""
    
    def __init__(self, db_path="improved_monitoring.db"):
        self.db_path = db_path
        self.conn = None
    
    def print_section(self, title):
        """섹션 헤더"""
        print(f"\n{'='*80}")
        print(f"[{title}]")
        print(f"{'='*80}")
    
    def normalize_upc(self, value):
        """UPC 정규화: 12~14자리 숫자를 14자리로 통일 (앞에 0 패딩)"""
        if pd.isna(value):
            return None
        
        # 문자열 변환, 작은따옴표 제거, 공백 제거
        cleaned = str(value).replace("'", "").strip()
        
        # .0 제거 (float 형식)
        if '.' in cleaned:
            try:
                cleaned = str(int(float(cleaned)))
            except:
                pass
        
        # 빈 문자열이나 'nan' 체크
        if cleaned.lower() in ['', 'nan', 'none']:
            return None
        
        # 패턴 체크: 12~14자리 숫자
        if cleaned.isdigit() and 12 <= len(cleaned) <= 14:
            # 14자리로 패딩
            return cleaned.zfill(14)
        else:
            return None
    
    def normalize_part_number(self, value):
        """파트넘버 정규화: XXX-XXXXX 형식만 유효"""
        if pd.isna(value):
            return None
        
        cleaned = str(value).strip()
        
        # 빈 문자열이나 'nan' 체크
        if cleaned.lower() in ['', 'nan', 'none']:
            return None
        
        # 패턴 체크: 글자-숫자 형식
        if re.match(r'^[A-Z]+-\d+$', cleaned):
            return cleaned
        else:
            return None
    
    def load_and_normalize_baseline(self):
        """기존 CSV 로드 및 정규화"""
        self.print_section("1. 기존 CSV 로드 및 정규화")
        
        df = pd.read_csv("coupang_iherb_products.csv", encoding='utf-8-sig')
        print(f"총 레코드: {len(df)}개")
        
        # product_id 추출
        df['product_id'] = df['쿠팡_상품URL'].str.extract(r'itemId=(\d+)')
        
        # 정규화
        df['upc_normalized'] = df['아이허브_UPC'].apply(self.normalize_upc)
        df['part_normalized'] = df['아이허브_파트넘버'].apply(self.normalize_part_number)
        
        upc_before = df['아이허브_UPC'].notna().sum()
        upc_after = df['upc_normalized'].notna().sum()
        part_before = df['아이허브_파트넘버'].notna().sum()
        part_after = df['part_normalized'].notna().sum()
        
        print(f"\n정규화 결과:")
        print(f"  UPC: {upc_before}개 → {upc_after}개 (제거: {upc_before - upc_after}개)")
        print(f"  파트넘버: {part_before}개 → {part_after}개 (제거: {part_before - part_after}개)")
        
        # 매칭 상태
        has_matching = (df['upc_normalized'].notna() | df['part_normalized'].notna()).sum()
        no_matching = len(df) - has_matching
        
        print(f"\n매칭 상태:")
        print(f"  매칭 있음: {has_matching}개")
        print(f"  매칭 없음: {no_matching}개")
        
        return df
    
    def load_and_normalize_updated(self):
        """업데이트 CSV 로드 및 정규화"""
        self.print_section("2. 업데이트 CSV 로드 및 정규화")
        
        df = pd.read_csv("updated.csv", encoding='utf-8-sig')
        print(f"총 레코드: {len(df)}개")
        
        # product_id 추출
        df['product_id'] = df['쿠팡 상품 URL'].str.extract(r'itemId=(\d+)')
        
        # 정규화
        df['upc_normalized'] = df['UPC (제품 바코드번호)'].apply(self.normalize_upc)
        df['part_normalized'] = df['아이허브파트넘버'].apply(self.normalize_part_number)
        
        upc_before = df['UPC (제품 바코드번호)'].notna().sum()
        upc_after = df['upc_normalized'].notna().sum()
        part_before = df['아이허브파트넘버'].notna().sum()
        part_after = df['part_normalized'].notna().sum()
        
        print(f"\n정규화 결과:")
        print(f"  UPC: {upc_before}개 → {upc_after}개 (제거: {upc_before - upc_after}개)")
        print(f"  파트넘버: {part_before}개 → {part_after}개 (제거: {part_before - part_after}개)")
        
        # 매칭 상태
        has_matching = (df['upc_normalized'].notna() | df['part_normalized'].notna()).sum()
        
        print(f"\n매칭 상태:")
        print(f"  매칭 있음: {has_matching}개")
        
        return df
    
    def merge_matching_data(self, baseline_df, updated_df):
        """매칭 데이터 통합"""
        self.print_section("3. 매칭 데이터 통합")
        
        # 통합 전략:
        # 1. 기존 CSV를 기준으로
        # 2. updated에 있는 상품의 매칭 정보만 업데이트
        # 3. 기존에 매칭 있으면 유지, 없었는데 updated에 있으면 추가
        
        result_df = baseline_df.copy()
        
        updated_count = 0
        improved_count = 0
        
        for idx, row in result_df.iterrows():
            product_id = row['product_id']
            
            if pd.isna(product_id):
                continue
            
            # updated에서 찾기
            updated_row = updated_df[updated_df['product_id'] == product_id]
            
            if updated_row.empty:
                # updated에 없으면 기존 정보 유지
                continue
            
            updated_row = updated_row.iloc[0]
            updated_count += 1
            
            # UPC 업데이트 (기존에 없었는데 updated에 있으면)
            if pd.isna(row['upc_normalized']) and pd.notna(updated_row['upc_normalized']):
                result_df.at[idx, 'upc_normalized'] = updated_row['upc_normalized']
                improved_count += 1
            
            # 파트넘버 업데이트 (기존에 없었는데 updated에 있으면)
            if pd.isna(row['part_normalized']) and pd.notna(updated_row['part_normalized']):
                result_df.at[idx, 'part_normalized'] = updated_row['part_normalized']
                # UPC가 이미 개선되었을 수 있으므로 중복 카운트 방지
                if pd.notna(row['upc_normalized']) or pd.isna(updated_row['upc_normalized']):
                    improved_count += 1
        
        print(f"통합 결과:")
        print(f"  updated와 매칭된 상품: {updated_count}개")
        print(f"  매칭 정보 개선된 상품: {improved_count}개")
        
        # 최종 매칭 상태
        final_has_matching = (result_df['upc_normalized'].notna() | result_df['part_normalized'].notna()).sum()
        final_no_matching = len(result_df) - final_has_matching
        
        print(f"\n최종 매칭 상태:")
        print(f"  매칭 있음: {final_has_matching}개 (증가: {final_has_matching - (baseline_df['upc_normalized'].notna() | baseline_df['part_normalized'].notna()).sum()}개)")
        print(f"  매칭 없음: {final_no_matching}개")
        
        return result_df
    
    def save_final_csv(self, result_df):
        """최종 CSV 저장"""
        self.print_section("4. 최종 CSV 저장")
        
        # 저장할 컬럼 구성
        output_df = pd.DataFrame({
            '수집순서': result_df['수집순서'],
            '카테고리': result_df['카테고리'],
            '쿠팡_상품URL': result_df['쿠팡_상품URL'],
            '쿠팡_제품명': result_df['쿠팡_제품명'],
            '쿠팡_비회원가격': result_df['쿠팡_비회원가격'],
            '아이허브_UPC': result_df['upc_normalized'],
            '아이허브_파트넘버': result_df['part_normalized'],
            '수집시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        output_path = "coupang_iherb_products_updated.csv"
        output_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"저장 완료: {output_path}")
        print(f"  총 레코드: {len(output_df)}개")
        print(f"  UPC 있음: {output_df['아이허브_UPC'].notna().sum()}개")
        print(f"  파트넘버 있음: {output_df['아이허브_파트넘버'].notna().sum()}개")
        print(f"  매칭 있음: {(output_df['아이허브_UPC'].notna() | output_df['아이허브_파트넘버'].notna()).sum()}개")
        
        return output_path
    
    def update_database(self, result_df):
        """DB 업데이트"""
        self.print_section("5. DB 업데이트")
        
        if not os.path.exists(self.db_path):
            print(f"⚠️  DB 파일 없음: {self.db_path}")
            return
        
        self.conn = sqlite3.connect(self.db_path)
        
        # 백업
        try:
            self.conn.execute("DROP TABLE IF EXISTS matching_reference_old")
            self.conn.execute("""
                CREATE TABLE matching_reference_old AS 
                SELECT * FROM matching_reference
            """)
            print(f"✅ 기존 데이터 백업 완료 (matching_reference_old)")
        except Exception as e:
            print(f"⚠️  백업 실패: {e}")
        
        # 기존 데이터 삭제
        self.conn.execute("DELETE FROM matching_reference")
        print(f"✅ 기존 데이터 삭제 완료")
        
        # 새 데이터 삽입
        inserted = 0
        skipped = 0
        
        for idx, row in result_df.iterrows():
            product_id = row['product_id']
            
            if pd.isna(product_id):
                skipped += 1
                continue
            
            upc = row['upc_normalized'] if pd.notna(row['upc_normalized']) else None
            part = row['part_normalized'] if pd.notna(row['part_normalized']) else None
            
            try:
                self.conn.execute("""
                    INSERT INTO matching_reference 
                    (coupang_product_id, first_discovered_name, first_discovered_category,
                     iherb_upc, iherb_part_number, first_discovered_at, matched_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (
                    product_id,
                    row['쿠팡_제품명'],
                    row['카테고리'],
                    upc,
                    part
                ))
                inserted += 1
            except Exception as e:
                skipped += 1
        
        self.conn.commit()
        
        print(f"\nDB 업데이트 완료:")
        print(f"  삽입: {inserted}개")
        print(f"  스킵: {skipped}개")
        
        # 최종 통계
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN iherb_upc IS NOT NULL THEN 1 END) as has_upc,
                COUNT(CASE WHEN iherb_part_number IS NOT NULL THEN 1 END) as has_part,
                COUNT(CASE WHEN iherb_upc IS NOT NULL OR iherb_part_number IS NOT NULL THEN 1 END) as has_matching
            FROM matching_reference
        """).fetchone()
        
        print(f"\n최종 DB 통계:")
        print(f"  총 레코드: {stats[0]}개")
        print(f"  UPC 있음: {stats[1]}개")
        print(f"  파트넘버 있음: {stats[2]}개")
        print(f"  매칭 있음: {stats[3]}개")
    
    def run_full_merge(self):
        """전체 통합 프로세스"""
        print(f"\n{'#'*80}")
        print(f"# 최종 매칭 데이터 통합 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#'*80}")
        
        try:
            # 1. 기존 CSV 로드 및 정규화
            baseline_df = self.load_and_normalize_baseline()
            
            # 2. 업데이트 CSV 로드 및 정규화
            updated_df = self.load_and_normalize_updated()
            
            # 3. 매칭 데이터 통합
            result_df = self.merge_matching_data(baseline_df, updated_df)
            
            # 4. 최종 CSV 저장
            output_path = self.save_final_csv(result_df)
            
            # 5. DB 업데이트
            self.update_database(result_df)
            
            print(f"\n{'#'*80}")
            print(f"# ✅ 통합 완료!")
            print(f"{'#'*80}")
            print(f"결과 파일: {output_path}")
            print(f"DB 업데이트: {self.db_path}")
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            if self.conn:
                self.conn.close()


def main():
    """메인 실행"""
    
    # 필수 파일 확인
    if not os.path.exists("coupang_iherb_products.csv"):
        print("❌ coupang_iherb_products.csv 파일이 없습니다")
        return
    
    if not os.path.exists("updated.csv"):
        print("❌ updated.csv 파일이 없습니다")
        return
    
    # 통합 실행
    merger = FinalMatchingMerger("improved_monitoring.db")
    merger.run_full_merge()


if __name__ == "__main__":
    main()