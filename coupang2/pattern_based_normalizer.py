#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
패턴 기반 매칭 데이터 정규화
1. 유효한 데이터 패턴 학습
2. 패턴에 맞는 것만 유효로 인정
3. 나머지는 모두 None 처리
"""

import pandas as pd
import re
from collections import Counter


class PatternBasedNormalizer:
    """패턴 기반 정규화 클래스"""
    
    def print_section(self, title):
        """섹션 헤더"""
        print(f"\n{'='*80}")
        print(f"[{title}]")
        print(f"{'='*80}")
    
    def learn_upc_pattern(self, df, upc_col):
        """UPC 패턴 학습"""
        print(f"\n--- UPC 패턴 학습: {upc_col} ---")
        
        # 모든 값을 문자열로 변환 (작은따옴표 제거, 공백 제거)
        cleaned = df[upc_col].apply(
            lambda x: str(x).replace("'", "").strip() if pd.notna(x) else ''
        )
        
        # 비어있지 않은 값들만
        non_empty = cleaned[cleaned != '']
        
        print(f"총 레코드: {len(df)}개")
        print(f"비어있지 않은 값: {len(non_empty)}개")
        
        # 길이 분포
        print(f"\n길이 분포:")
        length_dist = Counter(non_empty.apply(len))
        for length in sorted(length_dist.keys())[:15]:  # 상위 15개만
            count = length_dist[length]
            print(f"  {length}자리: {count}개")
        
        # 숫자만 있는지 체크
        print(f"\n숫자 체크:")
        all_digit = non_empty.apply(lambda x: x.isdigit()).sum()
        has_alpha = non_empty.apply(lambda x: not x.isdigit() and x != '').sum()
        print(f"  모두 숫자: {all_digit}개")
        print(f"  문자 포함: {has_alpha}개")
        
        # 12~14자리 숫자 체크
        valid_upc = non_empty.apply(
            lambda x: x.isdigit() and 12 <= len(x) <= 14
        )
        valid_count = valid_upc.sum()
        
        print(f"\n💡 유효한 UPC (12~14자리 숫자): {valid_count}개 ({valid_count/len(non_empty)*100:.1f}%)")
        
        # 유효하지 않은 값들 샘플
        invalid = non_empty[~valid_upc]
        if len(invalid) > 0:
            print(f"\n⚠️  유효하지 않은 값 샘플 (10개):")
            for val in invalid.head(10):
                print(f"  - '{val}'")
        
        return valid_count, len(non_empty)
    
    def learn_part_number_pattern(self, df, part_col):
        """파트넘버 패턴 학습"""
        print(f"\n--- 파트넘버 패턴 학습: {part_col} ---")
        
        # 모든 값을 문자열로 변환 (공백 제거)
        cleaned = df[part_col].apply(
            lambda x: str(x).strip() if pd.notna(x) else ''
        )
        
        # 비어있지 않은 값들만
        non_empty = cleaned[cleaned != '']
        
        print(f"총 레코드: {len(df)}개")
        print(f"비어있지 않은 값: {len(non_empty)}개")
        
        # 하이픈 포함 여부
        print(f"\n하이픈(-) 포함:")
        has_hyphen = non_empty.apply(lambda x: '-' in x).sum()
        print(f"  하이픈 있음: {has_hyphen}개")
        
        # 패턴 분석 (XXX-XXXXX 형식)
        # 다양한 패턴 시도
        patterns = {
            'XXX-XXXXX (3글자-5숫자)': r'^[A-Z]{3}-\d{5}$',
            'XX-XXXXX (2글자-5숫자)': r'^[A-Z]{2}-\d{5}$',
            'XXXX-XXXXX (4글자-5숫자)': r'^[A-Z]{4}-\d{5}$',
            'XXX-XXXX (3글자-4숫자)': r'^[A-Z]{3}-\d{4}$',
            '글자-숫자 (일반)': r'^[A-Z]+-\d+$',
        }
        
        print(f"\n패턴 매칭:")
        valid_counts = {}
        for pattern_name, pattern in patterns.items():
            matches = non_empty.apply(lambda x: bool(re.match(pattern, x))).sum()
            valid_counts[pattern_name] = matches
            print(f"  {pattern_name}: {matches}개")
        
        # 가장 많이 매칭되는 패턴
        best_pattern = max(valid_counts, key=valid_counts.get)
        best_count = valid_counts[best_pattern]
        
        print(f"\n💡 가장 일반적인 패턴: {best_pattern}")
        print(f"   매칭: {best_count}개 ({best_count/len(non_empty)*100:.1f}%)")
        
        # 어떤 패턴에도 안 맞는 값들
        any_pattern_match = non_empty.apply(
            lambda x: any(re.match(p, x) for p in patterns.values())
        )
        invalid = non_empty[~any_pattern_match]
        
        if len(invalid) > 0:
            print(f"\n⚠️  어떤 패턴에도 안 맞는 값 샘플 (10개):")
            for val in invalid.head(10):
                print(f"  - '{val}'")
        
        return best_count, len(non_empty)
    
    def normalize_upc(self, value):
        """UPC 정규화 함수"""
        if pd.isna(value):
            return None
        
        # 문자열 변환, 작은따옴표 제거, 공백 제거
        cleaned = str(value).replace("'", "").strip()
        
        # 빈 문자열 체크
        if cleaned in ['', 'nan', 'None', 'none']:
            return None
        
        # float인 경우 .0 제거
        if '.' in cleaned:
            try:
                cleaned = str(int(float(cleaned)))
            except:
                pass
        
        # 패턴 체크: 12~14자리 숫자
        if cleaned.isdigit() and 12 <= len(cleaned) <= 14:
            return cleaned
        else:
            return None
    
    def normalize_part_number(self, value):
        """파트넘버 정규화 함수"""
        if pd.isna(value):
            return None
        
        # 문자열 변환, 공백 제거
        cleaned = str(value).strip()
        
        # 빈 문자열 체크
        if cleaned in ['', 'nan', 'None', 'none']:
            return None
        
        # 패턴 체크: 글자-숫자 형식
        if re.match(r'^[A-Z]+-\d+$', cleaned):
            return cleaned
        else:
            return None
    
    def analyze_and_normalize(self, csv_path, upc_col, part_col, label):
        """분석 및 정규화"""
        self.print_section(f"{label} 분석 및 정규화")
        
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        print(f"총 레코드: {len(df)}개")
        
        # 1. 패턴 학습
        print(f"\n{'='*80}")
        print(f"[패턴 학습]")
        print(f"{'='*80}")
        
        upc_valid, upc_total = self.learn_upc_pattern(df, upc_col)
        part_valid, part_total = self.learn_part_number_pattern(df, part_col)
        
        # 2. 정규화 적용
        print(f"\n{'='*80}")
        print(f"[정규화 적용]")
        print(f"{'='*80}")
        
        df['normalized_upc'] = df[upc_col].apply(self.normalize_upc)
        df['normalized_part'] = df[part_col].apply(self.normalize_part_number)
        
        normalized_upc = df['normalized_upc'].notna().sum()
        normalized_part = df['normalized_part'].notna().sum()
        
        print(f"\nUPC:")
        print(f"  원본 비어있지 않음: {upc_total}개")
        print(f"  정규화 후 유효: {normalized_upc}개")
        print(f"  제거됨: {upc_total - normalized_upc}개")
        
        print(f"\n파트넘버:")
        print(f"  원본 비어있지 않음: {part_total}개")
        print(f"  정규화 후 유효: {normalized_part}개")
        print(f"  제거됨: {part_total - normalized_part}개")
        
        # 3. 제거된 값들 확인
        print(f"\n{'='*80}")
        print(f"[제거된 값 확인]")
        print(f"{'='*80}")
        
        # UPC 제거된 값
        removed_upc = df[df[upc_col].notna() & df['normalized_upc'].isna()][upc_col]
        if len(removed_upc) > 0:
            print(f"\nUPC 제거된 값 (10개):")
            for val in removed_upc.head(10):
                print(f"  - '{val}'")
        
        # 파트넘버 제거된 값
        removed_part = df[df[part_col].notna() & df['normalized_part'].isna()][part_col]
        if len(removed_part) > 0:
            print(f"\n파트넘버 제거된 값 (10개):")
            for val in removed_part.head(10):
                print(f"  - '{val}'")
        
        # 4. 매칭 상태
        print(f"\n{'='*80}")
        print(f"[매칭 상태 (정규화 후)]")
        print(f"{'='*80}")
        
        both = (df['normalized_upc'].notna() & df['normalized_part'].notna()).sum()
        only_upc = (df['normalized_upc'].notna() & df['normalized_part'].isna()).sum()
        only_part = (df['normalized_upc'].isna() & df['normalized_part'].notna()).sum()
        neither = (df['normalized_upc'].isna() & df['normalized_part'].isna()).sum()
        
        print(f"UPC와 파트넘버 둘 다 있음: {both}개")
        print(f"UPC만 있음: {only_upc}개")
        print(f"파트넘버만 있음: {only_part}개")
        print(f"둘 다 없음: {neither}개")
        
        return df
    
    def run_full_analysis(self):
        """전체 분석 실행"""
        print(f"\n{'#'*80}")
        print(f"# 패턴 기반 매칭 데이터 정규화")
        print(f"{'#'*80}")
        
        # 기존 CSV
        baseline_df = self.analyze_and_normalize(
            "coupang_iherb_products.csv",
            "아이허브_UPC",
            "아이허브_파트넘버",
            "coupang_iherb_products.csv"
        )
        
        # 업데이트 CSV
        updated_df = self.analyze_and_normalize(
            "updated.csv",
            "UPC (제품 바코드번호)",
            "아이허브파트넘버",
            "updated.csv"
        )
        
        print(f"\n{'#'*80}")
        print(f"# 분석 완료")
        print(f"{'#'*80}\n")
        
        return baseline_df, updated_df


def main():
    """메인 실행"""
    normalizer = PatternBasedNormalizer()
    baseline_df, updated_df = normalizer.run_full_analysis()
    
    print("\n💡 다음 단계: 이 정규화 규칙을 사용하여 CSV 통합")


if __name__ == "__main__":
    main()