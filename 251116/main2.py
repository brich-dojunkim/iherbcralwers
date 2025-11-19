"""
위해식품 × 아이허브 매칭 - 인덱스 기반 초고속 버전 (최종 패치)

전략:
1. 브랜드 인덱스 사전 구축 (빠름)
2. 키워드 인덱스 사전 구축 (빠름)
3. O(1) 조회로 후보 선택 (초고속)
4. UPC 불일치 거부 강화
5. 바코드 파싱 개선 (과학적 표기법 + 공백 포함 포맷 지원)
6. 최종 매칭(바코드/텍스트) 모두 바코드 재검증
"""

import pandas as pd
import re
import unicodedata
from difflib import SequenceMatcher
from collections import defaultdict
from tqdm import tqdm
from decimal import Decimal

# ==================== 설정 ====================
INPUT_HAZARD_FILE = '/Users/brich/Desktop/iherb_price/251116/2025.11.18_해외직구+위해식품+목록.xls'
INPUT_IHERB_FILE = '/Users/brich/Desktop/iherb_price/251116/iherb_item_filtered_by_all_list.xlsx'

OUTPUT_FILE = '위해식품목록_매칭결과_초고속.xlsx'
OUTPUT_DETAILS = '매칭상세정보_초고속2.xlsx'

INDEX_DEBUG_IHERB_FILE = 'iherb_index_debug.xlsx'
INDEX_DEBUG_HAZARD_FILE = 'hazard_index_debug.xlsx'

# Threshold
THRESHOLD_NAME_SIMILARITY = 0.80
THRESHOLD_BRAND_KEYWORDS = 0.70
THRESHOLD_KEYWORDS_ONLY = 0.60

# ==================== 유틸리티 ====================
def normalize_filename(filename):
    return unicodedata.normalize('NFD', filename)

def normalize_text(text):
    if pd.isna(text) or text is None:
        return ""
    text = str(text).lower().strip()
    
    # 한국어 접미사
    text = re.sub(r'\(유통사\)', '', text)
    text = re.sub(r'\(제조사\)', '', text)
    text = re.sub(r'유통사$', '', text)
    text = re.sub(r'제조사$', '', text)
    
    # 법인 접미사
    text = re.sub(r'\bllc\b', '', text)
    text = re.sub(r'\binc\.?\b', '', text)
    text = re.sub(r'\bcorp\.?\b', '', text)
    text = re.sub(r'\bltd\.?\b', '', text)
    text = re.sub(r'\bco\.?\b', '', text)
    
    # 괄호/대괄호 내용 제거
    text = re.sub(r'\([^)]*\)', '', text)
    text = re.sub(r'\[[^\]]*\]', '', text)
    # 특수문자 제거
    text = re.sub(r'[^\w\s]', ' ', text)
    # 공백 정리
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def extract_keywords(text):
    """
    키워드 추출 규칙:
    - normalize_text 적용
    - stop_words 제거
    - 길이 2 이하 토큰 제거
    - 숫자만으로 이루어진 토큰 제거
    """
    text = normalize_text(text)
    stop_words = {
        'with', 'and', 'or', 'the', 'of', 'for', 'in', 'on', 'at',
        'oz', 'mg', 'g', 'ml', 'fl', 'lb', 'count', 'pack',
        'softgels', 'capsules', 'tablets', 'gummies', 'powder',
        'liquid', 'cream', 'oil', 'supplement', 'dietary', 'health'
    }
    words = []
    for w in text.split():
        if w in stop_words:
            continue
        if len(w) <= 2:
            continue
        if w.isdigit():
            continue
        words.append(w)
    return set(words)

def calculate_keyword_overlap(kw1, kw2):
    if not kw1 or not kw2:
        return 0
    return len(kw1 & kw2) / len(kw1 | kw2)

def similarity_ratio(s1, s2):
    return SequenceMatcher(None, s1, s2).ratio()

# ==================== 바코드 파싱 (개선) ====================
def parse_barcodes(barcode_str):
    """
    바코드 파싱 (개선)
    - 공백 제거 ("6 56490 00872 1" → "656490008721")
    - 과학적 표기법 처리
    - 여러 형식 지원
    """
    if pd.isna(barcode_str):
        return set()
    
    barcode_str = str(barcode_str).strip()
    
    if not barcode_str or barcode_str.lower() in ['nan', 'none', '']:
        return set()
    
    # 🔥 공백 전체 제거
    barcode_str = barcode_str.replace(" ", "")
    
    barcodes = set()
    parts = barcode_str.split(',')
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
        
        try:
            # 1. 정수형 문자열
            if part.isdigit():
                barcodes.add(int(part))
                continue
            
            # 2. 소수점 있는 경우
            if '.' in part and 'e' not in part.lower():
                num = float(part)
                if num == int(num):
                    barcodes.add(int(num))
                continue
            
            # 3. 과학적 표기법
            if 'e' in part.lower():
                num = Decimal(part)
                barcodes.add(int(num))
                continue
            
            # 4. 일반 숫자 변환
            num = float(part)
            if abs(num) < 1e15:
                barcodes.add(int(num))
            
        except (ValueError, OverflowError, Exception):
            continue
    
    return barcodes

def normalize_iherb_barcode(upc):
    """아이허브 UPC 정규화"""
    try:
        if pd.isna(upc):
            return None
        
        upc_str = str(upc).strip()
        
        # 정수형
        if upc_str.isdigit():
            return int(upc_str)
        
        # 소수점
        if '.' in upc_str and 'e' not in upc_str.lower():
            num = float(upc_str)
            if num == int(num):
                return int(num)
            return None
        
        # 과학적 표기법
        if 'e' in upc_str.lower():
            num = Decimal(upc_str)
            return int(num)
        
        # 일반 변환
        num = float(upc_str)
        if abs(num) < 1e15:
            return int(num)
        
        return None
        
    except:
        return None

# ==================== 바코드 매칭 ====================
def match_by_barcode(hazard_df, iherb_df):
    print("\n[1단계] 바코드 매칭")
    
    hazard_with_barcode = hazard_df[hazard_df['유통바코드정보'].notna()].copy()
    
    print(f"  • 바코드 보유 행: {len(hazard_with_barcode)}건")
    
    expanded_rows = []
    parse_debug = []  # 🔹 디버깅용
    
    for idx, row in hazard_with_barcode.iterrows():
        barcodes = parse_barcodes(row['유통바코드정보'])
        
        # 🔹 디버깅: 파싱 결과 저장
        if barcodes:
            parse_debug.append({
                'index': idx,
                'raw': row['유통바코드정보'],
                'parsed': list(barcodes)
            })
        
        for barcode in barcodes:
            expanded_rows.append({
                'original_index': idx,
                'barcode_clean': barcode
            })
    
    print(f"  • 확장된 바코드: {len(expanded_rows)}개")
    
    # 🔹 디버깅: 파싱 샘플 출력
    if parse_debug[:5]:
        print(f"\n  [DEBUG] 바코드 파싱 샘플:")
        for item in parse_debug[:5]:
            print(f"    Row {item['index']}: {item['raw']} → {item['parsed']}")
    
    if not expanded_rows:
        print(f"  ✓ 0건 매칭")
        return []
    
    hazard_expanded = pd.DataFrame(expanded_rows)
    
    # 아이허브 바코드 정규화
    iherb_barcode = iherb_df[['product_upc', 'product_partno', 'product_id']].copy()
    iherb_barcode = iherb_barcode[iherb_barcode['product_upc'].notna()]
    
    # 🔹 디버깅: 정규화 전/후 비교
    iherb_barcode['upc_raw'] = iherb_barcode['product_upc']
    iherb_barcode['barcode_clean'] = iherb_barcode['product_upc'].apply(normalize_iherb_barcode)
    
    valid_barcodes = iherb_barcode[iherb_barcode['barcode_clean'].notna()]
    
    print(f"  • 아이허브 UPC 원본: {len(iherb_barcode)}개")
    print(f"  • 아이허브 정규화 성공: {len(valid_barcodes)}개")
    
    # 🔹 디버깅: 정규화 샘플 출력
    if len(valid_barcodes) > 0:
        print(f"\n  [DEBUG] 아이허브 UPC 정규화 샘플:")
        for _, row in valid_barcodes.head(5).iterrows():
            print(f"    {row['upc_raw']} → {row['barcode_clean']}")
    
    iherb_barcode = valid_barcodes
    
    # 매칭
    merged = hazard_expanded.merge(
        iherb_barcode[['barcode_clean', 'product_partno', 'product_id', 'upc_raw']], 
        on='barcode_clean', 
        how='inner'
    )
    
    print(f"\n  • 바코드 일치: {len(merged)}건")
    
    # 🔹 디버깅: 매칭 샘플 출력
    if len(merged) > 0:
        print(f"\n  [DEBUG] 바코드 매칭 샘플:")
        for _, row in merged.head(5).iterrows():
            print(f"    위해 idx {row['original_index']}: {row['barcode_clean']} → UPC {row['upc_raw']}")
    
    merged_unique = merged.drop_duplicates(subset=['original_index'], keep='first')
    
    print(f"  ✓ {len(merged_unique)}건 매칭 (중복 제거 후)")
    
    results = []
    for _, row in merged_unique.iterrows():
        results.append({
            'index': row['original_index'],
            'product_partno': row['product_partno'],
            'product_id': row['product_id'],
            'match_method': 'barcode',
            'match_score': 1.0
        })
    
    return results

def verify_matches_by_barcode(hazard_df, iherb_df, results, source_label=""):
    """
    매칭 결과 바코드 재검증 (공통 함수)
    - 위해식품 유통바코드 vs 아이허브 UPC 정규화 값 비교
    - 바코드 텍스트는 있는데 파싱 실패하면 강제 reject
    - 둘 다 바코드가 있으면 반드시 UPC 동치여야 통과
    """
    print(f"\n[바코드 재검증] {source_label} 매칭 결과 검증")
    
    verified_results = []
    rejected_results = []
    
    for r in results:
        idx = r['index']
        hazard_row = hazard_df.loc[idx]
        
        raw_barcode = hazard_row.get('유통바코드정보', '')
        raw_str = "" if pd.isna(raw_barcode) else str(raw_barcode).strip()
        has_raw = raw_str != "" and raw_str.lower() not in ['nan', 'none']
        
        hazard_barcodes = parse_barcodes(raw_barcode)
        
        # 🔥 패치 A:
        # 바코드 텍스트는 있는데 parse 후 빈 set → 포맷 문제로 인한 파싱 실패로 간주 → reject
        if has_raw and len(hazard_barcodes) == 0:
            rej = dict(r)
            rej.update({
                'hazard_barcode_raw': raw_barcode,
                'hazard_barcode_parsed': list(hazard_barcodes),
                'reason': '바코드 파싱 실패(비정규 포맷)',
                'reject_source': source_label
            })
            rejected_results.append(rej)
            continue
        
        # 아이허브 행 찾기
        iherb_rows = iherb_df[iherb_df['product_partno'] == r['product_partno']]
        if iherb_rows.empty:
            # 아이허브에 해당 partno가 없으면 보수적으로 통과
            verified_results.append(r)
            continue
        
        iherb_row = iherb_rows.iloc[0]
        iherb_upc = iherb_row.get('product_upc')
        norm_upc = normalize_iherb_barcode(iherb_upc)
        
        # 🔥 패치 B:
        # 바코드가 정상 파싱된 경우 + 아이허브 UPC도 있으면 반드시 비교
        if hazard_barcodes and norm_upc:
            if norm_upc in hazard_barcodes:
                verified_results.append(r)
            else:
                rej = dict(r)
                rej.update({
                    'hazard_barcode': list(hazard_barcodes),
                    'iherb_upc': norm_upc,
                    'reason': 'UPC 불일치(후검증)',
                    'reject_source': source_label
                })
                rejected_results.append(rej)
        else:
            # 바코드 자체가 원래부터 없는(공란/NaN) 경우는 통과
            verified_results.append(r)
    
    print(f"  ✓ 검증 통과: {len(verified_results)}건")
    print(f"  ✗ 검증 실패: {len(rejected_results)}건")
    
    if rejected_results:
        print(f"\n  [검증 실패 샘플 - {source_label}]")
        for item in rejected_results[:5]:
            print(f"    Row {item['index']}: raw={item.get('hazard_barcode_raw', item.get('hazard_barcode'))}, "
                  f"iherb_upc={item.get('iherb_upc')}, reason={item['reason']}")
    
    return verified_results, rejected_results

# ==================== 인덱스 구축 ====================
def build_indexes(iherb_df):
    """
    아이허브 데이터의 인덱스 사전 구축
    - brand_index: 정규화 브랜드명 -> item 리스트
    - keyword_index: 키워드 -> item 리스트
    - product_item_map: product_id -> item
    """
    print("\n인덱스 구축 중...")
    
    brand_index = defaultdict(list)
    keyword_index = defaultdict(list)
    product_item_map = {}
    
    for idx, row in tqdm(iherb_df.iterrows(), total=len(iherb_df), desc="  인덱싱"):
        name_brand_text = f"{row.get('product_name', '')} {row.get('product_brand', '')}"
        item = {
            'idx': idx,
            'row': row,
            'name_norm': normalize_text(row['product_name']),
            'brand_norm': normalize_text(row.get('product_brand', '')),
            'keywords': extract_keywords(name_brand_text),
        }
        
        product_id = row['product_id']
        product_item_map[product_id] = item
        
        # 브랜드 인덱스
        if item['brand_norm']:
            brand_index[item['brand_norm']].append(item)
        
        # 키워드 인덱스
        for keyword in item['keywords']:
            keyword_index[keyword].append(item)
    
    print(f"  ✓ 브랜드 인덱스: {len(brand_index)}개")
    print(f"  ✓ 키워드 인덱스: {len(keyword_index)}개")
    
    return brand_index, keyword_index, product_item_map

def export_iherb_index_to_excel(product_item_map, output_path=INDEX_DEBUG_IHERB_FILE):
    """아이허브 인덱스 디버그 파일"""
    rows = []
    for pid, item in product_item_map.items():
        row = item['row']
        rows.append({
            "product_id": pid,
            "product_partno": row.get("product_partno", ""),
            "product_name_raw": row.get("product_name", ""),
            "product_brand_raw": row.get("product_brand", ""),
            "name_norm": item["name_norm"],
            "brand_norm": item["brand_norm"],
            "keywords": ", ".join(sorted(list(item["keywords"]))),
        })
    
    df = pd.DataFrame(rows)
    df.to_excel(output_path, index=False)
    print(f"\n[DEBUG] 아이허브 인덱스 디버그 파일 저장: {output_path}")

def export_hazard_index_to_excel(hazard_df, output_path=INDEX_DEBUG_HAZARD_FILE):
    """위해식품 인덱스 디버그 파일"""
    rows = []
    for idx, row in hazard_df.iterrows():
        name_raw = row.get('제품명', '')
        brand_raw = row.get('제조사명', '')
        name_norm = normalize_text(name_raw)
        brand_norm = normalize_text(brand_raw)
        keywords = extract_keywords(f"{name_raw} {brand_raw}")
        
        rows.append({
            "hazard_index": idx,
            "위해_제품명_raw": name_raw,
            "위해_제조사_raw": brand_raw,
            "위해_제품명_norm": name_norm,
            "위해_제조사_norm": brand_norm,
            "위해_keywords": ", ".join(sorted(list(keywords))),
            "유통바코드정보": row.get('유통바코드정보', '')
        })
    
    df = pd.DataFrame(rows)
    df.to_excel(output_path, index=False)
    print(f"\n[DEBUG] 위해식품 인덱스 디버그 파일 저장: {output_path}")

# ==================== 텍스트 매칭 ====================
def match_by_text_indexed(hazard_df, brand_index, keyword_index, barcode_matched_indices):
    print("\n[2단계] 텍스트 매칭 (인덱스 기반)")
    
    remaining = hazard_df[~hazard_df.index.isin(barcode_matched_indices)]
    print(f"  대상: {len(remaining)}건")
    
    if len(remaining) == 0:
        return []
    
    results = []
    reject_stats = {
        'barcode_mismatch': 0,
        'brand_too_different': 0,
        'no_candidates': 0
    }
    
    for idx, hazard_row in tqdm(remaining.iterrows(), total=len(remaining), desc="  매칭 중"):
        hazard_name = normalize_text(hazard_row['제품명'])
        hazard_brand = normalize_text(hazard_row.get('제조사명', ''))
        hazard_keywords = extract_keywords(
            f"{hazard_row.get('제품명', '')} {hazard_row.get('제조사명', '')}"
        )
        
        # 위해식품 바코드 파싱
        hazard_barcodes = parse_barcodes(hazard_row.get('유통바코드정보', ''))
        
        if not hazard_name or not hazard_keywords:
            continue
        
        # 후보 선택
        candidates = set()
        
        if hazard_brand and hazard_brand in brand_index:
            for item in brand_index[hazard_brand]:
                candidates.add(item['idx'])
        
        keyword_candidates = None
        for keyword in hazard_keywords:
            if keyword in keyword_index:
                kw_items = set(item['idx'] for item in keyword_index[keyword])
                if keyword_candidates is None:
                    keyword_candidates = kw_items
                else:
                    keyword_candidates &= kw_items
        
        if keyword_candidates:
            candidates.update(keyword_candidates)
        
        if not candidates:
            # 백업: 상위 5개 키워드 기준으로 최대 50개씩 후보 모으기
            for keyword in list(hazard_keywords)[:5]:
                if keyword in keyword_index:
                    for item in keyword_index[keyword][:50]:
                        candidates.add(item['idx'])
        
        if not candidates:
            reject_stats['no_candidates'] += 1
            continue
        
        # 매칭
        best_match = None
        best_score = 0
        best_method = None
        
        for item_idx in candidates:
            item = None
            if hazard_brand and hazard_brand in brand_index:
                for i in brand_index[hazard_brand]:
                    if i['idx'] == item_idx:
                        item = i
                        break
            
            if not item:
                for keyword in hazard_keywords:
                    if keyword in keyword_index:
                        for i in keyword_index[keyword]:
                            if i['idx'] == item_idx:
                                item = i
                                break
                        if item:
                            break
            
            if not item:
                continue
            
            iherb_name = item['name_norm']
            iherb_brand = item['brand_norm']
            iherb_keywords = item['keywords']
            iherb_row = item['row']
            
            # ============================================
            # 🔹 UPC 불일치 1차 필터 (후보 단계)
            #    → 최종적으로는 verify_matches_by_barcode에서 한 번 더 재검증
            # ============================================
            iherb_upc = iherb_row.get('product_upc')
            if hazard_barcodes:
                if pd.notna(iherb_upc):
                    iherb_barcode_normalized = normalize_iherb_barcode(iherb_upc)
                    if iherb_barcode_normalized is not None:
                        if iherb_barcode_normalized not in hazard_barcodes:
                            reject_stats['barcode_mismatch'] += 1
                            continue  # ❌ 후보 단계에서 제거
            
            # ============================================
            # 🔹 제조사 불일치 체크
            # ============================================
            if hazard_brand and iherb_brand:
                brand_sim = similarity_ratio(hazard_brand, iherb_brand)
                if brand_sim < 0.5:
                    reject_stats['brand_too_different'] += 1
                    continue
            
            # 브랜드 + 키워드
            if hazard_brand and iherb_brand:
                brand_sim = similarity_ratio(hazard_brand, iherb_brand)
                if brand_sim > 0.75:
                    keyword_overlap = calculate_keyword_overlap(hazard_keywords, iherb_keywords)
                    if keyword_overlap > 0.3:
                        score = brand_sim * 0.4 + keyword_overlap * 0.6
                        if score >= THRESHOLD_BRAND_KEYWORDS and score > best_score:
                            best_score = score
                            best_match = iherb_row
                            best_method = 'brand+keywords'
                            if score >= 0.95:
                                break
            
            # 제품명 유사도
            name_sim = similarity_ratio(hazard_name, iherb_name)
            if name_sim >= THRESHOLD_NAME_SIMILARITY and name_sim > best_score:
                best_score = name_sim
                best_match = iherb_row
                best_method = 'name_similarity'
                if name_sim >= 0.95:
                    break
            
            # 키워드만
            keyword_overlap = calculate_keyword_overlap(hazard_keywords, iherb_keywords)
            if keyword_overlap >= THRESHOLD_KEYWORDS_ONLY and keyword_overlap > best_score:
                best_score = keyword_overlap
                best_match = iherb_row
                best_method = 'keywords_only'
        
        if best_match is not None:
            results.append({
                'index': idx,
                'product_partno': best_match['product_partno'],
                'product_id': best_match['product_id'],
                'match_method': best_method,
                'match_score': round(best_score, 3)
            })
    
    print(f"  ✓ {len(results)}건 매칭")
    print(f"  • 거부 통계:")
    print(f"    - UPC 둘다있는데 불일치(후보 단계): {reject_stats['barcode_mismatch']}건")
    print(f"    - 제조사 상이: {reject_stats['brand_too_different']}건")
    print(f"    - 후보 없음: {reject_stats['no_candidates']}건")
    
    return results

# ==================== 메인 ====================
def main():
    print("="*60)
    print("위해식품 × 아이허브 매칭 (인덱스 기반 초고속)")
    print("="*60)
    
    # 파일 읽기
    try:
        hazard_df = pd.read_excel(INPUT_HAZARD_FILE)
        iherb_df = pd.read_excel(INPUT_IHERB_FILE)
    except FileNotFoundError as e:
        print(f"\n❌ 오류: {e}")
        return
    
    print(f"\n위해식품: {len(hazard_df):,}건")
    print(f"아이허브: {len(iherb_df):,}건")
    
    # 🔹 검출성분 컬럼 찾기
    hazard_columns = hazard_df.columns.tolist()
    detected_component_col = None
    
    for col in hazard_columns:
        if '검출성분' in col:
            detected_component_col = col
            print(f"\n✓ 검출성분 컬럼 발견: '{detected_component_col}'")
            break
    
    if detected_component_col is None:
        print("\n⚠️ 경고: '검출성분' 컬럼을 찾을 수 없습니다.")
        detected_component_col = '검출성분(국문)'  # 기본값
    
    # 위해식품 인덱스 디버그
    export_hazard_index_to_excel(hazard_df, INDEX_DEBUG_HAZARD_FILE)
    
    # 인덱스 구축
    brand_index, keyword_index, product_item_map = build_indexes(iherb_df)
    
    # 아이허브 인덱스 디버그
    export_iherb_index_to_excel(product_item_map, INDEX_DEBUG_IHERB_FILE)
    
    # 1단계: 바코드 매칭
    barcode_results = match_by_barcode(hazard_df, iherb_df)
    
    # 1.5단계: 바코드 매칭 결과 바코드 재검증
    verified_barcode_results, rejected_barcode_from_barcode = verify_matches_by_barcode(
        hazard_df, iherb_df, barcode_results, source_label="barcode"
    )
    
    barcode_matched_indices = {r['index'] for r in verified_barcode_results}
    
    # 2단계: 텍스트 매칭
    text_results = match_by_text_indexed(hazard_df, brand_index, keyword_index, barcode_matched_indices)
    
    # 2.5단계: 텍스트 매칭 결과 바코드 재검증
    verified_text_results, rejected_barcode_from_text = verify_matches_by_barcode(
        hazard_df, iherb_df, text_results, source_label="text"
    )
    
    # 최종 결과 병합 (후검증 통과한 것만)
    all_results = verified_barcode_results + verified_text_results
    result_dict = {r['index']: r for r in all_results}
    
    final_data = []
    for idx, row in hazard_df.iterrows():
        row_dict = row.to_dict()
        if idx in result_dict:
            match = result_dict[idx]
            row_dict['product_partno'] = match['product_partno']
            row_dict['product_id'] = match['product_id']
            row_dict['match_method'] = match['match_method']
            row_dict['match_score'] = match['match_score']
        else:
            row_dict['product_partno'] = None
            row_dict['product_id'] = None
            row_dict['match_method'] = None
            row_dict['match_score'] = None
        final_data.append(row_dict)
    
    final_df = pd.DataFrame(final_data)
    
    # 저장
    output_file = OUTPUT_FILE
    final_df.to_excel(output_file, index=False)
    
    # ============================================
    # 🔹 상세 정보 (컬럼 순서 변경 + 검출성분 추가)
    # ============================================
    matched_df = final_df[final_df['product_partno'].notna()]
    
    if len(matched_df) > 0:
        # 아이허브 정보 병합
        details = matched_df.merge(
            iherb_df[['product_partno', 'product_name', 'product_brand', 'product_upc']],
            on='product_partno',
            how='left'
        )
        
        # 컬럼 선택 (순서대로)
        detail_columns = [
            'product_partno',          # 1
            'product_id',              # 2
            'match_method',            # 3
            'match_score',             # 4
            '제품명',                  # 5. 위해식품_제품명
            'product_name',            # 6. 아이허브_제품명
            '제조사명',                # 7. 위해식품_제조사
            'product_brand',           # 8. 아이허브_브랜드
            '유통바코드정보',          # 9. 위해식품_바코드
            'product_upc'              # 10. 아이허브_UPC
        ]
        
        # 검출성분 컬럼이 있으면 추가
        if detected_component_col in details.columns:
            detail_columns.append(detected_component_col)
        
        details_output = details[detail_columns].copy()
        
        # 컬럼명 변경
        rename_dict = {
            'match_method': '매칭방법',
            'match_score': '매칭스코어',
            '제품명': '위해식품_제품명',
            'product_name': '아이허브_제품명',
            '제조사명': '위해식품_제조사',
            'product_brand': '아이허브_브랜드',
            '유통바코드정보': '위해식품_바코드',
            'product_upc': '아이허브_UPC'
        }
        
        if detected_component_col in details_output.columns:
            rename_dict[detected_component_col] = '검출성분(국문)'
        
        details_output.rename(columns=rename_dict, inplace=True)
        
        details_file = OUTPUT_DETAILS
        details_output.to_excel(details_file, index=False)
    else:
        details_file = None
    
    # 통계
    print("\n" + "="*60)
    print("매칭 결과")
    print("="*60)
    
    total = len(hazard_df)
    matched = len(matched_df) if len(matched_df) > 0 else 0
    
    print(f"\n전체: {total:,}건")
    print(f"매칭 성공: {matched:,}건 ({matched/total*100:.1f}%)")
    print(f"미매칭: {total-matched:,}건")
    
    if matched > 0:
        print(f"\n매칭 방법별:")
        method_counts = final_df['match_method'].value_counts()
        for method, count in method_counts.items():
            print(f"  {method}: {count:,}건")
        
        print(f"\n신뢰도별:")
        high = len(matched_df[matched_df['match_score'] >= 0.9])
        mid = len(matched_df[(matched_df['match_score'] >= 0.8) & (matched_df['match_score'] < 0.9)])
        low = len(matched_df[matched_df['match_score'] < 0.8])
        
        print(f"  높음 (0.9+): {high:,}건")
        print(f"  중간 (0.8-0.9): {mid:,}건")
        print(f"  낮음 (0.8 미만): {low:,}건")
    
    print("\n[바코드 후검증 요약]")
    print(f"  - 바코드 매칭 단계에서 UPC 관련 reject: {len(rejected_barcode_from_barcode)}건")
    print(f"  - 텍스트 매칭 단계에서 UPC 관련 reject: {len(rejected_barcode_from_text)}건")
    
    print(f"\n✓ 결과 저장: {output_file}")
    if matched > 0 and details_file:
        print(f"✓ 상세 정보: {details_file}")
    print(f"✓ 아이허브 인덱스 디버그: {INDEX_DEBUG_IHERB_FILE}")
    print(f"✓ 위해식품 인덱스 디버그: {INDEX_DEBUG_HAZARD_FILE}")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main()
