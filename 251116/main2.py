"""
위해식품 × 아이허브 매칭 - 인덱스 기반 초고속 버전

전략:
1. 브랜드 인덱스 사전 구축 (빠름)
2. 키워드 인덱스 사전 구축 (빠름)
3. O(1) 조회로 후보 선택 (초고속)

예상 시간: 2-5분
"""

import pandas as pd
import re
import unicodedata
from difflib import SequenceMatcher
from collections import defaultdict
from tqdm import tqdm

# ==================== 설정 ====================
INPUT_HAZARD_FILE = '/Users/brich/Desktop/iherb_price/251116/2025.11.18_해외직구+위해식품+목록.xls'
INPUT_IHERB_FILE = '/Users/brich/Desktop/iherb_price/251116/iherb_item feed_en.xlsx'

OUTPUT_FILE = '위해식품목록_매칭결과_초고속.xlsx'
OUTPUT_DETAILS = '매칭상세정보_초고속.xlsx'

INDEX_DEBUG_IHERB_FILE = 'iherb_index_debug.xlsx'       # 아이허브 기준 인덱스 디버그
INDEX_DEBUG_HAZARD_FILE = 'hazard_index_debug.xlsx'     # 위해식품 기준 인덱스 디버그

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
    - 숫자만으로 이루어진 토큰 제거 (ex. '120', '2000')
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
        if w.isdigit():      # 🔹 순수 숫자는 키워드에서 제외
            continue
        words.append(w)
    return set(words)

def calculate_keyword_overlap(kw1, kw2):
    if not kw1 or not kw2:
        return 0
    return len(kw1 & kw2) / len(kw1 | kw2)

def similarity_ratio(s1, s2):
    return SequenceMatcher(None, s1, s2).ratio()

# ==================== 바코드 매칭 ====================
def match_by_barcode(hazard_df, iherb_df):
    print("\n[1단계] 바코드 매칭")
    
    hazard_with_barcode = hazard_df[hazard_df['유통바코드정보'].notna()].copy()
    
    def parse_barcodes(barcode_str):
        if pd.isna(barcode_str):
            return []
        try:
            parts = str(barcode_str).split(',')
            barcodes = []
            for part in parts:
                part = part.strip()
                if part:
                    try:
                        barcodes.append(int(float(part)))
                    except (ValueError, OverflowError):
                        pass
            return barcodes
        except:
            return []
    
    expanded_rows = []
    for idx, row in hazard_with_barcode.iterrows():
        barcodes = parse_barcodes(row['유통바코드정보'])
        for barcode in barcodes:
            expanded_rows.append({
                'original_index': idx,
                'barcode_clean': barcode
            })
    
    if not expanded_rows:
        print(f"  ✓ 0건 매칭")
        return []
    
    hazard_expanded = pd.DataFrame(expanded_rows)
    iherb_barcode = iherb_df[['product_upc', 'product_partno', 'product_id']].copy()
    iherb_barcode['barcode_clean'] = iherb_barcode['product_upc']
    
    merged = hazard_expanded.merge(iherb_barcode, on='barcode_clean', how='inner')
    merged_unique = merged.drop_duplicates(subset=['original_index'], keep='first')
    
    print(f"  ✓ {len(merged_unique)}건 매칭 (총 {len(expanded_rows)}개 바코드 검사)")
    
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

# ==================== 인덱스 구축 (아이허브) + 엑셀 덤프 ====================
def build_indexes(iherb_df):
    """
    아이허브 데이터의 인덱스 사전 구축
    - brand_index: 정규화 브랜드명 -> item 리스트
    - keyword_index: 키워드 -> item 리스트
    - product_item_map: product_id -> item (엑셀 덤프용)
    """
    print("\n인덱스 구축 중...")
    
    brand_index = defaultdict(list)
    keyword_index = defaultdict(list)
    product_item_map = {}  # product_id -> item
    
    for idx, row in tqdm(iherb_df.iterrows(), total=len(iherb_df), desc="  인덱싱"):
        # 🔹 아이허브 키워드는 "상품명 + 브랜드명"을 합쳐서 추출
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
        
        # 브랜드 인덱스 추가
        if item['brand_norm']:
            brand_index[item['brand_norm']].append(item)
        
        # 키워드 인덱스 추가
        for keyword in item['keywords']:
            keyword_index[keyword].append(item)
    
    print(f"  ✓ 브랜드 인덱스: {len(brand_index)}개")
    print(f"  ✓ 키워드 인덱스: {len(keyword_index)}개")
    
    return brand_index, keyword_index, product_item_map

def export_iherb_index_to_excel(product_item_map, output_path=INDEX_DEBUG_IHERB_FILE):
    """
    아이허브 상품별 인덱스(정규화 이름, 브랜드, 키워드)를 엑셀로 저장
    """
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

# ==================== 위해식품 기준 인덱스 덤프 ====================
def export_hazard_index_to_excel(hazard_df, output_path=INDEX_DEBUG_HAZARD_FILE):
    """
    위해식품 상품별 정규화 이름/브랜드/키워드 엑셀 저장
    - 실제 매칭 인덱스는 아니고, 위해식품 쪽에서 normalize/키워드 추출이 어떻게 되는지 보는 용도
    """
    rows = []
    for idx, row in hazard_df.iterrows():
        name_raw = row.get('제품명', '')
        brand_raw = row.get('제조사명', '')
        name_norm = normalize_text(name_raw)
        brand_norm = normalize_text(brand_raw)
        # 🔹 위해식품도 "제품명 + 제조사명"을 합쳐서 키워드 추출
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
    
    for idx, hazard_row in tqdm(remaining.iterrows(), total=len(remaining), desc="  매칭 중"):
        hazard_name = normalize_text(hazard_row['제품명'])
        hazard_brand = normalize_text(hazard_row.get('제조사명', ''))
        # 🔹 위해식품 키워드도 "제품명 + 제조사명" 기준
        hazard_keywords = extract_keywords(
            f"{hazard_row.get('제품명', '')} {hazard_row.get('제조사명', '')}"
        )
        
        if not hazard_name or not hazard_keywords:
            continue
        
        # 후보 선택 (O(1) 조회)
        candidates = set()
        
        # 1. 브랜드 인덱스에서 조회
        if hazard_brand and hazard_brand in brand_index:
            for item in brand_index[hazard_brand]:
                candidates.add(item['idx'])
        
        # 2. 키워드 인덱스에서 조회 (교집합)
        keyword_candidates = None
        for keyword in hazard_keywords:
            if keyword in keyword_index:
                kw_items = set(item['idx'] for item in keyword_index[keyword])
                if keyword_candidates is None:
                    keyword_candidates = kw_items
                else:
                    keyword_candidates &= kw_items  # 교집합
        
        if keyword_candidates:
            candidates.update(keyword_candidates)
        
        # 후보 없으면 키워드 합집합 (최소 1개 겹침)
        if not candidates:
            for keyword in list(hazard_keywords)[:5]:  # 상위 5개만
                if keyword in keyword_index:
                    for item in keyword_index[keyword][:50]:  # 각 키워드당 50개만
                        candidates.add(item['idx'])
        
        if not candidates:
            continue
        
        # 실제 매칭
        best_match = None
        best_score = 0
        best_method = None
        
        # 인덱스로 아이템 조회
        for item_idx in candidates:
            # 브랜드 인덱스나 키워드 인덱스에서 item 찾기
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
    
    # 위해식품 기준 인덱스 디버그 엑셀 (정규화/키워드 확인용)
    export_hazard_index_to_excel(hazard_df, INDEX_DEBUG_HAZARD_FILE)
    
    # 인덱스 구축 (아이허브)
    brand_index, keyword_index, product_item_map = build_indexes(iherb_df)
    
    # 아이허브 인덱스 디버그 엑셀
    export_iherb_index_to_excel(product_item_map, INDEX_DEBUG_IHERB_FILE)
    
    # 바코드 매칭
    barcode_results = match_by_barcode(hazard_df, iherb_df)
    barcode_matched_indices = {r['index'] for r in barcode_results}
    
    # 텍스트 매칭
    text_results = match_by_text_indexed(hazard_df, brand_index, keyword_index, barcode_matched_indices)
    
    # 결과 병합
    all_results = barcode_results + text_results
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
    
    # 상세 정보
    matched_df = final_df[final_df['product_partno'].notna()]
    if len(matched_df) > 0:
        details = matched_df.merge(
            iherb_df[['product_partno', 'product_name', 'product_brand', 'product_upc']],
            on='product_partno',
            how='left'
        )
        
        details_output = details[[
            '제품명', '제조사명', '유통바코드정보',
            'product_name', 'product_brand', 'product_upc',
            'product_partno', 'product_id',
            'match_method', 'match_score'
        ]].copy()
        
        details_output.columns = [
            '위해식품_제품명', '위해식품_제조사', '위해식품_바코드',
            '아이허브_제품명', '아이허브_브랜드', '아이허브_UPC',
            'product_partno', 'product_id',
            '매칭방법', '매칭스코어'
        ]
        
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
    
    print(f"\n✓ 결과 저장: {output_file}")
    if matched > 0 and details_file:
        print(f"✓ 상세 정보: {details_file}")
    print(f"✓ 아이허브 인덱스 디버그: {INDEX_DEBUG_IHERB_FILE}")
    print(f"✓ 위해식품 인덱스 디버그: {INDEX_DEBUG_HAZARD_FILE}")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main()
