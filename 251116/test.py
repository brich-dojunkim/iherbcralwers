import pandas as pd
import re

INPUT_HAZARD_FILE = '/Users/brich/Desktop/iherb_price/251116/2025.11.18_해외직구+위해식품+목록.xls'
INPUT_IHERB_FILE = '/Users/brich/Desktop/iherb_price/251116/iherb_item feed_en.xlsx'

def normalize_text(text):
    """브랜드명 정규화 (매칭 스크립트와 동일)"""
    if pd.isna(text):
        return ""
    text = str(text).lower().strip()
    
    # 한국어 접미사 제거
    text = re.sub(r'\(유통사\)', '', text)
    text = re.sub(r'\(제조사\)', '', text)
    text = re.sub(r'유통사$', '', text)
    text = re.sub(r'제조사$', '', text)
    
    # 법인 접미사 제거
    text = re.sub(r'\bllc\b', '', text)
    text = re.sub(r'\binc\.?\b', '', text)
    text = re.sub(r'\bcorp\.?\b', '', text)
    text = re.sub(r'\bltd\.?\b', '', text)
    text = re.sub(r'\bco\.?\b', '', text)
    
    # 괄호 내용 제거
    text = re.sub(r'\([^)]*\)', '', text)
    text = re.sub(r'\[[^\]]*\]', '', text)
    
    # 특수문자 제거
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

print("파일 읽는 중...")
hazard_df = pd.read_excel(INPUT_HAZARD_FILE)
iherb_df = pd.read_excel(INPUT_IHERB_FILE)

print("\n=== 위해식품 컬럼 ===")
print(hazard_df.columns.tolist())

print("\n=== 아이허브 컬럼 ===")
print(iherb_df.columns.tolist())

print("\n=== 브랜드 분석 ===")
print(f"위해식품 브랜드 컬럼명: {[col for col in hazard_df.columns if '제조' in col or '브랜드' in col]}")
print(f"아이허브 브랜드 컬럼명: {[col for col in iherb_df.columns if 'brand' in col.lower()]}")

# 위해식품 브랜드
hazard_brand_col = None
for col in hazard_df.columns:
    if '제조사' in col or '브랜드' in col:
        hazard_brand_col = col
        break

if hazard_brand_col:
    print(f"\n위해식품 브랜드 있음: {hazard_df[hazard_brand_col].notna().sum()}건")
    print(f"위해식품 브랜드 없음: {hazard_df[hazard_brand_col].isna().sum()}건")
    print(f"샘플 브랜드:")
    for brand in hazard_df[hazard_brand_col].dropna().head(5):
        print(f"  원본: {brand}")
        print(f"  정규화: {normalize_text(brand)}")

# 아이허브 브랜드
iherb_brand_col = None
for col in iherb_df.columns:
    if 'brand' in col.lower():
        iherb_brand_col = col
        break

if iherb_brand_col:
    print(f"\n아이허브 브랜드 있음: {iherb_df[iherb_brand_col].notna().sum()}건")
    print(f"아이허브 고유 브랜드 수: {iherb_df[iherb_brand_col].nunique()}건")
    print(f"샘플 브랜드:")
    for brand in iherb_df[iherb_brand_col].dropna().head(5):
        print(f"  원본: {brand}")
        print(f"  정규화: {normalize_text(brand)}")

# 브랜드 매칭 테스트
if hazard_brand_col and iherb_brand_col:
    print("\n=== 브랜드 매칭 테스트 ===")
    hazard_brands = set(hazard_df[hazard_brand_col].dropna().apply(normalize_text))
    iherb_brands = set(iherb_df[iherb_brand_col].dropna().apply(normalize_text))
    
    common = hazard_brands & iherb_brands
    print(f"공통 브랜드: {len(common)}개 / {len(hazard_brands)}개")
    print(f"매칭률: {len(common)/len(hazard_brands)*100:.1f}%")
    
    if common:
        print(f"\n공통 브랜드 예시 (10개):")
        for brand in sorted(list(common))[:10]:
            # 각 브랜드별 제품 수
            h_count = (hazard_df[hazard_brand_col].apply(normalize_text) == brand).sum()
            i_count = (iherb_df[iherb_brand_col].apply(normalize_text) == brand).sum()
            print(f"  {brand}: 위해식품 {h_count}건, 아이허브 {i_count}건")
    
    # 매칭 안 되는 브랜드 샘플
    unmatched = hazard_brands - iherb_brands
    if unmatched:
        print(f"\n매칭 안 되는 위해식품 브랜드 (상위 10개):")
        unmatched_with_count = []
        for brand in unmatched:
            count = (hazard_df[hazard_brand_col].apply(normalize_text) == brand).sum()
            unmatched_with_count.append((brand, count))
        unmatched_with_count.sort(key=lambda x: x[1], reverse=True)
        
        for brand, count in unmatched_with_count[:10]:
            print(f"  {brand}: {count}건")

print("\n=== 예상 성능 ===")
# 브랜드별 아이허브 제품 수 분포
if iherb_brand_col:
    brand_counts = iherb_df[iherb_brand_col].apply(normalize_text).value_counts()
    print(f"브랜드당 평균 아이허브 제품 수: {brand_counts.mean():.1f}건")
    print(f"브랜드당 중간값: {brand_counts.median():.1f}건")
    print(f"브랜드당 최대: {brand_counts.max():.0f}건")
    
    # 예상 처리 시간
    avg_candidates = brand_counts.mean()
    total_comparisons = len(hazard_df) * min(avg_candidates, 200)  # MAX_CANDIDATES=200
    items_per_sec = 50  # 보수적 추정
    estimated_time = total_comparisons / items_per_sec / 60
    
    print(f"\n예상 비교 횟수: {total_comparisons:,.0f}회")
    print(f"예상 처리 시간: {estimated_time:.1f}분")