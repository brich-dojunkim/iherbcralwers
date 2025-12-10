import pandas as pd
import re

# 1단계: hazard 파일 로드
print("=== 1단계: hazard 파일 로드 ===")
df_hazard = pd.read_csv('/Users/brich/Desktop/iherb_price/251204/hazard_iherb_matched_final.csv')
print(f"전체 행 수: {len(df_hazard)}")

# 2단계: kr.iherb.com URL만 필터링
print("\n=== 2단계: kr URL 필터링 ===")
df_kr = df_hazard[df_hazard['IHERB_URL'].str.contains('kr.iherb.com', na=False)].copy()
print(f"kr URL 개수: {len(df_kr)}")

# 3단계: URL에서 상품코드 추출
print("\n=== 3단계: 상품코드 추출 ===")
def extract_product_code(url):
    """URL에서 마지막 슬래시 뒤의 숫자 추출"""
    if pd.isna(url):
        return None
    # 마지막 슬래시 뒤의 부분 추출
    parts = url.split('/')
    if len(parts) > 0:
        last_part = parts[-1]
        # 숫자만 추출
        code = ''.join(filter(str.isdigit, last_part))
        return int(code) if code else None
    return None

df_kr['product_code'] = df_kr['IHERB_URL'].apply(extract_product_code)

# None 값 제거
df_kr = df_kr[df_kr['product_code'].notna()].copy()
print(f"상품코드 추출 완료: {len(df_kr)}개")
print(f"샘플 상품코드: {df_kr['product_code'].head().tolist()}")

# 4단계: sample 파일 로드
print("\n=== 4단계: sample 파일 로드 ===")
df_sample = pd.read_excel('/Users/brich/Desktop/iherb_price/251204/iherb_item feed_en.xlsx')
print(f"sample 파일 행 수: {len(df_sample)}")
print(f"sample product_id 범위: {df_sample['product_id'].min()} ~ {df_sample['product_id'].max()}")

# 5단계: 매칭
print("\n=== 5단계: 매칭 수행 ===")
# product_code와 product_id를 기준으로 조인
df_matched = df_kr.merge(
    df_sample[['product_id', 'product_brand', 'product_name']], 
    left_on='product_code', 
    right_on='product_id',
    how='inner'
)

print(f"매칭된 행 수: {len(df_matched)}")

# 6단계: 결과 정리 - 기존 hazard 데이터에 컬럼 추가
print("\n=== 6단계: 결과 정리 ===")

# 중복 처리: product_code별 첫 번째 매칭 결과만 사용
df_matched_unique = df_matched.drop_duplicates(subset=['product_code'], keep='first')
print(f"중복 제거 후 고유 상품코드 수: {len(df_matched_unique)}")

# 매칭 정보를 딕셔너리로 만들기
match_dict = df_matched_unique.set_index('product_code')[['product_brand', 'product_name']].to_dict('index')

# 기존 hazard 데이터에 컬럼 추가
df_kr['IHERB_제조사'] = df_kr['product_code'].map(lambda x: match_dict.get(x, {}).get('product_brand', None))
df_kr['IHERB_상품명'] = df_kr['product_code'].map(lambda x: match_dict.get(x, {}).get('product_name', None))

# 필요한 컬럼만 선택 (기존 컬럼 + 새 컬럼)
result = df_kr[['SELF_IMPORT_SEQ', 'PRDT_NM', 'MUFC_NM', 'MUFC_CNTRY_NM', 
                'INGR_NM_LST', 'CRET_DTM', 'IMAGE_URL_MFDS', 'IHERB_URL', 
                'STATUS', 'product_code', 'IHERB_제조사', 'IHERB_상품명']].copy()

print(f"\n결과 샘플 (처음 5개):")
print(result.head(5).to_string())

# 7단계: 결과 저장
output_path = '/Users/brich/Desktop/iherb_price/251204/hazard_iherb_kr_matched.csv'
result.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n=== 완료 ===")
print(f"결과 파일 저장: {output_path}")
print(f"총 kr URL 상품 수: {len(result)}")
print(f"매칭된 상품 수: {result['IHERB_제조사'].notna().sum()}")

# 매칭 통계
print(f"\n=== 매칭 통계 ===")
print(f"kr URL 총 개수: {len(df_kr)}")
print(f"sample 파일 총 개수: {len(df_sample)}")
print(f"매칭 성공: {len(result)}")
print(f"매칭 실패: {len(df_kr) - len(result)}")
print(f"매칭률: {len(result)/len(df_kr)*100:.2f}%")