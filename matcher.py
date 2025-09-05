import pandas as pd
import numpy as np
import re
from difflib import SequenceMatcher
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

class ImprovedProductMatcher:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(
            analyzer='word',
            token_pattern=r'\b\w+\b',
            lowercase=True,
            max_features=1000,
            ngram_range=(1, 2)
        )
    
    def normalize_text(self, text):
        """텍스트 정규화"""
        if pd.isna(text):
            return ""
        
        text = str(text).lower()
        # 브랜드명 제거
        text = re.sub(r'나우푸드|now foods?|now\b', '', text, flags=re.IGNORECASE)
        # 특수문자 처리
        text = re.sub(r'[,\-\(\)\[\]"]', ' ', text)
        # 연속 공백 제거
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def extract_core_features(self, text):
        """핵심 특징 추출 - 확장된 패턴"""
        normalized = self.normalize_text(text)
        
        features = {
            'normalized_text': normalized,
            'main_ingredient': None,
            'dosage_amount': None,
            'dosage_unit': None,
            'count': None,
            'form_type': None,
            'raw_text': text
        }
        
        # 주성분 추출 (대폭 확장)
        ingredient_patterns = [
            # 아미노산류
            (r'l[\s\-]*아르기닌|아르기닌(?!\s*산)|arginine', 'arginine'),
            (r'l[\s\-]*글루타민|글루타민|glutamine', 'glutamine'),
            (r'l[\s\-]*시스테인|시스테인|cysteine', 'cysteine'),
            (r'l[\s\-]*타이로신|타이로신|tyrosine', 'tyrosine'),
            (r'분지사슬아미노산|bcaa|branched.*chain', 'bcaa'),
            (r'아미노\s*컴플리트|amino\s*complete', 'amino_complete'),
            (r'아미노\s*산|amino\s*acid', 'amino_acid'),
            
            # 비타민류
            (r'비타민\s*d3?|vitamin\s*d', 'vitamin_d'),
            (r'비타민\s*c|vitamin\s*c', 'vitamin_c'),
            (r'비타민\s*b\d*|vitamin\s*b', 'vitamin_b'),
            (r'비타민\s*e|vitamin\s*e', 'vitamin_e'),
            (r'비타민\s*k2?|vitamin\s*k', 'vitamin_k'),
            (r'비타민\s*a|vitamin\s*a', 'vitamin_a'),
            (r'멀티비타민|multivitamin', 'multivitamin'),
            (r'비오틴|biotin', 'biotin'),
            (r'엽산|folate|folic', 'folate'),
            
            # 오메가 지방산
            (r'오메가[\s\-]*3|omega[\s\-]*3', 'omega3'),
            (r'오메가[\s\-]*6|omega[\s\-]*6', 'omega6'),
            (r'오메가[\s\-]*9|omega[\s\-]*9', 'omega9'),
            (r'dha|epa', 'fish_oil'),
            
            # 미네랄류
            (r'마그네슘|magnesium', 'magnesium'),
            (r'아연(?!\s*\d)|zinc', 'zinc'),
            (r'칼슘|calcium', 'calcium'),
            (r'철분|iron', 'iron'),
            (r'칼륨|potassium', 'potassium'),
            (r'셀레늄|selenium', 'selenium'),
            (r'크롬|chromium', 'chromium'),
            
            # 허브/식물추출물
            (r'글루타티온|glutathione', 'glutathione'),
            (r'실리마린|밀크\s*시슬|silymarin', 'silymarin'),
            (r'코엔자임\s*q10|coq10|ubiquinone', 'coq10'),
            (r'글루코사민|glucosamine', 'glucosamine'),
            (r'콘드로이틴|chondroitin', 'chondroitin'),
            (r'콜라겐|collagen', 'collagen'),
            (r'루테인|lutein', 'lutein'),
            (r'리코펜|lycopene', 'lycopene'),
            (r'레스베라트롤|resveratrol', 'resveratrol'),
            (r'커큐민|curcumin|turmeric', 'curcumin'),
            (r'스피룰리나|spirulina', 'spirulina'),
            (r'클로렐라|chlorella', 'chlorella'),
            
            # 프로바이오틱/효소
            (r'프로바이오틱|유산균|probiotic', 'probiotic'),
            (r'프리바이오틱|prebiotic', 'prebiotic'),
            (r'효소|enzyme|digestive', 'enzyme'),
            
            # 스포츠 영양
            (r'크레아틴|creatine', 'creatine'),
            (r'프로틴|protein|단백질', 'protein'),
            (r'웨이|whey', 'whey_protein'),
            (r'카제인|casein', 'casein'),
            
            # 기타
            (r'멜라토닌|melatonin', 'melatonin'),
            (r'파이버|섬유질|fiber', 'fiber'),
            (r'레시틴|lecithin', 'lecithin')
        ]
        
        for pattern, ingredient in ingredient_patterns:
            if re.search(pattern, normalized, re.IGNORECASE):
                features['main_ingredient'] = ingredient
                break
        
        # 용량 추출 (다양한 패턴 지원)
        dosage_patterns = [
            r'(\d+(?:[,\s]\d{3})*)\s*(mg|g|iu|mcg|억)\b',
            r'(\d+)\s*x\s*(\d+)\s*(mg|g|iu|mcg)',  # 2 x 500mg 같은 패턴
            r'(\d+)\s*(mg|g|iu|mcg)\s*x\s*(\d+)'   # 500mg x 2 같은 패턴
        ]
        
        for pattern in dosage_patterns:
            dosage_match = re.search(pattern, normalized, re.IGNORECASE)
            if dosage_match:
                if len(dosage_match.groups()) == 2:  # 일반적인 패턴
                    amount_str = dosage_match.group(1).replace(',', '').replace(' ', '')
                    features['dosage_amount'] = int(amount_str)
                    features['dosage_unit'] = dosage_match.group(2).lower()
                elif len(dosage_match.groups()) == 3:  # x가 포함된 패턴
                    # 첫 번째와 두 번째 숫자를 곱함
                    num1 = int(dosage_match.group(1))
                    num2 = int(dosage_match.group(2))
                    features['dosage_amount'] = num1 * num2
                    features['dosage_unit'] = dosage_match.group(3).lower()
                break
        
        # 정수 추출
        count_patterns = [
            r'(\d+)정\b',
            r'(\d+)\s*capsule',
            r'(\d+)\s*tablet',
            r'(\d+)\s*softgel',
            r'(\d+)\s*count'
        ]
        
        for pattern in count_patterns:
            count_match = re.search(pattern, normalized, re.IGNORECASE)
            if count_match:
                features['count'] = int(count_match.group(1))
                break
        
        # 제형 추출
        form_patterns = [
            (r'캡슐|capsule', 'capsule'),
            (r'타블렛|tablet', 'tablet'),
            (r'소프트젤|softgel', 'softgel'),
            (r'파우더|powder|분말', 'powder'),
            (r'액상|liquid|drops', 'liquid'),
            (r'크림|cream', 'cream'),
            (r'스프레이|spray', 'spray'),
            (r'젤리|gummy', 'gummy')
        ]
        
        for pattern, form_type in form_patterns:
            if re.search(pattern, normalized, re.IGNORECASE):
                features['form_type'] = form_type
                break
        
        return features
    
    def is_compatible(self, features1, features2):
        """호환성 검사 (Hard Filter보다 유연)"""
        
        # 1. 주성분 반드시 일치
        if not features1['main_ingredient'] or not features2['main_ingredient']:
            return False
        if features1['main_ingredient'] != features2['main_ingredient']:
            return False
        
        # 2. 용량 검사 (더 유연하게)
        if features1['dosage_amount'] and features2['dosage_amount']:
            # 단위가 다르면 실패
            if features1['dosage_unit'] != features2['dosage_unit']:
                return False
            # 용량이 정확히 일치하거나 비슷한 범위 (±10% 허용)
            ratio = min(features1['dosage_amount'], features2['dosage_amount']) / max(features1['dosage_amount'], features2['dosage_amount'])
            if ratio < 0.9:  # 10% 이상 차이나면 다른 상품으로 간주
                return False
        
        # 3. 정수 검사 (엄격)
        if features1['count'] and features2['count']:
            if features1['count'] != features2['count']:
                return False
        
        return True
    
    def calculate_text_similarity(self, text1, text2):
        """텍스트 유사도 계산"""
        seq_sim = SequenceMatcher(None, text1, text2).ratio()
        
        words1 = set(text1.split())
        words2 = set(text2.split())
        if len(words1.union(words2)) > 0:
            jaccard_sim = len(words1.intersection(words2)) / len(words1.union(words2))
        else:
            jaccard_sim = 0
        
        return (seq_sim + jaccard_sim) / 2
    
    def analyze_dataset_compatibility(self, coupang_df, iherb_df):
        """데이터셋 호환성 분석"""
        print("=== 데이터셋 분석 중 ===")
        
        # 아이허브 성분 분석
        iherb_ingredients = []
        iherb_no_ingredient = []
        
        for idx, row in iherb_df.iterrows():
            features = self.extract_core_features(row['상품명'])
            if features['main_ingredient']:
                iherb_ingredients.append(features['main_ingredient'])
            else:
                iherb_no_ingredient.append(row['상품명'])
        
        # 쿠팡 성분 분석
        coupang_ingredients = []
        coupang_no_ingredient = []
        
        for idx, row in coupang_df.iterrows():
            features = self.extract_core_features(row['product_name'])
            if features['main_ingredient']:
                coupang_ingredients.append(features['main_ingredient'])
            else:
                coupang_no_ingredient.append(row['product_name'])
        
        # 통계
        iherb_counts = Counter(iherb_ingredients)
        coupang_counts = Counter(coupang_ingredients)
        
        print(f"아이허브 주성분 추출 성공: {len(iherb_ingredients)}/{len(iherb_df)} ({len(iherb_ingredients)/len(iherb_df)*100:.1f}%)")
        print(f"쿠팡 주성분 추출 성공: {len(coupang_ingredients)}/{len(coupang_df)} ({len(coupang_ingredients)/len(coupang_df)*100:.1f}%)")
        
        # 공통 성분
        common_ingredients = set(iherb_counts.keys()).intersection(set(coupang_counts.keys()))
        
        print(f"\n공통 주성분: {len(common_ingredients)}개")
        print("상위 공통 성분:")
        common_summary = []
        for ingredient in sorted(common_ingredients):
            iherb_count = iherb_counts[ingredient]
            coupang_count = coupang_counts[ingredient]
            common_summary.append((ingredient, iherb_count, coupang_count))
            
        # 매칭 가능량이 많은 순으로 정렬
        common_summary.sort(key=lambda x: min(x[1], x[2]), reverse=True)
        
        for ingredient, iherb_count, coupang_count in common_summary[:15]:
            potential = min(iherb_count, coupang_count)
            print(f"  {ingredient}: 아이허브 {iherb_count}개, 쿠팡 {coupang_count}개 → 잠재 매칭 {potential}개")
        
        # 추출 실패 샘플
        print(f"\n주성분 추출 실패 샘플 (아이허브 {len(iherb_no_ingredient)}개 중 10개):")
        for sample in iherb_no_ingredient[:10]:
            print(f"  - {sample}")
        
        return {
            'common_ingredients': common_ingredients,
            'iherb_counts': iherb_counts,
            'coupang_counts': coupang_counts,
            'potential_matches': sum(min(iherb_counts.get(ing, 0), coupang_counts.get(ing, 0)) for ing in common_ingredients)
        }
    
    def find_matches(self, coupang_df, iherb_df, min_similarity=0.3):
        """매칭 수행"""
        
        # 먼저 데이터셋 분석
        analysis = self.analyze_dataset_compatibility(coupang_df, iherb_df)
        
        print(f"\n=== 매칭 시작 ===")
        print(f"이론적 최대 매칭 가능: {analysis['potential_matches']}개")
        
        # 쿠팡 상품들을 주성분별로 그룹화 (성능 향상)
        coupang_by_ingredient = {}
        
        for idx, row in coupang_df.iterrows():
            features = self.extract_core_features(row['product_name'])
            if features['main_ingredient']:
                ingredient = features['main_ingredient']
                if ingredient not in coupang_by_ingredient:
                    coupang_by_ingredient[ingredient] = []
                coupang_by_ingredient[ingredient].append({
                    'index': idx,
                    'row': row,
                    'features': features
                })
        
        matches = []
        unmatched = []
        
        for i, iherb_row in iherb_df.iterrows():
            iherb_features = self.extract_core_features(iherb_row['상품명'])
            
            # 진행상황 출력
            if i % 100 == 0:
                print(f"진행상황: {i}/{len(iherb_df)}")
            
            # 주성분이 없으면 스킵
            if not iherb_features['main_ingredient']:
                unmatched.append({
                    'iherb_seller_code': iherb_row['판매자상품코드'],
                    'iherb_product_name': iherb_row['상품명'],
                    'reason': 'No main ingredient extracted'
                })
                continue
            
            # 같은 주성분을 가진 쿠팡 상품들만 검토
            ingredient = iherb_features['main_ingredient']
            if ingredient not in coupang_by_ingredient:
                unmatched.append({
                    'iherb_seller_code': iherb_row['판매자상품코드'],
                    'iherb_product_name': iherb_row['상품명'],
                    'reason': f'No coupang products with {ingredient}'
                })
                continue
            
            # 호환 가능한 후보들 찾기
            candidates = []
            for coupang_item in coupang_by_ingredient[ingredient]:
                if self.is_compatible(iherb_features, coupang_item['features']):
                    similarity = self.calculate_text_similarity(
                        iherb_features['normalized_text'],
                        coupang_item['features']['normalized_text']
                    )
                    candidates.append((coupang_item, similarity))
            
            if not candidates:
                unmatched.append({
                    'iherb_seller_code': iherb_row['판매자상품코드'],
                    'iherb_product_name': iherb_row['상품명'],
                    'reason': f'No compatible candidates (ingredient: {ingredient})'
                })
                continue
            
            # 가장 유사한 후보 선택
            best_candidate, best_similarity = max(candidates, key=lambda x: x[1])
            
            if best_similarity >= min_similarity:
                match_data = {
                    # 아이허브 정보
                    'iherb_seller_code': iherb_row['판매자상품코드'],
                    'iherb_product_name': iherb_row['상품명'],
                    'iherb_seller_product_id': iherb_row['(CA) 셀러 상품번호'],
                    'iherb_regular_price': iherb_row['정상가'],
                    'iherb_stock': iherb_row['재고'],
                    
                    # 쿠팡 정보
                    'coupang_product_id': best_candidate['row']['product_id'],
                    'coupang_product_name': best_candidate['row']['product_name'],
                    'coupang_product_url': best_candidate['row']['product_url'],
                    'coupang_current_price': best_candidate['row']['current_price'],
                    'coupang_original_price': best_candidate['row']['original_price'],
                    'coupang_discount_rate': best_candidate['row']['discount_rate'],
                    'coupang_rating': best_candidate['row']['rating'],
                    'coupang_review_count': best_candidate['row']['review_count'],
                    'coupang_delivery_badge': best_candidate['row']['delivery_badge'],
                    'coupang_is_rocket': best_candidate['row']['is_rocket'],
                    'coupang_crawled_at': best_candidate['row']['crawled_at'],
                    
                    # 매칭 정보
                    'main_ingredient': ingredient,
                    'text_similarity': round(best_similarity, 4),
                    'candidates_count': len(candidates),
                    'match_confidence': 'HIGH' if best_similarity >= 0.7 else 'MEDIUM' if best_similarity >= 0.5 else 'LOW'
                }
                
                matches.append(match_data)
            else:
                unmatched.append({
                    'iherb_seller_code': iherb_row['판매자상품코드'],
                    'iherb_product_name': iherb_row['상품명'],
                    'reason': f'Low similarity {best_similarity:.3f} (ingredient: {ingredient})'
                })
        
        return matches, unmatched

def main():
    """메인 실행 함수"""
    
    # 데이터 로드
    print("CSV 파일 로드 중...")
    try:
        coupang_df = pd.read_csv('coupang_products_20250903_120440.csv')
        iherb_df = pd.read_csv('iherb_beflow_nowfood.csv')
        
        print(f"데이터 로드 완료:")
        print(f"- 쿠팡: {len(coupang_df)}개 상품")
        print(f"- 아이허브: {len(iherb_df)}개 상품")
        
    except Exception as e:
        print(f"파일 로드 오류: {e}")
        return
    
    # 매칭 실행
    matcher = ImprovedProductMatcher()
    matches, unmatched = matcher.find_matches(coupang_df, iherb_df)
    
    # 결과 저장
    if matches:
        matches_df = pd.DataFrame(matches)
        matches_df.to_csv('improved_matched_products.csv', index=False, encoding='utf-8-sig')
        print(f"\n매칭 결과를 'improved_matched_products.csv'에 저장했습니다.")
    
    if unmatched:
        unmatched_df = pd.DataFrame(unmatched)
        unmatched_df.to_csv('improved_unmatched_products.csv', index=False, encoding='utf-8-sig')
        print(f"매칭되지 않은 상품을 'improved_unmatched_products.csv'에 저장했습니다.")
    
    # 통계 출력
    print(f"\n=== 개선된 매칭 결과 ===")
    print(f"전체 아이허브 상품: {len(iherb_df)}개")
    print(f"매칭 성공: {len(matches)}개")
    print(f"매칭 실패: {len(unmatched)}개")
    print(f"실제 매칭률: {len(matches)/len(iherb_df)*100:.1f}%")
    
    if matches:
        matches_df = pd.DataFrame(matches)
        
        # 신뢰도별 분포
        confidence_counts = matches_df['match_confidence'].value_counts()
        print(f"\n신뢰도 분포:")
        for conf, count in confidence_counts.items():
            print(f"- {conf}: {count}개")
        
        # 주성분별 매칭 분포
        ingredient_counts = matches_df['main_ingredient'].value_counts()
        print(f"\n주성분별 매칭 (상위 10개):")
        for ingredient, count in ingredient_counts.head(10).items():
            print(f"- {ingredient}: {count}개")
        
        # 평균 유사도
        avg_similarity = matches_df['text_similarity'].mean()
        print(f"\n평균 텍스트 유사도: {avg_similarity:.3f}")
        
        print(f"\n=== 매칭 예시 (상위 10개) ===")
        top_matches = matches_df.nlargest(10, 'text_similarity')
        for idx, row in top_matches.iterrows():
            print(f"[{row['match_confidence']}] {row['iherb_product_name']}")
            print(f"         ↔ {row['coupang_product_name']}")
            print(f"         (성분: {row['main_ingredient']}, 유사도: {row['text_similarity']:.3f})")
            print()
    
    # 매칭 실패 사유 분석
    if unmatched:
        unmatched_df = pd.DataFrame(unmatched)
        failure_reasons = unmatched_df['reason'].value_counts()
        print(f"매칭 실패 사유:")
        for reason, count in failure_reasons.items():
            print(f"- {reason}: {count}개")

if __name__ == "__main__":
    main()