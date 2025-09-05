"""
상품 매칭 로직 모듈
"""

import re
import urllib.parse
from difflib import SequenceMatcher
from config import Config


class ProductMatcher:
    """상품 매칭 및 유사도 계산 담당"""
    
    def __init__(self, iherb_client):
        self.iherb_client = iherb_client
    
    def calculate_enhanced_similarity(self, korean_name, english_name, iherb_name):
        """한글명과 영어명 모두 고려한 유사도 계산"""
        
        # 영어명 기반 유사도 (주요)
        english_similarity = SequenceMatcher(
            None, 
            english_name.lower(), 
            iherb_name.lower()
        ).ratio()
        
        # 한글명 기반 유사도 (보조)
        korean_similarity = SequenceMatcher(
            None, 
            korean_name.lower(), 
            iherb_name.lower()
        ).ratio()
        
        # 브랜드 매칭 확인
        brand_similarity = 0.0
        for brand in Config.COMMON_BRANDS:
            if brand in english_name.lower() and brand in iherb_name.lower():
                brand_similarity = 1.0
                break
            if brand in korean_name.lower() and brand in iherb_name.lower():
                brand_similarity = 0.8
                break
        
        # 개수 정확 매칭
        exact_count_match = self._check_count_match(english_name, iherb_name)
        if exact_count_match is False:
            return 0.0, exact_count_match  # 개수 불일치 시 즉시 반환
        
        # mg 단위 매칭
        dosage_match = self._check_dosage_match(english_name, iherb_name)
        if dosage_match is False:
            return 0.0, dosage_match  # 용량 불일치 시 즉시 반환
        
        # 제형 체크
        form_penalty = self._calculate_form_penalty(english_name, iherb_name)
        
        # 가중 평균 계산 (설정에서 가져온 가중치 사용)
        weights = Config.SIMILARITY_WEIGHTS
        final_score = (
            english_similarity * weights['english'] + 
            korean_similarity * weights['korean'] + 
            brand_similarity * weights['brand']
        ) - form_penalty
        
        # 최소 임계값 설정
        if final_score < Config.MATCHING_THRESHOLDS['min_similarity']:
            return final_score * 0.5, {
                'reason': 'low_similarity', 
                'score': final_score,
                'english_similarity': english_similarity,
                'korean_similarity': korean_similarity
            }
        
        return final_score, {
            'english_similarity': english_similarity,
            'korean_similarity': korean_similarity,
            'brand_similarity': brand_similarity,
            'exact_count_match': exact_count_match,
            'form_penalty': form_penalty
        }
    
    def _check_count_match(self, english_name, iherb_name):
        """개수 매칭 확인"""
        english_count = re.search(Config.PATTERNS['english_count'], english_name.lower())
        iherb_count = re.search(Config.PATTERNS['english_count'], iherb_name.lower())
        
        if english_count and iherb_count:
            e_count = int(english_count.group(1))
            i_count = int(iherb_count.group(1))
            
            if e_count == i_count:
                return True
            else:
                return {
                    'reason': 'count_mismatch', 
                    'english_count': e_count, 
                    'iherb_count': i_count
                }
        
        return None  # 개수 정보 없음
    
    def _check_dosage_match(self, english_name, iherb_name):
        """용량 매칭 확인"""
        english_mg = re.search(Config.PATTERNS['dosage_mg'], english_name.lower())
        iherb_mg = re.search(Config.PATTERNS['dosage_mg'], iherb_name.lower())
        
        if english_mg and iherb_mg:
            e_mg = int(english_mg.group(1).replace(',', ''))
            i_mg = int(iherb_mg.group(1).replace(',', ''))
            
            if e_mg != i_mg:
                return {
                    'reason': 'dosage_mismatch', 
                    'english_mg': e_mg, 
                    'iherb_mg': i_mg
                }
        
        return None  # 용량 정보 없음 또는 일치
    
    def _calculate_form_penalty(self, english_name, iherb_name):
        """제형 불일치 페널티 계산"""
        english_forms = []
        iherb_forms = []
        
        for form_type, variations in Config.FORM_MAPPING.items():
            for variation in variations:
                if variation in english_name.lower():
                    english_forms.append(form_type)
                if variation in iherb_name.lower():
                    iherb_forms.append(form_type)
        
        if english_forms and iherb_forms:
            if not any(form in iherb_forms for form in english_forms):
                return 0.2  # 제형 불일치 페널티
        
        return 0
    
    def find_best_matching_product(self, korean_name, english_name, iherb_products):
        """최적 매칭 상품 찾기 - 영어명 기준"""
        print("  매칭 분석 중...")
        
        best_match = None
        best_score = 0
        best_details = None
        
        for idx, product in enumerate(iherb_products):
            score, details = self.calculate_enhanced_similarity(
                korean_name, english_name, product['title']
            )
            
            status = "매칭 가능" if score >= Config.MATCHING_THRESHOLDS['success_threshold'] else "매칭 불가"
            reason = self._get_matching_reason(score, details)
            
            print(f"    후보 {idx+1}: {status} ({score:.3f}) - {product['title'][:60]}...")
            if reason:
                print(f"      └ {reason}")
            
            if score > best_score:
                best_score = score
                best_match = product
                best_details = details
        
        print()
        if best_match and best_score >= Config.MATCHING_THRESHOLDS['success_threshold']:
            print(f"  최종 매칭: 성공 ({best_score:.3f})")
            print(f"    선택된 상품: {best_match['title'][:60]}...")
        else:
            print(f"  최종 매칭: 실패 (최고점수: {best_score:.3f})")
            if best_match:
                print(f"    가장 유사한 상품: {best_match['title'][:60]}...")
        
        return best_match, best_score, best_details
    
    def _get_matching_reason(self, score, details):
        """매칭 사유 문자열 생성"""
        if score < Config.MATCHING_THRESHOLDS['success_threshold']:
            if isinstance(details, dict) and 'reason' in details:
                if details['reason'] == 'count_mismatch':
                    return f"개수 불일치: 영어명 {details['english_count']}개 ≠ 아이허브 {details['iherb_count']}개"
                elif details['reason'] == 'dosage_mismatch':
                    return f"용량 불일치: 영어명 {details['english_mg']}mg ≠ 아이허브 {details['iherb_mg']}mg"
                elif details['reason'] == 'low_similarity':
                    return f"낮은 유사도 (영어:{details.get('english_similarity', 0):.2f}, 한글:{details.get('korean_similarity', 0):.2f})"
            else:
                return "제형 불일치" if isinstance(details, dict) and details.get('form_penalty', 0) > 0 else ""
        else:
            if isinstance(details, dict):
                if details.get('exact_count_match'):
                    return "개수/용량 정확 매칭"
                else:
                    return f"높은 유사도 (영어:{details.get('english_similarity', 0):.2f})"
        
        return ""
    
    def search_product_enhanced(self, korean_name, english_name):
        """영어 상품명으로 검색 (한글명은 로깅용)"""
        try:
            # 영어 이름 정리
            cleaned_english_name = re.sub(r'[^\w\s]', ' ', english_name)
            cleaned_english_name = re.sub(r'\s+', ' ', cleaned_english_name).strip()
            
            print(f"  검색어: {english_name[:50]}...")
            print(f"  원본 한글: {korean_name[:40]}...")
            
            search_url = f"{Config.BASE_URL}/search?kw={urllib.parse.quote(cleaned_english_name)}"
            
            products = self.iherb_client.get_multiple_products(search_url)
            
            if not products:
                return None, 0, None
            
            best_product, similarity_score, match_details = self.find_best_matching_product(
                korean_name, english_name, products
            )
            
            if best_product and similarity_score >= Config.MATCHING_THRESHOLDS['success_threshold']:
                return best_product['url'], similarity_score, match_details
            else:
                if products:
                    return products[0]['url'], similarity_score, match_details
                return None, 0, None
            
        except Exception as e:
            print(f"    검색 중 오류: {e}")
            return None, 0, None