"""
상품 매칭 로직 모듈 - 실패 분류 시스템 지원
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
        """영어명만 고려한 유사도 계산 (용량/개수 불일치 시 완전 탈락)"""
        
        # 1. 개수 정확 매칭 체크 - 불일치 시 즉시 탈락
        count_match_result = self._check_count_match(english_name, iherb_name)
        if count_match_result is False:
            return 0.0, {
                'reason': 'count_mismatch',
                'rejected': True,
                'details': count_match_result
            }
        
        # 2. mg 단위 매칭 체크 - 불일치 시 즉시 탈락  
        dosage_match_result = self._check_dosage_match(english_name, iherb_name)
        if dosage_match_result is False:
            return 0.0, {
                'reason': 'dosage_mismatch', 
                'rejected': True,
                'details': dosage_match_result
            }
        
        # 3. 영어명 기반 유사도만 계산
        english_similarity = SequenceMatcher(
            None, 
            english_name.lower(), 
            iherb_name.lower()
        ).ratio()
        
        # 4. 브랜드 매칭 보너스
        brand_bonus = 0.0
        for brand in Config.COMMON_BRANDS:
            if brand in english_name.lower() and brand in iherb_name.lower():
                brand_bonus = 0.1
                break
        
        # 5. 제형 불일치 페널티
        form_penalty = self._calculate_form_penalty(english_name, iherb_name)
        
        # 6. 최종 점수 = 영어 유사도 + 브랜드 보너스 - 제형 페널티
        final_score = english_similarity + brand_bonus - form_penalty
        
        # 7. 최소 임계값 미달 시 낮은 점수 반환
        if final_score < Config.MATCHING_THRESHOLDS['min_similarity']:
            return final_score * 0.5, {
                'reason': 'low_similarity', 
                'english_similarity': english_similarity,
                'brand_bonus': brand_bonus,
                'form_penalty': form_penalty,
                'final_score': final_score
            }
        
        return final_score, {
            'english_similarity': english_similarity,
            'brand_bonus': brand_bonus,
            'form_penalty': form_penalty,
            'exact_count_match': count_match_result,
            'dosage_match': dosage_match_result
        }
    
    def _check_count_match(self, english_name, iherb_name):
        """개수 매칭 확인 - 불일치 시 False 반환"""
        english_count = re.search(Config.PATTERNS['english_count'], english_name.lower())
        iherb_count = re.search(Config.PATTERNS['english_count'], iherb_name.lower())
        
        if english_count and iherb_count:
            e_count = int(english_count.group(1))
            i_count = int(iherb_count.group(1))
            
            if e_count == i_count:
                return True  # 정확 매칭
            else:
                return False  # 불일치 - 탈락
        
        return None  # 개수 정보 없음 - 통과
    
    def _check_dosage_match(self, english_name, iherb_name):
        """용량 매칭 확인 - 불일치 시 False 반환"""
        english_mg = re.search(Config.PATTERNS['dosage_mg'], english_name.lower())
        iherb_mg = re.search(Config.PATTERNS['dosage_mg'], iherb_name.lower())
        
        if english_mg and iherb_mg:
            e_mg = int(english_mg.group(1).replace(',', ''))
            i_mg = int(iherb_mg.group(1).replace(',', ''))
            
            if e_mg != i_mg:
                return False  # 불일치 - 탈락
            else:
                return True  # 정확 매칭
        
        return None  # 용량 정보 없음 - 통과
    
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
        """최적 매칭 상품 찾기 - 영어명 기준 + 엄격한 필터링"""
        print("  매칭 분석 중...")
        
        best_match = None
        best_score = 0
        best_details = None
        
        for idx, product in enumerate(iherb_products):
            score, details = self.calculate_enhanced_similarity(
                korean_name, english_name, product['title']
            )
            
            # 탈락 여부 확인
            is_rejected = details.get('rejected', False)
            status = "매칭 불가" if (score < Config.MATCHING_THRESHOLDS['success_threshold'] or is_rejected) else "매칭 가능"
            reason = self._get_matching_reason(score, details)
            
            # 상품명 전체 표시 (줄이지 않음)
            print(f"    후보 {idx+1}: {status} ({score:.3f}) - {product['title']}")
            if reason:
                print(f"      └ {reason}")
            
            # 탈락된 상품은 best_match 후보에서 제외
            if not is_rejected and score > best_score:
                best_score = score
                best_match = product
                best_details = details
        
        print()
        if best_match and best_score >= Config.MATCHING_THRESHOLDS['success_threshold']:
            print(f"  최종 매칭: 성공 ({best_score:.3f})")
            # 최종 선택된 상품명도 전체 표시
            print(f"    선택된 상품: {best_match['title']}")
        else:
            print(f"  최종 매칭: 실패 (최고점수: {best_score:.3f})")
            if best_match:
                # 가장 유사한 상품명도 전체 표시
                print(f"    가장 유사한 상품: {best_match['title']}")
        
        return best_match, best_score, best_details
    
    def _get_matching_reason(self, score, details):
        """매칭 사유 문자열 생성"""
        if details.get('rejected', False):
            if details['reason'] == 'count_mismatch':
                return "개수 불일치로 탈락"
            elif details['reason'] == 'dosage_mismatch':
                return "용량(mg) 불일치로 탈락"
        
        if score < Config.MATCHING_THRESHOLDS['success_threshold']:
            if details['reason'] == 'low_similarity':
                return f"낮은 유사도 (영어:{details.get('english_similarity', 0):.2f})"
            else:
                return f"임계값 미달 (영어:{details.get('english_similarity', 0):.2f})"
        else:
            if details.get('exact_count_match') and details.get('dosage_match'):
                return "개수/용량 정확 매칭"
            elif details.get('exact_count_match'):
                return "개수 정확 매칭"
            elif details.get('dosage_match'):
                return "용량 정확 매칭"
            else:
                return f"높은 유사도 (영어:{details.get('english_similarity', 0):.2f})"
        
        return ""
    
    def search_product_enhanced(self, korean_name, english_name):
        """영어 상품명으로 검색 (한글명은 로깅용) - 실패 분류 지원"""
        try:
            # 영어 이름 정리
            cleaned_english_name = re.sub(r'[^\w\s]', ' ', english_name)
            cleaned_english_name = re.sub(r'\s+', ' ', cleaned_english_name).strip()
            
            print(f"  검색어: {english_name}")
            print(f"  원본 한글: {korean_name}")
            
            search_url = f"{Config.BASE_URL}/search?kw={urllib.parse.quote(cleaned_english_name)}"
            
            products = self.iherb_client.get_multiple_products(search_url)
            
            if not products:
                # 검색 결과가 없을 때 no_results 정보 추가
                return None, 0, {'no_results': True}
            
            best_product, similarity_score, match_details = self.find_best_matching_product(
                korean_name, english_name, products
            )
            
            if best_product and similarity_score >= Config.MATCHING_THRESHOLDS['success_threshold']:
                return best_product['url'], similarity_score, match_details
            else:
                # 모든 후보가 탈락한 경우 첫 번째 상품도 반환하지 않음
                if best_product:  # 탈락하지 않은 상품이 있다면
                    return best_product['url'], similarity_score, match_details
                return None, 0, match_details or {'no_matching_products': True}
            
        except Exception as e:
            print(f"    검색 중 오류: {e}")
            return None, 0, {'search_error': str(e)}