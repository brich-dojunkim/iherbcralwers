"""
Gemini 기반 상품 매칭
- 후보 선택
- 상품명 비교
- 이미지 비교
"""

import google.generativeai as genai
from PIL import Image
import requests
from io import BytesIO
from typing import Tuple, Optional, List, Any


class CandidateSelector:
    """Gemini 후보 선택"""
    
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')
    
    def select_best_candidate(self, gnc_product: Any, candidates: List[Any]) -> Tuple[Optional[Any], str, str]:
        """GNC 상품과 쿠팡 후보 비교하여 최적 선택"""
        if not candidates:
            return None, "low", "후보 없음"
        
        try:
            # GNC 정보
            gnc_info = f"""
GNC 원본 상품:
- 상품명: {gnc_product.product_name}
- 브랜드: {gnc_product.brand}
- 정수: {gnc_product.count}개
"""
            
            # 쿠팡 후보들
            candidates_info = "\n\n".join([
                f"""후보 {i+1}:
- 상품명: {c.name}
- 정수: {c.count}개
- 브랜드: {c.brand if c.brand else '미확인'}
- 가격: {c.final_price:,}원
- 리뷰: {c.review_count}개, 평점 {c.rating}"""
                for i, c in enumerate(candidates)
            ])
            
            prompt = f"""
아래 GNC 상품과 가장 유사한 쿠팡 제품을 선택하세요.

{gnc_info}

쿠팡 후보:
{candidates_info}

선택 기준 (우선순위):
1. 브랜드명 일치
2. 제품 타입/성분 일치 (예: Vitamin D3, Omega-3)
3. 정수(개수) 일치
4. 용량/함량 유사 (예: 1000mg, 5000IU)
5. 가격 합리성

응답 형식:
선택: 후보 X
신뢰도: high/medium/low
이유: (브랜드, 성분, 정수 등에서 어떤 점이 일치하는지 구체적 설명)

주의: 한글/영문 번역도 같은 것으로 판단
"""
            
            response = self.model.generate_content(prompt)
            result = response.text.strip()
            
            # 신뢰도 파싱
            confidence = 'medium'
            if 'high' in result.lower() or '높음' in result:
                confidence = 'high'
            elif 'low' in result.lower() or '낮음' in result:
                confidence = 'low'
            
            # 선택된 후보 파싱
            for i in range(len(candidates)):
                if f"후보 {i+1}" in result:
                    return candidates[i], confidence, result
            
            return candidates[0], 'low', f"파싱 실패\n{result}"
            
        except Exception as e:
            print(f"  ✗ Gemini 선택 실패: {e}")
            # 폴백: 정수 일치 → 가격순
            if gnc_product.count:
                matched = [c for c in candidates if c.count == gnc_product.count]
                if matched:
                    return min(matched, key=lambda x: x.final_price), 'medium', "정수 일치, 가격 낮은 순"
            return min(candidates, key=lambda x: x.final_price), 'low', "가격 낮은 순"


class ImageMatcher:
    """Gemini Vision 이미지 비교"""
    
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')
    
    def compare_images(self, gnc_url: str, coupang_url: str) -> Tuple[bool, str, str]:
        """두 이미지 비교"""
        try:
            gnc_img = self._download_image(gnc_url)
            coupang_img = self._download_image(coupang_url)
            
            if not gnc_img or not coupang_img:
                return False, "low", "이미지 다운로드 실패"
            
            prompt = """
두 제품 이미지가 동일한 제품인지 판단하세요.

확인 사항:
1. 패키징 디자인/색상
2. 브랜드 로고
3. 제품명 유사성
4. 용량/개수 표시

응답 형식:
판정: 일치/불일치
신뢰도: high/medium/low
이유: (간단 설명)
"""
            
            response = self.model.generate_content([prompt, gnc_img, coupang_img])
            result = response.text.strip()
            
            is_match = '일치' in result and '불일치' not in result
            
            confidence = 'medium'
            if 'high' in result.lower():
                confidence = 'high'
            elif 'low' in result.lower():
                confidence = 'low'
            
            return is_match, confidence, result
            
        except Exception as e:
            print(f"  ✗ 이미지 비교 실패: {e}")
            return False, "error", str(e)
    
    def _download_image(self, url: str) -> Optional[Image.Image]:
        """이미지 다운로드"""
        try:
            if not url or url.startswith('data:'):
                return None
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return Image.open(BytesIO(response.content))
            
        except:
            return None