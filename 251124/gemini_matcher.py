"""
Gemini 기반 상품 매칭 - 개선 버전
- 후보 선택: 관대하게 (1차 필터링)
- 이미지 비교: 엄격하게 (최종 검증)
"""

import google.generativeai as genai
from PIL import Image
import requests
from io import BytesIO
from typing import Tuple, Optional, List, Any


class CandidateSelector:
    """Gemini 후보 선택 - 관대한 1차 필터링"""
    
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')
    
    def select_best_candidate(self, gnc_product: Any, candidates: List[Any]) -> Tuple[Optional[Any], str, str]:
        """
        GNC 상품과 쿠팡 후보 비교하여 최적 선택
        
        Args:
            gnc_product: GNC 상품 객체
            candidates: 쿠팡 후보 리스트
        
        Returns:
            (선택된 상품, 신뢰도, 이유)
            신뢰도: 'high', 'medium', 'none'
        """
        if not candidates:
            return None, "none", "후보가 없어 매칭 불가"
        
        try:
            gnc_name = getattr(gnc_product, "product_name", "") or getattr(gnc_product, "name", "")
            gnc_desc = getattr(gnc_product, "description", "")
            
            gnc_info = f"""
GNC 원본 상품:
- 상품명: {gnc_name}
- 추가 정보: {gnc_desc if gnc_desc else '없음'}
"""
            
            def fmt_num(value):
                if value is None:
                    return "미확인"
                if isinstance(value, (int, float)):
                    return f"{value:,}"
                return str(value)
            
            candidates_info = "\n\n".join([
                f"""후보 {i+1}:
- 상품명: {c.name}
- 가격: {fmt_num(getattr(c, "final_price", None))}원
- 리뷰 수: {fmt_num(getattr(c, "review_count", None))}개
- 평점: {fmt_num(getattr(c, "rating", None))}"""
                for i, c in enumerate(candidates)
            ])
            
            prompt = f"""
원본 상품과 쿠팡 후보들을 비교하여 가장 유사한 제품을 선택하세요.

원본 상품:
- 상품명: {gnc_name}

쿠팡 후보:
{candidates_info}

**선택 기준 (4가지 모두 확인):**

1. **브랜드 일치** (영한 표기 차이 허용)
2. **제품명 일치** (영한 표기 차이 허용, 다른 제품 라인 제외)
3. **주성분/맛/타입 일치** (성분이나 맛이 다르면 다른 제품)
4. **용량 유사** (단위 변환 고려, ±10% 허용)

**절대 선택 금지:**
- 묶음 상품 (2개/x2/세트 표시, 단 "1개"는 OK)
- 다른 성분/맛
- 다른 제품 라인

**응답 형식 (매우 간결하게):**
선택: 후보 X (또는 매칭 불가)
이유: (선택 시: "브랜드, 제품명, 맛, 용량 모두 일치" / 매칭 불가 시: "브랜드 불일치" 또는 "맛 다름" 등 불일치 이유)
"""
            
            response = self.model.generate_content(prompt)
            result = response.text.strip()
            
            # 🐛 디버그: Gemini 응답 확인
            print(f"  🔍 Gemini 응답:\n{result}\n")
            
            # 매칭 불가 확인
            if '매칭 불가' in result or '매칭불가' in result:
                return None, 'none', result
            
            # 신뢰도 파싱
            confidence = 'none'
            lower = result.lower()
            
            if 'high' in lower or '신뢰도: high' in result:
                confidence = 'high'
            elif 'medium' in lower or '신뢰도: medium' in result:
                confidence = 'medium'
            
            # none이면 매칭 불가
            if confidence == 'none':
                return None, 'none', result
            
            # 후보 파싱 (1부터 시작, 후보 1 = candidates[0])
            for i in range(1, len(candidates) + 1):  # ⭐ 1부터 len+1까지
                if f"후보 {i}" in result:
                    return candidates[i-1], confidence, result  # ⭐ i-1 인덱스
            
            # 파싱 실패
            return None, 'none', f"응답 파싱 실패\n{result}"
            
        except Exception as e:
            print(f"  ✗ Gemini 선택 실패: {e}")
            return None, 'none', f"API 오류: {str(e)}"


class ImageMatcher:
    """Gemini Vision 이미지 비교 - 엄격한 최종 검증"""
    
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')
    
    def compare_images(self, gnc_url: str, coupang_url: str) -> Tuple[bool, str, str]:
        """
        두 이미지 비교 - 엄격한 기준
        
        Returns:
            (일치 여부, 신뢰도, 이유)
        """
        try:
            gnc_img = self._download_image(gnc_url)
            coupang_img = self._download_image(coupang_url)
            
            if not gnc_img or not coupang_img:
                return False, "low", "이미지 다운로드 실패"
            
            prompt = """
두 이미지가 동일한 제품인지 판단하세요.

**체크리스트:**
1. 브랜드 로고 (영한 표기 허용)
2. 제품명 (영한 표기 허용)
3. 주성분/맛/타입
4. 패키징 디자인
5. 용량 표시 (단위 변환 고려)

**불일치 조건:**
- 브랜드 다름
- 제품명 다름
- 성분/맛 다름

**응답 형식 (매우 간결하게):**
판정: 일치 (또는 불일치)
이유: (일치 시: "브랜드, 제품명, 맛 모두 동일" / 불일치 시: "브랜드 다름" 또는 "맛 불일치" 등 불일치 이유)
"""
            
            response = self.model.generate_content([prompt, gnc_img, coupang_img])
            result = response.text.strip()
            
            # 일치 여부 확인
            is_match = '일치' in result and '불일치' not in result
            
            # 신뢰도 파싱
            confidence = 'medium'
            lower = result.lower()
            if 'high' in lower or '신뢰도: high' in result:
                confidence = 'high'
            elif 'low' in lower or '신뢰도: low' in result:
                confidence = 'low'
            
            # low 신뢰도면 불일치로 처리
            if confidence == 'low':
                return False, 'low', f"낮은 신뢰도로 불일치 처리\n{result}"
            
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