"""
Gemini 기반 상품 매칭
- 후보 선택 (강화된 필터링)
- 이미지 비교
"""

import google.generativeai as genai
from PIL import Image
import requests
from io import BytesIO
from typing import Tuple, Optional, List, Any


class CandidateSelector:
    """Gemini 후보 선택 (필터링 강화)"""
    
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
            신뢰도는 'high' 또는 'none'만 사용
        """
        if not candidates:
            return None, "none", "후보 없음"
        
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
당신은 영양제 상품 매칭 전문가입니다.
아래 GNC 원본 상품과 쿠팡 후보들을 비교하여 **정확히 동일한 제품**을 찾아주세요.

{gnc_info}

쿠팡 후보:
{candidates_info}

⚠️ **매우 중요한 규칙 (절대 준수):**

1. **브랜드가 다르면 → 매칭 불가**
   - 상품명에서 브랜드 추론
   - 예: GNC ≠ NOW, Solgar, Nature's Bounty

2. **주성분이 다르면 → 매칭 불가**
   - 예: Vitamin D3 ≠ Vitamin C
   - 예: Dandelion Root ≠ Ginger Root
   - 예: B-Complex 150 ≠ Balanced B-Complex

3. **정수가 명백히 다르면 → 매칭 불가**
   - 예: 180정 ≠ 30정
   - 예: 200정 ≠ 100정

4. **용량/함량이 다르면 → 매칭 불가**
   - 예: 1000mg ≠ 500mg
   - 예: 2000IU ≠ 5000IU

5. **묶음 상품은 → 매칭 불가** ⭐ 중요!
   - "100정 x 2개" → 매칭 불가
   - "60정 2SET" → 매칭 불가  
   - "3개 SET" → 매칭 불가
   - 묶음 표시: "x2", "x3", "SET", "세트", "2개", "3개"

6. **의심스러우면 → 매칭 불가**
   - 확신이 없으면 "매칭 불가"
   - **medium 신뢰도는 사실상 불일치**

**신뢰도 기준:**
- **high**: 브랜드, 성분, 용량, 정수 **모두** 정확히 일치
- **none**: 위 중 **하나라도** 불일치 OR 의심스러움 OR 묶음 상품

⚠️ **절대 금지: medium 신뢰도 사용 금지!**
- high 또는 none만 사용하세요
- medium이라고 생각되면 → none으로 처리

**응답 형식:**
선택: 후보 X (또는 "매칭 불가")
신뢰도: high (또는 none)
이유: (간결하게 1-2문장, 어떤 기준으로 판단했는지)

**예시:**
- ✅ "선택: 후보 1, 신뢰도: high, 이유: GNC 브랜드, 셀레늄 200mcg, 200정 모두 일치"
- ❌ "선택: 매칭 불가, 신뢰도: none, 이유: 100정 x 2개로 묶음 상품"
- ❌ "선택: 매칭 불가, 신뢰도: none, 이유: B-Complex 150 vs Balanced B-Complex, 제품명 불일치"
"""
            
            response = self.model.generate_content(prompt)
            result = response.text.strip()
            
            # 매칭 불가 확인
            if '매칭 불가' in result or '매칭불가' in result:
                return None, 'none', result
            
            # 신뢰도 파싱
            confidence = 'none'
            lower = result.lower()
            
            if 'high' in lower or '신뢰도: high' in result:
                confidence = 'high'
            
            # medium은 none으로 강제 변환
            if 'medium' in lower:
                return None, 'none', f"중간 신뢰도는 불충분. 매칭 불가.\n{result}"
            
            # none이면 매칭 불가
            if confidence == 'none':
                return None, 'none', result
            
            # 후보 파싱 (high인 경우만)
            for i in range(len(candidates)):
                if f"후보 {i+1}" in result:
                    return candidates[i], confidence, result
            
            # 파싱 실패
            return None, 'none', f"응답 파싱 실패\n{result}"
            
        except Exception as e:
            print(f"  ✗ Gemini 선택 실패: {e}")
            return None, 'none', f"API 오류: {str(e)}"


class ImageMatcher:
    """Gemini Vision 이미지 비교"""
    
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')
    
    def compare_images(self, gnc_url: str, coupang_url: str) -> Tuple[bool, str, str]:
        """
        두 이미지 비교
        
        Returns:
            (일치 여부, 신뢰도, 이유)
        """
        try:
            gnc_img = self._download_image(gnc_url)
            coupang_img = self._download_image(coupang_url)
            
            if not gnc_img or not coupang_img:
                return False, "low", "이미지 다운로드 실패"
            
            prompt = """
당신은 영양제 제품 이미지 분석 전문가입니다.
두 이미지를 보고 **정확히 동일한 제품**인지 판단하세요.

⚠️ **엄격한 기준:**

1. **브랜드 로고가 다르면 → 불일치**
   - GNC 로고 vs 다른 브랜드 로고

2. **패키징 디자인이 다르면 → 불일치**
   - 색상, 레이아웃, 글자 배치

3. **제품명이 다르면 → 불일치**
   - Vitamin D3 vs Vitamin C
   - Dandelion Root vs Ginger Root

4. **용량/개수 표시가 명백히 다르면 → 불일치**
   - 180정 vs 30정
   - 100 Capsules vs 200 Capsules

5. **패키징 문구가 다르면 → 불일치**
   - "Suitable for Vegetarian" vs "Antioxidant Support"
   - 서로 다른 제품일 가능성

6. **의심스러우면 → 불일치**

**확인 사항:**
✅ 브랜드 로고 완전 일치
✅ 패키징 디자인 일치
✅ 제품명 일치
✅ 용량/정수 표시 유사

**응답 형식:**
판정: 일치 (또는 불일치)
신뢰도: high/medium/low
이유: (구체적으로 1-2문장)

**예시:**
- ✅ "판정: 일치, 신뢰도: high, 이유: GNC 로고, Selenium 제품명, 200정 표시 모두 동일"
- ❌ "판정: 불일치, 신뢰도: high, 이유: 패키징 문구 다름 (Vegetarian vs Antioxidant Support)"
"""
            
            response = self.model.generate_content([prompt, gnc_img, coupang_img])
            result = response.text.strip()
            
            # 불일치 우선 확인
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