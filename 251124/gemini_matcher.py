"""
Gemini 기반 상품 매칭
- 후보 선택
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
        """
        GNC 상품과 쿠팡 후보 비교하여 최적 선택
        
        Args:
            gnc_product: GNC 상품 객체 (product_name 속성만 사용)
            candidates: 쿠팡 후보 리스트 (name, final_price, review_count, rating만 사용)
        """
        if not candidates:
            return None, "low", "후보 없음"
        
        try:
            # GNC 쪽은 상품명만 사용 (브랜드/정수 파라미터는 사용하지 않음)
            gnc_name = getattr(gnc_product, "product_name", "") or getattr(gnc_product, "name", "")
            gnc_desc = getattr(gnc_product, "description", "")  # 있으면 추가 정보로 사용, 없어도 무방
            
            gnc_info = f"""
GNC 원본 상품:
- 상품명: {gnc_name}
- 추가 정보(있으면 사용): {gnc_desc}
"""
            
            def fmt_num(value):
                if value is None:
                    return "미확인"
                if isinstance(value, (int, float)):
                    return f"{value:,}"
                return str(value)
            
            # 쿠팡 후보 정보: 상품명 / 가격 / 리뷰 / 평점만 사용
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

⚠️ **매우 중요한 규칙:**

1. **브랜드가 다르면 절대 매칭하지 마세요**
   - 브랜드 이름은 상품명 텍스트 안에서 추론하세요.
   - 예: GNC, 지앤씨, NOW, Solgar 등 상품명에 포함된 브랜드명을 기준으로 판단하세요.
   - 예: GNC 제품인데 후보 상품명에 NOW, Solgar 등이 들어가면 → "매칭 불가"
   
2. **주성분이 다르면 절대 매칭하지 마세요**
   - 예: "Vitamin D3" vs "Vitamin C" → "매칭 불가"
   - 예: "Omega-3" vs "Omega-6" → "매칭 불가"
   - 성분명도 모두 상품명 텍스트에서 판단하세요.
   
3. **정수(캡슐/정 개수)가 명백히 다르면 매칭하지 마세요**
   - 예: 180정 vs 30정 → "매칭 불가"
   - 예: 200 capsules vs 30 softgels → "매칭 불가"
   - 정수 정보 역시 상품명 텍스트에 나온 숫자(예: "180정", "200 capsules")로 판단하세요.
   
4. **용량/함량이 다르면 매칭하지 마세요**
   - 예: 1000mg vs 500mg, 1000IU vs 5000IU 등
   - 상품명에 포함된 숫자와 단위를 기준으로 비교하세요.
   
5. **의심스러우면 매칭하지 마세요**
   - 확실하지 않으면 "매칭 불가"를 선택하세요.
   - 잘못된 매칭보다 매칭 안 하는 게 낫습니다.

**매칭 기준 (가능한 한 모두 충족해야 함):**
1. ✅ 상품명에 나타난 브랜드가 동일
2. ✅ 상품명에 나타난 주성분/제품 타입이 동일
3. ✅ 상품명에 나타난 정수(캡슐/정 개수)가 유사
4. ✅ 상품명에 나타난 용량/함량이 유사

응답 형식:
선택: 후보 X (또는 "매칭 불가")
신뢰도: high/medium/low/none
이유: (구체적 설명)

예시:
✅ 좋은 매칭: GNC "Vitamin D3 1000 IU 180 Tablets" vs 쿠팡 "지앤씨 비타민 D3 1000IU 180정"
→ 선택: 후보 1, 신뢰도: high, 이유: 상품명에 표시된 브랜드(GNC/지앤씨), 성분(D3), 용량(1000IU), 정수(180정) 모두 일치

❌ 나쁜 매칭: GNC "Vitamin D3" vs 쿠팡 "나우푸드 비타민 D3"
→ 선택: 매칭 불가, 신뢰도: none, 이유: 상품명에 표시된 브랜드 불일치 (GNC ≠ 나우푸드)
"""
            
            response = self.model.generate_content(prompt)
            result = response.text.strip()
            
            # 매칭 불가 확인
            if '매칭 불가' in result or '매칭불가' in result:
                return None, 'none', result
            
            # 신뢰도 파싱
            confidence = 'low'
            lower = result.lower()
            if 'high' in lower or '신뢰도: high' in result or '높음' in result:
                confidence = 'high'
            elif 'medium' in lower or '신뢰도: medium' in result or '중간' in result:
                confidence = 'medium'
            elif 'none' in lower:
                return None, 'none', result
            
            # 후보 파싱
            for i in range(len(candidates)):
                if f"후보 {i+1}" in result:
                    # low 신뢰도면 매칭 불가로 처리
                    if confidence == 'low':
                        return None, 'none', f"낮은 신뢰도로 매칭 거부\n{result}"
                    return candidates[i], confidence, result
            
            # 파싱 실패 시 매칭 불가
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
        """두 이미지 비교"""
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
   - GNC 로고 vs 다른 브랜드 로고 → 불일치
   
2. **패키징 디자인이 다르면 → 불일치**
   - 색상, 레이아웃, 글자 위치 등
   
3. **제품명이 다르면 → 불일치**
   - "Vitamin D3" vs "Vitamin C" → 불일치
   
4. **용량/개수 표시가 명백히 다르면 → 불일치**
   - 180정 vs 30정 → 불일치
   
5. **의심스러우면 → 불일치**
   - 확실하지 않으면 "불일치"를 선택하세요

**확인 사항:**
✅ 브랜드 로고 완전 일치
✅ 패키징 디자인 일치
✅ 제품명 일치
✅ 용량/정수 표시 유사

응답 형식:
판정: 일치/불일치
신뢰도: high/medium/low
이유: (구체적 설명)
"""
            
            response = self.model.generate_content([prompt, gnc_img, coupang_img])
            result = response.text.strip()
            
            # 불일치 우선 확인
            is_match = '일치' in result and '불일치' not in result
            
            # 신뢰도 파싱
            confidence = 'medium'
            lower = result.lower()
            if 'high' in lower or '신뢰도: high' in result or '높음' in result:
                confidence = 'high'
            elif 'low' in lower or '신뢰도: low' in result or '낮음' in result:
                confidence = 'low'
            
            # low 신뢰도에서 일치라고 하면 불일치로 처리
            if is_match and confidence == 'low':
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
