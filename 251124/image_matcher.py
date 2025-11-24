"""
이미지 비교 및 상품명 매칭 모듈 - Gemini Vision API 사용
"""

import google.generativeai as genai
from PIL import Image
import requests
from io import BytesIO
from typing import Tuple, Optional, List, Any
import os


class CandidateSelector:
    """Gemini를 사용한 최적 후보 선택"""
    
    def __init__(self, api_key: str):
        """
        Args:
            api_key: Gemini API 키
        """
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
    
    def select_best_candidate(self, gnc_product: Any, candidates: List[Any]) -> Tuple[Optional[Any], str]:
        """
        GNC 상품과 쿠팡 후보들을 비교하여 가장 유사한 것 선택
        
        Args:
            gnc_product: GNC 상품 정보 (GNCProduct)
            candidates: 쿠팡 후보 리스트 (CoupangProduct list)
            
        Returns:
            (선택된 후보, 선택 이유)
        """
        if not candidates:
            return None, "후보 없음"
        
        try:
            # GNC 상품 정보
            gnc_info = f"""
GNC 원본 상품:
- 상품명: {gnc_product.product_name}
- 브랜드: {gnc_product.brand}
- 정수: {gnc_product.count}개
"""
            
            # 쿠팡 후보들 정보
            candidates_info = "\n\n".join([
                f"""후보 {i+1}:
- 상품명: {c.name}
- 정수: {c.count}개
- 브랜드: {c.brand if c.brand else '미확인'}
- 가격: {c.final_price:,}원 (상품 {c.price:,}원 + 배송비 {c.shipping_fee:,}원)
- 리뷰: {c.review_count}개, 평점 {c.rating}"""
                for i, c in enumerate(candidates)
            ])
            
            prompt = f"""
아래는 GNC 원본 상품과 쿠팡에서 검색된 후보 상품들입니다.
가장 유사한 제품을 선택해주세요.

{gnc_info}

쿠팡 후보 상품들:
{candidates_info}

**선택 기준 (우선순위 순):**
1. 브랜드명 일치 (가장 중요)
2. 제품 타입/성분 일치 (예: Vitamin D3, Omega-3 등)
3. 정수(캡슐/정 개수) 일치
4. 용량/함량 유사 (예: 1000mg, 5000IU 등)
5. 가격 합리성 (너무 비싸지 않음)

**응답 형식:**
선택: 후보 X
이유: (브랜드, 성분, 정수, 용량 등에서 어떤 점이 일치하는지 구체적으로 설명)

주의: 번역된 한글과 영문도 같은 것으로 판단하세요.
"""
            
            response = self.model.generate_content(prompt)
            result_text = response.text.strip()
            
            # 선택된 후보 파싱
            selected_idx = None
            for i in range(len(candidates)):
                if f"후보 {i+1}" in result_text:
                    selected_idx = i
                    break
            
            if selected_idx is not None:
                return candidates[selected_idx], result_text
            else:
                # 파싱 실패 시 첫 번째 후보 반환
                return candidates[0], f"파싱 실패, 첫 번째 선택\n{result_text}"
            
        except Exception as e:
            print(f"  ✗ Gemini 후보 선택 실패: {e}")
            # 오류 시 정수 일치 > 가격 낮은 순
            if gnc_product.count:
                matched = [c for c in candidates if c.count == gnc_product.count]
                if matched:
                    return min(matched, key=lambda x: x.final_price), "정수 일치, 가격 낮은 순"
            return min(candidates, key=lambda x: x.final_price), "가격 낮은 순"


class ProductNameMatcher:
    """상품명 매칭 클래스 - Gemini 사용"""
    
    def __init__(self, api_key: str):
        """
        Args:
            api_key: Gemini API 키
        """
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
    
    def match_product_names(self, gnc_name: str, coupang_name: str) -> Tuple[bool, str, str]:
        """
        두 상품명을 비교하여 동일 상품 여부 판단
        
        Args:
            gnc_name: GNC 상품명
            coupang_name: 쿠팡 상품명
            
        Returns:
            (일치여부, 신뢰도, 이유)
        """
        try:
            prompt = f"""
            두 제품명을 비교하여 동일한 제품인지 판단해주세요.
            
            **GNC 제품명:**
            {gnc_name}
            
            **쿠팡 제품명:**
            {coupang_name}
            
            **비교 기준:**
            1. 브랜드명이 동일한가?
            2. 주요 성분/제품 타입이 동일한가? (예: Vitamin D3, Omega-3 등)
            3. 용량/함량이 유사한가? (예: 1000mg, 5000IU 등)
            4. 정수/캡슐 개수가 유사한가?
            
            **응답 형식:**
            판정: 일치/불일치
            신뢰도: high/medium/low
            이유: (간단한 설명 - 어떤 부분이 일치하거나 다른지)
            
            주의: 번역된 한글 제품명과 영문 제품명도 같은 제품으로 판단해야 합니다.
            예: "Vitamin D3" = "비타민 D3"
            """
            
            response = self.model.generate_content(prompt)
            result_text = response.text.strip()
            
            # 결과 파싱
            is_match = '일치' in result_text and '불일치' not in result_text
            
            confidence = 'medium'
            if 'high' in result_text.lower() or '높음' in result_text:
                confidence = 'high'
            elif 'low' in result_text.lower() or '낮음' in result_text:
                confidence = 'low'
            
            return is_match, confidence, result_text
            
        except Exception as e:
            print(f"  ✗ 상품명 비교 실패: {e}")
            return False, "error", str(e)


class ImageMatcher:
    """썸네일 이미지 비교 클래스"""
    
    def __init__(self, api_key: str):
        """
        Args:
            api_key: Gemini API 키
        """
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        self.name_matcher = ProductNameMatcher(api_key)  # 상품명 매칭 추가
    
    def compare_images(self, gnc_image_url: str, coupang_image_url: str) -> Tuple[bool, str, str]:
        """
        두 이미지를 비교하여 동일 상품 여부 판단
        
        Args:
            gnc_image_url: GNC 썸네일 URL
            coupang_image_url: 쿠팡 썸네일 URL
            
        Returns:
            (일치여부, 신뢰도, 이유)
        """
        try:
            # 이미지 다운로드
            gnc_image = self._download_image(gnc_image_url)
            coupang_image = self._download_image(coupang_image_url)
            
            if not gnc_image or not coupang_image:
                return False, "low", "이미지 다운로드 실패"
            
            # Gemini Vision으로 비교
            prompt = """
            두 제품 이미지를 비교하여 동일한 제품인지 판단해주세요.
            
            다음 기준을 확인하세요:
            1. 제품 패키징의 디자인과 색상이 동일한가?
            2. 브랜드 로고가 동일한가?
            3. 제품명이 유사한가?
            4. 용량/개수 표시가 일치하는가?
            
            응답 형식:
            판정: 일치/불일치
            신뢰도: high/medium/low
            이유: (간단한 설명)
            """
            
            response = self.model.generate_content([prompt, gnc_image, coupang_image])
            result_text = response.text.strip()
            
            # 결과 파싱
            is_match = '일치' in result_text and '불일치' not in result_text
            
            confidence = 'medium'
            if 'high' in result_text.lower():
                confidence = 'high'
            elif 'low' in result_text.lower():
                confidence = 'low'
            
            return is_match, confidence, result_text
            
        except Exception as e:
            print(f"  ✗ 이미지 비교 실패: {e}")
            return False, "error", str(e)
    
    def _download_image(self, url: str) -> Optional[Image.Image]:
        """URL에서 이미지 다운로드"""
        try:
            if not url or url.startswith('data:'):
                return None
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            image = Image.open(BytesIO(response.content))
            return image
            
        except Exception as e:
            print(f"  ⚠ 이미지 다운로드 실패 ({url[:50]}...): {e}")
            return None


class SimpleImageMatcher:
    """간단한 이미지 비교 (API 없이 픽셀 비교)"""
    
    @staticmethod
    def compare_images_simple(gnc_image_url: str, coupang_image_url: str) -> Tuple[bool, str, str]:
        """
        단순 픽셀 해시 비교
        
        Returns:
            (일치여부, 신뢰도, 이유)
        """
        try:
            from PIL import Image
            import imagehash
            
            # 이미지 다운로드
            gnc_img = SimpleImageMatcher._download_image(gnc_image_url)
            coupang_img = SimpleImageMatcher._download_image(coupang_image_url)
            
            if not gnc_img or not coupang_img:
                return False, "error", "이미지 다운로드 실패"
            
            # perceptual hash 계산
            hash1 = imagehash.average_hash(gnc_img)
            hash2 = imagehash.average_hash(coupang_img)
            
            # 해시 차이 계산
            diff = hash1 - hash2
            
            # 판정
            if diff <= 5:
                return True, "high", f"해시 차이: {diff} (매우 유사)"
            elif diff <= 10:
                return True, "medium", f"해시 차이: {diff} (유사)"
            elif diff <= 15:
                return False, "medium", f"해시 차이: {diff} (다소 상이)"
            else:
                return False, "low", f"해시 차이: {diff} (매우 상이)"
                
        except Exception as e:
            return False, "error", str(e)
    
    @staticmethod
    def _download_image(url: str) -> Optional[Image.Image]:
        """URL에서 이미지 다운로드"""
        try:
            if not url or url.startswith('data:'):
                return None
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            image = Image.open(BytesIO(response.content))
            return image
            
        except Exception as e:
            return None