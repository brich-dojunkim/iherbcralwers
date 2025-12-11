"""
Gemini Vision 간단 검증 모듈

Google 검색 결과 썸네일로 제품 검증
"""

import os
import base64
import requests
from io import BytesIO
from dotenv import load_dotenv
import google.generativeai as genai

# .env 로드
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    print("[WARN] GEMINI_API_KEY not found. 검증 기능 비활성화")


def verify_with_gemini(
    hazard_image_path: str,
    search_thumbnail_url: str,
    hazard_name: str,
    hazard_brand: str
) -> tuple:
    """
    Gemini Vision으로 두 이미지 비교
    
    Args:
        hazard_image_path: 위해상품 이미지 경로
        search_thumbnail_url: Google 검색 결과 썸네일 URL
        hazard_name: 위해상품명
        hazard_brand: 제조사
    
    Returns:
        (is_match: bool, reason: str)
    """
    if not GEMINI_API_KEY:
        return False, "API key not configured"
    
    try:
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        # 로컬 이미지 읽기 (IMAGE 1)
        with open(hazard_image_path, 'rb') as f:
            image_data = f.read()
        
        # 썸네일 처리 (IMAGE 2)
        if search_thumbnail_url.startswith('data:image'):
            # base64 data URI인 경우
            try:
                header, encoded = search_thumbnail_url.split(',', 1)
                thumb_data = base64.b64decode(encoded)
            except Exception as e:
                print(f"  [WARN] base64 디코딩 실패: {e}")
                return False, "Base64 decode failed"
        else:
            # 일반 URL인 경우
            try:
                thumb_response = requests.get(search_thumbnail_url, timeout=10)
                thumb_response.raise_for_status()
                thumb_data = thumb_response.content
            except Exception as e:
                print(f"  [WARN] 썸네일 다운로드 실패: {e}")
                return False, "Thumbnail download failed"
        
        prompt = f"""Compare these two product images:

IMAGE 1: Official product - {hazard_brand} {hazard_name}
IMAGE 2: Search result thumbnail

Are they the SAME product?
- Same brand name?
- Same product name?
- Similar packaging?

Answer format: Start with YES or NO, then explain in one sentence.
Example: "YES: Same brand and product visible on packaging"
Example: "NO: Different brand logos"
"""
        
        # Gemini 호출 - 두 이미지 모두 명시적으로 전달
        response = model.generate_content([
            prompt,
            {
                'mime_type': 'image/jpeg',
                'data': base64.b64encode(image_data).decode()
            },
            {
                'mime_type': 'image/jpeg',
                'data': base64.b64encode(thumb_data).decode()
            }
        ])
        
        result = response.text.strip()
        
        # 첫 단어로 판단 (콜론이나 공백 전까지)
        first_word = result.split()[0].upper().rstrip(':,.')
        
        if first_word == 'YES':
            is_match = True
        elif first_word == 'NO':
            is_match = False
        else:
            # 첫 단어가 명확하지 않으면 전체 텍스트에서 판단
            result_lower = result.lower()
            if result_lower.startswith('yes'):
                is_match = True
            else:
                is_match = False
        
        return is_match, result
        
    except Exception as e:
        print(f"  [GEMINI ERROR] {e}")
        return False, str(e)