"""
Gemini Vision 간단 검증 모듈

Google 검색 결과 썸네일로 제품 검증
"""

import os
import base64
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
        
        # 로컬 이미지 읽기
        with open(hazard_image_path, 'rb') as f:
            image_data = f.read()
        
        prompt = f"""Compare these two product images:

LEFT: {hazard_brand} - {hazard_name}
RIGHT: Search result

Answer format:
- First line: YES or NO
- Second line: One short reason (brand/product/packaging match or mismatch)

Example:
YES
Same brand and product name visible
or
NO
Different brand logos"""
        
        # Gemini 호출
        response = model.generate_content([
            prompt,
            {
                'mime_type': 'image/jpeg',
                'data': base64.b64encode(image_data).decode()
            },
            search_thumbnail_url
        ])
        
        result = response.text.strip().upper()
        is_match = 'YES' in result
        
        return is_match, result
        
    except Exception as e:
        print(f"  [GEMINI ERROR] {e}")
        return False, str(e)