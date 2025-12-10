#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import google.generativeai as genai

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

def check_api_key():
    """API 키 종합 검사"""
    
    print("=" * 60)
    print("🔍 Gemini API 키 종합 검사 시작")
    print("=" * 60)
    
    # 1. 키 존재 여부
    print("\n[1/5] API 키 존재 확인")
    if not GEMINI_API_KEY or GEMINI_API_KEY.strip() == "":
        print("❌ API 키가 설정되지 않음")
        return False
    print(f"✅ 키 길이: {len(GEMINI_API_KEY)}자")
    
    # 2. 키 형식 검증
    print("\n[2/5] 키 형식 검증")
    if not GEMINI_API_KEY.startswith("AIza"):
        print("⚠️  일반적인 Gemini 키 형식(AIza~)이 아님")
    else:
        print("✅ 키 형식 정상")
    
    # 3. API 연결 테스트
    print("\n[3/5] API 연결 테스트")
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        print("✅ API 설정 완료")
    except Exception as e:
        print(f"❌ 설정 실패: {str(e)[:200]}")
        return False
    
    # 4. 모델 목록 조회
    print("\n[4/5] 사용 가능 모델 확인")
    try:
        models = genai.list_models()
        available = [m.name for m in models if 'generateContent' in m.supported_generation_methods]
        print(f"✅ 사용 가능 모델 {len(available)}개")
        for model_name in available[:3]:
            print(f"   - {model_name}")
        if len(available) > 3:
            print(f"   ... 외 {len(available)-3}개")
    except Exception as e:
        error_msg = str(e)
        print(f"❌ 모델 조회 실패")
        if "403" in error_msg or "API_KEY_INVALID" in error_msg:
            print("   → API 키가 유효하지 않거나 권한 없음")
        print(f"   상세: {error_msg[:200]}")
        return False
    
    # 5. 실제 요청 테스트
    print("\n[5/5] 실제 요청 테스트")
    model = genai.GenerativeModel("gemini-2.5-flash")
    
    try:
        start = time.time()
        resp = model.generate_content("Say 'OK' in one word")
        elapsed = time.time() - start
        
        print(f"✅ 요청 성공 ({elapsed:.2f}초)")
        print(f"   응답: {resp.text[:50]}")
        
        # 토큰 사용량
        if hasattr(resp, 'usage_metadata'):
            meta = resp.usage_metadata
            print(f"   토큰: input={meta.prompt_token_count}, output={meta.candidates_token_count}")
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ 요청 실패")
        
        if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
            print("   → 쿼터 초과 또는 Rate Limit")
        elif "403" in error_msg:
            print("   → 권한 없음 (키 무효 또는 API 미활성화)")
        elif "404" in error_msg:
            print("   → 모델을 찾을 수 없음")
        elif "Invalid API key" in error_msg:
            print("   → API 키가 잘못됨")
        else:
            print(f"   → 기타 에러")
        
        print(f"   상세: {error_msg[:300]}")
        return False
    
    print("\n" + "=" * 60)
    print("✅ 모든 검사 통과 - API 키 정상")
    print("=" * 60)
    return True

if __name__ == "__main__":
    check_api_key()