#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import google.generativeai as genai

# 1) 여기 키만 본인 거로 맞추면 됨
GEMINI_API_KEY = "AIzaSyDJbp9o6xlMa9z2JPc_Y-eM7DqIVKCjcgE"

def main():
    if not GEMINI_API_KEY or GEMINI_API_KEY.strip() == "":
        print("❌ GEMINI_API_KEY가 설정되지 않았습니다.")
        return

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    print("⏳ Gemini API 헬스체크 요청 중...")

    try:
        # 아주 가벼운 테스트 요청
        resp = model.generate_content("ping")
        print("✅ 요청 성공: 쿼터/키 상태 정상으로 보입니다.")
        # 토큰 수 같은 건 여기서 resp.usage_metadata 로 일부 확인 가능 (라이브러리 버전에 따라 다름)
        # print(resp.usage_metadata)
    except Exception as e:
        msg = str(e)
        if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
            print("⚠️  429 RESOURCE_EXHAUSTED 발생: 쿼터를 초과했거나 제한에 걸린 상태입니다.")
        else:
            print("❌ 요청 실패 (다른 에러)")
        print("   상세 메시지:", msg[:500])

if __name__ == "__main__":
    main()
