"""
Gemini를 활용한 제품 매칭 검증 시스템
matching_results.csv를 읽어서 각 매칭의 정확도를 검증
"""

import pandas as pd
import google.generativeai as genai
import time
import json
import os
from typing import Dict, Optional, List
from dataclasses import dataclass

@dataclass
class ValidationResult:
    """검증 결과"""
    part_number: str
    original_name: str
    matched_name: str
    is_match: bool
    confidence: str  # high/medium/low
    reason: str
    gemini_response: str  # 원본 응답

class GeminiValidator:
    """Gemini API를 사용한 제품 매칭 검증"""
    
    def __init__(self, api_key: str, model_name: str = "gemini-2.5-flash"):
        """
        Args:
            api_key: Gemini API 키
            model_name: 사용할 모델 (기본: gemini-1.5-flash)
        """
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)
        
    def validate_batch(
        self, 
        part_numbers: List[str],
        original_names: List[str], 
        matched_names: List[str]
    ) -> List[ValidationResult]:
        """
        여러 제품을 배치로 검증
        
        Args:
            part_numbers: Part Number 리스트
            original_names: 원본 제품명 리스트
            matched_names: 매칭된 제품명 리스트
            
        Returns:
            ValidationResult 리스트
        """
        
        # 배치 프롬프트 생성
        products_text = ""
        for i, (pn, orig, matched) in enumerate(zip(part_numbers, original_names, matched_names), 1):
            products_text += f"""
[제품 {i}]
Part Number: {pn}
원본: {orig}
매칭: {matched}
"""
        
        prompt = f"""
다음 제품들이 각각 동일한 제품인지 판단하세요.

{products_text}

**판단 기준:**
1. Part Number에서 브랜드를 추출하여 매칭 제품명에 해당 브랜드가 포함되어 있는지
   (예: DRB → 닥터스베스트, NOW → 나우푸드, LEX → 라이프익스텐션, SRE → 스포츠리서치, THR → 쏜리서치, JRW → 재로우, BLB → 블루보넷)
2. 주성분이 일치하는지 (예: 마그네슘, 비타민C, 오메가3, 콜라겐 등)
3. 용량/농도가 일치하는지 (예: 100mg, 1000mg, 500mg 등)
4. 정수(개수)가 일치하는지 (예: 60정, 120정, 180정 등)
5. 제품 형태가 일치하는지 (캡슐, 소프트젤, 타블렛, 액상 등)

**중요:**
- 브랜드가 명확히 다르면 → 다른 제품 (is_match: false)
- 주성분이 다르면 → 다른 제품 (is_match: false)
- 정수가 다르면 → 다른 제품
- 단순 한글/영문 표기, 띄어쓰기 차이는 같은 제품

**응답 형식 (JSON 배열):**
[
  {{
    "product_number": 1,
    "is_match": true,
    "confidence": "high",
    "reason": "브랜드(닥터스베스트) 일치, 주성분(마그네슘) 일치, 용량(100mg) 일치, 정수(240정) 일치"
  }},
  {{
    "product_number": 2,
    "is_match": false,
    "confidence": "high",
    "reason": "브랜드 불일치 (스포츠리서치 vs 나우푸드)"
  }}
]

JSON 배열만 출력하고 다른 텍스트는 절대 포함하지 마세요.
"""
        
        try:
            # Gemini API 호출
            response = self.model.generate_content(prompt)
            response_text = response.text.strip()
            
            # JSON 파싱 (마크다운 코드블록 제거)
            if response_text.startswith("```json"):
                response_text = response_text.replace("```json", "").replace("```", "").strip()
            elif response_text.startswith("```"):
                response_text = response_text.replace("```", "").strip()
            
            result_list = json.loads(response_text)
            
            # ValidationResult 객체로 변환
            results = []
            for i, item in enumerate(result_list):
                results.append(ValidationResult(
                    part_number=part_numbers[i],
                    original_name=original_names[i],
                    matched_name=matched_names[i],
                    is_match=item.get("is_match", False),
                    confidence=item.get("confidence", "low"),
                    reason=item.get("reason", "파싱 실패"),
                    gemini_response=response_text
                ))
            
            return results
            
        except json.JSONDecodeError as e:
            print(f"⚠ JSON 파싱 실패: {e}")
            print(f"응답: {response_text[:200]}")
            # 실패 시 모든 제품을 low confidence로 반환
            return [
                ValidationResult(
                    part_number=pn,
                    original_name=orig,
                    matched_name=matched,
                    is_match=False,
                    confidence="low",
                    reason=f"JSON 파싱 실패: {str(e)}",
                    gemini_response=response_text
                )
                for pn, orig, matched in zip(part_numbers, original_names, matched_names)
            ]
            
        except Exception as e:
            print(f"✗ 배치 검증 실패: {e}")
            return [
                ValidationResult(
                    part_number=pn,
                    original_name=orig,
                    matched_name=matched,
                    is_match=False,
                    confidence="low",
                    reason=f"오류 발생: {str(e)}",
                    gemini_response=""
                )
                for pn, orig, matched in zip(part_numbers, original_names, matched_names)
            ]
        
    def validate_match(
        self, 
        part_number: str, 
        original_name: str, 
        matched_name: str
    ) -> ValidationResult:
        """
        단일 제품 검증 (배치 처리 사용)
        """
        results = self.validate_batch([part_number], [original_name], [matched_name])
        return results[0]

    def validate_csv(
        self, 
        input_csv: str, 
        output_csv: str,
        batch_size: int = 5,
        delay_seconds: float = 2.0,
        skip_existing: bool = True
    ):
        """
        CSV 파일의 모든 매칭 검증 (배치 처리)
        
        Args:
            input_csv: 입력 CSV 파일 경로 (matching_results.csv)
            output_csv: 출력 CSV 파일 경로
            batch_size: 한 번에 처리할 제품 수 (기본: 5개)
            delay_seconds: 배치 간 대기 시간 (초)
            skip_existing: 기존 결과가 있으면 건너뛰기
        """
        print(f"\n{'='*60}")
        print(f"Gemini 제품 매칭 검증 시스템 (배치 처리)")
        print(f"{'='*60}")
        print(f"입력: {input_csv}")
        print(f"출력: {output_csv}")
        print(f"배치 크기: {batch_size}개")
        print(f"{'='*60}\n")
        
        # 입력 CSV 로드
        try:
            df = pd.read_csv(input_csv, encoding='utf-8-sig')
            print(f"✓ 입력 데이터 로드: {len(df)}개 행")
        except Exception as e:
            print(f"✗ 입력 파일 로드 실패: {e}")
            return
        
        # 기존 결과 확인 및 로드
        if skip_existing and os.path.exists(output_csv):
            try:
                existing_df = pd.read_csv(output_csv, encoding='utf-8-sig')
                # 기존 검증 결과가 있는 Part Number 추출
                processed_parts = set(
                    existing_df[existing_df['Gemini_일치여부'].notna()]['Part Number'].astype(str)
                )
                print(f"✓ 기존 결과 로드: {len(processed_parts)}개 처리 완료")
                
                # 기존 결과를 df에 병합
                df = df.merge(
                    existing_df[['Part Number', 'Gemini_일치여부', 'Gemini_신뢰도', 'Gemini_판단근거']],
                    on='Part Number',
                    how='left'
                )
            except Exception as e:
                print(f"⚠ 기존 결과 로드 실패: {e}")
                processed_parts = set()
        else:
            processed_parts = set()
            # 새로운 컬럼 추가
            df['Gemini_일치여부'] = None
            df['Gemini_신뢰도'] = None
            df['Gemini_판단근거'] = None
        
        # 매칭된 제품만 필터링 (매칭제품명이 있고, 아직 검증 안 된 것)
        mask = (
            df['매칭제품명'].notna() & 
            (df['매칭제품명'] != '') &
            (~df['Part Number'].astype(str).isin(processed_parts))
        )
        to_validate = df[mask].copy()
        
        if len(to_validate) == 0:
            print("✓ 검증할 제품이 없습니다 (모두 처리 완료 또는 매칭 제품 없음)")
            return
        
        print(f"✓ 검증 대상: {len(to_validate)}개")
        
        total_count = len(to_validate)
        success_count = 0
        batch_count = 0
        
        # 배치 단위로 처리
        for i in range(0, len(to_validate), batch_size):
            batch = to_validate.iloc[i:i+batch_size]
            batch_count += 1
            
            part_numbers = batch['Part Number'].astype(str).tolist()
            original_names = batch['원본제품명'].tolist()
            matched_names = batch['매칭제품명'].tolist()
            
            print(f"\n[배치 {batch_count}] {len(batch)}개 제품 검증 중...")
            for idx, (pn, orig) in enumerate(zip(part_numbers, original_names), 1):
                print(f"  {idx}. {pn}: {orig[:40]}...")
            
            # 배치 검증
            validations = self.validate_batch(part_numbers, original_names, matched_names)
            
            # 결과 적용
            for validation, (idx, row) in zip(validations, batch.iterrows()):
                match_icon = "✓" if validation.is_match else "✗"
                print(f"  {match_icon} {validation.part_number}: {validation.is_match} ({validation.confidence})")
                
                df.at[idx, 'Gemini_일치여부'] = validation.is_match
                df.at[idx, 'Gemini_신뢰도'] = validation.confidence
                df.at[idx, 'Gemini_판단근거'] = validation.reason
                
                if validation.is_match:
                    success_count += 1
            
            # 중간 저장
            df.to_csv(output_csv, index=False, encoding='utf-8-sig')
            print(f"  💾 중간 저장 완료 ({i+len(batch)}/{total_count})")
            
            # API 호출 제한 방지
            if i + batch_size < len(to_validate):
                time.sleep(delay_seconds)
        
        print(f"\n{'='*60}")
        print(f"검증 완료!")
        print(f"{'='*60}")
        print(f"전체: {total_count}개")
        print(f"일치: {success_count}개 ({success_count/total_count*100:.1f}%)")
        print(f"불일치: {total_count - success_count}개 ({(total_count-success_count)/total_count*100:.1f}%)")
        print(f"{'='*60}\n")
        print(f"✓ 최종 결과 저장: {output_csv}")


# 실행 예시
if __name__ == "__main__":
    # Gemini API 키 설정
    GEMINI_API_KEY = "AIzaSyC9m-6vYIRXBQLSctElXTCQfPdTzfV2Ck8"
    
    # 파일 경로
    INPUT_CSV = "/Users/brich/Desktop/iherb_price/coupang2/251111/outputs/matching_results.csv"
    OUTPUT_CSV = "/Users/brich/Desktop/iherb_price/coupang2/251111/outputs/matching_results_1.csv"  # 같은 파일에 컬럼 추가
    
    # 검증 실행
    validator = GeminiValidator(GEMINI_API_KEY)
    validator.validate_csv(
        input_csv=INPUT_CSV,
        output_csv=OUTPUT_CSV,
        batch_size=5,  # 한 번에 5개씩 처리
        delay_seconds=2.0,  # 배치 간 2초 대기
        skip_existing=True  # 이어서 진행
    )