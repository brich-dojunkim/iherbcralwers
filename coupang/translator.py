import pandas as pd
import google.generativeai as genai
import time
import os
import re
from typing import List, Optional
import json
from coupang_config import CoupangConfig

class GeminiCSVTranslator:
    def __init__(self, api_key: str = None):
        api_key = api_key or CoupangConfig.GEMINI_API_KEY
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(CoupangConfig.GEMINI_TEXT_MODEL)
    
    def preprocess_korean_text(self, text: str) -> str:
        """
        번역 전 한국어 텍스트에서 불필요한 부분 제거
        
        Args:
            text: 한국어 상품명
            
        Returns:
            전처리된 한국어 텍스트
        """
        # ", n개" 또는 ", n개입" 패턴 제거 (n은 숫자)
        text = re.sub(r',\s*\d+\s*개입?', '', text)
        
        # 연속된 공백 정리
        text = re.sub(r'\s{2,}', ' ', text)
        
        return text.strip()
    
    def preprocess_english_for_search(self, text: str) -> str:
        """
        영어 번역 결과의 검색 최적화 전처리
        
        Args:
            text: 번역된 영어 텍스트
            
        Returns:
            검색 최적화된 영어 텍스트
        """
        # ", 1 Bottle", ", 1 Pack", ", 1 Container" 등 포장 정보 제거
        text = re.sub(r',\s*1\s+(Bottle|Pack|Container|Box)', '', text, flags=re.IGNORECASE)
        # 연속된 쉼표 정리
        text = re.sub(r',{2,}', ',', text)
        # 연속된 공백 정리
        text = re.sub(r'\s{2,}', ' ', text)
        # 앞뒤 공백 제거
        text = text.strip()
        # 끝에 쉼표가 남아있다면 제거
        text = re.sub(r',$', '', text)
        return text
        
    def translate_batch(self, product_names: List[str], batch_size: int = 10) -> List[str]:
        """
        상품명들을 배치로 번역
        
        Args:
            product_names: 번역할 상품명 리스트
            batch_size: 한 번에 번역할 상품 수
            
        Returns:
            번역된 상품명 리스트
        """
        translated_names = []
        
        for i in range(0, len(product_names), batch_size):
            batch = product_names[i:i + batch_size]
            
            # 한국어 전처리 적용
            preprocessed_batch = [self.preprocess_korean_text(name) for name in batch]
            
            # 프롬프트 구성
            prompt = f"""
다음 한국어 상품명들을 영어로 번역해주세요. 
각 상품명을 한 줄씩, 순서대로 번역해주세요.
브랜드명은 그대로 유지하고, 상품의 특성을 잘 나타내도록 번역해주세요.

상품명들:
{chr(10).join([f"{idx+1}. {name}" for idx, name in enumerate(preprocessed_batch)])}

번역 결과를 다음 형식으로 제공해주세요:
1. [영어 번역]
2. [영어 번역]
...
"""
            
            try:
                print(f"\n--- 배치 {i//batch_size + 1} 번역 중 ---")
                print("전처리된 상품명:")
                for idx, (original, preprocessed) in enumerate(zip(batch, preprocessed_batch)):
                    if original != preprocessed:
                        print(f"  {idx+1}. {original} → {preprocessed}")
                    else:
                        print(f"  {idx+1}. {preprocessed}")
                
                # API 호출
                response = self.model.generate_content(prompt)
                
                # 응답 파싱
                translated_batch = self.parse_translation_response(response.text, len(batch))
                
                # 영어 번역 결과에 대한 검색 최적화 전처리 적용
                optimized_batch = [self.preprocess_english_for_search(trans) for trans in translated_batch]
                translated_names.extend(optimized_batch)
                
                print("\n번역 및 최적화 결과:")
                for idx, (original, translated, optimized) in enumerate(zip(batch, translated_batch, optimized_batch)):
                    if translated != optimized:
                        print(f"  {idx+1}. {original} → {translated} → {optimized}")
                    else:
                        print(f"  {idx+1}. {original} → {optimized}")
                
                print(f"\n✅ 배치 {i//batch_size + 1} 완료: {i+1}-{min(i+batch_size, len(product_names))} / {len(product_names)}")
                print("-" * 80)
                
                # API 호출 제한 고려하여 대기
                time.sleep(0.5)
                
            except Exception as e:
                print(f"\n❌ 배치 {i//batch_size + 1} 번역 실패: {e}")
                print("실패한 상품명들:")
                for idx, name in enumerate(batch):
                    print(f"  {idx+1}. {name}")
                print("원본 텍스트로 유지됩니다.")
                # 실패한 경우 원본 유지
                translated_names.extend(batch)
                print("-" * 80)
                
        return translated_names
    
    def parse_translation_response(self, response_text: str, expected_count: int) -> List[str]:
        """
        Gemini 응답에서 번역 결과 파싱
        
        Args:
            response_text: API 응답 텍스트
            expected_count: 예상되는 번역 결과 수
            
        Returns:
            파싱된 번역 결과 리스트
        """
        lines = response_text.strip().split('\n')
        translations = []
        
        for line in lines:
            line = line.strip()
            if line and (line[0].isdigit() or line.startswith('*')):
                # "1. " 또는 "* " 형식에서 번역 추출
                if '. ' in line:
                    translation = line.split('. ', 1)[1]
                elif line.startswith('* '):
                    translation = line[2:]
                else:
                    translation = line
                translations.append(translation.strip())
        
        # 예상 개수와 맞지 않으면 조정
        if len(translations) != expected_count:
            print(f"경고: 예상 번역 개수({expected_count})와 실제({len(translations)})가 다릅니다.")
            # 부족한 경우 빈 문자열로 채움
            while len(translations) < expected_count:
                translations.append("")
            # 초과한 경우 잘라냄
            translations = translations[:expected_count]
                
        return translations
    
    def translate_csv(self, 
                     input_file: str, 
                     output_file: str, 
                     column_name: str = 'product_name',
                     batch_size: int = 10,
                     save_progress: bool = True) -> pd.DataFrame:
        """
        CSV 파일의 특정 컬럼을 번역
        
        Args:
            input_file: 입력 CSV 파일 경로
            output_file: 출력 CSV 파일 경로
            column_name: 번역할 컬럼명
            batch_size: 배치 크기
            save_progress: 중간 저장 여부
            
        Returns:
            번역이 완료된 DataFrame
        """
        # CSV 읽기
        print(f"CSV 파일 읽는 중: {input_file}")
        df = pd.read_csv(input_file)
        
        if column_name not in df.columns:
            raise ValueError(f"컬럼 '{column_name}'이 CSV에 없습니다.")
        
        print(f"총 {len(df)} 개의 상품명을 번역합니다.")
        
        # 진행상황 파일 확인
        progress_file = f"{output_file}.progress.json"
        start_idx = 0
        
        if save_progress and os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                start_idx = progress.get('completed', 0)
                print(f"이전 진행상황 발견: {start_idx} 개 완료됨")
        
        # 번역할 상품명 추출
        product_names = df[column_name].fillna("").astype(str).tolist()
        
        # 이미 번역된 부분이 있다면 건너뛰기
        if start_idx > 0:
            product_names_to_translate = product_names[start_idx:]
        else:
            product_names_to_translate = product_names
        
        # 번역 실행
        print("번역 시작...")
        translated_names = self.translate_batch(product_names_to_translate, batch_size)
        
        # 전체 번역 결과 구성
        if start_idx > 0:
            # 이전 결과 로드
            partial_df = pd.read_csv(output_file)
            all_translated = partial_df[f'{column_name}_english'].tolist()
            all_translated.extend(translated_names)
        else:
            all_translated = translated_names
        
        # 새 컬럼 추가
        df[f'{column_name}_english'] = all_translated
        
        # 추가 정보 컬럼 생성 (원본, 전처리된 한국어, 번역 결과)
        df[f'{column_name}_preprocessed'] = [self.preprocess_korean_text(text) for text in df[column_name].fillna("").astype(str)]
        
        # 결과 저장
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"번역 완료! 결과 저장됨: {output_file}")
        
        # 진행상황 파일 삭제
        if save_progress and os.path.exists(progress_file):
            os.remove(progress_file)
        
        return df
    
    def translate_single(self, text: str) -> str:
        """
        단일 텍스트 번역
        
        Args:
            text: 번역할 텍스트
            
        Returns:
            번역된 텍스트
        """
        # 한국어 전처리 적용
        preprocessed_text = self.preprocess_korean_text(text)
        
        prompt = f"""
다음 한국어 상품명을 영어로 번역해주세요:
"{preprocessed_text}"

브랜드명은 그대로 유지하고, 상품의 특성을 잘 나타내도록 자연스럽게 번역해주세요.
번역 결과만 제공해주세요.
"""
        
        try:
            # 한국어 전처리 적용
            preprocessed_text = self.preprocess_korean_text(text)
            
            prompt = f"""
다음 한국어 상품명을 영어로 번역해주세요:
"{preprocessed_text}"

브랜드명은 그대로 유지하고, 상품의 특성을 잘 나타내도록 자연스럽게 번역해주세요.
번역 결과만 제공해주세요.
"""
            
            print(f"단일 번역 시도: '{text}' → '{preprocessed_text}'")
            
            # API 호출 (재시도 로직 적용)
            def api_call():
                return self.model.generate_content(prompt)
            
            response = self.retry_api_call(api_call, max_retries=3, base_delay=1.0)
            translated = response.text.strip()
            
            # 영어 검색 최적화 적용
            result = self.preprocess_english_for_search(translated)
            print(f"번역 결과: '{preprocessed_text}' → '{translated}' → '{result}'")
            return result
        except Exception as e:
            print(f"번역 실패: {e}")
            return text

# 사용 예시
def main():
    # 번역기 초기화
    translator = GeminiCSVTranslator()
    
    # 파일 경로 설정 (현재 프로젝트 구조에 맞게)
    input_file = '/Users/brich/Desktop/iherb_price/data/outputs/coupang_products_20250926_181517.csv'
    output_file = '/Users/brich/Desktop/iherb_price/data/outputs/coupang_products_20250926_181517_translated.csv'
        
    try:
        print(f"번역 시작: {input_file}")
        print(f"결과 저장 위치: {output_file}")
        
        # 번역 실행
        df = translator.translate_csv(
            input_file=input_file,
            output_file=output_file,
            column_name='product_name',
            batch_size=10,  # 한 번에 10개씩 번역
            save_progress=True
        )
        
        print("\n=== 번역 결과 미리보기 ===")
        print(df[['product_name', 'product_name_preprocessed', 'product_name_english']].head(10))
        
        # 번역 통계 출력
        total_products = len(df)
        translated_products = len(df[df['product_name_english'].notna() & (df['product_name_english'] != '')])
        print(f"\n=== 번역 통계 ===")
        print(f"전체 상품: {total_products}개")
        print(f"번역 완료: {translated_products}개")
        print(f"번역 성공률: {translated_products/total_products*100:.1f}%")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()  # 기본으로 전체 번역 실행