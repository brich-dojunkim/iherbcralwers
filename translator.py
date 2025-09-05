import pandas as pd
import google.generativeai as genai
import time
import os
import re
from typing import List, Optional
import json

class GeminiCSVTranslator:
    def __init__(self, api_key: str):
        """
        Gemini API를 사용한 CSV 번역기 초기화
        
        Args:
            api_key: Google Gemini API 키
        """
        genai.configure(api_key=api_key)
        # 유료 계정용 최신 모델 사용 (할당량 제한 없음)
        self.model = genai.GenerativeModel('gemini-2.5-flash')
    
    def preprocess_korean_text(self, text: str) -> str:
        """
        번역 전 한국어 텍스트에서 불필요한 부분 제거
        
        Args:
            text: 한국어 상품명
            
        Returns:
            전처리된 한국어 텍스트
        """
        # ", 1개" 패턴 제거
        text = re.sub(r',\s*1개', '', text)
        # ", 1개입" 패턴 제거  
        text = re.sub(r',\s*1개입', '', text)
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
    # API 키 설정
    API_KEY = 'AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE'
    
    # 번역기 초기화
    translator = GeminiCSVTranslator(API_KEY)
    
    # 파일 경로 설정 (현재 프로젝트 구조에 맞게)
    input_file = 'input/coupang/coupang_products_20250903_120440.csv'
    output_file = 'output/coupang/coupang_products_translated_optimized_20250903.csv'
    
    # output/coupang 디렉토리 생성
    os.makedirs('output/coupang', exist_ok=True)
    
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

# 개별 파일 번역 함수
def translate_specific_file(filename: str):
    """특정 파일만 번역하는 함수"""
    API_KEY = 'AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE'
    translator = GeminiCSVTranslator(API_KEY)
    
    input_file = f'input/coupang/{filename}'
    output_file = f'output/coupang/{filename.replace(".csv", "_translated_optimized.csv")}'
    
    os.makedirs('output/coupang', exist_ok=True)
    
    try:
        df = translator.translate_csv(
            input_file=input_file,
            output_file=output_file,
            column_name='product_name',
            batch_size=10,
            save_progress=True
        )
        return df
    except Exception as e:
        print(f"파일 {filename} 번역 실패: {e}")
        return None

# 테스트 함수들
def test_single_translation():
    """단일 번역 테스트"""
    API_KEY = 'AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE'
    translator = GeminiCSVTranslator(API_KEY)
    
    test_products = [
        "나우푸드 실리마린 밀크 시슬 추출물 300mg 베지 캡슐, 200정, 1개",
        "나우푸드 더블 스트랭스 L-아르기닌 1000mg 타블렛, 120정, 1개",
        "나우푸드 프로바이오틱-10 유산균 250억 베지 캡슐, 100정, 1개"
    ]
    
    print("=== 단일 번역 테스트 ===")
    for product in test_products:
        result = translator.translate_single(product)
        print(f"최종 결과: {result}")
        print()

def test_batch_translation():
    """배치 번역 테스트"""
    API_KEY = 'AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE'
    translator = GeminiCSVTranslator(API_KEY)
    
    test_products = [
        "나우푸드 실리마린 밀크 시슬 추출물 300mg 베지 캡슐, 200정, 1개",
        "나우푸드 더블 스트랭스 L-아르기닌 1000mg 타블렛, 120정, 1개",
        "나우푸드 프로바이오틱-10 유산균 250억 베지 캡슐, 100정, 1개",
        "나우푸드 울트라 오메가 3 500 EPA & 250 DHA 1000mg 피쉬 소프트젤, 180정, 1개",
        "나우푸드 데일리 비츠 멀티비타민 & 미네랄 타블렛, 250정, 1개"
    ]
    
    print("=== 배치 번역 테스트 ===")
    results = translator.translate_batch(test_products, batch_size=3)
    
    print("\n=== 최종 결과 ===")
    for original, translated in zip(test_products, results):
        print(f"{original} → {translated}")

def test_preprocessing():
    """전처리 함수 테스트"""
    translator = GeminiCSVTranslator('dummy_key')
    
    korean_samples = [
        "나우푸드 실리마린 밀크 시슬 추출물 300mg 베지 캡슐, 200정, 1개",
        "나우푸드 마카 500mg 베지 캡슐, 1개, 250정",
        "나우푸드 에센셜 아로마오일, 30ml, 1개입, Orange"
    ]
    
    english_samples = [
        "Now Foods Silymarin Milk Thistle Extract 300mg Veggie Capsules, 200 Count, 1 Bottle",
        "Now Foods Maca 500mg Veggie Capsules, 250 Count, 1 Pack",
        "Now Foods Essential Oil, 30ml, 1 Container, Orange"
    ]
    
    print("=== 한국어 전처리 테스트 ===")
    for text in korean_samples:
        result = translator.preprocess_korean_text(text)
        print(f"원본: {text}")
        print(f"전처리: {result}")
        print()
    
    print("=== 영어 검색 최적화 테스트 ===")
    for text in english_samples:
        result = translator.preprocess_english_for_search(text)
        print(f"원본: {text}")
        print(f"최적화: {result}")
        print()

if __name__ == "__main__":
    # 실행할 함수 선택
    # main()                    # 전체 CSV 번역
    # test_single_translation() # 단일 번역 테스트
    # test_batch_translation()  # 배치 번역 테스트
    # test_preprocessing()      # 전처리 함수 테스트
    
    main()  # 기본으로 전체 번역 실행