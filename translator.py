import google.generativeai as genai
import pandas as pd
import csv
import re
from typing import List, Dict, Tuple
import time

class ProductLabeler:
    """Google Generative AI를 활용한 상품 라벨링 시스템"""
    
    def __init__(self, api_key: str):
        """초기화"""
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-pro')
        
        # CSV 출력 컬럼 정의
        self.output_columns = [
            'source',           # 'coupang' or 'iherb'
            'original_id',      # 원본 product_id
            'original_name',    # 원본 상품명
            'ingredient',       # 주성분 (표준화)
            'dosage_amount',    # 함량 숫자
            'dosage_unit',      # 함량 단위 (mg, g, iu 등)
            'form_type',        # 제형 (capsule, tablet, softgel 등)
            'package_count',    # 개수
            'confidence_score', # AI 추출 신뢰도 (0-1)
            'extraction_notes'  # 추가 메모
        ]
    
    def create_prompt(self, product_names: List[str]) -> str:
        """AI 프롬프트 생성 (배치 처리용)"""
        prompt = """
다음 건강기능식품 상품명들에서 핵심 정보를 추출해주세요.

상품명들:
"""
        for i, name in enumerate(product_names, 1):
            prompt += f"{i}. {name}\n"
        
        prompt += """
각 상품에 대해 다음 정보를 추출하여 CSV 형태로 응답해주세요:

ingredient,dosage_amount,dosage_unit,form_type,package_count,confidence_score

추출 규칙:
1. ingredient: 주성분명을 영문 표준명으로 (예: L-Carnitine, Magnesium, Omega-3)
2. dosage_amount: 함량 숫자만 (예: 500, 1000, 25)  
3. dosage_unit: 단위만 소문자로 (mg, g, iu, billion 등)
4. form_type: 제형 표준명 (capsule, tablet, softgel, powder, liquid)
5. package_count: 총 개수 (예: 180, 120, 60)
6. confidence_score: 추출 신뢰도 0.0-1.0

예시:
L-Carnitine,500,mg,capsule,180,0.95
Magnesium,400,mg,capsule,180,0.90

응답은 헤더 없이 데이터만 주세요.
"""
        return prompt
    
    def parse_ai_response(self, response_text: str, original_data: List[Dict]) -> List[Dict]:
        """AI 응답을 파싱하여 구조화된 데이터로 변환"""
        lines = response_text.strip().split('\n')
        results = []
        
        for i, line in enumerate(lines):
            if i >= len(original_data):
                break
                
            parts = line.split(',')
            if len(parts) >= 6:
                result = {
                    'source': original_data[i]['source'],
                    'original_id': original_data[i]['original_id'],
                    'original_name': original_data[i]['original_name'],
                    'ingredient': parts[0].strip(),
                    'dosage_amount': self._safe_convert(parts[1].strip(), int, 0),
                    'dosage_unit': parts[2].strip().lower(),
                    'form_type': parts[3].strip().lower(),
                    'package_count': self._safe_convert(parts[4].strip(), int, 0),
                    'confidence_score': self._safe_convert(parts[5].strip(), float, 0.0),
                    'extraction_notes': ''
                }
                results.append(result)
            else:
                # 파싱 실패시 빈 결과 추가
                results.append({
                    'source': original_data[i]['source'],
                    'original_id': original_data[i]['original_id'], 
                    'original_name': original_data[i]['original_name'],
                    'ingredient': '',
                    'dosage_amount': 0,
                    'dosage_unit': '',
                    'form_type': '',
                    'package_count': 0,
                    'confidence_score': 0.0,
                    'extraction_notes': 'AI 파싱 실패'
                })
        
        return results
    
    def _safe_convert(self, value: str, convert_type, default):
        """안전한 타입 변환"""
        try:
            return convert_type(value)
        except:
            return default
    
    def process_coupang_data(self, coupang_df: pd.DataFrame, batch_size: int = 10) -> List[Dict]:
        """쿠팡 데이터 처리"""
        results = []
        
        for i in range(0, len(coupang_df), batch_size):
            batch = coupang_df.iloc[i:i+batch_size]
            
            # 배치 데이터 준비
            batch_data = []
            product_names = []
            
            for _, row in batch.iterrows():
                # 영문명 우선, 없으면 한국어명 사용
                product_name = row.get('product_name_english', '') or row.get('product_name', '')
                product_names.append(product_name)
                
                batch_data.append({
                    'source': 'coupang',
                    'original_id': row.get('product_id', ''),
                    'original_name': product_name
                })
            
            # AI 처리
            try:
                prompt = self.create_prompt(product_names)
                response = self.model.generate_content(prompt)
                batch_results = self.parse_ai_response(response.text, batch_data)
                results.extend(batch_results)
                
                print(f"쿠팡 배치 {i//batch_size + 1} 완료: {len(batch_results)}개 처리")
                time.sleep(1)  # API 레이트 리미트 고려
                
            except Exception as e:
                print(f"쿠팡 배치 {i//batch_size + 1} 오류: {e}")
                # 오류시 빈 결과 추가
                for data in batch_data:
                    data.update({
                        'ingredient': '', 'dosage_amount': 0, 'dosage_unit': '',
                        'form_type': '', 'package_count': 0, 'confidence_score': 0.0,
                        'extraction_notes': f'API 오류: {str(e)}'
                    })
                results.extend(batch_data)
        
        return results
    
    def process_iherb_data(self, iherb_df: pd.DataFrame, batch_size: int = 10) -> List[Dict]:
        """아이허브 데이터 처리 (NOW Foods만)"""
        # NOW Foods 필터링
        now_foods_df = iherb_df[
            iherb_df['product_brand'].str.contains('NOW', case=False, na=False)
        ].copy()
        
        print(f"아이허브 NOW Foods 제품: {len(now_foods_df)}개")
        
        results = []
        
        for i in range(0, len(now_foods_df), batch_size):
            batch = now_foods_df.iloc[i:i+batch_size]
            
            # 배치 데이터 준비
            batch_data = []
            product_names = []
            
            for _, row in batch.iterrows():
                product_name = row.get('product_name', '')
                product_names.append(product_name)
                
                batch_data.append({
                    'source': 'iherb',
                    'original_id': row.get('product_id', ''),
                    'original_name': product_name
                })
            
            # AI 처리
            try:
                prompt = self.create_prompt(product_names)
                response = self.model.generate_content(prompt)
                batch_results = self.parse_ai_response(response.text, batch_data)
                results.extend(batch_results)
                
                print(f"아이허브 배치 {i//batch_size + 1} 완료: {len(batch_results)}개 처리")
                time.sleep(1)  # API 레이트 리미트 고려
                
            except Exception as e:
                print(f"아이허브 배치 {i//batch_size + 1} 오류: {e}")
                # 오류시 빈 결과 추가
                for data in batch_data:
                    data.update({
                        'ingredient': '', 'dosage_amount': 0, 'dosage_unit': '',
                        'form_type': '', 'package_count': 0, 'confidence_score': 0.0,
                        'extraction_notes': f'API 오류: {str(e)}'
                    })
                results.extend(batch_data)
        
        return results
    
    def save_to_csv(self, results: List[Dict], filename: str):
        """결과를 CSV로 저장"""
        df = pd.DataFrame(results)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"결과 저장 완료: {filename} ({len(results)}개 레코드)")
    
    def run_full_labeling(self, coupang_csv: str, iherb_xlsx: str, output_csv: str):
        """전체 라벨링 프로세스 실행"""
        print("🤖 AI 상품 라벨링 시작")
        
        # 데이터 로드
        print("📁 데이터 로드 중...")
        coupang_df = pd.read_csv(coupang_csv)
        iherb_df = pd.read_excel(iherb_xlsx)
        
        print(f"쿠팡 데이터: {len(coupang_df)}개")
        print(f"아이허브 데이터: {len(iherb_df)}개")
        
        # 라벨링 실행
        all_results = []
        
        print("\n🛒 쿠팡 데이터 처리 중...")
        coupang_results = self.process_coupang_data(coupang_df)
        all_results.extend(coupang_results)
        
        print("\n🌿 아이허브 데이터 처리 중...")
        iherb_results = self.process_iherb_data(iherb_df)
        all_results.extend(iherb_results)
        
        # 결과 저장
        print(f"\n💾 결과 저장 중... (총 {len(all_results)}개)")
        self.save_to_csv(all_results, output_csv)
        
        # 요약 통계
        df = pd.DataFrame(all_results)
        print(f"\n📊 처리 결과:")
        print(f"- 쿠팡: {len(df[df['source']=='coupang'])}개")
        print(f"- 아이허브: {len(df[df['source']=='iherb'])}개") 
        print(f"- 평균 신뢰도: {df['confidence_score'].mean():.2f}")
        print(f"- 추출 성공률: {(df['confidence_score'] > 0.5).mean()*100:.1f}%")

# 사용 예시
if __name__ == "__main__":
    # API 키 설정
    API_KEY = "your_google_ai_api_key_here"
    
    # 라벨러 초기화
    labeler = ProductLabeler(API_KEY)
    
    # 전체 프로세스 실행
    labeler.run_full_labeling(
        coupang_csv="coupang_products_translated.csv",
        iherb_xlsx="US ITEM FEED  TITLE BRAND EN.xlsx", 
        output_csv="labeled_products.csv"
    )