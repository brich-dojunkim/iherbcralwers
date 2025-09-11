import google.generativeai as genai
import pandas as pd
import time
import os
import sys
import json
import re
from typing import List, Dict

# 설정
API_KEY = "AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE"
BATCH_SIZE = 10

class AILabeler:
    """CSV 파일을 입력받아 AI로 라벨링 (중단/재개 기능 포함)"""
    
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        # 가장 최신 안정 버전으로 변경
        self.model = genai.GenerativeModel('models/gemini-2.5-pro')
    
    def create_prompt(self, product_names: List[str]) -> str:
        """AI 프롬프트 생성 - 용량 추출 규칙 개선"""
        prompt = """
다음 건강기능식품 상품명들에서 핵심 정보를 추출해주세요.

상품명들:
"""
        for i, name in enumerate(product_names, 1):
            prompt += f"{i}. {name}\n"
        
        prompt += """
각 상품에 대해 다음 정보를 추출하여 CSV 형태로 응답해주세요:

ingredient,dosage_amount,dosage_unit,form_type,package_amount,package_unit,package_count

추출 규칙:
1. ingredient: 주성분명을 영문 표준명으로 (예: Calcium Carbonate, L-Carnitine, Omega-3)
2. dosage_amount: 1회 복용량 숫자 (예: 500, 1000, 25)  
3. dosage_unit: 1회 복용량 단위 소문자 (mg, g, iu, billion 등)
4. form_type: 제형 표준명 (capsule, tablet, softgel, powder, liquid)
5. package_amount: 전체 용량/함량 숫자 (예: 340, 473, 120, 180)
6. package_unit: 전체 용량/함량 단위 (g, ml, capsules, tablets)
7. package_count: 총 개수 (캡슐/정제 개수만, 예: 120, 180)

중요한 구분:
- package_amount + package_unit: 전체 제품의 용량 (340g, 473ml, 120 capsules)
- package_count: 캡슐/정제의 개수만 (powder, liquid 제품은 공란)

예시:
- "칼슘 카보네이트 퓨어 파우더, 340g" → package_amount: 340, package_unit: g, package_count: (공란)
- "포도씨 오일, 473ml" → package_amount: 473, package_unit: ml, package_count: (공란)  
- "비타민 D3 1000 IU, 180 캡슐" → package_amount: 180, package_unit: capsules, package_count: 180

중요: 해당 정보가 없으면 공란으로 두세요. 억지로 추측하지 마세요.

응답은 헤더 없이 데이터만 주세요.
"""
        return prompt
    
    def parse_response(self, response_text: str, original_data: List[Dict]) -> List[Dict]:
        """AI 응답 파싱 - 새로운 컬럼 구조에 맞게 수정"""
        lines = response_text.strip().split('\n')
        results = []
        
        for i, line in enumerate(lines):
            if i >= len(original_data):
                break
                
            parts = [p.strip() for p in line.split(',')]
            
            if len(parts) >= 7:
                result = original_data[i].copy()
                result.update({
                    'ingredient': parts[0] if parts[0] else '',
                    'dosage_amount': self._safe_int_convert(parts[1]),
                    'dosage_unit': parts[2].lower() if parts[2] else '',
                    'form_type': parts[3].lower() if parts[3] else '',
                    'package_amount': self._safe_int_convert(parts[4]),
                    'package_unit': parts[5].lower() if parts[5] else '',
                    'package_count': self._safe_int_convert(parts[6])
                })
                results.append(result)
            else:
                # 파싱 실패시 원본 데이터 유지
                results.append(original_data[i])
        
        return results
    
    def _safe_int_convert(self, value: str) -> str:
        """안전한 정수 변환"""
        if not value or not value.strip():
            return ''
        
        # 숫자만 추출
        numbers = re.findall(r'\d+', str(value).strip())
        if numbers:
            return int(numbers[0])
        return ''
    
    def fallback_extract_package_info(self, product_name: str) -> tuple:
        """프롬프트가 실패할 경우 정규식으로 패키지 정보 추출"""
        # 용량 패턴들
        patterns = [
            r'(\d+)\s*g\b',      # 340g
            r'(\d+)\s*ml\b',     # 473ml  
            r'(\d+)\s*mg\b',     # 500mg
            r'(\d+)\s*캡슐',      # 120캡슐
            r'(\d+)\s*정\b',      # 60정
            r'(\d+)\s*tablets?\b', # 120 tablets
            r'(\d+)\s*capsules?\b', # 180 capsules
        ]
        
        package_amount = ''
        package_unit = ''
        package_count = ''
        
        # g, ml 우선 검색 (용량)
        for pattern in patterns[:3]:
            match = re.search(pattern, product_name, re.IGNORECASE)
            if match:
                package_amount = int(match.group(1))
                if 'g' in pattern:
                    package_unit = 'g'
                elif 'ml' in pattern:
                    package_unit = 'ml'
                elif 'mg' in pattern:
                    package_unit = 'mg'
                break
        
        # 캡슐/정제 개수 검색
        for pattern in patterns[3:]:
            match = re.search(pattern, product_name, re.IGNORECASE)
            if match:
                count = int(match.group(1))
                package_count = count
                if not package_amount:  # 용량이 없으면 개수를 용량으로도 사용
                    package_amount = count
                    if '캡슐' in pattern or 'capsule' in pattern:
                        package_unit = 'capsules'
                    elif '정' in pattern or 'tablet' in pattern:
                        package_unit = 'tablets'
                break
        
        return package_amount, package_unit, package_count
    
    def save_progress(self, progress_file: str, batch_idx: int, processed_data: List[Dict]):
        """진행 상황 저장"""
        progress_info = {
            'last_batch_idx': batch_idx,
            'total_processed': len(processed_data)
        }
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_info, f, ensure_ascii=False, indent=2)
    
    def load_progress(self, progress_file: str) -> Dict:
        """진행 상황 로드"""
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {'last_batch_idx': -1, 'total_processed': 0}
        return {'last_batch_idx': -1, 'total_processed': 0}
    
    def is_quota_error(self, error_msg: str) -> bool:
        """쿼터/한도 관련 오류인지 확인"""
        quota_keywords = [
            'quota', 'limit', 'rate limit', 'too many requests',
            'exceeded', 'resource exhausted', '429', 'quota exceeded'
        ]
        error_msg_lower = str(error_msg).lower()
        return any(keyword in error_msg_lower for keyword in quota_keywords)
    
    def save_cumulative_results(self, output_csv: str, results: List[Dict]):
        """누적 결과 저장"""
        if results:
            result_df = pd.DataFrame(results)
            result_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    
    def process_csv(self, input_csv: str, output_csv: str, batch_size: int = 10, product_name_column: str = None):
        """CSV 파일 처리 (중단/재개 기능 포함)"""
        # 파일명 기반으로 진행상황 파일 생성
        progress_file = f"{input_csv.replace('.csv', '')}_progress.json"
        temp_output = f"{output_csv.replace('.csv', '')}_temp.csv"
        
        print(f"🤖 AI 라벨링 시작: {input_csv}")
        
        df = pd.read_csv(input_csv)
        print(f"총 {len(df)}개 상품")
        
        # 상품명 컬럼 자동 감지
        if product_name_column is None:
            if 'product_name_english' in df.columns:
                product_name_column = 'product_name_english'
            elif 'product_name' in df.columns:
                product_name_column = 'product_name'
            else:
                raise ValueError("상품명 컬럼을 찾을 수 없습니다.")
        
        # 기존 진행 상황 확인
        progress = self.load_progress(progress_file)
        start_batch_idx = progress['last_batch_idx'] + 1
        
        # 기존 결과 로드 (있다면)
        all_results = []
        if os.path.exists(temp_output) and progress['total_processed'] > 0:
            existing_df = pd.read_csv(temp_output)
            all_results = existing_df.to_dict('records')
            print(f"📄 기존 결과 로드: {len(all_results)}개 ({progress['total_processed']}개 처리 완료)")
        
        print(f"사용할 상품명 컬럼: {product_name_column}")
        print(f"배치 크기: {batch_size}개씩 처리")
        
        total_batches = (len(df) + batch_size - 1) // batch_size
        remaining_batches = total_batches - start_batch_idx
        
        if start_batch_idx > 0:
            print(f"🔄 중단된 작업 재개: 배치 {start_batch_idx + 1}부터 시작")
            print(f"남은 배치: {remaining_batches}개 / 전체: {total_batches}개")
        else:
            print(f"총 {total_batches}개 배치로 처리 예정")
        
        print("=" * 50)
        
        successful_batches = 0
        failed_batches = 0
        
        try:
            for i in range(start_batch_idx * batch_size, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                batch_num = i//batch_size + 1
                
                print(f"\n📦 배치 {batch_num}/{total_batches} 처리 중...")
                print(f"   범위: {i+1}~{min(i+batch_size, len(df))}번째 상품 ({len(batch_df)}개)")
                
                # 상품명 추출
                product_names = batch_df[product_name_column].tolist()
                
                # 첫 번째 상품명 미리보기
                if len(product_names) > 0:
                    print(f"   예시: {product_names[0][:60]}{'...' if len(product_names[0]) > 60 else ''}")
                
                # 원본 데이터
                batch_data = batch_df.to_dict('records')
                
                try:
                    print("   🔄 AI 처리 중...", end=" ", flush=True)
                    prompt = self.create_prompt(product_names)
                    response = self.model.generate_content(prompt)
                    results = self.parse_response(response.text, batch_data)
                    
                    # AI 파싱 실패한 경우 정규식 폴백 적용
                    for j, result in enumerate(results):
                        if not result.get('package_amount') and not result.get('package_unit'):
                            # 정규식으로 패키지 정보 추출 시도
                            product_name = product_names[j]
                            pkg_amount, pkg_unit, pkg_count = self.fallback_extract_package_info(product_name)
                            if pkg_amount:
                                result['package_amount'] = pkg_amount
                                result['package_unit'] = pkg_unit
                                result['package_count'] = pkg_count if pkg_count else ''
                    
                    all_results.extend(results)
                    
                    successful_batches += 1
                    print("✅ 성공!")
                    
                    # 진행률 계산
                    progress_pct = (batch_num / total_batches) * 100
                    processed_items = len(all_results)
                    print(f"   진행률: {progress_pct:.1f}% ({processed_items}/{len(df)}개 완료)")
                    
                    # 누적 결과 저장 (매 배치마다)
                    self.save_cumulative_results(temp_output, all_results)
                    
                    # 진행 상황 저장
                    self.save_progress(progress_file, batch_num - 1, all_results)
                    
                    print("   ⏱️ 2초 대기...", end=" ", flush=True)
                    time.sleep(2)  # API 제한 대응
                    print("완료")
                    
                except Exception as e:
                    print(f"❌ 실패!")
                    error_msg = str(e)
                    print(f"   오류: {error_msg[:100]}{'...' if len(error_msg) > 100 else ''}")
                    
                    # 쿼터/한도 관련 오류 확인
                    if self.is_quota_error(error_msg):
                        print("\n🚨 쿼터/한도 제한 오류 감지!")
                        print("💾 현재까지의 결과를 저장하고 작업을 중단합니다.")
                        
                        # 현재까지 결과 저장
                        if all_results:
                            self.save_cumulative_results(temp_output, all_results)
                            self.save_progress(progress_file, batch_num - 2, all_results)  # 실패한 배치 전까지
                        
                        print(f"📁 임시 결과 저장됨: {temp_output}")
                        print(f"🔄 다음 실행 시 배치 {batch_num}부터 재개됩니다.")
                        print("\n⏰ 잠시 후 다시 실행해주세요:")
                        print(f"   python ai_labeler.py {input_csv}")
                        return None
                    
                    else:
                        # 일반 오류인 경우 정규식 폴백 시도 후 원본 데이터로 계속 진행
                        failed_batches += 1
                        print("   🔧 정규식 폴백 시도 중...")
                        
                        # 정규식으로 패키지 정보만 추출
                        for j, data in enumerate(batch_data):
                            product_name = product_names[j]
                            pkg_amount, pkg_unit, pkg_count = self.fallback_extract_package_info(product_name)
                            if pkg_amount:
                                data['package_amount'] = pkg_amount
                                data['package_unit'] = pkg_unit  
                                data['package_count'] = pkg_count if pkg_count else ''
                        
                        print("   원본 데이터 유지하여 계속 진행...")
                        all_results.extend(batch_data)
                        
                        # 누적 결과 저장
                        self.save_cumulative_results(temp_output, all_results)
                        self.save_progress(progress_file, batch_num - 1, all_results)
        
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단되었습니다.")
            if all_results:
                self.save_cumulative_results(temp_output, all_results)
                self.save_progress(progress_file, batch_num - 1, all_results)
                print(f"💾 현재까지 결과 저장됨: {temp_output}")
            return None
        
        print("\n" + "=" * 50)
        print("🎯 배치 처리 완료!")
        print(f"✅ 성공: {successful_batches}개 배치")
        print(f"❌ 실패: {failed_batches}개 배치")
        print(f"📊 총 처리: {len(all_results)}개 상품")
        
        # 최종 결과 저장
        print(f"\n💾 최종 결과 저장 중: {output_csv}")
        result_df = pd.DataFrame(all_results)
        result_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        
        # 라벨링 성공률 계산
        labeled_count = sum(1 for item in all_results if item.get('ingredient', '') != '')
        package_count = sum(1 for item in all_results if item.get('package_amount', '') != '')
        success_rate = (labeled_count / len(all_results)) * 100 if all_results else 0
        package_rate = (package_count / len(all_results)) * 100 if all_results else 0
        
        print(f"✅ 저장 완료!")
        print(f"📈 라벨링 성공률: {success_rate:.1f}% ({labeled_count}/{len(all_results)}개)")
        print(f"📦 패키지 추출률: {package_rate:.1f}% ({package_count}/{len(all_results)}개)")
        
        # 임시 파일 및 진행상황 파일 정리
        if os.path.exists(temp_output):
            os.remove(temp_output)
            print("🗑️ 임시 파일 정리 완료")
        
        if os.path.exists(progress_file):
            os.remove(progress_file)
            print("🗑️ 진행상황 파일 정리 완료")
        
        return result_df
    
    def process_excel_now_foods(self, input_xlsx: str, output_csv: str, batch_size: int = 10):
        """아이허브 Excel 파일에서 NOW Foods만 처리 (중단/재개 기능 포함)"""
        print(f"🌿 아이허브 NOW Foods 라벨링 시작: {input_xlsx}")
        
        df = pd.read_excel(input_xlsx)
        print(f"전체 {len(df)}개 상품")
        
        # NOW Foods만 필터링
        now_foods_df = df[df['product_brand'].str.contains('NOW', case=False, na=False)]
        print(f"NOW Foods: {len(now_foods_df)}개")
        
        # source 컬럼 추가
        now_foods_df = now_foods_df.copy()
        now_foods_df['source'] = 'iherb'
        
        # CSV로 저장 후 처리
        temp_csv = 'temp_iherb_now_foods.csv'
        now_foods_df.to_csv(temp_csv, index=False, encoding='utf-8-sig')
        
        # 라벨링 처리 (중단/재개 기능 포함)
        result = self.process_csv(temp_csv, output_csv, batch_size, 'product_name')
        
        # 임시 파일 삭제
        if os.path.exists(temp_csv):
            os.remove(temp_csv)
        
        return result

def main():
    """메인 실행 함수"""
    if len(sys.argv) < 2:
        print("사용법:")
        print("python ai_labeler.py <input_file>")
        print("예시:")
        print("python ai_labeler.py coupang_products_translated.csv")
        print("python ai_labeler.py 'US ITEM FEED TITLE BRAND EN.xlsx'")
        return
    
    input_file = sys.argv[1]
    labeler = AILabeler(API_KEY)
    
    print(f"=== AI 라벨링 시작: {input_file} ===")
    
    try:
        if input_file.endswith('.xlsx'):
            # Excel 파일 (아이허브)
            output_file = input_file.replace('.xlsx', '_labeled.csv')
            result = labeler.process_excel_now_foods(input_file, output_file, BATCH_SIZE)
        else:
            # CSV 파일 (쿠팡)
            output_file = input_file.replace('.csv', '_labeled.csv')
            result = labeler.process_csv(input_file, output_file, BATCH_SIZE)
        
        if result is not None:
            print(f"✅ 라벨링 완료! 결과: {output_file}")
        
    except Exception as e:
        print(f"❌ 라벨링 실패: {e}")

if __name__ == "__main__":
    main()