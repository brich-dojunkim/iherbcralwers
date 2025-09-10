"""
제품 매칭 및 OCR 테스트 모듈
두 제품의 OCR 정보를 비교하여 일치율 계산
패턴 없는 단순 매칭 방식 사용
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from image import process_image


class ProductMatcher:
    """제품 정보 비교 및 일치율 계산"""
    
    def __init__(self):
        """매처 초기화"""
        pass
    
    def normalize_text(self, text: str) -> str:
        """텍스트 정규화 (비교용)"""
        if not text:
            return ""
        
        return text.lower().strip().replace(" ", "").replace("-", "")
    
    def compare_brand(self, brand1: str, brand2: str) -> float:
        """브랜드명 비교"""
        if not brand1 or not brand2:
            return 0.0
        
        norm1 = self.normalize_text(brand1)
        norm2 = self.normalize_text(brand2)
        
        if norm1 == norm2:
            return 1.0
        
        # 부분 매칭 (NOW Foods vs NOW)
        if norm1 in norm2 or norm2 in norm1:
            return 0.8
        
        return 0.0
    
    def compare_product_name(self, name1: str, name2: str) -> float:
        """제품명 비교"""
        if not name1 or not name2:
            return 0.0
        
        norm1 = self.normalize_text(name1)
        norm2 = self.normalize_text(name2)
        
        if norm1 == norm2:
            return 1.0
        
        # 부분 매칭
        if norm1 in norm2 or norm2 in norm1:
            return 0.8
        
        return 0.0
    
    def extract_number(self, dosage_str: str) -> Optional[int]:
        """용량/개수 문자열에서 숫자 추출"""
        if not dosage_str:
            return None
        
        # 숫자만 추출
        numbers = ''.join(filter(str.isdigit, dosage_str))
        if numbers:
            return int(numbers)
        
        return None
    
    def compare_dosage(self, dosage1: str, dosage2: str) -> float:
        """용량 비교"""
        if not dosage1 or not dosage2:
            return 0.0
        
        # 숫자 추출
        num1 = self.extract_number(dosage1)
        num2 = self.extract_number(dosage2)
        
        if not num1 or not num2:
            return 0.0
        
        if num1 == num2:
            return 1.0
        
        # 단위가 다를 수 있으므로 어느 정도 허용
        ratio = min(num1, num2) / max(num1, num2)
        if ratio > 0.8:
            return 0.7
        
        return 0.0
    
    def compare_count(self, count1: str, count2: str) -> float:
        """개수 비교"""
        if not count1 or not count2:
            return 0.0
        
        # 숫자 추출
        num1 = self.extract_number(count1)
        num2 = self.extract_number(count2)
        
        if not num1 or not num2:
            return 0.0
        
        if num1 == num2:
            return 1.0
        
        # 개수는 정확해야 하므로 엄격하게
        return 0.0
    
    def compare_products(self, product1: Dict, product2: Dict) -> Dict:
        """두 제품 정보 전체 비교"""
        
        # 각 항목별 점수 계산
        brand_score = self.compare_brand(product1.get('brand'), product2.get('brand'))
        name_score = self.compare_product_name(product1.get('product_name'), product2.get('product_name'))
        dosage_score = self.compare_dosage(product1.get('dosage'), product2.get('dosage'))
        count_score = self.compare_count(product1.get('count'), product2.get('count'))
        
        # 전체 점수 계산 (가중 평균)
        weights = {
            'brand': 0.3,
            'product_name': 0.3,
            'dosage': 0.2,
            'count': 0.2
        }
        
        total_score = (
            brand_score * weights['brand'] +
            name_score * weights['product_name'] +
            dosage_score * weights['dosage'] +
            count_score * weights['count']
        )
        
        return {
            'brand_score': brand_score,
            'name_score': name_score,
            'dosage_score': dosage_score,
            'count_score': count_score,
            'total_score': total_score,
            'match_quality': 'high' if total_score >= 0.8 else 'medium' if total_score >= 0.5 else 'low'
        }


class OCRTester:
    """OCR 테스트 실행기"""
    
    def __init__(self, csv_path: str, coupang_images_dir: str, iherb_images_dir: str):
        """테스터 초기화"""
        self.csv_path = csv_path
        self.coupang_images_dir = coupang_images_dir
        self.iherb_images_dir = iherb_images_dir
        self.matcher = ProductMatcher()
        self.results = []
    
    def load_matched_products(self) -> pd.DataFrame:
        """매칭된 제품 데이터 로드"""
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {self.csv_path}")
        
        df = pd.read_csv(self.csv_path, encoding='utf-8-sig')
        
        # 매칭 성공한 상품들만 필터링
        success_df = df[df['status'] == 'success'].copy()
        
        print(f"✅ CSV 로드 완료: 전체 {len(df)}개, 매칭 성공 {len(success_df)}개")
        return success_df
    
    def find_image_pairs(self, df: pd.DataFrame, max_pairs: int = 30) -> List[Dict]:
        """이미지 쌍이 있는 제품들 찾기"""
        pairs = []
        
        print(f"🔍 이미지 쌍 찾는 중...")
        print(f"  쿠팡 이미지 디렉토리: {self.coupang_images_dir}")
        print(f"  아이허브 이미지 디렉토리: {self.iherb_images_dir}")
        
        for idx, row in df.iterrows():
            if len(pairs) >= max_pairs:
                break
            
            # 쿠팡 이미지 확인
            coupang_id = row.get('coupang_product_id')
            if pd.isna(coupang_id):
                continue
            
            coupang_image_path = os.path.join(
                self.coupang_images_dir, 
                f"coupang_{int(coupang_id)}.jpg"
            )
            
            if not os.path.exists(coupang_image_path):
                continue
            
            # 아이허브 이미지 확인 (상품코드 기반)
            iherb_code = row.get('iherb_product_code')
            if pd.isna(iherb_code):
                continue
            
            # 아이허브 이미지 파일명 패턴: iherb_XX_CODE.jpg 또는 다른 패턴들
            if not os.path.exists(self.iherb_images_dir):
                print(f"❌ 아이허브 이미지 디렉토리가 없습니다: {self.iherb_images_dir}")
                break
                
            iherb_files = [f for f in os.listdir(self.iherb_images_dir) 
                          if f.endswith('.jpg') and str(iherb_code) in f]
            
            if not iherb_files:
                continue
            
            iherb_image_path = os.path.join(self.iherb_images_dir, iherb_files[0])
            
            pairs.append({
                'csv_index': idx,
                'coupang_product_id': int(coupang_id),
                'iherb_product_code': iherb_code,
                'coupang_image_path': coupang_image_path,
                'iherb_image_path': iherb_image_path,
                'coupang_product_name': row.get('coupang_product_name', ''),
                'iherb_product_name': row.get('iherb_product_name', ''),
                'text_similarity': row.get('similarity_score', 0),
                'original_row': row
            })
        
        print(f"✅ 이미지 쌍 발견: {len(pairs)}개")
        return pairs
    
    def test_single_pair(self, pair: Dict) -> Dict:
        """단일 제품 쌍 테스트"""
        product_name = pair['coupang_product_name']
        display_name = product_name[:50] + "..." if len(product_name) > 50 else product_name
        
        print(f"  테스트 중: {display_name}")
        
        # 쿠팡 이미지 OCR
        print(f"    쿠팡 OCR: {os.path.basename(pair['coupang_image_path'])}")
        coupang_info = process_image(pair['coupang_image_path'])
        
        # 아이허브 이미지 OCR
        print(f"    아이허브 OCR: {os.path.basename(pair['iherb_image_path'])}")
        iherb_info = process_image(pair['iherb_image_path'])
        
        # 매칭 비교
        match_result = self.matcher.compare_products(coupang_info, iherb_info)
        
        # 결과 정리
        result = {
            'csv_index': pair['csv_index'],
            'coupang_product_id': pair['coupang_product_id'],
            'iherb_product_code': pair['iherb_product_code'],
            'coupang_product_name': pair['coupang_product_name'],
            'iherb_product_name': pair['iherb_product_name'],
            'text_similarity': pair['text_similarity'],
            
            # 쿠팡 OCR 결과
            'coupang_brand': coupang_info.get('brand'),
            'coupang_product': coupang_info.get('product_name'),
            'coupang_dosage': coupang_info.get('dosage'),
            'coupang_count': coupang_info.get('count'),
            'coupang_confidence': coupang_info.get('confidence', 0),
            
            # 아이허브 OCR 결과
            'iherb_brand': iherb_info.get('brand'),
            'iherb_product': iherb_info.get('product_name'),
            'iherb_dosage': iherb_info.get('dosage'),
            'iherb_count': iherb_info.get('count'),
            'iherb_confidence': iherb_info.get('confidence', 0),
            
            # 매칭 결과
            'brand_match_score': match_result['brand_score'],
            'name_match_score': match_result['name_score'],
            'dosage_match_score': match_result['dosage_score'],
            'count_match_score': match_result['count_score'],
            'total_match_score': match_result['total_score'],
            'match_quality': match_result['match_quality'],
            
            'test_datetime': datetime.now().isoformat()
        }
        
        # 추출 정보 간단히 출력
        print(f"    쿠팡 → 브랜드:{coupang_info.get('brand','?')} 제품:{coupang_info.get('product_name','?')} 용량:{coupang_info.get('dosage','?')} 개수:{coupang_info.get('count','?')}")
        print(f"    아이허브 → 브랜드:{iherb_info.get('brand','?')} 제품:{iherb_info.get('product_name','?')} 용량:{iherb_info.get('dosage','?')} 개수:{iherb_info.get('count','?')}")
        
        return result
    
    def run_test(self, max_pairs: int = 20) -> str:
        """전체 테스트 실행"""
        print("🔍 OCR 제품 매칭 검증 테스트 시작")
        print("=" * 60)
        print("패턴 없는 단순 매칭 방식 사용")
        print("- 브랜드: 키워드 매칭 + 유사도")
        print("- 제품명: 영양소명 키워드")  
        print("- 용량/개수: 숫자와 단위 근접성")
        print("=" * 60)
        
        # 1. 데이터 로드
        df = self.load_matched_products()
        
        # 2. 이미지 쌍 찾기
        pairs = self.find_image_pairs(df, max_pairs)
        
        if not pairs:
            print("❌ 테스트할 이미지 쌍이 없습니다.")
            return None
        
        print(f"\n📋 테스트 실행: {len(pairs)}개 쌍")
        print("=" * 60)
        
        # 3. 각 쌍 테스트
        for i, pair in enumerate(pairs, 1):
            print(f"\n[{i:02d}/{len(pairs)}]", end=" ")
            
            try:
                result = self.test_single_pair(pair)
                self.results.append(result)
                
                # 간단한 결과 출력
                score = result['total_match_score']
                quality = result['match_quality']
                print(f"    ✅ 매칭 점수: {score:.2f} ({quality})")
                
            except Exception as e:
                print(f"    ❌ 오류: {e}")
                continue
        
        # 4. 결과 저장
        output_file = self.save_results()
        
        # 5. 요약 출력
        self.print_summary()
        
        return output_file
    
    def save_results(self) -> str:
        """결과 CSV로 저장"""
        if not self.results:
            return None
        
        results_df = pd.DataFrame(self.results)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"ocr_test_results_{timestamp}.csv"
        
        results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n📁 결과 저장: {output_file}")
        
        return output_file
    
    def print_summary(self):
        """결과 요약 출력"""
        if not self.results:
            return
        
        total = len(self.results)
        high_quality = len([r for r in self.results if r['match_quality'] == 'high'])
        medium_quality = len([r for r in self.results if r['match_quality'] == 'medium'])
        low_quality = len([r for r in self.results if r['match_quality'] == 'low'])
        
        avg_score = sum(r['total_match_score'] for r in self.results) / total
        
        print(f"\n📊 테스트 결과 요약 (패턴 없는 단순 매칭)")
        print("=" * 50)
        print(f"총 테스트: {total}개")
        print(f"고품질 매칭 (≥0.8): {high_quality}개 ({high_quality/total*100:.1f}%)")
        print(f"중품질 매칭 (0.5-0.8): {medium_quality}개 ({medium_quality/total*100:.1f}%)")
        print(f"저품질 매칭 (<0.5): {low_quality}개 ({low_quality/total*100:.1f}%)")
        print(f"평균 매칭 점수: {avg_score:.3f}")
        
        # 항목별 성공률
        brand_success = len([r for r in self.results if r['brand_match_score'] > 0.5]) / total * 100
        name_success = len([r for r in self.results if r['name_match_score'] > 0.5]) / total * 100
        dosage_success = len([r for r in self.results if r['dosage_match_score'] > 0.5]) / total * 100
        count_success = len([r for r in self.results if r['count_match_score'] > 0.5]) / total * 100
        
        print(f"\n📈 항목별 성공률 (50% 이상):")
        print(f"브랜드: {brand_success:.1f}%")
        print(f"제품명: {name_success:.1f}%")
        print(f"용량: {dosage_success:.1f}%")
        print(f"개수: {count_success:.1f}%")
        
        # 신뢰도 분석
        avg_coupang_conf = sum(r['coupang_confidence'] for r in self.results) / total
        avg_iherb_conf = sum(r['iherb_confidence'] for r in self.results) / total
        
        print(f"\n🔍 OCR 신뢰도:")
        print(f"쿠팡 이미지 평균 신뢰도: {avg_coupang_conf:.3f}")
        print(f"아이허브 이미지 평균 신뢰도: {avg_iherb_conf:.3f}")
        
        # 우수 매칭 사례
        high_matches = [r for r in self.results if r['match_quality'] == 'high']
        if high_matches:
            print(f"\n🏆 우수 매칭 사례 (상위 3개):")
            sorted_matches = sorted(high_matches, key=lambda x: x['total_match_score'], reverse=True)
            for i, match in enumerate(sorted_matches[:3], 1):
                print(f"  {i}. {match['coupang_product_name'][:40]}...")
                print(f"     점수: {match['total_match_score']:.3f}")
                print(f"     브랜드: {match['coupang_brand']} ↔ {match['iherb_brand']}")
                print(f"     제품명: {match['coupang_product']} ↔ {match['iherb_product']}")
                print()


def main():
    """메인 실행 함수"""
    print("🚀 패턴 없는 단순 OCR 매칭 테스트")
    print("=" * 50)
    
    # 경로 설정
    csv_path = "../coupang/iherb_english_results_modular_1.csv"
    coupang_images_dir = "../coupang_images"
    iherb_images_dir = "../iherb_images"
    
    # 경로 존재 확인
    paths_to_check = [
        ("CSV 파일", csv_path),
        ("쿠팡 이미지", coupang_images_dir),
        ("아이허브 이미지", iherb_images_dir)
    ]
    
    print("📁 경로 확인:")
    for name, path in paths_to_check:
        exists = "✅" if os.path.exists(path) else "❌"
        print(f"  {exists} {name}: {path}")
    
    print()
    
    # 테스트 실행
    try:
        tester = OCRTester(csv_path, coupang_images_dir, iherb_images_dir)
        result_file = tester.run_test(max_pairs=15)  # 15개 샘플로 테스트
        
        if result_file:
            print(f"\n✅ 테스트 완료! 결과: {result_file}")
            print("\n📋 다음 단계:")
            print("1. 결과 CSV 파일에서 상세 분석")
            print("2. 성능이 70% 미만이면 VLM 추가 고려")
            print("3. 성능이 만족스러우면 전체 워크플로우 통합")
        else:
            print("\n❌ 테스트 실패")
            
    except FileNotFoundError as e:
        print(f"\n❌ 파일 오류: {e}")
        print("필요한 파일들이 올바른 위치에 있는지 확인하세요.")
    except Exception as e:
        print(f"\n❌ 실행 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()