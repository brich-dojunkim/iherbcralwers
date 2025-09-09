"""
테스트 실행 스크립트
아이허브 50개 이미지 매칭 검증
"""

import os
import pandas as pd
from datetime import datetime
from image import OCRProcessor

# ==================== 경로 설정 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "..", "coupang", "iherb_english_results_modular_1.csv")
IHERB_IMAGES_DIR = os.path.join(BASE_DIR, "..", "iherb_images")
COUPANG_IMAGES_DIR = os.path.join(BASE_DIR, "..", "coupang_images")


class Tester:
    """테스트 실행 클래스"""
    
    def __init__(self):
        self.ocr = OCRProcessor()
        self.results = []
    
    def load_csv(self) -> pd.DataFrame:
        """CSV 데이터 로드"""
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {CSV_PATH}")
        
        df = pd.read_csv(CSV_PATH, encoding='utf-8-sig')
        print(f"✅ CSV 로드 완료: {len(df)}개 레코드")
        return df
    
    def find_matching_pairs(self, df: pd.DataFrame) -> list:
        """아이허브 이미지가 있는 매칭 쌍들 찾기"""
        if not os.path.exists(IHERB_IMAGES_DIR):
            raise FileNotFoundError(f"아이허브 이미지 디렉토리를 찾을 수 없습니다: {IHERB_IMAGES_DIR}")
        
        # 아이허브 이미지 파일 목록
        iherb_files = [f for f in os.listdir(IHERB_IMAGES_DIR) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
        print(f"📂 아이허브 이미지 파일: {len(iherb_files)}개")
        
        # 매칭 쌍들 수집
        pairs = []
        
        for iherb_file in iherb_files:
            # 파일명에서 상품코드 추출
            product_code = self.ocr.extract_product_code_from_filename(iherb_file)
            if not product_code:
                print(f"⚠️  상품코드 추출 실패: {iherb_file}")
                continue
            
            # CSV에서 해당 상품코드로 매칭된 레코드 찾기
            matches = df[df['iherb_product_code'] == product_code]
            
            if len(matches) == 0:
                print(f"⚠️  CSV에서 매칭 레코드 없음: {product_code}")
                continue
            
            # 여러 매칭이 있으면 첫 번째 것 사용
            match_row = matches.iloc[0]
            
            # 쿠팡 상품ID 확인
            coupang_product_id = match_row.get('coupang_product_id')
            if pd.isna(coupang_product_id):
                print(f"⚠️  쿠팡 상품ID 없음: {product_code}")
                continue
            
            # 쿠팡 이미지 파일 존재 확인
            coupang_image_file = f"coupang_{int(coupang_product_id)}.jpg"
            coupang_image_path = os.path.join(COUPANG_IMAGES_DIR, coupang_image_file)
            
            if not os.path.exists(coupang_image_path):
                print(f"⚠️  쿠팡 이미지 없음: {coupang_image_file}")
                continue
            
            # 매칭 쌍 정보 저장
            pairs.append({
                'iherb_image_file': iherb_file,
                'iherb_product_code': product_code,
                'coupang_image_file': coupang_image_file,
                'coupang_product_id': int(coupang_product_id),
                'csv_index': match_row.name,
                'text_similarity_score': match_row.get('similarity_score', 0),
                'iherb_product_name': match_row.get('iherb_product_name', ''),
                'coupang_product_name': match_row.get('coupang_product_name', ''),
                'coupang_product_name_english': match_row.get('coupang_product_name_english', ''),
                'text_matching_reason': match_row.get('matching_reason', ''),
                'match_status': match_row.get('status', ''),
                'iherb_product_url': match_row.get('iherb_product_url', ''),
                'coupang_url': match_row.get('coupang_url', ''),
                'original_row': match_row.to_dict()
            })
        
        print(f"✅ 매칭 쌍 발견: {len(pairs)}개")
        return pairs
    
    def run_validation(self):
        """매칭 검증 실행"""
        print("🔍 아이허브 이미지 매칭 검증 시작")
        print("=" * 60)
        
        # 1. 데이터 로드
        df = self.load_csv()
        
        # 2. 매칭 쌍들 찾기
        pairs = self.find_matching_pairs(df)
        
        if len(pairs) == 0:
            print("❌ 검증할 매칭 쌍이 없습니다.")
            return
        
        print(f"\n📋 검증 대상: {len(pairs)}개 매칭 쌍")
        print("=" * 60)
        
        # 3. 각 쌍에 대해 검증 수행
        for i, pair in enumerate(pairs):
            print(f"\n[{i+1}/{len(pairs)}] 검증 중...")
            
            # 이미지 경로
            iherb_path = os.path.join(IHERB_IMAGES_DIR, pair['iherb_image_file'])
            coupang_path = os.path.join(COUPANG_IMAGES_DIR, pair['coupang_image_file'])
            
            print(f"  📄 아이허브: {pair['iherb_image_file']}")
            print(f"  📄 쿠팡: {pair['coupang_image_file']}")
            print(f"  📊 기존 텍스트 점수: {pair['text_similarity_score']:.3f}")
            
            # OCR 수행
            print("  🔍 OCR 수행 중...")
            print("  --- 아이허브 OCR ---")
            iherb_info = self.ocr.extract_text_from_image(iherb_path)
            print("  --- 쿠팡 OCR ---")
            coupang_info = self.ocr.extract_text_from_image(coupang_path)
            
            # OCR 결과 표시
            if 'error' not in iherb_info and 'error' not in coupang_info:
                print(f"  ✅ OCR 성공")
                print(f"     아이허브: 브랜드={iherb_info.get('brand', 'N/A')}, 개수={iherb_info.get('count', 'N/A')}, 용량={iherb_info.get('dosage_mg', 'N/A')}mg")
                print(f"     쿠팡:     브랜드={coupang_info.get('brand', 'N/A')}, 개수={coupang_info.get('count', 'N/A')}, 용량={coupang_info.get('dosage_mg', 'N/A')}mg")
                
                # 이미지 유사도 계산
                image_score, details = self.ocr.calculate_similarity(iherb_info, coupang_info)
                validation_status = "success" if image_score > 0.6 else "failed"
                
                print(f"  📊 이미지 매칭 점수: {image_score:.3f}")
                print(f"  🎯 검증 결과: {validation_status}")
                print(f"     브랜드 매칭: {details.get('brand_match', False)}")
                print(f"     개수 매칭: {details.get('count_match', False)}")
                print(f"     용량 매칭: {details.get('dosage_match', False)}")
                
            else:
                image_score = 0.0
                details = {}
                validation_status = "ocr_error"
                print(f"  ❌ OCR 실패")
                if 'error' in iherb_info:
                    print(f"     아이허브 오류: {iherb_info['error']}")
                if 'error' in coupang_info:
                    print(f"     쿠팡 오류: {coupang_info['error']}")
            
            # 결과 저장
            result = {
                'validation_index': i + 1,
                'csv_index': pair['csv_index'],
                'iherb_image_file': pair['iherb_image_file'],
                'coupang_image_file': pair['coupang_image_file'],
                'iherb_product_code': pair['iherb_product_code'],
                'coupang_product_id': pair['coupang_product_id'],
                'iherb_product_name': pair['iherb_product_name'],
                'coupang_product_name': pair['coupang_product_name'],
                'coupang_product_name_english': pair['coupang_product_name_english'],
                'iherb_product_url': pair['iherb_product_url'],
                'coupang_url': pair['coupang_url'],
                'original_match_status': pair['match_status'],
                'text_similarity_score': pair['text_similarity_score'],
                'text_matching_reason': pair['text_matching_reason'],
                'image_similarity_score': image_score,
                'image_validation_status': validation_status,
                'score_difference': image_score - pair['text_similarity_score'],
                'brand_match': details.get('brand_match', False),
                'count_match': details.get('count_match', False),
                'dosage_match': details.get('dosage_match', False),
                'keyword_match': details.get('keyword_match', False),
                'text_similarity': details.get('text_similarity', 0),
                'iherb_ocr_brand': iherb_info.get('brand', ''),
                'coupang_ocr_brand': coupang_info.get('brand', ''),
                'iherb_ocr_count': iherb_info.get('count', ''),
                'coupang_ocr_count': coupang_info.get('count', ''),
                'iherb_ocr_dosage_mg': iherb_info.get('dosage_mg', ''),
                'coupang_ocr_dosage_mg': coupang_info.get('dosage_mg', ''),
                'iherb_ocr_keywords': str(iherb_info.get('keywords', [])),
                'coupang_ocr_keywords': str(coupang_info.get('keywords', [])),
                'validation_datetime': datetime.now().isoformat()
            }
            
            self.results.append(result)
        
        # 4. 결과 저장 및 요약
        self.save_results()
    
    def save_results(self):
        """결과 저장 및 요약 출력"""
        # CSV 저장
        results_df = pd.DataFrame(self.results)
        output_file = 'results.csv'
        results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        # 요약 통계
        total = len(self.results)
        successful_ocr = len([r for r in self.results if r['image_validation_status'] != 'ocr_error'])
        image_success = len([r for r in self.results if r['image_validation_status'] == 'success'])
        
        print(f"\n" + "="*60)
        print(f"✅ 검증 완료!")
        print(f"\n📊 최종 결과:")
        print(f"  총 검증 쌍: {total}개")
        print(f"  OCR 성공: {successful_ocr}개 ({successful_ocr/total*100:.1f}%)")
        print(f"  이미지 매칭 성공: {image_success}개 ({image_success/total*100:.1f}%)")
        
        if successful_ocr > 0:
            # 브랜드 매칭 통계
            brand_matches = len([r for r in self.results if r['brand_match']])
            count_matches = len([r for r in self.results if r['count_match']])
            dosage_matches = len([r for r in self.results if r['dosage_match']])
            
            print(f"\n📈 세부 매칭 통계:")
            print(f"  브랜드 매칭: {brand_matches}개 ({brand_matches/successful_ocr*100:.1f}%)")
            print(f"  개수 매칭: {count_matches}개 ({count_matches/successful_ocr*100:.1f}%)")
            print(f"  용량 매칭: {dosage_matches}개 ({dosage_matches/successful_ocr*100:.1f}%)")
            
            # 점수 비교
            score_improved = len([r for r in self.results if r['score_difference'] > 0.1])
            score_degraded = len([r for r in self.results if r['score_difference'] < -0.1])
            
            print(f"\n📊 점수 비교:")
            print(f"  이미지가 더 좋음: {score_improved}개")
            print(f"  텍스트가 더 좋음: {score_degraded}개")
        
        print(f"\n📁 결과 파일: {output_file}")
        print(f"💡 이 파일을 열어서 상세한 분석을 진행하세요.")


def main():
    """메인 실행 함수"""
    print("🔍 아이허브 50개 이미지 매칭 검증")
    print("=" * 50)
    
    try:
        tester = Tester()
        tester.run_validation()
        
    except FileNotFoundError as e:
        print(f"❌ 파일/디렉토리 오류: {e}")
    except Exception as e:
        print(f"❌ 실행 중 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()