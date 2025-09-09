"""
단순화된 이미지 비교 테스트
- 기존에 다운로드된 이미지만 사용
- 새로운 크롤링 없음
- URL 정보 포함한 상세 결과
"""

import os
import pandas as pd
import json
from datetime import datetime
from image import (
    load_rgb, ocr_text, extract_fields, same_product_by_text
)

class SimpleImageTest:
    """기존 이미지로 비교 테스트만 수행"""
    
    def __init__(self, output_dir="simple_test_results"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self.test_results = []
    
    def load_existing_data(self, csv_file_path, coupang_csv_path, coupang_images_dir):
        """기존 데이터 로드"""
        try:
            print("=== 기존 데이터 로드 ===")
            
            # 1. 기존 매칭 결과 로드
            if not os.path.exists(csv_file_path):
                print(f"매칭 결과 파일 없음: {csv_file_path}")
                return None
            
            matching_df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
            success_df = matching_df[matching_df['status'] == 'success'].copy()
            print(f"매칭 성공 상품: {len(success_df)}개")
            
            # 2. 쿠팡 크롤링 결과 로드
            if not os.path.exists(coupang_csv_path):
                print(f"쿠팡 크롤링 결과 없음: {coupang_csv_path}")
                return None
            
            coupang_df = pd.read_csv(coupang_csv_path, encoding='utf-8-sig')
            print(f"쿠팡 상품 데이터: {len(coupang_df)}개")
            
            # 3. 이미지 매핑 생성
            coupang_image_mapping = {}
            image_count = 0
            
            for _, row in coupang_df.iterrows():
                product_id = str(row['product_id'])
                image_filename = f"coupang_{product_id}.jpg"
                image_path = os.path.join(coupang_images_dir, image_filename)
                
                if os.path.exists(image_path):
                    coupang_image_mapping[product_id] = image_path
                    image_count += 1
            
            print(f"사용 가능한 쿠팡 이미지: {image_count}개")
            
            return success_df, coupang_df, coupang_image_mapping
            
        except Exception as e:
            print(f"데이터 로드 실패: {e}")
            return None
    
    def select_test_samples(self, success_df, coupang_image_mapping, sample_size=50):
        """테스트 샘플 선택 (이미지가 있는 상품만)"""
        try:
            print(f"\n=== {sample_size}개 테스트 샘플 선택 ===")
            
            # 쿠팡 이미지가 있는 상품만 필터링
            available_samples = []
            
            for _, row in success_df.iterrows():
                product_id = str(row['coupang_product_id'])
                if product_id in coupang_image_mapping:
                    available_samples.append(row)
            
            print(f"이미지 사용 가능한 상품: {len(available_samples)}개")
            
            if len(available_samples) < sample_size:
                sample_size = len(available_samples)
                print(f"샘플 크기를 {sample_size}개로 조정")
            
            # 유사도별 샘플링
            available_df = pd.DataFrame(available_samples)
            
            high_sim = available_df[available_df['similarity_score'] >= 0.9]
            mid_sim = available_df[(available_df['similarity_score'] >= 0.8) & (available_df['similarity_score'] < 0.9)]
            low_sim = available_df[available_df['similarity_score'] < 0.8]
            
            # 비례적 샘플링
            samples = pd.concat([
                high_sim.sample(n=min(20, len(high_sim)), random_state=42) if len(high_sim) > 0 else pd.DataFrame(),
                mid_sim.sample(n=min(20, len(mid_sim)), random_state=42) if len(mid_sim) > 0 else pd.DataFrame(),
                low_sim.sample(n=min(10, len(low_sim)), random_state=42) if len(low_sim) > 0 else pd.DataFrame()
            ]).head(sample_size)
            
            print(f"샘플링 완료: {len(samples)}개")
            print(f"  - 높은 유사도 (0.9+): {min(20, len(high_sim))}개")
            print(f"  - 중간 유사도 (0.8-0.89): {min(20, len(mid_sim))}개") 
            print(f"  - 낮은 유사도 (<0.8): {min(10, len(low_sim))}개")
            
            return samples.reset_index(drop=True)
            
        except Exception as e:
            print(f"샘플 선택 실패: {e}")
            return None
    
    def run_image_comparison_test(self, samples, coupang_image_mapping, coupang_df):
        """이미지 비교 테스트 실행"""
        try:
            print(f"\n=== 이미지 비교 테스트 실행 ===")
            print(f"테스트 대상: {len(samples)}개 샘플")
            
            success_count = 0
            
            for idx, row in samples.iterrows():
                sample_id = idx + 1
                product_id = str(row['coupang_product_id'])
                
                print(f"\n[{sample_id:02d}] {row['coupang_product_name']}")
                
                # 쿠팡 이미지 경로
                coupang_image_path = coupang_image_mapping.get(product_id)
                if not coupang_image_path:
                    print(f"  쿠팡 이미지 없음")
                    continue
                
                # 아이허브 이미지 경로 (기존에 다운로드된 것이 있다면)
                iherb_image_path = self.find_existing_iherb_image(row, sample_id)
                
                # 쿠팡 URL 정보 (coupang_df에서 추출)
                coupang_url = self.get_coupang_url(product_id, coupang_df)
                
                # 결과 레코드 생성
                result = {
                    'sample_id': sample_id,
                    'coupang_product_id': product_id,
                    'iherb_product_code': row['iherb_product_code'],
                    'coupang_product_name': row['coupang_product_name'],
                    'iherb_product_name': row['iherb_product_name'],
                    'original_similarity_score': row['similarity_score'],
                    'original_matching_reason': row['matching_reason'],
                    
                    # URL 정보 추가
                    'coupang_url': coupang_url,
                    'iherb_url': row['iherb_product_url'],
                    
                    # 이미지 경로
                    'coupang_image_path': coupang_image_path,
                    'iherb_image_path': iherb_image_path,
                    'both_images_available': bool(coupang_image_path and iherb_image_path),
                    
                    'processed_at': datetime.now().isoformat()
                }
                
                # 이미지 비교 실행
                if result['both_images_available']:
                    comparison_result = self.compare_images(coupang_image_path, iherb_image_path)
                    result.update(comparison_result)
                    success_count += 1
                    print(f"  비교 완료: {'매칭' if comparison_result.get('image_match_result') else '불일치'}")
                else:
                    print(f"  이미지 누락 - 비교 불가")
                
                self.test_results.append(result)
            
            print(f"\n테스트 완료: {success_count}개 이미지 쌍 비교")
            return success_count
            
        except Exception as e:
            print(f"테스트 실행 실패: {e}")
            return 0
    
    def find_existing_iherb_image(self, row, sample_id):
        """기존 아이허브 이미지 찾기"""
        try:
            # 가능한 아이허브 이미지 경로들
            possible_paths = [
                f"../iherb_images/iherb_{sample_id:02d}_{row['iherb_product_code']}.jpg",
                f"iherb_images/iherb_{sample_id:02d}_{row['iherb_product_code']}.jpg",
                f"../image_experiment_results/iherb_images/iherb_{sample_id:02d}_{row['iherb_product_code']}.jpg"
            ]
            
            for path in possible_paths:
                if os.path.exists(path):
                    return path
            
            return None
            
        except Exception:
            return None
    
    def get_coupang_url(self, product_id, coupang_df):
        """쿠팡 URL 추출"""
        try:
            matching_rows = coupang_df[coupang_df['product_id'].astype(str) == product_id]
            if len(matching_rows) > 0:
                return matching_rows.iloc[0]['product_url']
            return ""
        except Exception:
            return ""
    
    def compare_images(self, coupang_image_path, iherb_image_path):
        """두 이미지 OCR 비교"""
        try:
            print(f"    OCR 비교 중...")
            
            # 이미지 로드 및 OCR
            coupang_img = load_rgb(coupang_image_path)
            iherb_img = load_rgb(iherb_image_path)
            
            coupang_text = ocr_text(coupang_img)
            iherb_text = ocr_text(iherb_img)
            
            # 필드 추출 및 비교
            coupang_fields = extract_fields(coupang_text)
            iherb_fields = extract_fields(iherb_text)
            
            same, reason = same_product_by_text(coupang_fields, iherb_fields)
            
            return {
                'image_comparison_success': True,
                'image_match_result': same,
                'image_match_reason': reason,
                'coupang_ocr_text': coupang_text[:300] + "..." if len(coupang_text) > 300 else coupang_text,
                'iherb_ocr_text': iherb_text[:300] + "..." if len(iherb_text) > 300 else iherb_text,
                'coupang_extracted_fields': str(coupang_fields),
                'iherb_extracted_fields': str(iherb_fields)
            }
            
        except Exception as e:
            print(f"    비교 실패: {e}")
            return {
                'image_comparison_success': False,
                'image_comparison_error': str(e)
            }
    
    def save_results(self):
        """결과 저장 및 요약"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # CSV 저장
        results_df = pd.DataFrame(self.test_results)
        csv_path = os.path.join(self.output_dir, f"image_test_results_{timestamp}.csv")
        results_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        print(f"\n결과 저장: {csv_path}")
        
        # 요약 통계
        total = len(results_df)
        with_both_images = len(results_df[results_df['both_images_available'] == True])
        comparison_success = len(results_df[results_df.get('image_comparison_success', False) == True])
        
        print(f"\n=== 테스트 결과 요약 ===")
        print(f"총 샘플: {total}개")
        print(f"이미지 쌍 확보: {with_both_images}개 ({with_both_images/total*100:.1f}%)")
        print(f"비교 성공: {comparison_success}개 ({comparison_success/total*100:.1f}%)")
        
        if comparison_success > 0:
            successful_comparisons = results_df[results_df.get('image_comparison_success', False) == True]
            matches = len(successful_comparisons[successful_comparisons.get('image_match_result', False) == True])
            print(f"이미지 기반 매칭: {matches}개 ({matches/comparison_success*100:.1f}%)")
            
            # 텍스트 vs 이미지 매칭 비교
            both_agree_match = 0
            text_match_image_no = 0
            text_no_image_match = 0
            both_agree_no_match = 0
            
            for _, row in successful_comparisons.iterrows():
                original_score = row['original_similarity_score']
                image_match = row.get('image_match_result', False)
                
                text_high = original_score >= 0.8
                
                if text_high and image_match:
                    both_agree_match += 1
                elif text_high and not image_match:
                    text_match_image_no += 1
                elif not text_high and image_match:
                    text_no_image_match += 1
                else:
                    both_agree_no_match += 1
            
            print(f"\n=== 텍스트 vs 이미지 매칭 비교 ===")
            print(f"양쪽 모두 매칭: {both_agree_match}개")
            print(f"텍스트만 매칭: {text_match_image_no}개")
            print(f"이미지만 매칭: {text_no_image_match}개")
            print(f"양쪽 모두 불일치: {both_agree_no_match}개")
        
        return csv_path
    
    def run_complete_test(self, csv_file_path, coupang_csv_path, coupang_images_dir, sample_size=50):
        """전체 테스트 실행"""
        try:
            print("=== 단순화된 이미지 비교 테스트 시작 ===")
            print("- 기존 이미지만 사용")
            print("- 새로운 크롤링 없음")
            print("- URL 정보 포함")
            
            # 1. 기존 데이터 로드
            data = self.load_existing_data(csv_file_path, coupang_csv_path, coupang_images_dir)
            if data is None:
                return False
            
            success_df, coupang_df, coupang_image_mapping = data
            
            # 2. 테스트 샘플 선택
            samples = self.select_test_samples(success_df, coupang_image_mapping, sample_size)
            if samples is None:
                return False
            
            # 3. 이미지 비교 테스트 실행
            success_count = self.run_image_comparison_test(samples, coupang_image_mapping, coupang_df)
            
            # 4. 결과 저장
            results_path = self.save_results()
            
            print(f"\n=== 테스트 완료 ===")
            print(f"결과 파일: {results_path}")
            
            return True
            
        except Exception as e:
            print(f"테스트 실행 중 오류: {e}")
            return False


# 실행 스크립트
if __name__ == "__main__":
    print("단순화된 이미지 비교 테스트")
    print("기존에 다운로드된 이미지만 사용하여 OCR 비교 테스트")
    
    # 파일 경로 설정
    csv_file_path = "../coupang/iherb_english_results_modular_1.csv"
    coupang_csv_path = "../coupang/coupang_products_20250909_114933.csv"
    coupang_images_dir = "../coupang_images"
    
    print(f"\n파일 경로:")
    print(f"매칭 결과: {csv_file_path}")
    print(f"쿠팡 데이터: {coupang_csv_path}")
    print(f"쿠팡 이미지: {coupang_images_dir}")
    
    # 파일 존재 확인
    missing_files = []
    if not os.path.exists(csv_file_path):
        missing_files.append(csv_file_path)
    if not os.path.exists(coupang_csv_path):
        missing_files.append(coupang_csv_path)
    if not os.path.exists(coupang_images_dir):
        missing_files.append(coupang_images_dir)
    
    if missing_files:
        print(f"\n다음 파일/폴더를 찾을 수 없습니다:")
        for file in missing_files:
            print(f"  - {file}")
        print("파일 경로를 확인해주세요.")
        exit(1)
    
    # 테스트 실행
    tester = SimpleImageTest()
    
    success = tester.run_complete_test(
        csv_file_path=csv_file_path,
        coupang_csv_path=coupang_csv_path,
        coupang_images_dir=coupang_images_dir,
        sample_size=30  # 테스트용으로 30개
    )
    
    if success:
        print("테스트 성공적으로 완료")
    else:
        print("테스트 실패")