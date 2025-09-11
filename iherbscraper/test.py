"""
실제 파일을 사용한 iHerb 스크래퍼 테스트
- 실제 쿠팡 CSV 파일 감지
- 소량 테스트 (5개 상품)
- 단계별 진행 상황 출력
- 오류 처리 및 결과 분석
"""

import os
import sys
import pandas as pd
import glob
from datetime import datetime

# iherbscraper 모듈 import를 위한 경로 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(current_dir)
sys.path.append(parent_dir)

try:
    from main import EnglishIHerbScraper
    from config import Config
    print("✅ 모듈 import 성공")
except ImportError as e:
    print(f"❌ 모듈 import 실패: {e}")
    print("현재 디렉토리에서 실행해주세요: cd iherbscraper && python test_real.py")
    sys.exit(1)


class RealFileTestRunner:
    """실제 파일 기반 테스트 실행기"""
    
    def __init__(self):
        self.project_root = os.path.dirname(current_dir)
        self.test_results = {
            'start_time': datetime.now(),
            'files_found': {},
            'test_status': 'starting'
        }
    
    def find_coupang_files(self):
        """쿠팡 크롤링 결과 파일 찾기"""
        print("\n🔍 쿠팡 CSV 파일 찾는 중...")
        
        # 가능한 파일 경로들
        search_patterns = [
            os.path.join(self.project_root, "coupang_products_*.csv"),
            os.path.join(self.project_root, "coupang", "coupang_products_*.csv"),
            "./coupang_products_*.csv",
            "../coupang_products_*.csv"
        ]
        
        found_files = []
        for pattern in search_patterns:
            files = glob.glob(pattern)
            found_files.extend(files)
        
        if found_files:
            # 가장 최근 파일 선택
            latest_file = max(found_files, key=os.path.getmtime)
            file_size = os.path.getsize(latest_file)
            mod_time = datetime.fromtimestamp(os.path.getmtime(latest_file))
            
            print(f"✅ 쿠팡 CSV 파일 발견: {os.path.basename(latest_file)}")
            print(f"   경로: {latest_file}")
            print(f"   크기: {file_size:,} bytes")
            print(f"   수정시간: {mod_time}")
            
            self.test_results['files_found']['coupang_csv'] = {
                'path': latest_file,
                'size': file_size,
                'modified': mod_time.isoformat()
            }
            
            return latest_file
        else:
            print("❌ 쿠팡 CSV 파일을 찾을 수 없습니다.")
            print("다음 중 하나를 먼저 실행해주세요:")
            print("1. python coupang.py (쿠팡 크롤링)")
            print("2. 기존 CSV 파일을 프로젝트 폴더에 복사")
            return None
    
    def analyze_coupang_csv(self, csv_path):
        """쿠팡 CSV 파일 분석"""
        print(f"\n📊 쿠팡 CSV 파일 분석: {os.path.basename(csv_path)}")
        
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            
            print(f"   총 상품 수: {len(df)}개")
            print(f"   컬럼 수: {len(df.columns)}개")
            print(f"   컬럼 목록: {', '.join(df.columns.tolist())}")
            
            # 필수 컬럼 확인
            required_columns = ['product_name', 'product_id']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"❌ 필수 컬럼 누락: {missing_columns}")
                return None
            
            # 유효한 상품명 확인
            valid_products = df[df['product_name'].notna() & (df['product_name'].str.strip() != '')]
            print(f"   유효한 상품명: {len(valid_products)}개")
            
            # 가격 정보 확인
            price_info = {}
            if 'current_price' in df.columns:
                price_count = len(df[df['current_price'].notna()])
                price_info['current_price'] = price_count
                print(f"   현재가격 정보: {price_count}개")
            
            if 'original_price' in df.columns:
                orig_price_count = len(df[df['original_price'].notna()])
                price_info['original_price'] = orig_price_count
                print(f"   원가 정보: {orig_price_count}개")
            
            # 이미지 정보 확인
            image_info = {}
            if 'image_local_path' in df.columns:
                image_count = len(df[df['image_local_path'].notna()])
                image_info['local_images'] = image_count
                print(f"   로컬 이미지: {image_count}개")
            
            if 'image_url' in df.columns:
                url_count = len(df[df['image_url'].notna()])
                image_info['image_urls'] = url_count
                print(f"   이미지 URL: {url_count}개")
            
            # 상품명 샘플 출력
            print(f"\n   상품명 샘플 (상위 3개):")
            for i, product_name in enumerate(valid_products['product_name'].head(3), 1):
                print(f"     {i}. {product_name[:60]}...")
            
            self.test_results['files_found']['coupang_analysis'] = {
                'total_products': len(df),
                'valid_products': len(valid_products),
                'columns': df.columns.tolist(),
                'price_info': price_info,
                'image_info': image_info
            }
            
            return valid_products
            
        except Exception as e:
            print(f"❌ CSV 분석 실패: {e}")
            return None
    
    def check_image_directory(self):
        """이미지 디렉토리 확인"""
        print(f"\n🖼️  이미지 디렉토리 확인...")
        
        image_dirs = [
            "./coupang_images",
            "../coupang_images",
            os.path.join(self.project_root, "coupang_images")
        ]
        
        for img_dir in image_dirs:
            if os.path.exists(img_dir):
                image_files = [f for f in os.listdir(img_dir) if f.endswith(('.jpg', '.jpeg', '.png'))]
                print(f"✅ 이미지 디렉토리 발견: {img_dir}")
                print(f"   이미지 파일 수: {len(image_files)}개")
                
                if image_files:
                    print(f"   샘플 파일: {image_files[0]}")
                
                self.test_results['files_found']['image_directory'] = {
                    'path': img_dir,
                    'image_count': len(image_files)
                }
                
                return img_dir
        
        print("⚠️  이미지 디렉토리를 찾을 수 없습니다.")
        print("이미지 비교 기능이 제한될 수 있습니다.")
        return None
    
    def check_api_configuration(self):
        """Gemini API 설정 확인"""
        print(f"\n🤖 Gemini API 설정 확인...")
        
        api_key = Config.GEMINI_API_KEY
        if api_key == "YOUR_GEMINI_API_KEY_HERE":
            print("❌ Gemini API 키가 설정되지 않았습니다!")
            print("config.py에서 GEMINI_API_KEY를 실제 API 키로 변경해주세요.")
            print("참고: https://makersuite.google.com/app/apikey")
            return False
        elif api_key and len(api_key) > 20:
            print("✅ Gemini API 키 설정됨")
            print(f"   키 길이: {len(api_key)}자")
            print(f"   키 시작: {api_key[:8]}...")
            
            # 추가 설정 확인
            print(f"   텍스트 모델: {Config.GEMINI_TEXT_MODEL}")
            print(f"   Vision 모델: {Config.GEMINI_VISION_MODEL}")
            print(f"   이미지 비교: {'활성화' if Config.IMAGE_COMPARISON_ENABLED else '비활성화'}")
            
            return True
        else:
            print("⚠️  API 키가 올바르지 않을 수 있습니다.")
            return False
    
    def create_test_dataset(self, df, test_size=5):
        """테스트용 데이터셋 생성"""
        print(f"\n📋 테스트 데이터셋 생성 (상위 {test_size}개 상품)...")
        
        # 상위 N개 상품 선택
        test_df = df.head(test_size).copy()
        
        # 테스트 CSV 파일 생성
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        test_csv_path = f"test_input_{timestamp}.csv"
        
        test_df.to_csv(test_csv_path, index=False, encoding='utf-8-sig')
        
        print(f"✅ 테스트 CSV 생성: {test_csv_path}")
        print(f"   테스트 상품 목록:")
        
        for i, row in test_df.iterrows():
            product_name = row['product_name']
            product_id = row.get('product_id', 'N/A')
            current_price = row.get('current_price', 'N/A')
            
            print(f"     {i+1}. {product_name[:50]}...")
            print(f"        ID: {product_id}, 가격: {current_price}")
        
        self.test_results['test_dataset'] = {
            'csv_path': test_csv_path,
            'product_count': len(test_df),
            'products': test_df[['product_name', 'product_id']].to_dict('records')
        }
        
        return test_csv_path
    
    def run_scraper_test(self, input_csv, test_size=5):
        """실제 스크래퍼 테스트 실행"""
        print(f"\n🚀 iHerb 스크래퍼 테스트 시작...")
        print(f"   입력 파일: {input_csv}")
        print(f"   테스트 상품 수: {test_size}개")
        
        # 출력 파일명 생성
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_csv = f"test_results_{timestamp}.csv"
        
        scraper = None
        try:
            # 스크래퍼 초기화
            print("   스크래퍼 초기화 중...")
            scraper = EnglishIHerbScraper(
                headless=False,  # 테스트시에는 브라우저 표시
                delay_range=(1, 2),  # 테스트용 빠른 설정
                max_products_to_compare=3  # 테스트용 축소
            )
            
            print("✅ 스크래퍼 초기화 완료")
            
            # 실제 처리 실행
            print(f"   처리 시작... (최대 {test_size}개 상품)")
            print("   ⚠️  브라우저 창이 열리고 자동으로 아이허브 사이트에 접속합니다.")
            print("   ⚠️  언어 설정이 자동으로 영어로 변경됩니다.")
            
            results = scraper.process_products_complete(
                csv_file_path=input_csv,
                output_file_path=output_csv,
                limit=test_size
            )
            
            self.test_results['test_status'] = 'completed'
            self.test_results['output_file'] = output_csv
            
            return output_csv
            
        except KeyboardInterrupt:
            print("\n⚠️  사용자에 의해 테스트 중단됨")
            self.test_results['test_status'] = 'interrupted'
            return output_csv
            
        except Exception as e:
            print(f"\n❌ 테스트 실행 중 오류: {e}")
            self.test_results['test_status'] = 'failed'
            self.test_results['error'] = str(e)
            return None
            
        finally:
            if scraper:
                print("   브라우저 종료 중...")
                scraper.close()
    
    def analyze_test_results(self, output_csv):
        """테스트 결과 분석"""
        if not output_csv or not os.path.exists(output_csv):
            print("❌ 결과 파일이 없습니다.")
            return
        
        print(f"\n📈 테스트 결과 분석: {output_csv}")
        
        try:
            results_df = pd.read_csv(output_csv, encoding='utf-8-sig')
            
            total_count = len(results_df)
            success_count = len(results_df[results_df['status'] == 'success'])
            
            print(f"   총 처리: {total_count}개")
            print(f"   성공: {success_count}개 ({success_count/total_count*100:.1f}%)")
            
            if success_count > 0:
                print(f"\n   성공 사례:")
                success_df = results_df[results_df['status'] == 'success']
                
                for i, (_, row) in enumerate(success_df.iterrows(), 1):
                    korean_name = row['coupang_product_name'][:40]
                    iherb_name = row.get('iherb_product_name', 'N/A')[:40]
                    iherb_code = row.get('iherb_product_code', 'N/A')
                    similarity = row.get('similarity_score', 0)
                    
                    print(f"     {i}. {korean_name}...")
                    print(f"        → {iherb_name}... (코드: {iherb_code})")
                    print(f"        유사도: {similarity:.3f}")
            
            # 실패 유형 분석
            failed_df = results_df[results_df['status'] != 'success']
            if len(failed_df) > 0:
                print(f"\n   실패 유형별 통계:")
                if 'failure_type' in results_df.columns:
                    failure_counts = failed_df['failure_type'].value_counts()
                    for failure_type, count in failure_counts.items():
                        print(f"     {failure_type}: {count}개")
            
            # Gemini API 사용량
            if 'gemini_api_calls' in results_df.columns:
                total_api_calls = results_df['gemini_api_calls'].sum()
                avg_calls = total_api_calls / total_count if total_count > 0 else 0
                print(f"\n   Gemini API 사용량:")
                print(f"     총 호출: {total_api_calls}회")
                print(f"     평균: {avg_calls:.1f}회/상품")
            
            self.test_results['final_analysis'] = {
                'total_processed': total_count,
                'success_count': success_count,
                'success_rate': success_count/total_count if total_count > 0 else 0,
                'api_calls': total_api_calls if 'gemini_api_calls' in results_df.columns else 0
            }
            
        except Exception as e:
            print(f"❌ 결과 분석 실패: {e}")
    
    def run_complete_test(self, test_size=5):
        """완전한 테스트 실행"""
        print("=" * 60)
        print("🧪 실제 파일 기반 iHerb 스크래퍼 테스트")
        print("=" * 60)
        
        # 1. 파일 찾기
        coupang_csv = self.find_coupang_files()
        if not coupang_csv:
            return False
        
        # 2. CSV 분석
        df = self.analyze_coupang_csv(coupang_csv)
        if df is None or len(df) == 0:
            return False
        
        # 3. 이미지 디렉토리 확인
        self.check_image_directory()
        
        # 4. API 설정 확인
        if not self.check_api_configuration():
            print("\n⚠️  API 설정 문제가 있지만 테스트를 계속 진행합니다.")
            print("텍스트 매칭만 작동하고 이미지 비교는 제한될 수 있습니다.")
        
        # 5. 테스트 데이터셋 생성
        test_csv = self.create_test_dataset(df, test_size)
        
        # 6. 사용자 확인
        print(f"\n🔔 테스트 실행 준비 완료!")
        print(f"   테스트 상품 수: {test_size}개")
        print(f"   예상 소요 시간: {test_size * 2}-{test_size * 4}분")
        print(f"   브라우저가 자동으로 열립니다.")
        
        user_input = input("\n테스트를 시작하시겠습니까? (y/n): ").strip().lower()
        if user_input != 'y':
            print("테스트 취소됨")
            return False
        
        # 7. 스크래퍼 테스트 실행
        output_csv = self.run_scraper_test(test_csv, test_size)
        
        # 8. 결과 분석
        if output_csv:
            self.analyze_test_results(output_csv)
        
        # 9. 최종 요약
        self.print_final_summary()
        
        return True
    
    def print_final_summary(self):
        """최종 요약 출력"""
        print(f"\n" + "=" * 60)
        print("📋 테스트 최종 요약")
        print("=" * 60)
        
        end_time = datetime.now()
        duration = end_time - self.test_results['start_time']
        
        print(f"테스트 시간: {self.test_results['start_time'].strftime('%H:%M:%S')} ~ {end_time.strftime('%H:%M:%S')}")
        print(f"소요 시간: {duration}")
        print(f"테스트 상태: {self.test_results['test_status']}")
        
        # 파일 정보
        if 'coupang_csv' in self.test_results['files_found']:
            csv_info = self.test_results['files_found']['coupang_csv']
            print(f"\n사용된 파일:")
            print(f"  쿠팡 CSV: {os.path.basename(csv_info['path'])}")
            
        # 처리 결과
        if 'final_analysis' in self.test_results:
            analysis = self.test_results['final_analysis']
            print(f"\n처리 결과:")
            print(f"  총 처리: {analysis['total_processed']}개")
            print(f"  성공: {analysis['success_count']}개")
            print(f"  성공률: {analysis['success_rate']*100:.1f}%")
            print(f"  API 호출: {analysis['api_calls']}회")
        
        # 결론
        print(f"\n결론:")
        if self.test_results['test_status'] == 'completed':
            print("✅ 테스트 성공적으로 완료!")
            print("실제 운영 환경에서 사용 가능합니다.")
        elif self.test_results['test_status'] == 'interrupted':
            print("⚠️  테스트 중단됨")
            print("지금까지의 결과는 저장되었습니다.")
        else:
            print("❌ 테스트 실패")
            if 'error' in self.test_results:
                print(f"오류: {self.test_results['error']}")


# 실행
if __name__ == "__main__":
    test_runner = RealFileTestRunner()
    
    # 기본 5개 상품으로 테스트
    # 더 많이 테스트하려면 숫자 변경: test_runner.run_complete_test(10)
    success = test_runner.run_complete_test(test_size=5)
    
    if success:
        print(f"\n🎉 테스트 완료! 실제 운영시에는 main.py를 실행하세요.")
    else:
        print(f"\n😞 테스트 실패. 설정을 확인해주세요.")