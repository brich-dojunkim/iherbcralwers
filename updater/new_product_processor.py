"""
신규 상품 처리 모듈
신규 상품에 대해 번역 → 아이허브 매칭 → 마스터 포맷 변환
"""

import pandas as pd
import tempfile
import os
import sys
from datetime import datetime
from typing import List, Dict

# 설정 임포트
from config import CONFIG

# 기존 모듈 임포트 (절대 경로 사용)
try:
    # 기존 모듈 경로 확인
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    COUPANG_MODULE_PATH = os.path.join(BASE_DIR, 'coupang')
    IHERB_MODULE_PATH = os.path.join(BASE_DIR, 'iherbscraper')
    
    # 경로가 sys.path에 없으면 추가
    if COUPANG_MODULE_PATH not in sys.path:
        sys.path.insert(0, COUPANG_MODULE_PATH)
    if IHERB_MODULE_PATH not in sys.path:
        sys.path.insert(0, IHERB_MODULE_PATH)
    
    # 명시적 모듈 임포트
    import importlib.util
    
    # translator 모듈 로드
    translator_path = os.path.join(COUPANG_MODULE_PATH, 'translator.py')
    if os.path.exists(translator_path):
        spec = importlib.util.spec_from_file_location("coupang_translator", translator_path)
        translator_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(translator_module)
        GeminiCSVTranslator = translator_module.GeminiCSVTranslator
        print("✓ Coupang 번역 모듈 로드 성공")
    else:
        raise ImportError("translator.py not found")
    
    # main 모듈 로드 (iherb scraper)
    main_path = os.path.join(IHERB_MODULE_PATH, 'main.py')
    if os.path.exists(main_path):
        spec = importlib.util.spec_from_file_location("iherb_main", main_path)
        main_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(main_module)
        EnglishIHerbScraper = main_module.EnglishIHerbScraper
        print("✓ iHerb 스크래퍼 모듈 로드 성공")
    else:
        raise ImportError("main.py not found")
    
    MODULES_AVAILABLE = True
    
except Exception as e:
    print(f"⚠️ 기존 모듈 로드 실패: {e}")
    print(f"  COUPANG_MODULE_PATH: {COUPANG_MODULE_PATH if 'COUPANG_MODULE_PATH' in locals() else 'N/A'}")
    print(f"  IHERB_MODULE_PATH: {IHERB_MODULE_PATH if 'IHERB_MODULE_PATH' in locals() else 'N/A'}")
    MODULES_AVAILABLE = False
    GeminiCSVTranslator = None
    EnglishIHerbScraper = None


class NewProductProcessor:
    """신규 상품 처리 (번역 + 아이허브 매칭)"""
    
    def __init__(self):
        self.config = CONFIG
        self.translator = None
        self.iherb_scraper = None
        self.processing_stats = {
            'total_new_products': 0,
            'translated_products': 0,
            'matched_products': 0,
            'successful_matches': 0
        }
    
    def process_new_products(self, new_products: List[dict]) -> pd.DataFrame:
        """신규 상품들을 번역 → 매칭 → 마스터 포맷으로 처리"""
        try:
            print(f"\n신규 상품 처리 시작 ({len(new_products)}개)")
            
            if not new_products:
                return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
            
            if not MODULES_AVAILABLE:
                print("  기존 모듈을 사용할 수 없습니다.")
                return self._create_failed_records(new_products)
            
            self.processing_stats['total_new_products'] = len(new_products)
            
            # 배치 크기 제한
            max_batch = self.config.MAX_NEW_PRODUCTS_PER_RUN
            if len(new_products) > max_batch:
                print(f"  배치 크기 제한: {len(new_products)}개 → {max_batch}개")
                new_products = new_products[:max_batch]
            
            # 1. 임시 CSV 생성
            temp_csv = self._create_temp_csv(new_products)
            if not temp_csv:
                return self._create_failed_records(new_products)
            
            try:
                # 2. 번역 실행
                translated_csv = self._translate_products(temp_csv)
                if not translated_csv:
                    return self._create_failed_records(new_products)
                
                # 3. 아이허브 매칭 실행
                matched_csv = self._match_with_iherb(translated_csv)
                if not matched_csv:
                    return self._create_failed_records(new_products)
                
                # 4. 결과 로드 및 마스터 포맷 변환
                result_df = self._load_and_convert_results(matched_csv)
                
                # 5. 통계 출력
                self._print_processing_stats()
                
                return result_df
                
            finally:
                # 임시 파일 정리
                self._cleanup_temp_files([temp_csv, translated_csv if 'translated_csv' in locals() else None])
            
        except Exception as e:
            print(f"  신규 상품 처리 중 오류: {e}")
            return self._create_failed_records(new_products)
    
    def _create_temp_csv(self, new_products: List[dict]) -> str:
        """신규 상품들로 임시 CSV 생성"""
        try:
            print("  임시 CSV 생성 중...")
            
            # DataFrame 생성
            df = pd.DataFrame(new_products)
            
            # 필수 컬럼 확인 및 추가
            required_columns = ['product_id', 'product_name', 'product_url', 'current_price']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = ''
            
            # 임시 파일 생성
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig')
            temp_csv_path = temp_file.name
            temp_file.close()
            
            # CSV 저장
            df.to_csv(temp_csv_path, index=False, encoding='utf-8-sig')
            
            print(f"    임시 CSV 생성 완료: {len(df)}개 상품")
            return temp_csv_path
            
        except Exception as e:
            print(f"    임시 CSV 생성 실패: {e}")
            return ""
    
    def _translate_products(self, input_csv: str) -> str:
        """상품명 번역"""
        try:
            print("  상품명 번역 중...")
            
            # Gemini 번역기 초기화
            iherb_config = self.config.get_iherb_config()
            if not iherb_config or not hasattr(iherb_config, 'GEMINI_API_KEY'):
                print("    Gemini API 키를 찾을 수 없습니다.")
                return ""
            
            self.translator = GeminiCSVTranslator(iherb_config.GEMINI_API_KEY)
            
            # 번역된 파일 경로 생성
            translated_csv = input_csv.replace('.csv', '_translated.csv')
            
            # 번역 실행
            result_df = self.translator.translate_csv(
                input_file=input_csv,
                output_file=translated_csv,
                column_name='product_name',
                batch_size=5,  # 신규 상품이므로 작은 배치로
                save_progress=False
            )
            
            if result_df is not None and len(result_df) > 0:
                translated_count = len(result_df[result_df['product_name_english'].notna() & (result_df['product_name_english'] != '')])
                self.processing_stats['translated_products'] = translated_count
                print(f"    번역 완료: {translated_count}개")
                return translated_csv
            else:
                print("    번역 실패")
                return ""
                
        except Exception as e:
            print(f"    번역 중 오류: {e}")
            return ""
    
    def _match_with_iherb(self, translated_csv: str) -> str:
        """아이허브 매칭"""
        try:
            print("  아이허브 매칭 중...")
            
            # 결과 파일 경로 생성
            matched_csv = translated_csv.replace('.csv', '_matched.csv')
            
            # 아이허브 스크래퍼 초기화
            self.iherb_scraper = EnglishIHerbScraper(
                headless=True,  # 백그라운드 실행
                delay_range=(1, 2),  # 빠른 처리
                max_products_to_compare=3  # 신규 상품이므로 제한
            )
            
            # 매칭 실행
            result_csv = self.iherb_scraper.process_products_complete(
                csv_file_path=translated_csv,
                output_file_path=matched_csv,
                limit=None,
                start_from=0
            )
            
            if result_csv and os.path.exists(result_csv):
                # 매칭 통계 계산
                result_df = pd.read_csv(result_csv, encoding='utf-8-sig')
                self.processing_stats['matched_products'] = len(result_df)
                self.processing_stats['successful_matches'] = len(result_df[result_df['status'] == 'success'])
                
                print(f"    매칭 완료: {self.processing_stats['successful_matches']}/{len(result_df)}개 성공")
                return result_csv
            else:
                print("    매칭 실패")
                return ""
                
        except Exception as e:
            print(f"    매칭 중 오류: {e}")
            return ""
        finally:
            # 스크래퍼 정리
            if self.iherb_scraper:
                try:
                    self.iherb_scraper.close()
                except:
                    pass
    
    def _load_and_convert_results(self, matched_csv: str) -> pd.DataFrame:
        """매칭 결과를 마스터 포맷으로 변환"""
        try:
            print("  결과 변환 중...")
            
            # 매칭 결과 로드
            result_df = pd.read_csv(matched_csv, encoding='utf-8-sig')
            
            if len(result_df) == 0:
                return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
            
            # 마스터 포맷으로 변환
            for idx in result_df.index:
                # 메타 정보 추가
                result_df.at[idx, 'last_updated'] = datetime.now().isoformat()
                result_df.at[idx, 'data_source'] = 'new_product'
                result_df.at[idx, 'update_count'] = 0
            
            # 컬럼 순서 맞추기
            result_df = result_df.reindex(columns=self.config.MASTER_COLUMNS, fill_value='')
            
            print(f"    변환 완료: {len(result_df)}개")
            return result_df
            
        except Exception as e:
            print(f"    결과 변환 실패: {e}")
            return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
    
    def _create_failed_records(self, new_products: List[dict]) -> pd.DataFrame:
        """실패한 상품들을 위한 기본 레코드 생성"""
        try:
            print("  실패 레코드 생성 중...")
            
            failed_records = []
            
            for product in new_products:
                record = {}
                
                # 기본 정보
                record['coupang_product_id'] = product.get('product_id', '')
                record['coupang_product_name'] = product.get('product_name', '')
                record['coupang_product_name_english'] = ''
                record['coupang_url'] = product.get('product_url', '')
                record['coupang_current_price_krw'] = product.get('current_price', '')
                record['coupang_original_price_krw'] = product.get('original_price', '')
                record['coupang_discount_rate'] = product.get('discount_rate', '')
                
                # 실패 정보
                record['status'] = 'processing_failed'
                record['failure_type'] = 'MODULE_NOT_AVAILABLE'
                record['matching_reason'] = '모듈 불가용으로 처리 실패'
                record['similarity_score'] = 0
                record['gemini_confidence'] = 'NONE'
                
                # 메타 정보
                record['last_updated'] = datetime.now().isoformat()
                record['data_source'] = 'failed_processing'
                record['update_count'] = 0
                
                # 나머지 필드는 빈 값
                for col in self.config.MASTER_COLUMNS:
                    if col not in record:
                        record[col] = ''
                
                failed_records.append(record)
            
            result_df = pd.DataFrame(failed_records)
            result_df = result_df.reindex(columns=self.config.MASTER_COLUMNS, fill_value='')
            
            print(f"    실패 레코드 생성 완료: {len(result_df)}개")
            return result_df
            
        except Exception as e:
            print(f"    실패 레코드 생성 오류: {e}")
            return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
    
    def _cleanup_temp_files(self, file_paths: List[str]):
        """임시 파일 정리"""
        for file_path in file_paths:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"    임시 파일 삭제: {os.path.basename(file_path)}")
                except:
                    pass
    
    def _print_processing_stats(self):
        """처리 통계 출력"""
        stats = self.processing_stats
        print(f"\n신규 상품 처리 완료:")
        print(f"  총 신규 상품: {stats['total_new_products']}개")
        print(f"  번역 완료: {stats['translated_products']}개")
        print(f"  매칭 시도: {stats['matched_products']}개")
        print(f"  매칭 성공: {stats['successful_matches']}개")
        
        if stats['total_new_products'] > 0:
            success_rate = (stats['successful_matches'] / stats['total_new_products']) * 100
            print(f"  전체 성공률: {success_rate:.1f}%")
    
    def process_single_product(self, product: dict) -> dict:
        """단일 상품 처리 (테스트/디버그용)"""
        try:
            result_df = self.process_new_products([product])
            
            if len(result_df) > 0:
                return result_df.iloc[0].to_dict()
            else:
                return self._create_failed_records([product]).iloc[0].to_dict()
                
        except Exception as e:
            print(f"  단일 상품 처리 실패: {e}")
            return self._create_failed_records([product]).iloc[0].to_dict()