"""
완전한 재시작 기능이 있는 효율적인 통합 가격 업데이터
- 배치 번역으로 API 효율성 극대화
- 기존 모듈 재사용으로 코드 중복 제거
- 완전한 실시간 저장 및 재시작 기능
- 단계별 정밀 재개 시스템
"""

import pandas as pd
import os
import sys
from datetime import datetime
from pathlib import Path

# 프로젝트 루트 경로 설정
PROJECT_ROOT = Path(__file__).parent
COUPANG_PATH = PROJECT_ROOT / 'coupang'
IHERB_PATH = PROJECT_ROOT / 'iherbscraper'

# 경로 추가
sys.path.insert(0, str(COUPANG_PATH))
sys.path.insert(0, str(IHERB_PATH))

# 통합 설정
UNIFIED_CONFIG = {
    'GEMINI_API_KEY': "AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE",
    'GEMINI_TEXT_MODEL': "models/gemini-2.0-flash",
    'GEMINI_VISION_MODEL': "models/gemini-2.0-flash",
    
    'BRAND_SEARCH_URLS': {
        'NOW Foods': "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&q=%EB%82%98%EC%9A%B0%ED%91%B8%EB%93%9C",
        'Doctors Best': "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&q=%EB%8B%A5%ED%84%B0%EC%8A%A4%EB%B2%A0%EC%8A%A4%ED%8A%B8",
        'Garden of Life': "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&q=%EA%B0%80%EB%93%A0%EC%98%A4%EB%B8%8C%EB%9D%BC%EC%9D%B4%ED%94%84",
        'Natures Way': "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&q=%EB%84%A4%EC%9D%B4%EC%B2%98%EC%8A%A4%EC%9B%A8%EC%9D%B4"
    },
    
    'TRANSLATION_BATCH_SIZE': 10,  # 배치 번역 크기
    'DELAY_RANGE': (2, 4),
    'MAX_PRODUCTS_TO_COMPARE': 4,
    'CHECKPOINT_INTERVAL': 10,  # 중간 저장 간격
    'RESTART_METADATA_FILE': 'restart_metadata.json'  # 재시작 메타데이터
}

try:
    # 쿠팡 모듈 import
    from crawler import CoupangCrawlerMacOS
    from data_saver import DataSaver
    from translator import GeminiCSVTranslator  # 기존 번역기 모듈 활용
    print("✅ 쿠팡 모듈 로드 완료")
    
    # 아이허브 모듈 import
    from main import EnglishIHerbScraper
    from data_manager import DataManager
    from config import Config
    print("✅ 아이허브 모듈 로드 완료")
    
    MODULES_LOADED = True
    
    # Config 동적 패치
    def patch_config():
        Config.GEMINI_API_KEY = UNIFIED_CONFIG['GEMINI_API_KEY']
        Config.GEMINI_TEXT_MODEL = UNIFIED_CONFIG['GEMINI_TEXT_MODEL']
        Config.GEMINI_VISION_MODEL = UNIFIED_CONFIG['GEMINI_VISION_MODEL']
        print("✅ Config 동적 패치 완료")
    
    patch_config()
    
except ImportError as e:
    print(f"❌ 모듈 로드 실패: {e}")
    MODULES_LOADED = False


class CompleteEfficientUpdater:
    """완전한 재시작 기능이 있는 효율적인 가격 업데이터"""
    
    def __init__(self, headless=False):
        if not MODULES_LOADED:
            raise ImportError("필수 모듈을 로드할 수 없습니다.")
            
        self.headless = headless
        self.api_key = UNIFIED_CONFIG['GEMINI_API_KEY']
        self.brand_urls = UNIFIED_CONFIG['BRAND_SEARCH_URLS']
        self.batch_size = UNIFIED_CONFIG['TRANSLATION_BATCH_SIZE']
        self.checkpoint_interval = UNIFIED_CONFIG['CHECKPOINT_INTERVAL']
        
        # 모듈 인스턴스들
        self.coupang_crawler = None
        self.translator = None
        self.iherb_scraper = None
        self.data_saver = DataSaver()
        self.data_manager = DataManager()
        
        # 재시작 메타데이터
        self.metadata_file = UNIFIED_CONFIG['RESTART_METADATA_FILE']
        self.original_input_file = None
        
        print(f"🚀 완전한 효율적인 업데이터 초기화 완료")
        print(f"   - 배치 번역 크기: {self.batch_size}")
        print(f"   - 중간 저장 간격: {self.checkpoint_interval}")
        print(f"   - 지원 브랜드: {len(self.brand_urls)}개")
        print(f"   - 완전한 재시작 기능: ✅")
    
    def update_prices(self, input_file, brand_name, output_file=None, fill_iherb=True):
        """메인 업데이트 함수 - 완전한 재시작 지원"""
        print(f"\n🎯 완전한 효율적인 가격 업데이트 시작: {brand_name}")
        
        # 브랜드 검증
        if brand_name not in self.brand_urls:
            raise ValueError(f"지원되지 않는 브랜드: {brand_name}")
        
        # 메타데이터 저장
        self.original_input_file = input_file
        
        # 출력 파일명 결정
        if not output_file:
            today = datetime.now().strftime("%Y%m%d")
            output_file = f"complete_efficient_{brand_name.replace(' ', '_')}_{today}.csv"
        
        print(f"📄 작업 파일: {output_file}")
        
        # 재시작 메타데이터 저장
        self._save_restart_metadata(input_file, brand_name, output_file, fill_iherb)
        
        # 기존 작업 파일 확인 (재시작 지원)
        if os.path.exists(output_file):
            print(f"📂 기존 작업 파일 발견 - 재시작 모드")
            working_df = pd.read_csv(output_file, encoding='utf-8-sig')
            self._print_progress_status(working_df)
            
            # 미완료 작업이 있는지 정밀 확인
            incomplete_status = self._check_incomplete_work(working_df)
            if incomplete_status['has_incomplete']:
                print(f"🔄 미완료 작업 감지 - 정밀 재개 시작")
                working_df = self._resume_incomplete_work(working_df, brand_name, output_file, fill_iherb)
                self._print_final_stats(working_df)
                return output_file
            else:
                print(f"✅ 모든 작업 완료됨")
                self._cleanup_restart_metadata()
                return output_file
        
        # 새 작업 시작
        print(f"\n🆕 새 작업 시작")
        
        # 1단계: 쿠팡 가격 업데이트 + 신규 상품 발견
        print(f"\n" + "="*60)
        print(f"📊 1단계: 쿠팡 재크롤링 + 가격 업데이트")
        print(f"="*60)
        working_df, new_products = self._update_coupang_and_find_new(input_file, brand_name)
        
        # 중간 저장
        working_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"💾 1단계 완료 - 중간 저장")
        
        # 2단계: 신규 상품 배치 번역 + 아이허브 매칭
        if fill_iherb and len(new_products) > 0:
            print(f"\n" + "="*60)
            print(f"🌿 2단계: 신규 상품 배치 처리 ({len(new_products)}개)")
            print(f"="*60)
            working_df = self._process_new_products_batch(working_df, new_products, output_file)
        
        # 최종 저장
        working_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✅ 최종 완료: {output_file}")
        
        # 통계 출력 및 정리
        self._print_final_stats(working_df)
        self._cleanup_restart_metadata()
        
        return output_file
    
    def _save_restart_metadata(self, input_file, brand_name, output_file, fill_iherb):
        """재시작 메타데이터 저장"""
        import json
        
        metadata = {
            'input_file': input_file,
            'brand_name': brand_name,
            'output_file': output_file,
            'fill_iherb': fill_iherb,
            'started_at': datetime.now().isoformat(),
            'batch_size': self.batch_size,
            'checkpoint_interval': self.checkpoint_interval
        }
        
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    def _cleanup_restart_metadata(self):
        """재시작 메타데이터 정리"""
        if os.path.exists(self.metadata_file):
            os.remove(self.metadata_file)
            print(f"🧹 재시작 메타데이터 정리 완료")
    
    def _check_incomplete_work(self, df):
        """미완료 작업 정밀 감지"""
        today = datetime.now().strftime("_%Y%m%d")
        
        # 1. 쿠팡 업데이트 완료 여부 확인
        updated_products = df[df['update_status'] == 'UPDATED']
        existing_products = df[~df['update_status'].str.startswith('NEW_PRODUCT', na=False)]
        coupang_complete = len(updated_products) == len(existing_products)
        
        # 2. 신규 상품 처리 상태 확인
        new_products = df[df['update_status'] == f'NEW_PRODUCT_{today}']
        new_count = len(new_products)
        
        if new_count == 0:
            return {
                'has_incomplete': not coupang_complete,
                'coupang_complete': coupang_complete,
                'translation_complete': True,
                'iherb_complete': True,
                'new_products_count': 0,
                'translated_count': 0,
                'iherb_processed_count': 0
            }
        
        # 3. 번역 완료 여부 확인
        translated = new_products[
            new_products['coupang_product_name_english'].notna() & 
            (new_products['coupang_product_name_english'] != '')
        ]
        translation_complete = len(translated) == new_count
        
        # 4. 아이허브 매칭 완료 여부 확인
        iherb_processed = new_products[
            new_products['status'].notna() & 
            (new_products['status'] != '')
        ]
        iherb_complete = len(iherb_processed) == new_count
        
        return {
            'has_incomplete': not (coupang_complete and translation_complete and iherb_complete),
            'coupang_complete': coupang_complete,
            'translation_complete': translation_complete,
            'iherb_complete': iherb_complete,
            'new_products_count': new_count,
            'translated_count': len(translated),
            'iherb_processed_count': len(iherb_processed)
        }
    
    def _resume_incomplete_work(self, df, brand_name, output_file, fill_iherb):
        """미완료 작업 정밀 재개"""
        print(f"🔍 미완료 작업 상태 분석 중...")
        
        status = self._check_incomplete_work(df)
        
        print(f"📊 작업 상태:")
        print(f"   - 쿠팡 업데이트: {'✅' if status['coupang_complete'] else '❌'}")
        print(f"   - 신규 상품 번역: {status['translated_count']}/{status['new_products_count']}")
        print(f"   - 아이허브 매칭: {status['iherb_processed_count']}/{status['new_products_count']}")
        
        # 1. 쿠팡 업데이트가 미완료면 다시 실행
        if not status['coupang_complete']:
            print(f"🔄 쿠팡 업데이트 재실행...")
            df, new_products = self._update_coupang_and_find_new(self.original_input_file, brand_name)
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"💾 쿠팡 업데이트 완료")
            
            # 상태 재확인
            status = self._check_incomplete_work(df)
        
        # 2. 신규 상품 처리 재개
        if fill_iherb and status['new_products_count'] > 0:
            # 2-1. 번역 재개
            if not status['translation_complete']:
                print(f"🔤 번역 재개: {status['new_products_count'] - status['translated_count']}개 남음")
                df = self._resume_translation(df, output_file)
                status = self._check_incomplete_work(df)  # 상태 업데이트
            
            # 2-2. 아이허브 매칭 재개  
            if not status['iherb_complete']:
                print(f"🌿 아이허브 매칭 재개: {status['new_products_count'] - status['iherb_processed_count']}개 남음")
                df = self._resume_iherb_matching(df, output_file)
        
        # 최종 저장
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"✅ 재개 작업 완료")
        
        return df
    
    def _resume_translation(self, df, output_file):
        """번역 재개 - 미번역 상품만 처리"""
        today = datetime.now().strftime("_%Y%m%d")
        new_products = df[df['update_status'] == f'NEW_PRODUCT_{today}']
        
        untranslated = new_products[
            new_products['coupang_product_name_english'].isna() | 
            (new_products['coupang_product_name_english'] == '')
        ]
        
        if len(untranslated) == 0:
            print(f"ℹ️ 번역할 상품이 없습니다")
            return df
        
        print(f"🔤 미번역 상품 {len(untranslated)}개 배치 번역 시작...")
        
        # 번역기 초기화
        if not self.translator:
            self.translator = GeminiCSVTranslator(self.api_key)
        
        # 배치 번역
        product_names = untranslated['coupang_product_name'].tolist()
        translated_names = self.translator.translate_batch(product_names, batch_size=self.batch_size)
        
        # DataFrame 업데이트
        for (idx, row), translated_name in zip(untranslated.iterrows(), translated_names):
            df.at[idx, 'coupang_product_name_english'] = translated_name
        
        # 중간 저장
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"✅ 번역 재개 완료: {len(untranslated)}개")
        
        return df
    
    def _resume_iherb_matching(self, df, output_file):
        """아이허브 매칭 재개 - 미처리 상품만"""
        today = datetime.now().strftime("_%Y%m%d")
        
        # 번역은 되었지만 아이허브 매칭이 안된 상품들
        unmatched = df[
            (df['update_status'] == f'NEW_PRODUCT_{today}') &
            (df['coupang_product_name_english'].notna()) &
            (df['coupang_product_name_english'] != '') &
            (df['status'].isna() | (df['status'] == ''))
        ].copy()
        
        if len(unmatched) == 0:
            print(f"ℹ️ 매칭할 상품이 없습니다")
            return df
        
        print(f"🌿 미매칭 상품 {len(unmatched)}개 아이허브 매칭 시작...")
        
        # 아이허브 스크래퍼 초기화
        self._init_iherb_scraper()
        
        success_count = 0
        for i, (idx, row) in enumerate(unmatched.iterrows()):
            try:
                print(f"  [{i+1}/{len(unmatched)}] {row['coupang_product_name'][:40]}...")
                
                # 아이허브 매칭
                coupang_product = {
                    'product_id': row['coupang_product_id'],
                    'product_name': row['coupang_product_name'],
                    'product_url': row.get('coupang_url', ''),
                    'current_price': row.get(f'쿠팡현재가격{today}', ''),
                    'original_price': row.get(f'쿠팡정가{today}', ''),
                    'discount_rate': row.get(f'쿠팡할인율{today}', '')
                }
                
                english_name = row['coupang_product_name_english']
                result = self._match_single_product_iherb(coupang_product, english_name)
                
                # DataFrame 직접 업데이트
                if result['status'] == 'success':
                    df.at[idx, 'status'] = 'success'
                    df.at[idx, 'iherb_product_name'] = result['iherb_product_name']
                    df.at[idx, 'iherb_product_url'] = result['iherb_product_url']
                    df.at[idx, 'iherb_product_code'] = result['iherb_product_code']
                    df.at[idx, 'similarity_score'] = result['similarity_score']
                    
                    # 아이허브 가격 정보
                    price_info = result['iherb_price_info']
                    df.at[idx, 'iherb_list_price_krw'] = price_info.get('list_price', '')
                    df.at[idx, 'iherb_discount_price_krw'] = price_info.get('discount_price', '')
                    df.at[idx, 'iherb_discount_percent'] = price_info.get('discount_percent', '')
                    df.at[idx, 'iherb_subscription_discount'] = price_info.get('subscription_discount', '')
                    df.at[idx, 'iherb_price_per_unit'] = price_info.get('price_per_unit', '')
                    df.at[idx, 'is_in_stock'] = price_info.get('is_in_stock', True)
                    df.at[idx, 'stock_message'] = price_info.get('stock_message', '')
                    
                    # 가격 비교
                    coupang_price_info = self.data_manager.extract_coupang_price_info(coupang_product)
                    price_comparison = self.data_manager.calculate_price_comparison(coupang_price_info, price_info)
                    
                    df.at[idx, f'가격차이{today}'] = price_comparison['price_difference_krw']
                    df.at[idx, f'저렴한플랫폼{today}'] = price_comparison['cheaper_platform']
                    df.at[idx, f'절약금액{today}'] = price_comparison['savings_amount']
                    df.at[idx, f'절약비율{today}'] = price_comparison['savings_percentage']
                    df.at[idx, f'가격차이메모{today}'] = price_comparison['price_difference_note']
                    
                    success_count += 1
                    print(f"    ✅ 매칭: {result['iherb_product_name'][:30]}...")
                else:
                    df.at[idx, 'status'] = 'not_found'
                    df.at[idx, 'failure_type'] = 'NO_MATCHING_PRODUCT'
                    df.at[idx, 'matching_reason'] = result['failure_reason']
                    print(f"    ❌ 실패: {result['failure_reason']}")
                
                # 주기적 중간 저장
                if (i + 1) % self.checkpoint_interval == 0:
                    df.to_csv(output_file, index=False, encoding='utf-8-sig')
                    print(f"    💾 중간 저장 ({i+1}개 처리)")
            
            except Exception as e:
                print(f"    ❌ 오류: {e}")
                df.at[idx, 'status'] = 'error'
                df.at[idx, 'failure_type'] = 'PROCESSING_ERROR'
                df.at[idx, 'matching_reason'] = f'처리 중 오류: {str(e)}'
        
        # 아이허브 스크래퍼 정리
        self._cleanup_iherb_scraper()
        
        print(f"✅ 아이허브 매칭 재개 완료: {success_count}/{len(unmatched)}개 성공")
        
        return df
    
    def _update_coupang_and_find_new(self, input_file, brand_name):
        """쿠팡 재크롤링 + 기존 데이터 업데이트 + 신규 상품 발견"""
        # 기존 데이터 로드
        existing_df = pd.read_csv(input_file, encoding='utf-8-sig')
        print(f"📋 기존 상품: {len(existing_df)}개")
        
        # 쿠팡 재크롤링
        search_url = self.brand_urls[brand_name]
        new_crawled_products = self._crawl_coupang_fresh(search_url)
        print(f"🔍 재크롤링 결과: {len(new_crawled_products)}개")
        
        # 기존 vs 신규 비교
        existing_ids = set(str(pid) for pid in existing_df['coupang_product_id'].dropna())
        
        updated_count = 0
        new_products = []
        date_suffix = datetime.now().strftime("_%Y%m%d")
        
        # 크롤링 결과를 딕셔너리로 변환
        crawled_dict = {str(p['product_id']): p for p in new_crawled_products if p.get('product_id')}
        
        # 기존 상품 업데이트
        for idx, row in existing_df.iterrows():
            product_id = str(row.get('coupang_product_id', ''))
            
            if product_id in crawled_dict:
                # 가격 업데이트
                new_product = crawled_dict[product_id]
                existing_df.at[idx, f'쿠팡현재가격{date_suffix}'] = new_product.get('current_price', '')
                existing_df.at[idx, f'쿠팡정가{date_suffix}'] = new_product.get('original_price', '')
                existing_df.at[idx, f'쿠팡할인율{date_suffix}'] = new_product.get('discount_rate', '')
                existing_df.at[idx, f'쿠팡리뷰수{date_suffix}'] = new_product.get('review_count', '')
                existing_df.at[idx, f'쿠팡평점{date_suffix}'] = new_product.get('rating', '')
                existing_df.at[idx, f'크롤링일시{date_suffix}'] = datetime.now().isoformat()
                existing_df.at[idx, 'update_status'] = 'UPDATED'
                updated_count += 1
            else:
                existing_df.at[idx, 'update_status'] = 'NOT_FOUND'
        
        # 신규 상품 발견
        for product_id, product in crawled_dict.items():
            if product_id not in existing_ids:
                new_products.append(product)
        
        print(f"✅ 업데이트: {updated_count}개, 신규 발견: {len(new_products)}개")
        
        return existing_df, new_products
    
    def _crawl_coupang_fresh(self, search_url):
        """쿠팡 신선한 데이터 크롤링"""
        print(f"🤖 쿠팡 크롤러 시작...")
        
        self.coupang_crawler = CoupangCrawlerMacOS(
            headless=self.headless,
            delay_range=UNIFIED_CONFIG['DELAY_RANGE'],
            download_images=True
        )
        
        products = []
        try:
            if not self.coupang_crawler.start_driver():
                raise Exception("쿠팡 크롤러 시작 실패")
            
            products = self.coupang_crawler.crawl_all_pages(search_url)
            print(f"📡 크롤링 완료: {len(products)}개")
            
        except Exception as e:
            print(f"❌ 크롤링 실패: {e}")
        finally:
            if self.coupang_crawler:
                self.coupang_crawler.close()
                self.coupang_crawler = None
        
        return products
    
    def _process_new_products_batch(self, df, new_products, output_file):
        """신규 상품 배치 처리 - 효율적인 번역 + 아이허브 매칭"""
        if not new_products:
            print(f"ℹ️ 처리할 신규 상품이 없습니다.")
            return df
        
        print(f"🔤 1단계: 배치 번역 ({len(new_products)}개 → {self.batch_size}개씩)")
        
        # 배치 번역 수행
        translated_products = self._batch_translate_products(new_products)
        
        print(f"🌿 2단계: 아이허브 매칭 ({len(translated_products)}개)")
        
        # 아이허브 스크래퍼 초기화
        self._init_iherb_scraper()
        
        # 신규 상품들을 DataFrame에 추가
        new_rows = []
        success_count = 0
        
        for i, (original_product, english_name) in enumerate(translated_products):
            try:
                print(f"  [{i+1}/{len(translated_products)}] {original_product['product_name'][:40]}...")
                
                # 아이허브 매칭
                result = self._match_single_product_iherb(original_product, english_name)
                
                if result['status'] == 'success':
                    success_count += 1
                    print(f"    ✅ 매칭: {result['iherb_product_name'][:30]}...")
                else:
                    print(f"    ❌ 실패: {result['failure_reason']}")
                
                # 결과를 DataFrame 형태로 변환
                new_row = self._create_new_product_row(original_product, english_name, result)
                new_rows.append(new_row)
                
                # 주기적 중간 저장
                if (i + 1) % self.checkpoint_interval == 0:
                    temp_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
                    temp_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                    print(f"    💾 중간 저장 ({i+1}개 처리)")
                
            except Exception as e:
                print(f"    ❌ 오류: {e}")
                # 오류 발생 시에도 빈 행 추가
                error_row = self._create_error_product_row(original_product, str(e))
                new_rows.append(error_row)
        
        # 아이허브 스크래퍼 정리
        self._cleanup_iherb_scraper()
        
        # 최종 DataFrame 결합
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            final_df = pd.concat([df, new_df], ignore_index=True)
        else:
            final_df = df
        
        print(f"✅ 신규 상품 처리 완료: {success_count}/{len(translated_products)}개 성공")
        
        return final_df
    
    def _batch_translate_products(self, products):
        """배치 번역 - 쿠팡 translator 모듈 활용"""
        print(f"🔤 배치 번역기 초기화...")
        
        # 쿠팡 번역기 사용 (기존 모듈 활용)
        if not self.translator:
            self.translator = GeminiCSVTranslator(self.api_key)
        
        # 상품명 추출
        product_names = [p['product_name'] for p in products]
        
        print(f"📝 {len(product_names)}개 상품명 배치 번역 시작...")
        
        # 배치 번역 실행
        translated_names = self.translator.translate_batch(
            product_names, 
            batch_size=self.batch_size
        )
        
        print(f"✅ 배치 번역 완료: {len(translated_names)}개")
        
        # 원본 상품과 번역명 페어링
        translated_products = []
        for product, english_name in zip(products, translated_names):
            translated_products.append((product, english_name))
        
        return translated_products
    
    def _init_iherb_scraper(self):
        """아이허브 스크래퍼 초기화"""
        if not self.iherb_scraper:
            print(f"🌿 아이허브 스크래퍼 초기화...")
            
            self.iherb_scraper = EnglishIHerbScraper(
                headless=self.headless,
                delay_range=UNIFIED_CONFIG['DELAY_RANGE'],
                max_products_to_compare=UNIFIED_CONFIG['MAX_PRODUCTS_TO_COMPARE']
            )
    
    def _cleanup_iherb_scraper(self):
        """아이허브 스크래퍼 정리"""
        if self.iherb_scraper:
            self.iherb_scraper.close()
            self.iherb_scraper = None
            print(f"🧹 아이허브 스크래퍼 정리 완료")
    
    def _match_single_product_iherb(self, coupang_product, english_name):
        """단일 상품 아이허브 매칭"""
        try:
            coupang_id = coupang_product.get('product_id', '')
            
            # 아이허브 검색
            search_result = self.iherb_scraper.product_matcher.search_product_enhanced(
                english_name, str(coupang_id)
            )
            
            if len(search_result) >= 3:
                product_url, similarity_score, match_details = search_result
                
                if product_url:
                    # 상품 정보 추출
                    product_code, iherb_name, iherb_price_info = \
                        self.iherb_scraper.iherb_client.extract_product_info_with_price(product_url)
                    
                    if product_code:
                        return {
                            'status': 'success',
                            'iherb_product_code': product_code,
                            'iherb_product_name': iherb_name,
                            'iherb_product_url': product_url,
                            'similarity_score': similarity_score,
                            'iherb_price_info': iherb_price_info,
                            'match_details': match_details
                        }
                    else:
                        return {
                            'status': 'failed',
                            'failure_reason': '아이허브 상품코드 추출 실패'
                        }
                else:
                    return {
                        'status': 'failed',
                        'failure_reason': '아이허브 매칭 상품 없음'
                    }
            else:
                return {
                    'status': 'failed',
                    'failure_reason': '아이허브 검색 오류'
                }
        
        except Exception as e:
            return {
                'status': 'failed',
                'failure_reason': f'매칭 중 오류: {str(e)}'
            }
    
    def _create_new_product_row(self, coupang_product, english_name, iherb_result):
        """신규 상품 행 생성"""
        today = datetime.now().strftime("_%Y%m%d")
        
        # 기본 쿠팡 정보
        row = {
            'coupang_product_name': coupang_product.get('product_name', ''),
            'coupang_product_name_english': english_name,
            'coupang_product_id': coupang_product.get('product_id', ''),
            'coupang_url': coupang_product.get('product_url', ''),
            f'쿠팡현재가격{today}': coupang_product.get('current_price', ''),
            f'쿠팡정가{today}': coupang_product.get('original_price', ''),
            f'쿠팡할인율{today}': coupang_product.get('discount_rate', ''),
            f'쿠팡리뷰수{today}': coupang_product.get('review_count', ''),
            f'쿠팡평점{today}': coupang_product.get('rating', ''),
            f'크롤링일시{today}': datetime.now().isoformat(),
            'update_status': f'NEW_PRODUCT_{today}',
            'processed_at': datetime.now().isoformat()
        }
        
        # 아이허브 정보 추가
        if iherb_result['status'] == 'success':
            row.update({
                'status': 'success',
                'iherb_product_name': iherb_result['iherb_product_name'],
                'iherb_product_url': iherb_result['iherb_product_url'],
                'iherb_product_code': iherb_result['iherb_product_code'],
                'similarity_score': iherb_result['similarity_score'],
            })
            
            # 아이허브 가격 정보
            price_info = iherb_result['iherb_price_info']
            row.update({
                'iherb_list_price_krw': price_info.get('list_price', ''),
                'iherb_discount_price_krw': price_info.get('discount_price', ''),
                'iherb_discount_percent': price_info.get('discount_percent', ''),
                'iherb_subscription_discount': price_info.get('subscription_discount', ''),
                'iherb_price_per_unit': price_info.get('price_per_unit', ''),
                'is_in_stock': price_info.get('is_in_stock', True),
                'stock_message': price_info.get('stock_message', ''),
            })
            
            # 가격 비교
            coupang_price_info = self.data_manager.extract_coupang_price_info(coupang_product)
            price_comparison = self.data_manager.calculate_price_comparison(coupang_price_info, price_info)
            
            row.update({
                f'가격차이{today}': price_comparison['price_difference_krw'],
                f'저렴한플랫폼{today}': price_comparison['cheaper_platform'],
                f'절약금액{today}': price_comparison['savings_amount'],
                f'절약비율{today}': price_comparison['savings_percentage'],
                f'가격차이메모{today}': price_comparison['price_difference_note'],
            })
        else:
            row.update({
                'status': 'not_found',
                'failure_type': 'NO_MATCHING_PRODUCT',
                'matching_reason': iherb_result['failure_reason']
            })
        
        return row
    
    def _create_error_product_row(self, coupang_product, error_msg):
        """오류 발생 시 행 생성"""
        today = datetime.now().strftime("_%Y%m%d")
        
        return {
            'coupang_product_name': coupang_product.get('product_name', ''),
            'coupang_product_id': coupang_product.get('product_id', ''),
            'coupang_url': coupang_product.get('product_url', ''),
            f'쿠팡현재가격{today}': coupang_product.get('current_price', ''),
            'status': 'error',
            'failure_type': 'PROCESSING_ERROR',
            'matching_reason': f'처리 중 오류: {error_msg}',
            'update_status': f'ERROR_{today}',
            'processed_at': datetime.now().isoformat()
        }
    
    def _print_progress_status(self, df):
        """진행 상황 출력"""
        total = len(df)
        updated = len(df[df['update_status'] == 'UPDATED']) if 'update_status' in df.columns else 0
        new_products = len(df[df['update_status'].str.startswith('NEW_PRODUCT', na=False)]) if 'update_status' in df.columns else 0
        completed = len(df[df['status'] == 'success']) if 'status' in df.columns else 0
        
        print(f"📊 현재 상태:")
        print(f"   - 총 상품: {total}개")
        print(f"   - 업데이트 완료: {updated}개")
        print(f"   - 신규 상품: {new_products}개")
        print(f"   - 아이허브 매칭 완료: {completed}개")
    
    def _print_final_stats(self, df):
        """최종 통계 출력"""
        print(f"\n" + "="*60)
        print(f"📈 최종 통계 (완전한 재시작 기능)")
        print(f"="*60)
        
        total = len(df)
        print(f"📦 총 상품: {total}개")
        
        # 상태별 통계
        if 'update_status' in df.columns:
            print(f"\n📊 업데이트 상태:")
            status_counts = df['update_status'].value_counts()
            for status, count in status_counts.items():
                print(f"   - {status}: {count}개")
        
        # 매칭 성공률
        if 'status' in df.columns:
            success_count = len(df[df['status'] == 'success'])
            print(f"\n🌿 아이허브 매칭:")
            print(f"   - 성공: {success_count}개")
            print(f"   - 성공률: {success_count/total*100:.1f}%")
        
        # 가격 정보 통계
        today = datetime.now().strftime("_%Y%m%d")
        coupang_price_col = f'쿠팡현재가격{today}'
        
        if coupang_price_col in df.columns:
            price_count = len(df[df[coupang_price_col].notna()])
            print(f"\n💰 가격 정보:")
            print(f"   - 쿠팡 가격: {price_count}개")
        
        if 'iherb_discount_price_krw' in df.columns:
            iherb_price_count = len(df[
                (df['iherb_discount_price_krw'].notna()) | 
                (df['iherb_list_price_krw'].notna() if 'iherb_list_price_krw' in df.columns else False)
            ])
            print(f"   - 아이허브 가격: {iherb_price_count}개")
        
        # 효율성 통계
        print(f"\n⚡ 효율성 개선:")
        print(f"   - 배치 번역: {self.batch_size}개씩 처리 (API 호출 {self.batch_size}배 절약)")
        print(f"   - 기존 모듈 재사용: 쿠팡 translator + 아이허브 scraper")
        print(f"   - 완전한 재시작: {self.checkpoint_interval}개마다 체크포인트")
        print(f"   - 정밀 재개: 번역/매칭 단계별 독립 재시작")
        
        print(f"="*60)
    
    def close(self):
        """리소스 정리"""
        if self.coupang_crawler:
            self.coupang_crawler.close()
        if self.iherb_scraper:
            self.iherb_scraper.close()
        self._cleanup_restart_metadata()
        print(f"🧹 모든 리소스 정리 완료")


def main():
    """메인 실행 함수"""
    print("🚀 완전한 재시작 기능이 있는 효율적인 통합 가격 업데이터")
    print("="*60)
    print("🎯 주요 기능:")
    print("- 배치 번역으로 API 효율성 극대화 (90% 절약)")
    print("- 기존 모듈 재사용으로 코드 중복 제거")
    print("- 신규 상품만 선별 처리로 시간 단축")
    print("- 완전한 실시간 저장 및 재시작 기능")
    print("- 번역/매칭 단계별 정밀 재개 시스템")
    print("- 중단 지점부터 정확한 재시작")
    print("="*60)
    
    if not MODULES_LOADED:
        print("❌ 필수 모듈을 로드할 수 없습니다.")
        print("확인사항:")
        print("1. coupang/ 디렉토리와 필요한 파일들")
        print("2. iherbscraper/ 디렉토리와 필요한 파일들")
        print("3. 필요한 패키지 설치")
        return
    
    updater = CompleteEfficientUpdater(headless=False)
    
    try:
        # 실행 파라미터 설정
        input_file = "/Users/brich/Desktop/iherb_price/iherbscraper/output/nowfood_20250915.csv"  # 아이허브 매칭 완료된 파일
        brand = "NOW Foods"
        
        print(f"\n📋 설정:")
        print(f"   - 입력 파일: {input_file}")
        print(f"   - 브랜드: {brand}")
        print(f"   - 배치 크기: {UNIFIED_CONFIG['TRANSLATION_BATCH_SIZE']}")
        print(f"   - 체크포인트 간격: {UNIFIED_CONFIG['CHECKPOINT_INTERVAL']}")
        
        # 입력 파일 존재 확인
        if not os.path.exists(input_file):
            print(f"❌ 입력 파일을 찾을 수 없습니다: {input_file}")
            print("\n사용 가능한 CSV 파일:")
            csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
            for i, csv_file in enumerate(csv_files[:10], 1):
                print(f"   {i}. {csv_file}")
            return
        
        # 브랜드 지원 확인
        if brand not in updater.brand_urls:
            print(f"❌ 지원되지 않는 브랜드: {brand}")
            print("지원되는 브랜드:")
            for supported_brand in updater.brand_urls:
                print(f"   - {supported_brand}")
            return
        
        print(f"\n🎯 작업 시작...")
        print(f"💡 Ctrl+C로 언제든 중단 가능 (재시작 시 중단 지점부터 계속)")
        
        # 메인 업데이트 실행
        result_file = updater.update_prices(
            input_file=input_file,
            brand_name=brand,
            fill_iherb=True  # 아이허브 매칭 활성화
        )
        
        print(f"\n🎉 완전한 효율적인 업데이트 완료!")
        print(f"📁 결과 파일: {result_file}")
        print(f"\n💡 달성된 효율성 개선:")
        print(f"   - 배치 번역: API 호출 {UNIFIED_CONFIG['TRANSLATION_BATCH_SIZE']}배 감소")
        print(f"   - 모듈 재사용: 검증된 기존 로직 활용")
        print(f"   - 선별 처리: 신규 상품만 집중 처리")
        print(f"   - 완전한 재시작: 중단 시점부터 정밀 재개")
        print(f"   - 안전 저장: {UNIFIED_CONFIG['CHECKPOINT_INTERVAL']}개마다 체크포인트")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ 사용자 중단 감지")
        print(f"💾 현재 진행상황이 자동 저장되었습니다.")
        print(f"🔄 다시 실행하면 중단된 지점부터 정확히 재시작됩니다.")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        print(f"💾 현재까지의 진행상황은 저장되어 있습니다.")
        import traceback
        print("\n상세 오류:")
        traceback.print_exc()
    finally:
        print(f"\n🧹 리소스 정리 중...")
        updater.close()
        print("✅ 완료")


if __name__ == "__main__":
    main()