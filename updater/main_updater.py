"""
메인 업데이터 - 마스터 파일 시스템 (공통 패턴 적용)
"""

import os
import pandas as pd
from datetime import datetime

from settings import UPDATER_CONFIG, validate_config
from coupang_manager import CoupangManager
from translation_manager import TranslationManager
from iherb_manager import IHerbManager
from restart_manager import RestartManager
from common import MasterFilePatterns, get_new_products_filter


class CompleteEfficientUpdater:
    """마스터 파일 시스템 - 단일 파일 관리 가격 업데이터 (공통 패턴 적용)"""
    
    def __init__(self, headless=False):
        # 설정 검증
        try:
            validate_config()
        except Exception as e:
            print(f"❌ 설정 검증 실패: {e}")
            raise
        
        self.headless = headless
        self.checkpoint_interval = UPDATER_CONFIG['CHECKPOINT_INTERVAL']
        
        # 매니저들 초기화
        try:
            self.coupang_manager = CoupangManager(headless)
            print("✅ 쿠팡 매니저 초기화 완료")
        except Exception as e:
            print(f"❌ 쿠팡 매니저 초기화 실패: {e}")
            raise
            
        try:
            self.translation_manager = TranslationManager()
            print("✅ 번역 매니저 초기화 완료")
        except Exception as e:
            print(f"❌ 번역 매니저 초기화 실패: {e}")
            raise
            
        try:
            self.iherb_manager = IHerbManager(headless)
            print("✅ 아이허브 매니저 초기화 완료")
        except Exception as e:
            print(f"❌ 아이허브 매니저 초기화 실패: {e}")
            raise
            
        try:
            self.restart_manager = RestartManager()
            print("✅ 재시작 매니저 초기화 완료")
        except Exception as e:
            print(f"❌ 재시작 매니저 초기화 실패: {e}")
            raise
        
        print(f"🚀 마스터 파일 시스템 업데이터 초기화 완료 (공통 패턴 적용)")
        print(f"   - 배치 번역 크기: {UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']}")
        print(f"   - 중간 저장 간격: {self.checkpoint_interval}")
        print(f"   - 지원 브랜드: {len(UPDATER_CONFIG['BRAND_SEARCH_URLS'])}개")
        print(f"   - 마스터 파일 시스템: ✅")
        print(f"   - 공통 패턴 모듈: ✅")
    
    def update_prices(self, initial_file, brand_name, fill_iherb=True):
        """메인 업데이트 함수 - 마스터 파일 시스템 (공통 패턴 적용)"""
        print(f"\n🎯 마스터 파일 시스템 가격 업데이트 시작: {brand_name}")
        
        # 브랜드 검증
        if brand_name not in UPDATER_CONFIG['BRAND_SEARCH_URLS']:
            raise ValueError(f"지원되지 않는 브랜드: {brand_name}")
        
        # 마스터 파일명 결정
        master_file = f"master_{brand_name.replace(' ', '_')}.csv"
        print(f"📄 마스터 파일: {master_file}")
        
        # 재시작 메타데이터 저장
        self.restart_manager.save_metadata(initial_file, brand_name, master_file, fill_iherb)
        
        # 마스터 파일 초기화 또는 로드
        if not os.path.exists(master_file):
            print(f"🆕 마스터 파일 생성 - 초기 데이터로부터")
            master_df = self._initialize_master_file(initial_file, master_file)
        else:
            print(f"📂 기존 마스터 파일 로드")
            master_df = pd.read_csv(master_file, encoding='utf-8-sig')
            
            # DataFrame 구조 검증
            if not self.restart_manager.validate_dataframe_structure(master_df):
                print(f"⚠️ 마스터 파일 구조에 문제가 있어 백업 후 재생성합니다.")
                backup_file = f"{master_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                master_df.to_csv(backup_file, index=False, encoding='utf-8-sig')
                print(f"📦 기존 파일 백업: {backup_file}")
                master_df = self._initialize_master_file(initial_file, master_file)
            else:
                self.restart_manager.print_progress_status(master_df)
                
                # 미완료 작업이 있는지 확인
                incomplete_status = self.restart_manager.check_incomplete_work(master_df)
                
                if incomplete_status['has_incomplete']:
                    print(f"🔄 미완료 작업 감지 - 정밀 재개 시작")
                    master_df = self._resume_incomplete_work(master_df, brand_name, master_file, fill_iherb)
                    self.restart_manager.print_final_stats(master_df)
                    return master_file
        
        # 메인 업데이트 실행
        print(f"\n" + "="*60)
        print(f"📊 마스터 파일 업데이트")
        print(f"="*60)
        
        # 1. 쿠팡 가격 업데이트 + 신규 상품 발견
        master_df, new_products = self._update_master_with_coupang(master_df, brand_name, master_file)
        
        # 2. 신규 상품 처리
        if fill_iherb and len(new_products) > 0:
            print(f"\n🌿 신규 상품 처리: {len(new_products)}개")
            master_df = self._process_new_products_in_master(master_df, new_products, master_file)
        
        # 최종 저장
        master_df.to_csv(master_file, index=False, encoding='utf-8-sig')
        print(f"\n✅ 마스터 파일 업데이트 완료: {master_file}")
        
        # 통계 출력 및 정리
        self.restart_manager.print_final_stats(master_df)
        self.restart_manager.cleanup_metadata()
        
        return master_file
    
    def _initialize_master_file(self, initial_file, master_file):
        """초기 데이터로부터 마스터 파일 생성"""
        print(f"📋 초기 데이터 로드: {initial_file}")
        
        if not os.path.exists(initial_file):
            raise FileNotFoundError(f"초기 데이터 파일을 찾을 수 없습니다: {initial_file}")
        
        # 초기 데이터 로드
        initial_df = pd.read_csv(initial_file, encoding='utf-8-sig')
        print(f"   - 초기 상품: {len(initial_df)}개")
        
        # 마스터 파일 구조로 변환
        master_df = self._convert_to_master_structure(initial_df)
        
        # 마스터 파일 저장
        master_df.to_csv(master_file, index=False, encoding='utf-8-sig')
        print(f"✅ 마스터 파일 생성 완료: {master_file}")
        
        return master_df
    
    def _convert_to_master_structure(self, df):
        """기존 데이터를 마스터 파일 구조로 변환"""
        # 기본 컬럼들은 유지
        master_columns = [
            'coupang_product_id', 'coupang_product_name', 'coupang_product_name_english',
            'coupang_url', 'iherb_product_name', 'iherb_product_url', 'iherb_product_code',
            'similarity_score', 'matching_reason', 'gemini_confidence', 'failure_type',
            'status', 'iherb_list_price_krw', 'iherb_discount_price_krw', 'iherb_discount_percent',
            'iherb_subscription_discount', 'iherb_price_per_unit', 'is_in_stock', 'stock_message',
            'price_difference_note', 'processed_at', 'actual_index', 'search_language', 'update_status'
        ]
        
        # 새 DataFrame 생성 (기존 컬럼 유지)
        master_df = df.copy()
        
        # 마스터 파일 전용 컬럼 추가
        master_df['created_at'] = datetime.now().isoformat()
        master_df['last_updated'] = datetime.now().isoformat()
        
        return master_df
    
    def _resume_incomplete_work(self, df, brand_name, master_file, fill_iherb):
        """미완료 작업 정밀 재개 - 공통 패턴 적용"""
        print(f"🔍 미완료 작업 상태 분석 중...")
        
        status = self.restart_manager.check_incomplete_work(df)
        
        print(f"📊 작업 상태:")
        print(f"   - 쿠팡 업데이트: {'✅' if status['coupang_complete'] else '❌'}")
        print(f"   - 신규 상품 번역: {status['translated_count']}/{status['new_products_count']}")
        print(f"   - 아이허브 매칭: {status['iherb_processed_count']}/{status['new_products_count']}")
        
        # 1. 쿠팡 업데이트가 미완료면 다시 실행
        if not status['coupang_complete']:
            print(f"🔄 쿠팡 업데이트 재실행...")
            try:
                df, new_products = self._update_master_with_coupang(df, brand_name, master_file)
                status = self.restart_manager.check_incomplete_work(df)
            except Exception as e:
                print(f"❌ 쿠팡 업데이트 실패: {e}")
        
        # 2. 신규 상품 처리 재개
        if fill_iherb and status['new_products_count'] > 0:
            # 2-1. 번역 재개
            if not status['translation_complete']:
                print(f"🔤 번역 재개: {status['new_products_count'] - status['translated_count']}개 남음")
                try:
                    df = self.translation_manager.translate_untranslated_products(df, master_file)
                    status = self.restart_manager.check_incomplete_work(df)
                except Exception as e:
                    print(f"❌ 번역 재개 실패: {e}")
            
            # 2-2. 아이허브 매칭 재개  
            if not status['iherb_complete']:
                print(f"🌿 아이허브 매칭 재개: {status['new_products_count'] - status['iherb_processed_count']}개 남음")
                try:
                    df = self.iherb_manager.match_new_products_for_updated_prices(df, master_file, self.checkpoint_interval)
                except Exception as e:
                    print(f"❌ 아이허브 매칭 재개 실패: {e}")
                    self._mark_failed_products_as_error(df, status)
        
        # 최종 저장
        df['last_updated'] = datetime.now().isoformat()
        df.to_csv(master_file, index=False, encoding='utf-8-sig')
        print(f"✅ 재개 작업 완료")
        
        return df
    
    def _mark_failed_products_as_error(self, df, status):
        """실패한 상품들을 error 상태로 마킹 - 공통 패턴 적용"""
        
        # ✅ 공통 패턴 사용
        unmatched = df[
            get_new_products_filter(df) &
            (df['coupang_product_name_english'].notna()) &
            (df['coupang_product_name_english'] != '') &
            (df['status'].isna() | (df['status'] == ''))
        ]
        
        for idx, row in unmatched.iterrows():
            df.at[idx, 'status'] = 'error'
            df.at[idx, 'failure_type'] = 'RESUME_ERROR'
            df.at[idx, 'matching_reason'] = '재개 중 오류로 처리 중단'
        
        print(f"⚠️ {len(unmatched)}개 상품을 오류 상태로 처리")
    
    def _update_master_with_coupang(self, master_df, brand_name, master_file):
        """마스터 파일에 쿠팡 데이터 업데이트"""
        print(f"🤖 쿠팡 재크롤링 시작...")
        
        # 쿠팡 재크롤링
        try:
            new_crawled_products = self.coupang_manager.crawl_brand_products(brand_name)
            print(f"🔍 재크롤링 결과: {len(new_crawled_products)}개")
        except Exception as e:
            print(f"❌ 쿠팡 크롤링 실패: {e}")
            return master_df, []
        
        # 기존 상품 가격 업데이트
        master_df, updated_count = self.coupang_manager.update_master_prices(master_df, new_crawled_products)
        
        # 신규 상품 발견
        new_products = self.coupang_manager.find_new_products_for_master(master_df, new_crawled_products)
        
        print(f"✅ 가격 업데이트: {updated_count}개, 신규 발견: {len(new_products)}개")
        
        # 중간 저장
        master_df['last_updated'] = datetime.now().isoformat()
        master_df.to_csv(master_file, index=False, encoding='utf-8-sig')
        print(f"💾 중간 저장 완료")
        
        return master_df, new_products
    
    def _process_new_products_in_master(self, master_df, new_products, master_file):
        """마스터 파일에 신규 상품 추가 처리 - 공통 패턴 적용"""
        if not new_products:
            return master_df
        
        print(f"🔤 1단계: 배치 번역 ({len(new_products)}개)")
        
        # 배치 번역
        try:
            translated_products = self.translation_manager.batch_translate_products(new_products)
        except Exception as e:
            print(f"❌ 배치 번역 실패: {e}")
            translated_products = [(product, product['product_name']) for product in new_products]
        
        print(f"🌿 2단계: 아이허브 매칭 ({len(translated_products)}개)")
        
        # 신규 상품들을 마스터 파일에 추가
        new_rows = []
        success_count = 0
        
        for i, (original_product, english_name) in enumerate(translated_products):
            try:
                print(f"  [{i+1}/{len(translated_products)}] {original_product['product_name'][:40]}...")
                
                # 아이허브 매칭
                result = self.iherb_manager.match_single_product(original_product, english_name)
                
                if result['status'] == 'success':
                    success_count += 1
                    print(f"    ✅ 매칭: {result['iherb_product_name'][:30]}...")
                else:
                    print(f"    ❌ 실패: {result['failure_reason']}")
                
                # 마스터 파일용 새 행 생성
                new_row = self._create_master_new_row(original_product, english_name, result)
                new_rows.append(new_row)
                
                # 주기적 중간 저장
                if (i + 1) % self.checkpoint_interval == 0:
                    temp_df = pd.concat([master_df, pd.DataFrame(new_rows)], ignore_index=True)
                    temp_df['last_updated'] = datetime.now().isoformat()
                    temp_df.to_csv(master_file, index=False, encoding='utf-8-sig')
                    print(f"    💾 중간 저장 ({i+1}개 처리)")
                
            except Exception as e:
                print(f"    ❌ 오류: {e}")
                error_row = self._create_master_error_row(original_product, str(e))
                new_rows.append(error_row)
        
        # 최종 DataFrame 결합
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            final_df = pd.concat([master_df, new_df], ignore_index=True)
        else:
            final_df = master_df
        
        print(f"✅ 신규 상품 처리 완료: {success_count}/{len(translated_products)}개 성공")
        
        return final_df
    
    def _create_master_new_row(self, coupang_product, english_name, iherb_result):
        """마스터 파일용 신규 상품 행 생성 - 공통 패턴 적용"""
        
        # ✅ 공통 컬럼명 사용
        coupang_columns = MasterFilePatterns.get_daily_coupang_columns()
        iherb_columns = MasterFilePatterns.get_daily_iherb_columns()
        
        # 기본 정보
        row = {
            'coupang_product_name': coupang_product.get('product_name', ''),
            'coupang_product_name_english': english_name,
            'coupang_product_id': coupang_product.get('product_id', ''),
            'coupang_url': coupang_product.get('product_url', ''),
            coupang_columns['current_price']: coupang_product.get('current_price', ''),
            coupang_columns['original_price']: coupang_product.get('original_price', ''),
            coupang_columns['discount_rate']: coupang_product.get('discount_rate', ''),
            coupang_columns['review_count']: coupang_product.get('review_count', ''),
            coupang_columns['rating']: coupang_product.get('rating', ''),
            'update_status': MasterFilePatterns.get_new_product_status(),
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
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
            coupang_price_info = self.iherb_manager.data_manager.extract_coupang_price_info(coupang_product)
            price_comparison = self.iherb_manager.data_manager.calculate_price_comparison(coupang_price_info, price_info)
            
            row.update({
                iherb_columns['price_difference']: price_comparison['price_difference_krw'],
                iherb_columns['cheaper_platform']: price_comparison['cheaper_platform'],
                iherb_columns['savings_amount']: price_comparison['savings_amount'],
                iherb_columns['savings_percentage']: price_comparison['savings_percentage'],
                iherb_columns['price_difference_note']: price_comparison['price_difference_note'],
            })
        else:
            row.update({
                'status': 'not_found',
                'failure_type': 'NO_MATCHING_PRODUCT',
                'matching_reason': iherb_result['failure_reason']
            })
        
        return row
    
    def _create_master_error_row(self, coupang_product, error_msg):
        """마스터 파일용 오류 행 생성 - 공통 패턴 적용"""
        
        # ✅ 공통 컬럼명 사용
        coupang_columns = MasterFilePatterns.get_daily_coupang_columns()
        
        return {
            'coupang_product_name': coupang_product.get('product_name', ''),
            'coupang_product_id': coupang_product.get('product_id', ''),
            'coupang_url': coupang_product.get('product_url', ''),
            coupang_columns['current_price']: coupang_product.get('current_price', ''),
            'status': 'error',
            'failure_type': 'PROCESSING_ERROR',
            'matching_reason': f'처리 중 오류: {error_msg}',
            'update_status': f'ERROR_{MasterFilePatterns.get_today_suffix()}',
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'processed_at': datetime.now().isoformat()
        }
    
    def close(self):
        """리소스 정리"""
        try:
            self.coupang_manager.close()
        except:
            pass
        
        try:
            self.iherb_manager.close()
        except:
            pass
        
        try:
            self.restart_manager.cleanup_metadata()
        except:
            pass
        
        print(f"🧹 모든 리소스 정리 완료")