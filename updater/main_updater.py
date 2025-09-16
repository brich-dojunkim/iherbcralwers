"""
메인 업데이터 - 모든 모듈을 조합하는 핵심 클래스 (재시작 로직 개선)
"""

import os
import pandas as pd
from datetime import datetime

from settings import UPDATER_CONFIG, validate_config
from coupang_manager import CoupangManager
from translation_manager import TranslationManager
from iherb_manager import IHerbManager
from restart_manager import RestartManager


class CompleteEfficientUpdater:
    """완전한 재시작 기능이 있는 효율적인 가격 업데이터"""
    
    def __init__(self, headless=False):
        # 설정 검증
        try:
            validate_config()
        except Exception as e:
            print(f"❌ 설정 검증 실패: {e}")
            raise
        
        self.headless = headless
        self.checkpoint_interval = UPDATER_CONFIG['CHECKPOINT_INTERVAL']
        
        # 매니저들 초기화 (오류 체크 추가)
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
        
        print(f"🚀 완전한 효율적인 업데이터 초기화 완료")
        print(f"   - 배치 번역 크기: {UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']}")
        print(f"   - 중간 저장 간격: {self.checkpoint_interval}")
        print(f"   - 지원 브랜드: {len(UPDATER_CONFIG['BRAND_SEARCH_URLS'])}개")
        print(f"   - 완전한 재시작 기능: ✅")
    
    def update_prices(self, input_file, brand_name, output_file=None, fill_iherb=True):
        """메인 업데이트 함수 - 완전한 재시작 지원"""
        print(f"\n🎯 완전한 효율적인 가격 업데이트 시작: {brand_name}")
        
        # 브랜드 검증
        if brand_name not in UPDATER_CONFIG['BRAND_SEARCH_URLS']:
            raise ValueError(f"지원되지 않는 브랜드: {brand_name}")
        
        # 출력 파일명 결정
        if not output_file:
            today = datetime.now().strftime("%Y%m%d")
            output_file = f"complete_efficient_{brand_name.replace(' ', '_')}_{today}.csv"
        
        print(f"📄 작업 파일: {output_file}")
        
        # 재시작 메타데이터 저장
        self.restart_manager.save_metadata(input_file, brand_name, output_file, fill_iherb)
        
        # 기존 작업 파일 확인 (재시작 지원)
        if os.path.exists(output_file):
            print(f"📂 기존 작업 파일 발견 - 재시작 모드")
            working_df = pd.read_csv(output_file, encoding='utf-8-sig')
            
            # DataFrame 구조 검증
            if not self.restart_manager.validate_dataframe_structure(working_df):
                print(f"⚠️ 작업 파일 구조에 문제가 있어 새로 시작합니다.")
                # 기존 파일을 백업하고 새로 시작
                backup_file = f"{output_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                working_df.to_csv(backup_file, index=False, encoding='utf-8-sig')
                print(f"📦 기존 파일 백업: {backup_file}")
            else:
                self.restart_manager.print_progress_status(working_df)
                
                # 미완료 작업이 있는지 정밀 확인
                incomplete_status = self.restart_manager.check_incomplete_work(working_df)
                
                if incomplete_status['has_incomplete']:
                    print(f"🔄 미완료 작업 감지 - 정밀 재개 시작")
                    
                    # 재시작 권장사항 출력
                    recommendations = self.restart_manager.get_restart_recommendations(incomplete_status)
                    if recommendations:
                        print(f"📋 재시작 계획:")
                        for rec in recommendations:
                            print(f"   - {rec}")
                    
                    working_df = self._resume_incomplete_work(working_df, input_file, brand_name, output_file, fill_iherb)
                    self.restart_manager.print_final_stats(working_df)
                    return output_file
                else:
                    print(f"✅ 모든 작업 완료됨")
                    self.restart_manager.cleanup_metadata()
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
        print(f"💾 쿠팡 업데이트 완료")
        
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
        self.restart_manager.print_final_stats(working_df)
        self.restart_manager.cleanup_metadata()
        
        return output_file
    
    def _resume_incomplete_work(self, df, input_file, brand_name, output_file, fill_iherb):
        """미완료 작업 정밀 재개 - 더 세밀한 제어"""
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
                df, new_products = self._update_coupang_and_find_new(input_file, brand_name)
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"💾 쿠팡 업데이트 완료")
                
                # 상태 재확인
                status = self.restart_manager.check_incomplete_work(df)
            except Exception as e:
                print(f"❌ 쿠팡 업데이트 실패: {e}")
                # 오류가 발생해도 기존 작업 계속 진행
        
        # 2. 신규 상품 처리 재개
        if fill_iherb and status['new_products_count'] > 0:
            # 2-1. 번역 재개
            if not status['translation_complete']:
                print(f"🔤 번역 재개: {status['new_products_count'] - status['translated_count']}개 남음")
                try:
                    df = self.translation_manager.translate_untranslated_products(df, output_file)
                    status = self.restart_manager.check_incomplete_work(df)  # 상태 업데이트
                except Exception as e:
                    print(f"❌ 번역 재개 실패: {e}")
            
            # 2-2. 아이허브 매칭 재개  
            if not status['iherb_complete']:
                print(f"🌿 아이허브 매칭 재개: {status['new_products_count'] - status['iherb_processed_count']}개 남음")
                try:
                    df = self.iherb_manager.match_unmatched_products(df, output_file, self.checkpoint_interval)
                except Exception as e:
                    print(f"❌ 아이허브 매칭 재개 실패: {e}")
                    # 오류 상품들을 error 상태로 처리
                    self._mark_failed_products_as_error(df, status)
        
        # 최종 저장
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"✅ 재개 작업 완료")
        
        return df
    
    def _mark_failed_products_as_error(self, df, status):
        """실패한 상품들을 error 상태로 마킹"""
        today = datetime.now().strftime("_%Y%m%d")
        
        # 번역은 되었지만 아이허브 매칭이 안된 상품들
        unmatched = df[
            (df['update_status'] == f'NEW_PRODUCT__{today}') &
            (df['coupang_product_name_english'].notna()) &
            (df['coupang_product_name_english'] != '') &
            (df['status'].isna() | (df['status'] == ''))
        ]
        
        for idx, row in unmatched.iterrows():
            df.at[idx, 'status'] = 'error'
            df.at[idx, 'failure_type'] = 'RESUME_ERROR'
            df.at[idx, 'matching_reason'] = '재개 중 오류로 처리 중단'
        
        print(f"⚠️ {len(unmatched)}개 상품을 오류 상태로 처리")
    
    def _update_coupang_and_find_new(self, input_file, brand_name):
        """쿠팡 재크롤링 + 기존 데이터 업데이트 + 신규 상품 발견"""
        # 기존 데이터 로드
        existing_df = pd.read_csv(input_file, encoding='utf-8-sig')
        print(f"📋 기존 상품: {len(existing_df)}개")
        
        # 쿠팡 재크롤링
        try:
            new_crawled_products = self.coupang_manager.crawl_brand_products(brand_name)
            print(f"🔍 재크롤링 결과: {len(new_crawled_products)}개")
        except Exception as e:
            print(f"❌ 쿠팡 크롤링 실패: {e}")
            # 크롤링 실패 시 기존 데이터에 실패 상태 추가
            for idx, row in existing_df.iterrows():
                existing_df.at[idx, 'update_status'] = 'CRAWLING_FAILED'
            return existing_df, []
        
        # 기존 상품 업데이트
        try:
            existing_df, updated_count = self.coupang_manager.update_existing_products(existing_df, new_crawled_products)
        except Exception as e:
            print(f"❌ 기존 상품 업데이트 실패: {e}")
            updated_count = 0
        
        # 신규 상품 발견
        try:
            new_products = self.coupang_manager.find_new_products(existing_df, new_crawled_products)
        except Exception as e:
            print(f"❌ 신규 상품 발견 실패: {e}")
            new_products = []
        
        print(f"✅ 업데이트: {updated_count}개, 신규 발견: {len(new_products)}개")
        
        return existing_df, new_products
    
    def _process_new_products_batch(self, df, new_products, output_file):
        """신규 상품 배치 처리 - 효율적인 번역 + 아이허브 매칭"""
        if not new_products:
            print(f"ℹ️ 처리할 신규 상품이 없습니다.")
            return df
        
        print(f"🔤 1단계: 배치 번역 ({len(new_products)}개 → {UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']}개씩)")
        
        # 배치 번역 수행
        try:
            translated_products = self.translation_manager.batch_translate_products(new_products)
        except Exception as e:
            print(f"❌ 배치 번역 실패: {e}")
            # 번역 실패 시 원본 이름 사용
            translated_products = [(product, product['product_name']) for product in new_products]
        
        print(f"🌿 2단계: 아이허브 매칭 ({len(translated_products)}개)")
        
        # 신규 상품들을 DataFrame에 추가
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
                
                # 결과를 DataFrame 형태로 변환
                new_row = self.iherb_manager.create_new_product_row(original_product, english_name, result)
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
        
        # 최종 DataFrame 결합
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            final_df = pd.concat([df, new_df], ignore_index=True)
        else:
            final_df = df
        
        print(f"✅ 신규 상품 처리 완료: {success_count}/{len(translated_products)}개 성공")
        
        return final_df
    
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
            'update_status': f'ERROR__{today}',
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