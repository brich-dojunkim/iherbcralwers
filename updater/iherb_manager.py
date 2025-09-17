"""
아이허브 매칭 관리자 - 마스터 파일 시스템 지원 (공통 패턴 적용)
"""

import sys
import importlib.util
from datetime import datetime
from settings import IHERB_PATH, UPDATER_CONFIG
from common import MasterFilePatterns, get_new_products_filter

# 아이허브 모듈 경로 추가
sys.path.insert(0, str(IHERB_PATH))

try:
    # 명시적으로 iherbscraper의 config 모듈을 import
    iherb_config_spec = importlib.util.spec_from_file_location("iherb_config", IHERB_PATH / "config.py")
    iherb_config = importlib.util.module_from_spec(iherb_config_spec)
    iherb_config_spec.loader.exec_module(iherb_config)
    
    from main import EnglishIHerbScraper
    from data_manager import DataManager
    
    IHERB_AVAILABLE = True
    print("✅ 아이허브 모듈 로드 성공")
    
    # Config 동적 패치
    iherb_config.Config.GEMINI_API_KEY = UPDATER_CONFIG['GEMINI_API_KEY']
    iherb_config.Config.GEMINI_TEXT_MODEL = UPDATER_CONFIG['GEMINI_TEXT_MODEL']
    iherb_config.Config.GEMINI_VISION_MODEL = UPDATER_CONFIG['GEMINI_VISION_MODEL']
    
except Exception as e:
    print(f"❌ 아이허브 모듈 로드 실패: {e}")
    IHERB_AVAILABLE = False


class IHerbManager:
    """아이허브 매칭 전담 관리자 - 마스터 파일 시스템 (공통 패턴 적용)"""
    
    def __init__(self, headless=False):
        if not IHERB_AVAILABLE:
            raise ImportError("아이허브 모듈을 사용할 수 없습니다.")
        
        self.headless = headless
        self.scraper = None
        self.data_manager = DataManager()
        self.delay_range = UPDATER_CONFIG['DELAY_RANGE']
        self.max_products = UPDATER_CONFIG['MAX_PRODUCTS_TO_COMPARE']
    
    def init_scraper(self):
        """아이허브 스크래퍼 초기화"""
        if not self.scraper:
            print(f"🌿 아이허브 스크래퍼 초기화...")
            
            try:
                self.scraper = EnglishIHerbScraper(
                    headless=self.headless,
                    delay_range=self.delay_range,
                    max_products_to_compare=self.max_products
                )
                print(f"✅ 아이허브 스크래퍼 초기화 성공")
            except Exception as e:
                print(f"❌ 아이허브 스크래퍼 초기화 실패: {e}")
                import traceback
                traceback.print_exc()
                raise
    
    def match_single_product(self, coupang_product, english_name):
        """단일 상품 아이허브 매칭"""
        try:
            self.init_scraper()
            
            coupang_id = coupang_product.get('product_id', '')
            
            # 아이허브 검색
            search_result = self.scraper.product_matcher.search_product_enhanced(
                english_name, str(coupang_id)
            )
            
            if len(search_result) >= 3:
                product_url, similarity_score, match_details = search_result
                
                if product_url:
                    # 상품 정보 추출
                    product_code, iherb_name, iherb_price_info = \
                        self.scraper.iherb_client.extract_product_info_with_price(product_url)
                    
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
            error_msg = str(e)
            print(f"    매칭 중 오류: {error_msg}")
            
            if "BrowserManager" in error_msg:
                return {
                    'status': 'failed',
                    'failure_reason': f'브라우저 초기화 오류: {error_msg}'
                }
            elif "GEMINI" in error_msg.upper():
                return {
                    'status': 'failed',
                    'failure_reason': f'Gemini API 오류: {error_msg}'
                }
            else:
                return {
                    'status': 'failed',
                    'failure_reason': f'매칭 중 오류: {error_msg}'
                }
    
    def match_new_products_for_updated_prices(self, master_df, master_file, checkpoint_interval):
        """마스터 파일의 신규 상품들에 대한 아이허브 매칭 - 공통 패턴 적용"""
        
        # ✅ 공통 패턴 사용 - 신규 상품 중에서 아이허브 매칭이 필요한 상품들 선별
        new_products_needing_iherb = master_df[
            get_new_products_filter(master_df) &
            (master_df['coupang_product_name_english'].notna()) &
            (master_df['coupang_product_name_english'] != '')
        ].copy()
        
        print(f"🔍 신규 상품 아이허브 매칭 대상 분석:")
        print(f"   - 사용된 패턴: '{MasterFilePatterns.get_new_product_status()}'")
        print(f"   - 총 신규 상품: {len(new_products_needing_iherb)}개")
        
        # ✅ 공통 컬럼명 사용 - 오늘 날짜 기준 아이허브 가격 정보가 이미 있는 상품 제외
        iherb_columns = MasterFilePatterns.get_daily_iherb_columns()
        today_iherb_columns = [
            iherb_columns['list_price'], 
            iherb_columns['discount_price'], 
            iherb_columns['discount_percent'], 
            iherb_columns['price_per_unit']
        ]
        
        already_processed_today = new_products_needing_iherb[
            new_products_needing_iherb[today_iherb_columns].notna().any(axis=1)
        ]
        
        # 실제 매칭이 필요한 상품들
        needs_matching = new_products_needing_iherb[
            ~new_products_needing_iherb[today_iherb_columns].notna().any(axis=1)
        ]
        
        print(f"   - 오늘 아이허브 정보 이미 있음: {len(already_processed_today)}개")
        print(f"   - 아이허브 매칭 필요: {len(needs_matching)}개")
        
        if len(needs_matching) == 0:
            print(f"ℹ️ 아이허브 매칭할 신규 상품이 없습니다")
            return master_df
        
        print(f"🌿 신규 상품 아이허브 매칭 시작: {len(needs_matching)}개")
        
        # 스크래퍼 초기화 시도
        try:
            self.init_scraper()
        except Exception as e:
            print(f"❌ 아이허브 스크래퍼 초기화 실패: {e}")
            # 모든 상품을 오류로 처리
            for idx, row in needs_matching.iterrows():
                master_df.at[idx, iherb_columns['matching_status']] = 'scraper_init_error'
                master_df.at[idx, iherb_columns['matching_reason']] = f'스크래퍼 초기화 실패: {str(e)}'
            return master_df
        
        success_count = 0
        
        for i, (idx, row) in enumerate(needs_matching.iterrows()):
            try:
                print(f"  [{i+1}/{len(needs_matching)}] {row['coupang_product_name'][:40]}...")
                
                # ✅ 공통 컬럼명 사용 - 쿠팡 상품 정보 구성 (오늘 날짜 기준)
                coupang_columns = MasterFilePatterns.get_daily_coupang_columns()
                coupang_product = {
                    'product_id': row['coupang_product_id'],
                    'product_name': row['coupang_product_name'],
                    'product_url': row.get('coupang_url', ''),
                    'current_price': row.get(coupang_columns['current_price'], ''),
                    'original_price': row.get(coupang_columns['original_price'], ''),
                    'discount_rate': row.get(coupang_columns['discount_rate'], '')
                }
                
                english_name = row['coupang_product_name_english']
                result = self.match_single_product(coupang_product, english_name)
                
                # ✅ 공통 컬럼명 사용 - 오늘 날짜 기준 아이허브 컬럼에 결과 저장
                master_df = self._update_master_with_iherb_result(master_df, idx, result, coupang_product)
                
                if result['status'] == 'success':
                    success_count += 1
                    print(f"    ✅ 매칭: {result['iherb_product_name'][:30]}...")
                else:
                    print(f"    ❌ 실패: {result['failure_reason']}")
                
                # 주기적 중간 저장
                if (i + 1) % checkpoint_interval == 0:
                    master_df['last_updated'] = datetime.now().isoformat()
                    master_df.to_csv(master_file, index=False, encoding='utf-8-sig')
                    print(f"    💾 중간 저장 ({i+1}개 처리)")
            
            except Exception as e:
                print(f"    ❌ 오류: {e}")
                master_df.at[idx, iherb_columns['matching_status']] = 'processing_error'
                master_df.at[idx, iherb_columns['matching_reason']] = f'처리 중 오류: {str(e)}'
        
        print(f"✅ 신규 상품 아이허브 매칭 완료: {success_count}/{len(needs_matching)}개 성공")
        return master_df
    
    def _update_master_with_iherb_result(self, master_df, idx, result, coupang_product):
        """마스터 파일에 오늘 날짜 기준 아이허브 매칭 결과 업데이트 - 공통 컬럼명 적용"""
        
        # ✅ 공통 컬럼명 사용
        iherb_columns = MasterFilePatterns.get_daily_iherb_columns()
        
        if result['status'] == 'success':
            # 아이허브 매칭 성공
            master_df.at[idx, iherb_columns['matching_status']] = 'success'
            master_df.at[idx, f'아이허브상품명_{MasterFilePatterns.get_today_suffix()}'] = result['iherb_product_name']
            master_df.at[idx, f'아이허브상품URL_{MasterFilePatterns.get_today_suffix()}'] = result['iherb_product_url']
            master_df.at[idx, f'아이허브상품코드_{MasterFilePatterns.get_today_suffix()}'] = result['iherb_product_code']
            master_df.at[idx, f'유사도점수_{MasterFilePatterns.get_today_suffix()}'] = result['similarity_score']
            
            # 아이허브 가격 정보 (오늘 날짜 기준)
            price_info = result['iherb_price_info']
            master_df.at[idx, iherb_columns['list_price']] = price_info.get('list_price', '')
            master_df.at[idx, iherb_columns['discount_price']] = price_info.get('discount_price', '')
            master_df.at[idx, iherb_columns['discount_percent']] = price_info.get('discount_percent', '')
            master_df.at[idx, iherb_columns['subscription_discount']] = price_info.get('subscription_discount', '')
            master_df.at[idx, iherb_columns['price_per_unit']] = price_info.get('price_per_unit', '')
            master_df.at[idx, iherb_columns['stock_status']] = price_info.get('is_in_stock', True)
            master_df.at[idx, iherb_columns['stock_message']] = price_info.get('stock_message', '')
            
            # 가격 비교 (오늘 날짜 기준)
            coupang_price_info = self.data_manager.extract_coupang_price_info(coupang_product)
            price_comparison = self.data_manager.calculate_price_comparison(coupang_price_info, price_info)
            
            master_df.at[idx, iherb_columns['price_difference']] = price_comparison['price_difference_krw']
            master_df.at[idx, iherb_columns['cheaper_platform']] = price_comparison['cheaper_platform']
            master_df.at[idx, iherb_columns['savings_amount']] = price_comparison['savings_amount']
            master_df.at[idx, iherb_columns['savings_percentage']] = price_comparison['savings_percentage']
            master_df.at[idx, iherb_columns['price_difference_note']] = price_comparison['price_difference_note']
            
            # 기본 매칭 정보도 업데이트 (마스터 파일은 최신 정보 유지)
            master_df.at[idx, 'status'] = 'success'
            master_df.at[idx, 'iherb_product_name'] = result['iherb_product_name']
            master_df.at[idx, 'iherb_product_url'] = result['iherb_product_url']
            master_df.at[idx, 'iherb_product_code'] = result['iherb_product_code']
            master_df.at[idx, 'similarity_score'] = result['similarity_score']
            
        else:
            # 아이허브 매칭 실패
            master_df.at[idx, iherb_columns['matching_status']] = 'not_found'
            master_df.at[idx, iherb_columns['matching_reason']] = result['failure_reason']
            
            # 기본 상태도 업데이트
            master_df.at[idx, 'status'] = 'not_found'
            master_df.at[idx, 'failure_type'] = 'NO_MATCHING_PRODUCT'
            master_df.at[idx, 'matching_reason'] = result['failure_reason']
        
        # 매칭 처리 시각 기록
        master_df.at[idx, iherb_columns['matched_at']] = datetime.now().isoformat()
        master_df.at[idx, 'last_updated'] = datetime.now().isoformat()
        
        return master_df
    
    def update_iherb_prices_for_existing(self, master_df, master_file, checkpoint_interval):
        """기존 매칭된 상품들의 아이허브 가격 재수집 - 공통 컬럼명 적용"""
        
        # ✅ 공통 컬럼명 사용
        iherb_columns = MasterFilePatterns.get_daily_iherb_columns()
        
        # 이미 매칭된 상품들 중 오늘 아이허브 가격이 없는 상품들
        matched_products = master_df[
            (master_df['status'] == 'success') &
            (master_df['iherb_product_url'].notna()) &
            (master_df['iherb_product_url'] != '')
        ].copy()
        
        # 오늘 아이허브 가격이 이미 있는지 확인
        today_iherb_columns = [iherb_columns['list_price'], iherb_columns['discount_price']]
        already_has_today_price = matched_products[
            matched_products[today_iherb_columns].notna().any(axis=1)
        ]
        
        needs_price_update = matched_products[
            ~matched_products[today_iherb_columns].notna().any(axis=1)
        ]
        
        print(f"🔄 기존 매칭 상품 아이허브 가격 업데이트:")
        print(f"   - 매칭된 총 상품: {len(matched_products)}개")
        print(f"   - 오늘 가격 이미 있음: {len(already_has_today_price)}개")
        print(f"   - 가격 업데이트 필요: {len(needs_price_update)}개")
        
        if len(needs_price_update) == 0:
            print(f"ℹ️ 가격 업데이트가 필요한 상품이 없습니다")
            return master_df
        
        try:
            self.init_scraper()
        except Exception as e:
            print(f"❌ 아이허브 스크래퍼 초기화 실패: {e}")
            return master_df
        
        success_count = 0
        
        for i, (idx, row) in enumerate(needs_price_update.iterrows()):
            try:
                product_url = row['iherb_product_url']
                product_name = row['coupang_product_name']
                
                print(f"  [{i+1}/{len(needs_price_update)}] {product_name[:40]}...")
                
                # 아이허브 가격 정보만 재수집
                product_code, iherb_name, iherb_price_info = \
                    self.scraper.iherb_client.extract_product_info_with_price(product_url)
                
                if iherb_price_info:
                    # ✅ 공통 컬럼명 사용 - 오늘 날짜 아이허브 가격 정보 업데이트
                    master_df.at[idx, iherb_columns['list_price']] = iherb_price_info.get('list_price', '')
                    master_df.at[idx, iherb_columns['discount_price']] = iherb_price_info.get('discount_price', '')
                    master_df.at[idx, iherb_columns['discount_percent']] = iherb_price_info.get('discount_percent', '')
                    master_df.at[idx, iherb_columns['subscription_discount']] = iherb_price_info.get('subscription_discount', '')
                    master_df.at[idx, iherb_columns['price_per_unit']] = iherb_price_info.get('price_per_unit', '')
                    master_df.at[idx, iherb_columns['stock_status']] = iherb_price_info.get('is_in_stock', True)
                    master_df.at[idx, iherb_columns['stock_message']] = iherb_price_info.get('stock_message', '')
                    master_df.at[idx, f'아이허브가격수집일시_{MasterFilePatterns.get_today_suffix()}'] = datetime.now().isoformat()
                    
                    # 기본 아이허브 정보도 최신으로 업데이트
                    master_df.at[idx, 'iherb_list_price_krw'] = iherb_price_info.get('list_price', '')
                    master_df.at[idx, 'iherb_discount_price_krw'] = iherb_price_info.get('discount_price', '')
                    master_df.at[idx, 'iherb_discount_percent'] = iherb_price_info.get('discount_percent', '')
                    master_df.at[idx, 'iherb_subscription_discount'] = iherb_price_info.get('subscription_discount', '')
                    master_df.at[idx, 'iherb_price_per_unit'] = iherb_price_info.get('price_per_unit', '')
                    master_df.at[idx, 'is_in_stock'] = iherb_price_info.get('is_in_stock', True)
                    master_df.at[idx, 'stock_message'] = iherb_price_info.get('stock_message', '')
                    
                    # ✅ 공통 컬럼명 사용 - 가격 비교 재계산 (오늘 쿠팡 가격과 비교)
                    coupang_columns = MasterFilePatterns.get_daily_coupang_columns()
                    coupang_product = {
                        'current_price': row.get(coupang_columns['current_price'], ''),
                        'original_price': row.get(coupang_columns['original_price'], ''),
                        'discount_rate': row.get(coupang_columns['discount_rate'], '')
                    }
                    
                    if coupang_product['current_price']:
                        coupang_price_info = self.data_manager.extract_coupang_price_info(coupang_product)
                        price_comparison = self.data_manager.calculate_price_comparison(coupang_price_info, iherb_price_info)
                        
                        master_df.at[idx, iherb_columns['price_difference']] = price_comparison['price_difference_krw']
                        master_df.at[idx, iherb_columns['cheaper_platform']] = price_comparison['cheaper_platform']
                        master_df.at[idx, iherb_columns['savings_amount']] = price_comparison['savings_amount']
                        master_df.at[idx, iherb_columns['savings_percentage']] = price_comparison['savings_percentage']
                        master_df.at[idx, iherb_columns['price_difference_note']] = price_comparison['price_difference_note']
                    
                    success_count += 1
                    print(f"    ✅ 가격 업데이트 성공")
                else:
                    print(f"    ❌ 가격 수집 실패")
                    master_df.at[idx, f'아이허브가격수집일시_{MasterFilePatterns.get_today_suffix()}'] = datetime.now().isoformat()
                    master_df.at[idx, f'아이허브가격수집상태_{MasterFilePatterns.get_today_suffix()}'] = 'failed'
                
                master_df.at[idx, 'last_updated'] = datetime.now().isoformat()
                
                # 주기적 중간 저장
                if (i + 1) % checkpoint_interval == 0:
                    master_df.to_csv(master_file, index=False, encoding='utf-8-sig')
                    print(f"    💾 중간 저장 ({i+1}개 처리)")
            
            except Exception as e:
                print(f"    ❌ 오류: {e}")
                master_df.at[idx, f'아이허브가격수집일시_{MasterFilePatterns.get_today_suffix()}'] = datetime.now().isoformat()
                master_df.at[idx, f'아이허브가격수집상태_{MasterFilePatterns.get_today_suffix()}'] = f'error: {str(e)}'
        
        print(f"✅ 아이허브 가격 업데이트 완료: {success_count}/{len(needs_price_update)}개 성공")
        return master_df
    
    def match_unmatched_products(self, df, output_file, checkpoint_interval):
        """기존 함수 - 호환성 유지를 위해 새 함수로 리다이렉트"""
        print("🔄 match_unmatched_products → match_new_products_for_updated_prices 리다이렉트")
        return self.match_new_products_for_updated_prices(df, output_file, checkpoint_interval)
    
    def create_new_product_row(self, coupang_product, english_name, iherb_result):
        """신규 상품 행 생성 (기존 함수 유지 - 호환성) - 공통 컬럼명 적용"""
        
        # ✅ 공통 컬럼명 사용
        coupang_columns = MasterFilePatterns.get_daily_coupang_columns()
        iherb_columns = MasterFilePatterns.get_daily_iherb_columns()
        
        # 기본 쿠팡 정보
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
            coupang_columns['crawled_at']: datetime.now().isoformat(),
            'update_status': MasterFilePatterns.get_new_product_status(),
            'processed_at': datetime.now().isoformat(),
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat()
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
    
    def analyze_iherb_price_trends(self, master_df):
        """아이허브 가격 트렌드 분석 (마스터 파일 전용)"""
        iherb_price_columns = [col for col in master_df.columns if col.startswith('아이허브할인가_')]
        iherb_price_columns.sort()
        
        if len(iherb_price_columns) < 2:
            print(f"ℹ️ 아이허브 가격 트렌드 분석을 위한 데이터가 부족합니다 ({len(iherb_price_columns)}개 날짜)")
            return {}
        
        print(f"📊 아이허브 가격 트렌드 분석:")
        print(f"   - 분석 기간: {iherb_price_columns[0]} ~ {iherb_price_columns[-1]}")
        
        trends = {
            'price_increases': 0,
            'price_decreases': 0,
            'price_stable': 0,
            'out_of_stock_increases': 0,
            'back_in_stock': 0
        }
        
        # 최근 2개 날짜 비교
        latest_col = iherb_price_columns[-1]
        prev_col = iherb_price_columns[-2]
        
        for idx, row in master_df.iterrows():
            try:
                latest_price = row.get(latest_col, '')
                prev_price = row.get(prev_col, '')
                
                if not latest_price or not prev_price:
                    continue
                
                latest_val = int(str(latest_price).replace(',', '').replace('원', ''))
                prev_val = int(str(prev_price).replace(',', '').replace('원', ''))
                
                if latest_val > prev_val:
                    trends['price_increases'] += 1
                elif latest_val < prev_val:
                    trends['price_decreases'] += 1
                else:
                    trends['price_stable'] += 1
                    
            except (ValueError, TypeError):
                continue
        
        print(f"   - 가격 상승: {trends['price_increases']}개")
        print(f"   - 가격 하락: {trends['price_decreases']}개")
        print(f"   - 가격 동일: {trends['price_stable']}개")
        
        return trends
    
    def get_matching_success_rate(self, master_df):
        """매칭 성공률 분석"""
        total_products = len(master_df)
        successful_matches = len(master_df[master_df['status'] == 'success'])
        failed_matches = len(master_df[master_df['status'] == 'not_found'])
        error_matches = len(master_df[master_df['status'] == 'error'])
        unprocessed = total_products - successful_matches - failed_matches - error_matches
        
        success_rate = (successful_matches / total_products * 100) if total_products > 0 else 0
        
        print(f"📈 매칭 성공률 분석:")
        print(f"   - 총 상품: {total_products}개")
        print(f"   - 매칭 성공: {successful_matches}개 ({success_rate:.1f}%)")
        print(f"   - 매칭 실패: {failed_matches}개")
        print(f"   - 처리 오류: {error_matches}개")
        print(f"   - 미처리: {unprocessed}개")
        
        return {
            'total': total_products,
            'success': successful_matches,
            'failed': failed_matches,
            'error': error_matches,
            'unprocessed': unprocessed,
            'success_rate': success_rate
        }
    
    def close(self):
        """스크래퍼 정리"""
        if self.scraper:
            self.scraper.close()
            self.scraper = None