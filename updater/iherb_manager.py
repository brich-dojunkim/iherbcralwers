"""
아이허브 매칭 관리자 - 마스터 파일 시스템 지원 (BrowserManager 인터페이스 수정)
"""

import sys
import importlib.util
from datetime import datetime
from settings import IHERB_PATH, UPDATER_CONFIG

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
    """아이허브 매칭 전담 관리자 - 마스터 파일 시스템"""
    
    def __init__(self, headless=False):
        if not IHERB_AVAILABLE:
            raise ImportError("아이허브 모듈을 사용할 수 없습니다.")
        
        self.headless = headless
        self.scraper = None
        self.data_manager = DataManager()
        self.delay_range = UPDATER_CONFIG['DELAY_RANGE']
        self.max_products = UPDATER_CONFIG['MAX_PRODUCTS_TO_COMPARE']
    
    def init_scraper(self):
        """아이허브 스크래퍼 초기화 - BrowserManager 인터페이스 수정"""
        if not self.scraper:
            print(f"🌿 아이허브 스크래퍼 초기화...")
            
            try:
                # FIX: BrowserManager는 headless만 받으므로 delay_range 제거
                self.scraper = EnglishIHerbScraper(
                    headless=self.headless,
                    # delay_range 파라미터 제거
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
        """마스터 파일의 신규 상품들에 대한 아이허브 매칭"""
        today = datetime.now().strftime("_%Y%m%d")
        
        # 신규 상품 중에서 아이허브 매칭이 필요한 상품들 선별
        new_products_needing_iherb = master_df[
            (master_df['update_status'] == f'NEW_PRODUCT{today}') &
            (master_df['coupang_product_name_english'].notna()) &
            (master_df['coupang_product_name_english'] != '')
        ].copy()
        
        print(f"🔍 신규 상품 아이허브 매칭 대상 분석:")
        print(f"   - 총 신규 상품: {len(new_products_needing_iherb)}개")
        
        # 오늘 날짜 기준 아이허브 가격 정보가 이미 있는 상품 제외
        today_iherb_columns = [
            f'아이허브정가{today}', f'아이허브할인가{today}', 
            f'아이허브할인율{today}', f'아이허브단위가격{today}'
        ]
        
        # 컬럼 존재 여부 확인 후 필터링
        existing_iherb_columns = [col for col in today_iherb_columns if col in master_df.columns]
        
        if existing_iherb_columns:
            already_processed_today = new_products_needing_iherb[
                new_products_needing_iherb[existing_iherb_columns].notna().any(axis=1)
            ]
            
            needs_matching = new_products_needing_iherb[
                ~new_products_needing_iherb[existing_iherb_columns].notna().any(axis=1)
            ]
        else:
            already_processed_today = new_products_needing_iherb.iloc[0:0]  # 빈 DataFrame
            needs_matching = new_products_needing_iherb
        
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
                master_df.at[idx, f'아이허브매칭상태{today}'] = 'scraper_init_error'
                master_df.at[idx, f'아이허브매칭사유{today}'] = f'스크래퍼 초기화 실패: {str(e)}'
            return master_df
        
        success_count = 0
        
        for i, (idx, row) in enumerate(needs_matching.iterrows()):
            try:
                print(f"  [{i+1}/{len(needs_matching)}] {row['coupang_product_name'][:40]}...")
                
                # 쿠팡 상품 정보 구성 (오늘 날짜 기준)
                coupang_product = {
                    'product_id': row['coupang_product_id'],
                    'product_name': row['coupang_product_name'],
                    'product_url': row.get('coupang_url', ''),
                    'current_price': row.get(f'쿠팡현재가격{today}', ''),
                    'original_price': row.get(f'쿠팡정가{today}', ''),
                    'discount_rate': row.get(f'쿠팡할인율{today}', '')
                }
                
                english_name = row['coupang_product_name_english']
                result = self.match_single_product(coupang_product, english_name)
                
                # 오늘 날짜 기준 아이허브 컬럼에 결과 저장
                master_df = self._update_master_with_iherb_result(master_df, idx, result, coupang_product, today)
                
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
                master_df.at[idx, f'아이허브매칭상태{today}'] = 'processing_error'
                master_df.at[idx, f'아이허브매칭사유{today}'] = f'처리 중 오류: {str(e)}'
        
        print(f"✅ 신규 상품 아이허브 매칭 완료: {success_count}/{len(needs_matching)}개 성공")
        return master_df
    
    def _update_master_with_iherb_result(self, master_df, idx, result, coupang_product, today):
        """마스터 파일에 오늘 날짜 기준 아이허브 매칭 결과 업데이트"""
        if result['status'] == 'success':
            # 아이허브 매칭 성공
            master_df.at[idx, f'아이허브매칭상태{today}'] = 'success'
            master_df.at[idx, f'아이허브상품명{today}'] = result['iherb_product_name']
            master_df.at[idx, f'아이허브상품URL{today}'] = result['iherb_product_url']
            master_df.at[idx, f'아이허브상품코드{today}'] = result['iherb_product_code']
            master_df.at[idx, f'유사도점수{today}'] = result['similarity_score']
            
            # 아이허브 가격 정보 (오늘 날짜 기준)
            price_info = result['iherb_price_info']
            master_df.at[idx, f'아이허브정가{today}'] = price_info.get('list_price', '')
            master_df.at[idx, f'아이허브할인가{today}'] = price_info.get('discount_price', '')
            master_df.at[idx, f'아이허브할인율{today}'] = price_info.get('discount_percent', '')
            master_df.at[idx, f'아이허브구독할인{today}'] = price_info.get('subscription_discount', '')
            master_df.at[idx, f'아이허브단위가격{today}'] = price_info.get('price_per_unit', '')
            master_df.at[idx, f'재고상태{today}'] = price_info.get('is_in_stock', True)
            master_df.at[idx, f'재고메시지{today}'] = price_info.get('stock_message', '')
            
            # 가격 비교 (오늘 날짜 기준)
            coupang_price_info = self.data_manager.extract_coupang_price_info(coupang_product)
            price_comparison = self.data_manager.calculate_price_comparison(coupang_price_info, price_info)
            
            master_df.at[idx, f'가격차이{today}'] = price_comparison['price_difference_krw']
            master_df.at[idx, f'저렴한플랫폼{today}'] = price_comparison['cheaper_platform']
            master_df.at[idx, f'절약금액{today}'] = price_comparison['savings_amount']
            master_df.at[idx, f'절약비율{today}'] = price_comparison['savings_percentage']
            master_df.at[idx, f'가격차이메모{today}'] = price_comparison['price_difference_note']
            
            # 기본 매칭 정보도 업데이트 (마스터 파일은 최신 정보 유지)
            master_df.at[idx, 'status'] = 'success'
            master_df.at[idx, 'iherb_product_name'] = result['iherb_product_name']
            master_df.at[idx, 'iherb_product_url'] = result['iherb_product_url']
            master_df.at[idx, 'iherb_product_code'] = result['iherb_product_code']
            master_df.at[idx, 'similarity_score'] = result['similarity_score']
            
        else:
            # 아이허브 매칭 실패
            master_df.at[idx, f'아이허브매칭상태{today}'] = 'not_found'
            master_df.at[idx, f'아이허브매칭사유{today}'] = result['failure_reason']
            
            # 기본 상태도 업데이트
            master_df.at[idx, 'status'] = 'not_found'
            master_df.at[idx, 'failure_type'] = 'NO_MATCHING_PRODUCT'
            master_df.at[idx, 'matching_reason'] = result['failure_reason']
        
        # 매칭 처리 시각 기록
        master_df.at[idx, f'아이허브매칭일시{today}'] = datetime.now().isoformat()
        master_df.at[idx, 'last_updated'] = datetime.now().isoformat()
        
        return master_df
    
    def close(self):
        """스크래퍼 정리"""
        if self.scraper:
            self.scraper.close()
            self.scraper = None