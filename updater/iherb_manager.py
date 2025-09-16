"""
아이허브 매칭 관리자
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
    """아이허브 매칭 전담 관리자"""
    
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
            self.scraper = EnglishIHerbScraper(
                headless=self.headless,
                delay_range=self.delay_range,
                max_products_to_compare=self.max_products
            )
    
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
            return {
                'status': 'failed',
                'failure_reason': f'매칭 중 오류: {str(e)}'
            }
    
    def match_unmatched_products(self, df, output_file, checkpoint_interval):
        """미매칭 상품 처리"""
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
        
        self.init_scraper()
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
                result = self.match_single_product(coupang_product, english_name)
                
                # DataFrame 업데이트
                df = self._update_dataframe_with_result(df, idx, result, coupang_product, today)
                
                if result['status'] == 'success':
                    success_count += 1
                    print(f"    ✅ 매칭: {result['iherb_product_name'][:30]}...")
                else:
                    print(f"    ❌ 실패: {result['failure_reason']}")
                
                # 주기적 중간 저장
                if (i + 1) % checkpoint_interval == 0:
                    df.to_csv(output_file, index=False, encoding='utf-8-sig')
                    print(f"    💾 중간 저장 ({i+1}개 처리)")
            
            except Exception as e:
                print(f"    ❌ 오류: {e}")
                df.at[idx, 'status'] = 'error'
                df.at[idx, 'failure_type'] = 'PROCESSING_ERROR'
                df.at[idx, 'matching_reason'] = f'처리 중 오류: {str(e)}'
        
        print(f"✅ 아이허브 매칭 완료: {success_count}/{len(unmatched)}개 성공")
        return df
    
    def _update_dataframe_with_result(self, df, idx, result, coupang_product, today):
        """결과로 DataFrame 업데이트"""
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
        else:
            df.at[idx, 'status'] = 'not_found'
            df.at[idx, 'failure_type'] = 'NO_MATCHING_PRODUCT'
            df.at[idx, 'matching_reason'] = result['failure_reason']
        
        return df
    
    def create_new_product_row(self, coupang_product, english_name, iherb_result):
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
    
    def close(self):
        """스크래퍼 정리"""
        if self.scraper:
            self.scraper.close()
            self.scraper = None