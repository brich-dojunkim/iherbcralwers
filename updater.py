"""
통합 가격 업데이터 - 쿠팡 업데이트 + 아이허브 데이터 보완 (실시간 저장)
"""

import pandas as pd
import os
import sys
from datetime import datetime

# 쿠팡 모듈
sys.path.append('coupang')
from crawler import CoupangCrawlerMacOS
from data_saver import DataSaver
from settings import BRAND_SEARCH_URLS

# 아이허브 모듈
sys.path.append('iherbscraper')
from main import EnglishIHerbScraper
from data_manager import DataManager


class PriceUpdater:
    """통합 가격 업데이터 - 쿠팡 + 아이허브 (실시간 저장)"""
    
    def __init__(self, headless=False):
        self.headless = headless
        self.coupang_crawler = None
        self.iherb_scraper = None
        self.data_saver = DataSaver()
        self.data_manager = DataManager()
        
    def update_prices(self, input_file, brand_name, output_file=None, fill_iherb=True):
        """메인 업데이트 함수 - 자동 재시작 지원"""
        print(f"통합 가격 업데이트 시작: {brand_name}")
        
        # 출력 파일명 결정 - 날짜별 고정 파일명
        if not output_file:
            today = datetime.now().strftime("%Y%m%d")
            output_file = f"complete_updated_{brand_name.replace(' ', '_')}_{today}.csv"
        
        print(f"작업 파일: {output_file}")
        
        # 기존 작업 파일이 있으면 그것을 사용 (재시작)
        if os.path.exists(output_file):
            print(f"기존 작업 파일 발견: {output_file}")
            print("중단된 작업을 이어서 진행합니다.")
            working_df = pd.read_csv(output_file, encoding='utf-8-sig')
            
            # 진행 상황 체크
            total_products = len(working_df)
            updated_products = len(working_df[working_df['update_status'] == 'UPDATED'])
            new_products = len(working_df[working_df['update_status'].str.startswith('NEW_PRODUCT')])
            completed_products = len(working_df[working_df['update_status'].str.startswith('COMPLETED')])
            
            print(f"현재 진행상황:")
            print(f"  - 총 상품: {total_products}개")
            print(f"  - 쿠팡 업데이트 완료: {updated_products}개")
            print(f"  - 신규 상품: {new_products}개")
            print(f"  - 아이허브 매칭 완료: {completed_products}개")
        else:
            # 1. 쿠팡 가격 업데이트
            print("\n=== 1단계: 쿠팡 가격 업데이트 ===")
            working_df = self._update_coupang_prices(input_file, brand_name, output_file)
            
            # 1단계 완료 후 중간 저장
            working_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"1단계 완료 - 중간 저장: {output_file}")
        
        # 2. 아이허브 데이터 보완 (옵션)
        if fill_iherb:
            print("\n=== 2단계: 아이허브 데이터 보완 ===")
            working_df = self._fill_missing_iherb_data(working_df)
        
        # 3. 최종 저장
        working_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✅ 최종 저장 완료: {output_file}")
        
        # 4. 통계 출력
        self._print_final_stats(working_df)
        
        return output_file
        
    def _update_coupang_prices(self, input_file, brand_name, output_file=None):
        """쿠팡 가격 업데이트 (기존 로직) - 실시간 저장"""
        # 파일 로드
        existing_df = pd.read_csv(input_file, encoding='utf-8-sig')
        print(f"기존 상품: {len(existing_df)}개")
        
        # 크롤링 (실시간 저장 포함)
        search_url = BRAND_SEARCH_URLS[brand_name]
        new_products = self._crawl_coupang_data(search_url, output_file)
        print(f"새 크롤링: {len(new_products)}개")
        
        # 매칭 업데이트
        updated_df = self._update_by_product_id(existing_df, new_products)
        
        return updated_df
        
    def _crawl_coupang_data(self, search_url, output_file=None):
        """쿠팡 데이터 크롤링 - 실시간 저장"""
        self.coupang_crawler = CoupangCrawlerMacOS(
            headless=self.headless,
            delay_range=(2, 4),
            download_images=True  # 이미지 다운로드 활성화
        )
        
        # 실시간 저장 경로 설정
        if output_file:
            temp_coupang_file = output_file.replace('.csv', '_coupang_temp.csv')
            print(f"쿠팡 크롤링 실시간 저장: {temp_coupang_file}")
            
            # 크롤러의 navigator에 실시간 저장 설정
            products = []
            try:
                self.coupang_crawler.start_driver()
                navigator = self.coupang_crawler.navigator
                navigator.set_csv_file_path(temp_coupang_file)
                
                products = navigator.crawl_all_pages(
                    search_url, 
                    realtime_save_path=temp_coupang_file
                )
            finally:
                self.coupang_crawler.close()
        else:
            products = self.coupang_crawler.crawl_all_pages(search_url)
            self.coupang_crawler.close()
        
        return products
    
    def _update_by_product_id(self, existing_df, new_products):
        """product_id 기준 업데이트 - 날짜별 새 칼럼 추가"""
        new_dict = {p['product_id']: p for p in new_products if p.get('product_id')}
        updated_df = existing_df.copy()
        updated_count = 0
        date_suffix = datetime.now().strftime("_%Y%m%d")
        
        # 기존 상품 업데이트
        for idx, row in updated_df.iterrows():
            product_id = str(row.get('coupang_product_id', ''))
            
            if product_id in new_dict:
                new_product = new_dict[product_id]
                
                # 날짜별 새 칼럼에 쿠팡 정보 추가
                updated_df.at[idx, f'쿠팡현재가격{date_suffix}'] = new_product.get('current_price', '')
                updated_df.at[idx, f'쿠팡정가{date_suffix}'] = new_product.get('original_price', '')
                updated_df.at[idx, f'쿠팡할인율{date_suffix}'] = new_product.get('discount_rate', '')
                updated_df.at[idx, f'쿠팡리뷰수{date_suffix}'] = new_product.get('review_count', '')
                updated_df.at[idx, f'쿠팡평점{date_suffix}'] = new_product.get('rating', '')
                
                # 가격 비교 재계산
                coupang_price_info = self.data_manager.extract_coupang_price_info(new_product)
                iherb_price_info = {
                    'discount_price': row.get('iherb_discount_price_krw', ''),
                    'list_price': row.get('iherb_list_price_krw', ''),
                    'discount_percent': row.get('iherb_discount_percent', ''),
                    'subscription_discount': row.get('iherb_subscription_discount', ''),
                    'price_per_unit': row.get('iherb_price_per_unit', '')
                }
                
                price_comparison = self.data_manager.calculate_price_comparison(coupang_price_info, iherb_price_info)
                
                # 날짜별 가격 비교 결과 추가
                updated_df.at[idx, f'가격차이{date_suffix}'] = price_comparison['price_difference_krw']
                updated_df.at[idx, f'저렴한플랫폼{date_suffix}'] = price_comparison['cheaper_platform']
                updated_df.at[idx, f'절약금액{date_suffix}'] = price_comparison['savings_amount']
                updated_df.at[idx, f'절약비율{date_suffix}'] = price_comparison['savings_percentage']
                updated_df.at[idx, f'가격차이메모{date_suffix}'] = price_comparison['price_difference_note']
                
                # 메타 정보 업데이트
                updated_df.at[idx, f'크롤링일시{date_suffix}'] = datetime.now().isoformat()
                updated_df.at[idx, 'update_status'] = 'UPDATED'
                
                updated_count += 1
            else:
                updated_df.at[idx, 'update_status'] = 'NOT_FOUND'
        
        # 새 상품 추가
        existing_ids = set(str(pid) for pid in updated_df['coupang_product_id'].dropna())
        new_products_to_add = []
        
        for product_id, product in new_dict.items():
            if product_id not in existing_ids:
                coupang_price_info = self.data_manager.extract_coupang_price_info(product)
                iherb_price_info = {}
                
                new_row = self.data_manager.create_result_record(
                    row={'product_name': product.get('product_name', ''),
                         'product_id': product_id,
                         'product_url': product.get('product_url', '')},
                    actual_idx=len(updated_df) + len(new_products_to_add),
                    english_name='',
                    product_url='',
                    similarity_score=0,
                    product_code='',
                    iherb_product_name='',
                    coupang_price_info=coupang_price_info,
                    iherb_price_info=iherb_price_info,
                    matching_reason='새로 발견된 상품',
                    failure_type='NEW_PRODUCT'
                )
                new_row['update_status'] = 'NEW_PRODUCT'
                new_products_to_add.append(new_row)
        
        if new_products_to_add:
            new_df = pd.DataFrame(new_products_to_add)
            updated_df = pd.concat([updated_df, new_df], ignore_index=True)
        
        print(f"쿠팡 업데이트: {updated_count}개, 신규: {len(new_products_to_add)}개")
        
        return updated_df
    
    def _fill_missing_iherb_data(self, df):
        """신규 상품들의 아이허브 데이터 보완 - 완전한 매칭"""
        today = datetime.now().strftime("%Y%m%d")
        
        # 오늘 처리할 대상 필터링
        new_products = df[
            (df['update_status'].str.startswith('NEW_PRODUCT')) & 
            (df['iherb_product_code'].isna() | (df['iherb_product_code'] == ''))
        ].copy()
        
        if len(new_products) == 0:
            print("보완할 신규 상품이 없습니다.")
            return df
        
        print(f"아이허브 데이터 보완 대상: {len(new_products)}개 (중단 재시작 지원)")
        
        # 쿠팡 번역기 사용
        sys.path.insert(0, 'coupang')
        from translator import GeminiCSVTranslator
        translator = GeminiCSVTranslator("AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE")
        
        # 아이허브 스크래퍼 초기화 - 경로 수정
        sys.path.insert(0, 'iherbscraper')
        self.iherb_scraper = EnglishIHerbScraper(
            headless=self.headless,
            delay_range=(2, 4),
            max_products_to_compare=4
        )
        
        updated_count = 0
        for idx, row in new_products.iterrows():
            try:
                coupang_name = row['coupang_product_name']
                coupang_id = row['coupang_product_id']
                
                # 영문명 번역 (쿠팡 번역기 사용)
                english_name = row.get('coupang_product_name_english', '')
                if not english_name or english_name.strip() == '':
                    print(f"영문명 번역 중: {coupang_name[:30]}...")
                    english_name = translator.translate_single(coupang_name)
                    if english_name:
                        df.at[idx, 'coupang_product_name_english'] = english_name
                        print(f"번역 완료: {english_name[:30]}...")
                    else:
                        english_name = coupang_name
                
                search_name = english_name if english_name else coupang_name
                print(f"아이허브 검색: {search_name[:50]}...")
                
                # 아이허브 검색 및 매칭
                result = self.iherb_scraper.product_matcher.search_product_enhanced(
                    search_name, str(coupang_id)
                )
                
                if len(result) >= 3:
                    product_url, similarity_score, match_details = result
                    
                    if product_url:
                        # 아이허브 상품 정보 추출
                        product_code, iherb_name, iherb_price_info = \
                            self.iherb_scraper.iherb_client.extract_product_info_with_price(product_url)
                        
                        if product_code:
                            # DataFrame 업데이트
                            df.at[idx, 'iherb_product_name'] = iherb_name or ''
                            df.at[idx, 'iherb_product_url'] = product_url
                            df.at[idx, 'iherb_product_code'] = product_code
                            df.at[idx, 'status'] = 'success'
                            df.at[idx, 'similarity_score'] = similarity_score
                            
                            # 아이허브 가격 정보
                            df.at[idx, 'iherb_list_price_krw'] = iherb_price_info.get('list_price', '')
                            df.at[idx, 'iherb_discount_price_krw'] = iherb_price_info.get('discount_price', '')
                            df.at[idx, 'iherb_discount_percent'] = iherb_price_info.get('discount_percent', '')
                            df.at[idx, 'iherb_subscription_discount'] = iherb_price_info.get('subscription_discount', '')
                            df.at[idx, 'iherb_price_per_unit'] = iherb_price_info.get('price_per_unit', '')
                            df.at[idx, 'is_in_stock'] = iherb_price_info.get('is_in_stock', True)
                            df.at[idx, 'stock_message'] = iherb_price_info.get('stock_message', '')
                            
                            # 가격 비교 재계산
                            coupang_price_info = {
                                'current_price': row.get(f'쿠팡현재가격_{today}', ''),
                                'original_price': row.get(f'쿠팡정가_{today}', ''),
                                'discount_rate': row.get(f'쿠팡할인율_{today}', '')
                            }
                            
                            price_comparison = self.data_manager.calculate_price_comparison(
                                coupang_price_info, iherb_price_info
                            )
                            
                            df.at[idx, f'가격차이_{today}'] = price_comparison['price_difference_krw']
                            df.at[idx, f'저렴한플랫폼_{today}'] = price_comparison['cheaper_platform']
                            df.at[idx, f'절약금액_{today}'] = price_comparison['savings_amount']
                            df.at[idx, f'절약비율_{today}'] = price_comparison['savings_percentage']
                            df.at[idx, f'가격차이메모_{today}'] = price_comparison['price_difference_note']
                            
                            df.at[idx, 'update_status'] = f'COMPLETED_{today}'
                            updated_count += 1
                            
                            print(f"✅ 매칭 성공: {iherb_name[:30]}...")
                        else:
                            df.at[idx, 'update_status'] = f'IHERB_CODE_NOT_FOUND_{today}'
                            print(f"❌ 상품코드 추출 실패")
                    else:
                        df.at[idx, 'update_status'] = f'IHERB_NOT_MATCHED_{today}'
                        print(f"❌ 아이허브 매칭 실패")
                else:
                    df.at[idx, 'update_status'] = f'IHERB_SEARCH_ERROR_{today}'
                    print(f"❌ 검색 오류")
                    
            except Exception as e:
                print(f"❌ 처리 오류: {e}")
                df.at[idx, 'update_status'] = f'IHERB_ERROR_{today}'
                continue
        
        # 리소스 정리
        if self.iherb_scraper:
            self.iherb_scraper.close()
        
        print(f"\n아이허브 데이터 보완 완료: {updated_count}/{len(new_products)}개 성공")
        
        return df
    
    def _print_final_stats(self, df):
        """최종 통계 출력"""
        print(f"\n=== 최종 통계 ===")
        print(f"총 상품: {len(df)}개")
        
        if 'update_status' in df.columns:
            status_counts = df['update_status'].value_counts()
            print(f"\n상태별 통계:")
            for status, count in status_counts.items():
                print(f"  {status}: {count}개")
        
        # 성공적으로 매칭된 상품들
        successful_matches = len(df[df['status'] == 'success'])
        print(f"\n아이허브 매칭 성공: {successful_matches}개")
        
        # 가격 정보가 있는 상품들
        date_suffix = datetime.now().strftime("_%Y%m%d")
        coupang_prices = len(df[df[f'쿠팡현재가격{date_suffix}'].notna()])
        iherb_prices = len(df[df['iherb_discount_price_krw'].notna() | df['iherb_list_price_krw'].notna()])
        
        print(f"쿠팡 가격 정보: {coupang_prices}개")
        print(f"아이허브 가격 정보: {iherb_prices}개")
    
    def close(self):
        """리소스 정리"""
        if self.coupang_crawler:
            self.coupang_crawler.close()
        if self.iherb_scraper:
            self.iherb_scraper.close()


if __name__ == "__main__":
    updater = PriceUpdater(headless=False)
    
    try:
        # 예시 실행
        input_file = "updated_NOW_Foods_20250915_160755.csv"
        brand = "NOW Foods"
        
        result_file = updater.update_prices(
            input_file=input_file,
            brand_name=brand,
            fill_iherb=True  # 아이허브 데이터 보완 활성화
        )
        
        print(f"\n🎉 전체 업데이트 완료: {result_file}")
        
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        updater.close()