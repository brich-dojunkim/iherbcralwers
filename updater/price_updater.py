"""
가격 업데이터 모듈
기존 매칭된 상품들의 쿠팡+아이허브 가격 정보만 업데이트
"""

import pandas as pd
import time
from datetime import datetime
from typing import List, Dict
from config import CONFIG

# 기존 모듈 임포트
try:
    from browser_manager import BrowserManager
    from iherb_client import IHerbClient
    MODULES_AVAILABLE = True
except ImportError:
    print("⚠️ 기존 모듈을 찾을 수 없습니다. 모듈 경로를 확인해주세요.")
    MODULES_AVAILABLE = False


class PriceUpdater:
    """기존 상품의 가격 정보 업데이트"""
    
    def __init__(self):
        self.config = CONFIG
        self.browser_manager = None
        self.iherb_client = None
        self.update_stats = {
            'total_processed': 0,
            'coupang_updated': 0,
            'iherb_updated': 0,
            'failed_updates': 0
        }
    
    def _initialize_browser(self):
        """브라우저 초기화"""
        if not MODULES_AVAILABLE:
            print("  기존 모듈을 사용할 수 없습니다.")
            return False
        
        try:
            print("  브라우저 초기화 중...")
            self.browser_manager = BrowserManager(headless=True)
            self.iherb_client = IHerbClient(self.browser_manager)
            
            # 아이허브 언어 설정
            self.iherb_client.set_language_to_english()
            
            print("  브라우저 초기화 완료 ✓")
            return True
            
        except Exception as e:
            print(f"  브라우저 초기화 실패: {e}")
            return False
    
    def update_existing_products(self, existing_products: List[dict], master_df: pd.DataFrame) -> pd.DataFrame:
        """기존 상품들의 가격 정보 업데이트"""
        try:
            print(f"\n기존 상품 가격 업데이트 시작 ({len(existing_products)}개)")
            
            if not existing_products:
                return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
            
            # 브라우저 초기화
            if not self._initialize_browser():
                print("  브라우저 초기화 실패 - 기존 데이터 그대로 반환")
                return self._get_existing_records_without_update(existing_products, master_df)
            
            updated_records = []
            
            for i, product in enumerate(existing_products):
                print(f"\n[{i+1}/{len(existing_products)}] {product.get('product_name', '')[:50]}...")
                
                try:
                    # 마스터에서 기존 레코드 찾기
                    existing_record = self._find_existing_record(product, master_df)
                    if not existing_record:
                        print("  기존 레코드를 찾을 수 없음 - 건너뜀")
                        continue
                    
                    # 가격 정보 업데이트
                    updated_record = self._update_single_product_prices(product, existing_record)
                    updated_records.append(updated_record)
                    
                    self.update_stats['total_processed'] += 1
                    
                    # 배치 간 딜레이
                    if i > 0 and i % 5 == 0:
                        print(f"  진행률: {i+1}/{len(existing_products)} ({(i+1)/len(existing_products)*100:.1f}%)")
                        time.sleep(2)
                
                except Exception as e:
                    print(f"  업데이트 실패: {e}")
                    self.update_stats['failed_updates'] += 1
                    
                    # 실패 시 기존 데이터라도 유지
                    if existing_record:
                        updated_records.append(existing_record)
            
            # 결과 정리
            self._close_browser()
            self._print_update_stats()
            
            if updated_records:
                result_df = pd.DataFrame(updated_records)
                result_df = result_df.reindex(columns=self.config.MASTER_COLUMNS, fill_value='')
                return result_df
            else:
                return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
            
        except Exception as e:
            print(f"  가격 업데이트 중 오류: {e}")
            self._close_browser()
            return self._get_existing_records_without_update(existing_products, master_df)
    
    def _find_existing_record(self, product: dict, master_df: pd.DataFrame) -> dict:
        """마스터 데이터에서 기존 레코드 찾기"""
        try:
            product_id = str(product.get('product_id', ''))
            
            matching_rows = master_df[master_df['coupang_product_id'].astype(str) == product_id]
            
            if len(matching_rows) > 0:
                return matching_rows.iloc[0].to_dict()
            else:
                return None
                
        except Exception as e:
            print(f"    기존 레코드 검색 실패: {e}")
            return None
    
    def _update_single_product_prices(self, coupang_product: dict, existing_record: dict) -> dict:
        """단일 상품의 가격 정보 업데이트"""
        updated_record = existing_record.copy()
        
        try:
            # 1. 쿠팡 가격 정보 업데이트 (크롤링 데이터에서)
            self._update_coupang_prices(updated_record, coupang_product)
            
            # 2. 아이허브 가격 정보 업데이트 (기존 매칭이 성공한 경우만)
            if updated_record.get('status') == 'success' and updated_record.get('iherb_product_url'):
                self._update_iherb_prices(updated_record)
            
            # 3. 가격 비교 재계산
            self._recalculate_price_comparison(updated_record)
            
            # 4. 메타 정보 업데이트
            updated_record['last_updated'] = datetime.now().isoformat()
            updated_record['data_source'] = 'update'
            updated_record['update_count'] = int(updated_record.get('update_count', 0)) + 1
            
            return updated_record
            
        except Exception as e:
            print(f"    가격 업데이트 실패: {e}")
            return existing_record
    
    def _update_coupang_prices(self, record: dict, coupang_product: dict):
        """쿠팡 가격 정보 업데이트"""
        try:
            # 새로운 쿠팡 크롤링 데이터로 가격 정보 업데이트
            record['coupang_current_price_krw'] = coupang_product.get('current_price', '')
            record['coupang_original_price_krw'] = coupang_product.get('original_price', '')
            record['coupang_discount_rate'] = coupang_product.get('discount_rate', '')
            record['coupang_url'] = coupang_product.get('product_url', record.get('coupang_url', ''))
            
            self.update_stats['coupang_updated'] += 1
            print("    쿠팡 가격 업데이트 ✓")
            
        except Exception as e:
            print(f"    쿠팡 가격 업데이트 실패: {e}")
    
    def _update_iherb_prices(self, record: dict):
        """아이허브 가격 정보 업데이트"""
        try:
            iherb_url = record.get('iherb_product_url', '')
            if not iherb_url:
                return
            
            print("    아이허브 가격 확인 중...")
            
            # 아이허브에서 최신 가격 정보 추출
            _, _, price_info = self.iherb_client.extract_product_info_with_price(iherb_url)
            
            if price_info:
                # 가격 정보 업데이트
                record['iherb_list_price_krw'] = price_info.get('list_price', '')
                record['iherb_discount_price_krw'] = price_info.get('discount_price', '')
                record['iherb_discount_percent'] = price_info.get('discount_percent', '')
                record['iherb_subscription_discount'] = price_info.get('subscription_discount', '')
                record['iherb_price_per_unit'] = price_info.get('price_per_unit', '')
                record['is_in_stock'] = price_info.get('is_in_stock', True)
                record['stock_message'] = price_info.get('stock_message', '')
                record['back_in_stock_date'] = price_info.get('back_in_stock_date', '')
                
                self.update_stats['iherb_updated'] += 1
                print("    아이허브 가격 업데이트 ✓")
            else:
                print("    아이허브 가격 정보 없음")
            
        except Exception as e:
            print(f"    아이허브 가격 업데이트 실패: {e}")
    
    def _recalculate_price_comparison(self, record: dict):
        """가격 비교 정보 재계산"""
        try:
            coupang_price = record.get('coupang_current_price_krw', '')
            iherb_price = record.get('iherb_discount_price_krw', '') or record.get('iherb_list_price_krw', '')
            
            if coupang_price and iherb_price:
                try:
                    coupang_price_int = int(str(coupang_price).replace(',', ''))
                    iherb_price_int = int(str(iherb_price).replace(',', ''))
                    
                    price_diff = coupang_price_int - iherb_price_int
                    record['price_difference_krw'] = str(price_diff)
                    
                    if price_diff > 0:
                        record['cheaper_platform'] = '아이허브'
                        record['savings_amount'] = str(price_diff)
                        savings_pct = round((price_diff / coupang_price_int) * 100, 1)
                        record['savings_percentage'] = str(savings_pct)
                        record['price_difference_note'] = f'아이허브가 {price_diff:,}원 ({savings_pct}%) 더 저렴'
                    elif price_diff < 0:
                        abs_diff = abs(price_diff)
                        record['cheaper_platform'] = '쿠팡'
                        record['savings_amount'] = str(abs_diff)
                        savings_pct = round((abs_diff / iherb_price_int) * 100, 1)
                        record['savings_percentage'] = str(savings_pct)
                        record['price_difference_note'] = f'쿠팡이 {abs_diff:,}원 ({savings_pct}%) 더 저렴'
                    else:
                        record['cheaper_platform'] = '동일'
                        record['savings_amount'] = '0'
                        record['savings_percentage'] = '0'
                        record['price_difference_note'] = '두 플랫폼 가격 동일'
                        
                except (ValueError, TypeError):
                    record['price_difference_note'] = '가격 비교 계산 오류'
            else:
                record['price_difference_note'] = '가격 정보 부족'
                
        except Exception as e:
            print(f"    가격 비교 재계산 실패: {e}")
    
    def _get_existing_records_without_update(self, existing_products: List[dict], master_df: pd.DataFrame) -> pd.DataFrame:
        """업데이트 없이 기존 레코드 반환 (실패 시 대안)"""
        try:
            existing_records = []
            
            for product in existing_products:
                existing_record = self._find_existing_record(product, master_df)
                if existing_record:
                    # 최소한 메타 정보는 업데이트
                    existing_record['last_updated'] = datetime.now().isoformat()
                    existing_record['data_source'] = 'update_failed'
                    existing_records.append(existing_record)
            
            if existing_records:
                result_df = pd.DataFrame(existing_records)
                result_df = result_df.reindex(columns=self.config.MASTER_COLUMNS, fill_value='')
                return result_df
            else:
                return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
                
        except Exception as e:
            print(f"  기존 레코드 추출 실패: {e}")
            return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
    
    def _close_browser(self):
        """브라우저 안전 종료"""
        try:
            if self.browser_manager:
                self.browser_manager.close()
                print("  브라우저 종료 완료")
        except:
            pass
    
    def _print_update_stats(self):
        """업데이트 통계 출력"""
        stats = self.update_stats
        print(f"\n가격 업데이트 완료:")
        print(f"  총 처리: {stats['total_processed']}개")
        print(f"  쿠팡 업데이트: {stats['coupang_updated']}개")
        print(f"  아이허브 업데이트: {stats['iherb_updated']}개")
        print(f"  실패: {stats['failed_updates']}개")
        
        if stats['total_processed'] > 0:
            success_rate = ((stats['coupang_updated'] + stats['iherb_updated']) / (stats['total_processed'] * 2)) * 100
            print(f"  성공률: {success_rate:.1f}%")
    
    def update_specific_products(self, product_ids: List[str], master_df: pd.DataFrame) -> pd.DataFrame:
        """특정 상품들만 가격 업데이트"""
        try:
            print(f"\n특정 상품 가격 업데이트 ({len(product_ids)}개)")
            
            # 마스터에서 해당 상품들 찾기
            target_products = []
            for product_id in product_ids:
                matching_rows = master_df[master_df['coupang_product_id'].astype(str) == str(product_id)]
                if len(matching_rows) > 0:
                    target_products.append(matching_rows.iloc[0].to_dict())
            
            if not target_products:
                print("  업데이트할 상품이 없습니다.")
                return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
            
            # 가격 업데이트 (쿠팡 크롤링 데이터 없이)
            if not self._initialize_browser():
                return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
            
            updated_records = []
            
            for i, product in enumerate(target_products):
                print(f"\n[{i+1}/{len(target_products)}] {product.get('coupang_product_name', '')[:50]}...")
                
                try:
                    # 아이허브 가격만 업데이트
                    if product.get('status') == 'success' and product.get('iherb_product_url'):
                        self._update_iherb_prices(product)
                    
                    # 메타 정보 업데이트
                    product['last_updated'] = datetime.now().isoformat()
                    product['data_source'] = 'manual_update'
                    product['update_count'] = int(product.get('update_count', 0)) + 1
                    
                    updated_records.append(product)
                    
                except Exception as e:
                    print(f"  업데이트 실패: {e}")
                    updated_records.append(product)  # 실패해도 기존 데이터 유지
            
            self._close_browser()
            
            if updated_records:
                result_df = pd.DataFrame(updated_records)
                result_df = result_df.reindex(columns=self.config.MASTER_COLUMNS, fill_value='')
                return result_df
            else:
                return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
                
        except Exception as e:
            print(f"  특정 상품 업데이트 실패: {e}")
            self._close_browser()
            return pd.DataFrame(columns=self.config.MASTER_COLUMNS)