"""
쿠팡 크롤링 관리자 - 마스터 파일 시스템 지원 (공통 패턴 적용)
"""

import sys
from datetime import datetime
from settings import COUPANG_PATH, UPDATER_CONFIG
from common import MasterFilePatterns

# 쿠팡 모듈 경로 추가
sys.path.insert(0, str(COUPANG_PATH))

try:
    from crawler import CoupangCrawlerMacOS
    COUPANG_AVAILABLE = True
    print("✅ 쿠팡 모듈 로드 성공")
except ImportError as e:
    print(f"❌ 쿠팡 모듈 로드 실패: {e}")
    COUPANG_AVAILABLE = False


class CoupangManager:
    """쿠팡 크롤링 전담 관리자 - 마스터 파일 시스템 (공통 패턴 적용)"""
    
    def __init__(self, headless=False):
        if not COUPANG_AVAILABLE:
            raise ImportError("쿠팡 모듈을 사용할 수 없습니다.")
        
        self.headless = headless
        self.crawler = None
        self.brand_urls = UPDATER_CONFIG['BRAND_SEARCH_URLS']
        self.delay_range = UPDATER_CONFIG['DELAY_RANGE']
    
    def crawl_brand_products(self, brand_name):
        """브랜드별 상품 크롤링"""
        if brand_name not in self.brand_urls:
            raise ValueError(f"지원되지 않는 브랜드: {brand_name}")
        
        search_url = self.brand_urls[brand_name]
        print(f"🤖 쿠팡 크롤링 시작: {brand_name}")
        
        products = []
        try:
            self.crawler = CoupangCrawlerMacOS(
                headless=self.headless,
                delay_range=self.delay_range,
                download_images=True
            )
            
            if not self.crawler.start_driver():
                raise Exception("쿠팡 크롤러 시작 실패")
            
            products = self.crawler.crawl_all_pages(search_url)
            print(f"📡 크롤링 완료: {len(products)}개")
            
        except Exception as e:
            print(f"❌ 크롤링 실패: {e}")
        finally:
            self.close()
        
        return products
    
    def update_master_prices(self, master_df, new_products):
        """마스터 파일의 기존 상품 가격 업데이트 - 공통 패턴 적용"""
        existing_ids = set(str(pid) for pid in master_df['coupang_product_id'].dropna())
        crawled_dict = {str(p['product_id']): p for p in new_products if p.get('product_id')}
        
        updated_count = 0
        
        # ✅ 공통 컬럼명 사용
        coupang_columns = MasterFilePatterns.get_daily_coupang_columns()
        
        print(f"📊 가격 업데이트 시작:")
        print(f"   - 마스터 파일 상품: {len(existing_ids)}개")
        print(f"   - 크롤링된 상품: {len(crawled_dict)}개")
        
        for idx, row in master_df.iterrows():
            product_id = str(row.get('coupang_product_id', ''))
            
            if product_id in crawled_dict:
                new_product = crawled_dict[product_id]
                
                # ✅ 공통 컬럼명 사용 - 날짜별 히스토리 컬럼 업데이트
                master_df.at[idx, coupang_columns['current_price']] = new_product.get('current_price', '')
                master_df.at[idx, coupang_columns['original_price']] = new_product.get('original_price', '')
                master_df.at[idx, coupang_columns['discount_rate']] = new_product.get('discount_rate', '')
                master_df.at[idx, coupang_columns['review_count']] = new_product.get('review_count', '')
                master_df.at[idx, coupang_columns['rating']] = new_product.get('rating', '')
                master_df.at[idx, coupang_columns['crawled_at']] = datetime.now().isoformat()
                
                # 상태 업데이트
                if 'update_status' not in master_df.columns:
                    master_df['update_status'] = ''
                master_df.at[idx, 'update_status'] = 'UPDATED'
                master_df.at[idx, 'last_updated'] = datetime.now().isoformat()
                
                updated_count += 1
                
                if updated_count % 50 == 0:
                    print(f"   - 진행상황: {updated_count}개 업데이트됨")
            else:
                # 찾을 수 없는 상품
                if 'update_status' not in master_df.columns:
                    master_df['update_status'] = ''
                master_df.at[idx, 'update_status'] = 'NOT_FOUND'
                master_df.at[idx, coupang_columns['crawled_at']] = datetime.now().isoformat()
                master_df.at[idx, 'last_updated'] = datetime.now().isoformat()
        
        print(f"✅ 가격 업데이트 완료: {updated_count}개")
        
        return master_df, updated_count
    
    def find_new_products_for_master(self, master_df, crawled_products):
        """마스터 파일에 없는 신규 상품 발견"""
        existing_ids = set(str(pid) for pid in master_df['coupang_product_id'].dropna())
        crawled_dict = {str(p['product_id']): p for p in crawled_products if p.get('product_id')}
        
        new_products = []
        for product_id, product in crawled_dict.items():
            if product_id not in existing_ids:
                new_products.append(product)
        
        print(f"🔍 신규 상품 발견: {len(new_products)}개")
        
        if len(new_products) > 0:
            print(f"   신규 상품 샘플:")
            for i, product in enumerate(new_products[:3]):
                product_name = product.get('product_name', 'N/A')[:40] + "..."
                print(f"   {i+1}. {product_name}")
        
        return new_products
    
    def update_existing_products(self, existing_df, new_products):
        """기존 상품 가격 업데이트 (호환성 유지) - 공통 패턴 적용"""
        existing_ids = set(str(pid) for pid in existing_df['coupang_product_id'].dropna())
        crawled_dict = {str(p['product_id']): p for p in new_products if p.get('product_id')}
        
        updated_count = 0
        
        # ✅ 공통 컬럼명 사용
        coupang_columns = MasterFilePatterns.get_daily_coupang_columns()
        
        for idx, row in existing_df.iterrows():
            product_id = str(row.get('coupang_product_id', ''))
            
            if product_id in crawled_dict:
                new_product = crawled_dict[product_id]
                existing_df.at[idx, coupang_columns['current_price']] = new_product.get('current_price', '')
                existing_df.at[idx, coupang_columns['original_price']] = new_product.get('original_price', '')
                existing_df.at[idx, coupang_columns['discount_rate']] = new_product.get('discount_rate', '')
                existing_df.at[idx, coupang_columns['review_count']] = new_product.get('review_count', '')
                existing_df.at[idx, coupang_columns['rating']] = new_product.get('rating', '')
                existing_df.at[idx, coupang_columns['crawled_at']] = datetime.now().isoformat()
                existing_df.at[idx, 'update_status'] = 'UPDATED'
                updated_count += 1
            else:
                existing_df.at[idx, 'update_status'] = 'NOT_FOUND'
        
        return existing_df, updated_count
    
    def find_new_products(self, existing_df, crawled_products):
        """신규 상품 발견 (호환성 유지)"""
        existing_ids = set(str(pid) for pid in existing_df['coupang_product_id'].dropna())
        crawled_dict = {str(p['product_id']): p for p in crawled_products if p.get('product_id')}
        
        new_products = []
        for product_id, product in crawled_dict.items():
            if product_id not in existing_ids:
                new_products.append(product)
        
        return new_products
    
    def analyze_price_changes(self, master_df):
        """가격 변화 분석 (마스터 파일 전용) - 공통 패턴 적용"""
        
        price_changes = {
            'increased': 0,
            'decreased': 0,
            'unchanged': 0,
            'new_prices': 0,
            'missing_prices': 0
        }
        
        # ✅ 공통 패턴 사용 - 오늘과 이전 가격 컬럼들 찾기
        price_columns = [col for col in master_df.columns if col.startswith('쿠팡현재가격_')]
        price_columns.sort()  # 날짜순 정렬
        
        if len(price_columns) < 2:
            print(f"ℹ️ 가격 비교를 위한 히스토리가 부족합니다 ({len(price_columns)}개 날짜)")
            return price_changes
        
        today_col = f'쿠팡현재가격_{MasterFilePatterns.get_today_suffix()}'
        if today_col not in price_columns:
            print(f"ℹ️ 오늘 가격 정보가 없습니다: {today_col}")
            return price_changes
        
        # 가장 최근 2개 날짜 비교
        prev_col = price_columns[-2] if price_columns[-1] == today_col else price_columns[-1]
        
        print(f"📊 가격 변화 분석: {prev_col} vs {today_col}")
        
        for idx, row in master_df.iterrows():
            try:
                today_price = row.get(today_col, '')
                prev_price = row.get(prev_col, '')
                
                if not today_price or today_price == '':
                    price_changes['missing_prices'] += 1
                    continue
                
                if not prev_price or prev_price == '':
                    price_changes['new_prices'] += 1
                    continue
                
                today_val = int(str(today_price).replace(',', '').replace('원', ''))
                prev_val = int(str(prev_price).replace(',', '').replace('원', ''))
                
                if today_val > prev_val:
                    price_changes['increased'] += 1
                elif today_val < prev_val:
                    price_changes['decreased'] += 1
                else:
                    price_changes['unchanged'] += 1
                    
            except (ValueError, TypeError):
                price_changes['missing_prices'] += 1
                continue
        
        print(f"   - 가격 상승: {price_changes['increased']}개")
        print(f"   - 가격 하락: {price_changes['decreased']}개")
        print(f"   - 가격 동일: {price_changes['unchanged']}개")
        print(f"   - 신규 가격: {price_changes['new_prices']}개")
        print(f"   - 가격 없음: {price_changes['missing_prices']}개")
        
        return price_changes
    
    def get_price_history_summary(self, master_df):
        """가격 히스토리 요약"""
        price_columns = [col for col in master_df.columns if col.startswith('쿠팡현재가격_')]
        price_columns.sort()
        
        history_summary = {
            'total_dates': len(price_columns),
            'date_range': price_columns if price_columns else [],
            'products_with_full_history': 0,
            'products_with_partial_history': 0
        }
        
        if not price_columns:
            return history_summary
        
        print(f"📈 가격 히스토리 요약:")
        print(f"   - 총 추적 날짜: {len(price_columns)}개")
        print(f"   - 날짜 범위: {price_columns[0]} ~ {price_columns[-1]}")
        
        for idx, row in master_df.iterrows():
            non_empty_prices = sum(1 for col in price_columns if row.get(col, '') != '')
            
            if non_empty_prices == len(price_columns):
                history_summary['products_with_full_history'] += 1
            elif non_empty_prices > 0:
                history_summary['products_with_partial_history'] += 1
        
        print(f"   - 완전한 히스토리: {history_summary['products_with_full_history']}개")
        print(f"   - 부분적 히스토리: {history_summary['products_with_partial_history']}개")
        
        return history_summary
    
    def close(self):
        """크롤러 정리"""
        if self.crawler:
            self.crawler.close()
            self.crawler = None