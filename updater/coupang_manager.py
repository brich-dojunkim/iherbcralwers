"""
쿠팡 크롤링 관리자
"""

import sys
from datetime import datetime
from settings import COUPANG_PATH, UPDATER_CONFIG

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
    """쿠팡 크롤링 전담 관리자"""
    
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
    
    def update_existing_products(self, existing_df, new_products):
        """기존 상품 가격 업데이트"""
        existing_ids = set(str(pid) for pid in existing_df['coupang_product_id'].dropna())
        crawled_dict = {str(p['product_id']): p for p in new_products if p.get('product_id')}
        
        updated_count = 0
        date_suffix = datetime.now().strftime("_%Y%m%d")
        
        for idx, row in existing_df.iterrows():
            product_id = str(row.get('coupang_product_id', ''))
            
            if product_id in crawled_dict:
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
        
        return existing_df, updated_count
    
    def find_new_products(self, existing_df, crawled_products):
        """신규 상품 발견"""
        existing_ids = set(str(pid) for pid in existing_df['coupang_product_id'].dropna())
        crawled_dict = {str(p['product_id']): p for p in crawled_products if p.get('product_id')}
        
        new_products = []
        for product_id, product in crawled_dict.items():
            if product_id not in existing_ids:
                new_products.append(product)
        
        return new_products
    
    def close(self):
        """크롤러 정리"""
        if self.crawler:
            self.crawler.close()
            self.crawler = None