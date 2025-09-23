import csv
from datetime import datetime

class DataSaver:
    def save_to_csv(self, products, filename=None):
        """CSV 저장 - 필요한 컬럼만"""
        if not products:
            print("저장할 상품이 없습니다.")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f'coupang_products_{timestamp}.csv'
        
        # 필요한 컬럼만 정의
        essential_fieldnames = [
            'product_id', 'product_name', 'product_url',
            'current_price', 'original_price', 'discount_rate',
            'rating', 'review_count', 'delivery_badge',
            'is_rocket', 'image_url', 'image_local_path', 
            'image_filename', 'crawled_at'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=essential_fieldnames)
                writer.writeheader()
                
                for product in products:
                    # 필요한 필드만 추출해서 저장
                    clean_product = {
                        field: product.get(field, '') 
                        for field in essential_fieldnames
                    }
                    writer.writerow(clean_product)
            
            print(f"✅ 쿠팡 데이터 저장: {filename}")
            print(f"📊 핵심 컬럼 {len(essential_fieldnames)}개 저장")
            
            return filename
            
        except Exception as e:
            print(f"CSV 저장 오류: {e}")
            return None
    
    def print_summary(self, products, image_downloader=None):
        """결과 요약"""
        if not products:
            print("수집된 상품이 없습니다.")
            return
        
        print(f"\n=== 쿠팡 크롤링 결과 ===")
        print(f"총 상품: {len(products)}개")
        
        # 데이터 품질 확인
        with_names = len([p for p in products if p.get('product_name')])
        with_prices = len([p for p in products if p.get('current_price')])
        
        print(f"상품명: {with_names}/{len(products)}개")
        print(f"가격: {with_prices}/{len(products)}개")
        
        if image_downloader:
            stats = image_downloader.image_download_stats
            print(f"이미지: {stats['successful_downloads']}개 수집")
        
        print(f"✅ 핵심 데이터만 저장됨")