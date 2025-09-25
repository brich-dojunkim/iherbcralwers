import sys
import os
import csv
from datetime import datetime
from config import PathConfig
from coupang_config import CoupangConfig

class DataSaver:
    def save_to_csv(self, products, filename=None):
        """CSV 저장 - 핵심 필드 추가"""
        if not products:
            print("저장할 상품이 없습니다.")
            return
        
        if not filename:
            sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(PathConfig.UNIFIED_OUTPUTS_DIR, f'coupang_products_{timestamp}.csv')
        
        essential_fieldnames = CoupangConfig.REQUIRED_COLUMNS
        
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
        """결과 요약 - 간단한 품질 체크"""
        if not products:
            print("수집된 상품이 없습니다.")
            return
        
        print(f"\n=== 쿠팡 크롤링 결과 ===")
        print(f"총 상품: {len(products)}개")
        
        # 기본 데이터 품질 확인
        with_names = len([p for p in products if p.get('product_name')])
        with_prices = len([p for p in products if p.get('current_price')])
        with_unit_price = len([p for p in products if p.get('unit_price')])
        
        print(f"상품명: {with_names}/{len(products)}개")
        print(f"가격: {with_prices}/{len(products)}개")
        print(f"단위가격: {with_unit_price}/{len(products)}개")
        
        # 품절 상품 확인
        out_of_stock = len([p for p in products if p.get('stock_status') == 'out_of_stock'])
        if out_of_stock > 0:
            print(f"품절: {out_of_stock}개")
        
        if image_downloader:
            stats = image_downloader.image_download_stats
            print(f"이미지: {stats['successful_downloads']}개 수집")
        
        print(f"✅ 핵심 데이터만 저장됨")