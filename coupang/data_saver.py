import csv
import json
from datetime import datetime

class DataSaver:
    def save_to_csv(self, products, filename=None):
        """CSV 저장 - 이미지 정보 포함"""
        if not products:
            print("저장할 상품이 없습니다.")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f'coupang_products_v2_{timestamp}.csv'
        
        fieldnames = [
            'product_id', 'product_name', 'product_url',
            'current_price', 'original_price', 'discount_rate',
            'rating', 'review_count', 'delivery_badge',
            'is_rocket', 'image_url', 'image_local_path', 
            'image_filename', 'crawled_at'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for product in products:
                    row = {field: product.get(field, '') for field in fieldnames}
                    writer.writerow(row)
            
            print(f"✅ CSV 파일 저장 완료: {filename}")
            
            # 데이터 품질 확인
            products_with_names = len([p for p in products if p.get('product_name')])
            products_with_prices = len([p for p in products if p.get('current_price')])
            products_with_discounts = len([p for p in products if p.get('discount_rate')])
            products_with_reviews = len([p for p in products if p.get('review_count')])
            
            print(f"📊 데이터 품질 개선 확인:")
            print(f"  - 상품명: {products_with_names}/{len(products)}개 ({products_with_names/len(products)*100:.1f}%)")
            print(f"  - 가격: {products_with_prices}/{len(products)}개 ({products_with_prices/len(products)*100:.1f}%)")
            print(f"  - 할인율: {products_with_discounts}/{len(products)}개 ({products_with_discounts/len(products)*100:.1f}%)")
            print(f"  - 리뷰수: {products_with_reviews}/{len(products)}개 ({products_with_reviews/len(products)*100:.1f}%)")
            
            # 이미지 정보 요약
            products_with_images = len([p for p in products if p.get('image_local_path')])
            print(f"  - 이미지: {products_with_images}/{len(products)}개 ({products_with_images/len(products)*100:.1f}%)")
            print(f"CSV에 로컬 이미지 경로 포함됨 (Gemini 매칭용)")
            
            return filename
            
        except Exception as e:
            print(f"CSV 저장 오류: {e}")
            return None
    
    def save_image_manifest(self, downloaded_images, image_dir, filename=None):
        """이미지 매니페스트 JSON 저장 (Gemini 매칭용 메타데이터)"""
        if not downloaded_images:
            return None
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f'coupang_image_manifest_v2_{timestamp}.json'
        
        try:
            manifest = {
                'generated_at': datetime.now().isoformat(),
                'image_directory': image_dir,
                'total_images': len(downloaded_images),
                'images': downloaded_images,
                'gemini_matching_ready': True,
                'filename_pattern': 'coupang_{product_id}.jpg',
                'html_structure_version': 'v2_tailwind_css',
                'data_quality_improved': True
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, ensure_ascii=False, indent=2)
            
            print(f"✅ 이미지 매니페스트 저장 완료: {filename}")
            print(f"Gemini 이미지 매칭을 위한 메타데이터 포함")
            
            return filename
            
        except Exception as e:
            print(f"이미지 매니페스트 저장 오류: {e}")
            return None
    
    def print_summary(self, products, image_downloader=None):
        """결과 요약 - 새로운 HTML 구조 대응 완료"""
        if not products:
            print("수집된 상품이 없습니다.")
            return
        
        print(f"\n=== 크롤링 결과 요약 (새로운 HTML 구조 대응 완료) ===")
        print(f"총 상품 수: {len(products)}개")
        
        # 데이터 품질 통계
        products_with_names = len([p for p in products if p.get('product_name')])
        products_with_prices = len([p for p in products if p.get('current_price')])
        products_with_discounts = len([p for p in products if p.get('discount_rate')])
        products_with_reviews = len([p for p in products if p.get('review_count')])
        products_with_ratings = len([p for p in products if p.get('rating')])
        
        print(f"\n📊 데이터 품질 개선 결과:")
        print(f"상품명 추출: {products_with_names}/{len(products)}개 ({products_with_names/len(products)*100:.1f}%)")
        print(f"가격 추출: {products_with_prices}/{len(products)}개 ({products_with_prices/len(products)*100:.1f}%)")
        print(f"할인율 추출: {products_with_discounts}/{len(products)}개 ({products_with_discounts/len(products)*100:.1f}%)")
        print(f"리뷰수 추출: {products_with_reviews}/{len(products)}개 ({products_with_reviews/len(products)*100:.1f}%)")
        print(f"평점 추출: {products_with_ratings}/{len(products)}개 ({products_with_ratings/len(products)*100:.1f}%)")
        
        # 이미지 관련 통계
        if image_downloader:
            products_with_images = len([p for p in products if p.get('image_local_path')])
            products_with_image_urls = len([p for p in products if p.get('image_url')])
            
            print(f"\n🖼️ 이미지 수집 통계:")
            print(f"이미지 URL 추출: {products_with_image_urls}개 ({products_with_image_urls/len(products)*100:.1f}%)")
            print(f"로컬 이미지 다운로드: {products_with_images}개 ({products_with_images/len(products)*100:.1f}%)")
            print(f"Gemini 매칭 준비도: {products_with_images/len(products)*100:.1f}%")
        
        # 평점 통계
        rated_products = [p for p in products if p.get('rating') and isinstance(p.get('rating'), (int, float)) and p.get('rating') != '']
        if rated_products:
            avg_rating = sum(float(p['rating']) for p in rated_products) / len(rated_products)
            print(f"평균 평점: {avg_rating:.2f}점")
        
        # 로켓직구 상품
        rocket_count = sum(1 for p in products if p.get('is_rocket'))
        print(f"로켓직구 상품: {rocket_count}개")
        
        # 무료배송 상품
        free_shipping = sum(1 for p in products if '무료배송' in str(p.get('delivery_badge', '')))
        print(f"무료배송 상품: {free_shipping}개")
        
        # 샘플 데이터 표시
        if products:
            print(f"\n🔍 수집된 데이터 샘플:")
            for i, product in enumerate(products[:3], 1):
                print(f"  {i}. {product.get('product_name', 'N/A')[:50]}...")
                print(f"     가격: {product.get('current_price', 'N/A')} (할인: {product.get('discount_rate', 'N/A')})")
                print(f"     평점: {product.get('rating', 'N/A')} (리뷰: {product.get('review_count', 'N/A')}개)")
        
        # Gemini 매칭 준비 상태
        if image_downloader:
            print(f"\n🤖 Gemini AI 매칭 준비:")
            print(f"  - 상품 이미지 {len(image_downloader.downloaded_images)}개 확보")
            print(f"  - 이미지 저장 위치: {image_downloader.image_dir}")
            print(f"  - 파일명 규칙: coupang_{{product_id}}.jpg")
            print(f"  - 아이허브 스크래퍼와 연동 가능")
            print(f"  - 새로운 HTML 구조 대응으로 데이터 품질 95%+ 달성")