"""
상품 업데이터 모듈 - 단순화된 버전 (기본 이미지 경로 사용)
"""

import pandas as pd
import sys
import os
import tempfile

# 상위 디렉토리와 coupang, iherbscraper 경로를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
coupang_dir = os.path.join(parent_dir, 'coupang')
iherbscraper_dir = os.path.join(parent_dir, 'iherbscraper')

sys.path.insert(0, parent_dir)
sys.path.insert(0, coupang_dir)
sys.path.insert(0, iherbscraper_dir)

from coupang.crawler import CoupangCrawlerMacOS
from iherbscraper.main import EnglishIHerbScraper
from coupang.translator import GeminiCSVTranslator
from iherbscraper.config import Config


class ProductUpdater:
    """쿠팡 크롤링과 아이허브 매칭을 통합한 상품 업데이터 - 단순화된 버전"""
    
    def __init__(self, enable_images: bool = True):
        """
        초기화
        
        Args:
            enable_images: 이미지 다운로드 활성화 여부
        """
        self.enable_images = enable_images
        self.coupang_crawler = None
        self.iherb_scraper = None
        self.translator = None
        
        print(f"📋 상품 업데이터 초기화")
        print(f"   이미지 기능: {'활성화' if enable_images else '비활성화'}")
        if enable_images:
            print(f"   이미지 저장: 기본 경로 (coupang/coupang_images)")
    
    def crawl_coupang_products(self, search_url: str) -> pd.DataFrame:
        """
        쿠팡 상품 크롤링 - 기본 이미지 경로 사용
        
        Args:
            search_url: 쿠팡 검색 결과 URL
            
        Returns:
            크롤링된 상품 DataFrame
        """
        try:
            print(f"   🔄 쿠팡 크롤링 시작")
            print(f"   📍 URL: {search_url[:80]}...")
            
            # 크롤러 초기화 - 기본 이미지 경로 자동 사용
            self.coupang_crawler = CoupangCrawlerMacOS(
                headless=False,
                delay_range=(3, 6),
                download_images=self.enable_images,
                image_dir=None  # 🔥 None으로 설정하여 기본 경로(coupang/coupang_images) 자동 사용
            )
            
            if self.enable_images:
                print(f"   🖼️ 이미지 저장 활성화: 자동 경로 사용")
            else:
                print(f"   📝 텍스트 전용 크롤링")
            
            # 크롤링 실행
            products = self.coupang_crawler.crawl_all_pages(search_url, max_pages=None)
            
            if products:
                # DataFrame 변환
                crawled_df = pd.DataFrame(products)
                
                # 필수 컬럼 확인 및 생성
                required_columns = [
                    'product_id', 'product_name', 'current_price', 
                    'original_price', 'discount_rate'
                ]
                for col in required_columns:
                    if col not in crawled_df.columns:
                        crawled_df[col] = ''
                
                print(f"   ✅ 크롤링 완료: {len(crawled_df)}개 상품")
                
                # 이미지 통계 출력
                if self.enable_images and self.coupang_crawler.image_downloader:
                    stats = self.coupang_crawler.image_downloader.image_download_stats
                    successful_images = stats.get('successful_downloads', 0)
                    total_attempts = stats.get('total_attempts', 0)
                    print(f"   📸 이미지 수집: {successful_images}/{total_attempts}개")
                
                return crawled_df
            else:
                print(f"   ⚠️ 크롤링 결과 없음")
                empty_df = pd.DataFrame(columns=required_columns)
                return empty_df
                
        except Exception as e:
            print(f"   ❌ 크롤링 실패: {e}")
            import traceback
            print(f"   상세 오류: {traceback.format_exc()}")
            
            empty_columns = ['product_id', 'product_name', 'current_price', 'original_price', 'discount_rate']
            empty_df = pd.DataFrame(columns=empty_columns)
            return empty_df
    
    def match_iherb_products(self, new_products_df: pd.DataFrame) -> pd.DataFrame:
        """
        신규 상품들을 아이허브와 매칭 - 단순화된 버전
        
        Args:
            new_products_df: 신규 쿠팡 상품 DataFrame
            
        Returns:
            매칭 결과 DataFrame
        """
        if len(new_products_df) == 0:
            print(f"   📝 매칭할 신규 상품이 없습니다")
            return pd.DataFrame()
        
        try:
            print(f"   🔄 아이허브 매칭 시작: {len(new_products_df)}개 상품")
            
            # 1. 임시 파일 생성 (자동 정리)
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as temp_file:
                temp_csv_path = temp_file.name
                new_products_df.to_csv(temp_csv_path, index=False, encoding='utf-8-sig')
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as temp_translated_file:
                translated_csv_path = temp_translated_file.name
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as temp_matched_file:
                matched_csv_path = temp_matched_file.name
            
            # 2. 한국어 → 영어 번역
            print(f"   📝 번역 시작: {len(new_products_df)}개 상품명")
            self.translator = GeminiCSVTranslator(Config.GEMINI_API_KEY)
            
            translated_df = self.translator.translate_csv(
                input_file=temp_csv_path,
                output_file=translated_csv_path,
                column_name='product_name',
                batch_size=10
            )
            print(f"   ✅ 번역 완료")
            
            # 3. 아이허브 매칭 수행 (기본 이미지 경로 자동 사용)
            print(f"   🔄 아이허브 매칭 수행")
            print(f"   💡 기본 이미지 경로 자동 사용 (단순화)")
            
            self.iherb_scraper = EnglishIHerbScraper(
                headless=False,
                delay_range=(2, 4),
                max_products_to_compare=4
            )
            
            # 매칭 실행
            matched_csv = self.iherb_scraper.process_products_complete(
                csv_file_path=translated_csv_path,
                output_file_path=matched_csv_path,
                limit=None,
                start_from=None
            )
            
            # 4. 매칭 결과 로드
            matched_df = pd.read_csv(matched_csv, encoding='utf-8-sig')
            
            # 5. 성공 통계 출력
            if 'status' in matched_df.columns:
                success_count = len(matched_df[matched_df['status'] == 'success'])
                success_rate = success_count / len(matched_df) * 100 if len(matched_df) > 0 else 0
                
                print(f"   ✅ 매칭 완료: {success_count}/{len(matched_df)}개 ({success_rate:.1f}%)")
                
                # Gemini API 통계 (있으면 표시)
                if hasattr(self.iherb_scraper, 'product_matcher'):
                    api_stats = self.iherb_scraper.product_matcher.get_api_usage_stats()
                    total_calls = api_stats.get('total_calls', 0)
                    vision_calls = api_stats.get('vision_calls', 0)
                    print(f"   🤖 Gemini API: {total_calls}회 (Vision: {vision_calls}회)")
            else:
                print(f"   ⚠️ 상태 컬럼이 없어서 성공률 계산 불가")
            
            # 6. 임시 파일 정리
            for temp_path in [temp_csv_path, translated_csv_path, matched_csv_path]:
                try:
                    os.unlink(temp_path)
                except:
                    pass
            
            return matched_df
            
        except Exception as e:
            print(f"   ❌ 매칭 실패: {e}")
            import traceback
            print(f"   상세 오류: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def close(self):
        """리소스 정리"""
        try:
            if self.coupang_crawler:
                self.coupang_crawler.close()
                print(f"   🔄 쿠팡 크롤러 종료")
            
            if self.iherb_scraper:
                self.iherb_scraper.close()
                print(f"   🔄 아이허브 스크래퍼 종료")
            
            print(f"   ✅ 리소스 정리 완료")
            
        except Exception as e:
            print(f"   ⚠️ 리소스 정리 중 오류: {e}")


# 테스트용 실행 (독립 실행 시에만)
if __name__ == "__main__":
    print("🧪 Product Updater 테스트 실행")
    
    updater = ProductUpdater(enable_images=True)
    
    try:
        # 테스트용 쿠팡 URL
        test_url = "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=14420&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=thorne"
        
        print("\n1️⃣ 쿠팡 크롤링 테스트")
        crawled_df = updater.crawl_coupang_products(test_url)
        print(f"크롤링 결과: {len(crawled_df)}개")
        
        if len(crawled_df) > 0:
            print("\n2️⃣ 아이허브 매칭 테스트 (처음 3개만)")
            test_df = crawled_df.head(3)
            matched_df = updater.match_iherb_products(test_df)
            print(f"매칭 결과: {len(matched_df)}개")
        
        print("\n✅ 테스트 완료")
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
    finally:
        updater.close()