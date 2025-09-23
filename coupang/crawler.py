from coupang_manager import BrowserManager
from scraper import ProductScraper
from image_downloader import ImageDownloader
from page_navigator import PageNavigator
from data_saver import DataSaver
from datetime import datetime

class CoupangCrawlerMacOS:
    def __init__(self, headless=False, delay_range=(2, 5), download_images=True, image_dir=None):
        """
        macOS 최적화 쿠팡 크롤러 - 단순화된 이미지 관리
        
        Args:
            headless: 헤드리스 모드 여부
            delay_range: 딜레이 범위
            download_images: 이미지 다운로드 여부 (Gemini 이미지 매칭용)
            image_dir: 이미지 저장 디렉토리 (None이면 coupang/coupang_images 자동 사용)
        """
        self.headless = headless
        self.delay_range = delay_range
        self.download_images = download_images
        
        # 모듈 초기화
        self.browser = BrowserManager(headless)
        self.scraper = ProductScraper()
        
        # 이미지 다운로더 초기화 (None이면 자동으로 coupang/coupang_images 사용)
        self.image_downloader = ImageDownloader(image_dir) if download_images else None
        
        self.navigator = PageNavigator(self.browser, self.scraper, self.image_downloader)
        self.data_saver = DataSaver()
        
        self.products = []
    
    def start_driver(self):
        """Chrome 드라이버 시작"""
        return self.browser.start_driver()
    
    def crawl_all_pages(self, start_url, max_pages=None):
        """모든 페이지 크롤링"""
        if not self.start_driver():
            print("드라이버 시작 실패")
            return []
        
        self.products = self.navigator.crawl_all_pages(start_url, max_pages, self.delay_range)
        return self.products
    
    def save_to_csv(self, filename=None):
        """CSV 저장"""
        return self.data_saver.save_to_csv(self.products, filename)
    
    def save_image_manifest(self, filename=None):
        """이미지 매니페스트 저장"""
        if not self.image_downloader:
            return None
        return self.data_saver.save_image_manifest(
            self.image_downloader.downloaded_images, 
            self.image_downloader.image_dir, 
            filename
        )
    
    def print_summary(self):
        """결과 요약"""
        self.data_saver.print_summary(self.products, self.image_downloader)
    
    def close(self):
        """브라우저 종료"""
        self.browser.close()


# 실행 부분 (독립 실행 시에만)
if __name__ == "__main__":
    print("🎯 macOS용 쿠팡 크롤러 시작...")
    print("🔧 주요 특징:")
    print("  - 새로운 Tailwind CSS 구조 완전 대응")
    print("  - 자동 이미지 저장 (coupang/coupang_images)")
    print("  - Gemini 이미지 매칭 지원")
    print("  - 단순화된 구조")
    
    # 크롤러 생성
    crawler = CoupangCrawlerMacOS(
        headless=False,
        delay_range=(3, 6),
        download_images=True,  # 이미지 다운로드 활성화
        image_dir=None  # 기본 경로 사용 (coupang/coupang_images)
    )
    
    # 검색 URL (예시)
    search_url = "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=4302&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=Jarrow+Formulas"
    
    try:
        # 크롤링 실행
        products = crawler.crawl_all_pages(search_url, max_pages=None)
        
        # 결과 저장
        if products:
            csv_filename = crawler.save_to_csv()
            manifest_filename = crawler.save_image_manifest()
            
            crawler.print_summary()
            
            print(f"\n🎉 크롤링 완료!")
            print(f"CSV 파일: {csv_filename}")
            if manifest_filename:
                print(f"이미지 매니페스트: {manifest_filename}")
            
            print(f"\n✅ 단순화된 구조 적용:")
            print(f"  - 불필요한 컬럼 제거")
            print(f"  - 자동 이미지 경로 관리")
            print(f"  - Gemini 매칭 준비 완료")
        else:
            print("❌ 크롤링된 상품이 없습니다.")
    
    except KeyboardInterrupt:
        print("\n👋 크롤링을 중단했습니다.")
        if crawler.products:
            crawler.save_to_csv()
            if crawler.image_downloader:
                crawler.save_image_manifest()
            print("지금까지 수집한 데이터를 저장했습니다.")
    
    finally:
        crawler.close()
    
    print("🎉 단순화된 쿠팡 크롤러 완료!")