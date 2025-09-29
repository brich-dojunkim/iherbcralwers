"""
쿠팡 크롤러 - DB 버전
CSV 저장 대신 SQLite DB에 직접 저장
"""

import sys
import os

# 경로 설정
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coupang_manager import BrowserManager
from scraper import ProductScraper
from image_downloader import ImageDownloader
from page_navigator import PageNavigator
from datetime import datetime
from db import Database
from config import PathConfig


class CoupangCrawlerDB:
    """DB 연동 쿠팡 크롤러"""
    
    def __init__(self, db: Database, brand_name: str, 
                 headless: bool = False, 
                 delay_range: tuple = (2, 5),
                 download_images: bool = True):
        """
        Args:
            db: Database 인스턴스
            brand_name: 브랜드명
            headless: 헤드리스 모드
            delay_range: 딜레이 범위
            download_images: 이미지 다운로드 여부
        """
        self.db = db
        self.brand_name = brand_name
        self.headless = headless
        self.delay_range = delay_range
        self.download_images = download_images
        
        # 기존 모듈 초기화
        self.browser = BrowserManager(headless)
        self.scraper = ProductScraper()
        self.image_downloader = ImageDownloader() if download_images else None
        self.navigator = PageNavigator(self.browser, self.scraper, self.image_downloader)
        
        self.crawled_count = 0
        self.new_products = 0
        self.updated_products = 0
    
    def start_driver(self):
        """Chrome 드라이버 시작"""
        return self.browser.start_driver()
    
    def crawl_and_save(self, search_url: str, max_pages: int = None) -> dict:
        """
        크롤링 및 DB 저장
        
        Args:
            search_url: 쿠팡 검색 URL
            max_pages: 최대 페이지 수
            
        Returns:
            통계 딕셔너리
        """
        print(f"\n{'='*80}")
        print(f"🎯 DB 연동 쿠팡 크롤링 시작")
        print(f"{'='*80}")
        print(f"브랜드: {self.brand_name}")
        print(f"이미지: {'활성화' if self.download_images else '비활성화'}")
        print(f"{'='*80}\n")
        
        # 드라이버 시작
        if not self.start_driver():
            print("❌ 드라이버 시작 실패")
            return self._get_stats()
        
        try:
            # 기존 상품 수 확인
            initial_stats = self.db.get_brand_stats(self.brand_name)
            initial_count = initial_stats.get('total_products', 0)
            
            print(f"📊 기존 상품: {initial_count}개\n")
            
            # 크롤링 실행
            products = self.navigator.crawl_all_pages(
                search_url, 
                max_pages, 
                self.delay_range
            )
            
            if not products:
                print("⚠️ 크롤링된 상품이 없습니다")
                return self._get_stats()
            
            print(f"\n{'='*80}")
            print(f"💾 DB 저장 시작: {len(products)}개")
            print(f"{'='*80}")
            
            # DB 저장
            for idx, product_data in enumerate(products, 1):
                try:
                    self._save_product(product_data, idx, len(products))
                except Exception as e:
                    print(f"  ❌ 상품 {idx} 저장 실패: {e}")
                    continue
            
            # 브랜드 크롤링 시간 업데이트
            self.db.update_brand_crawled(self.brand_name)
            
            # 최종 통계
            self._print_summary(initial_count)
            
            # 사라진 상품 감지
            self._detect_missing_products()
            
            return self._get_stats()
            
        except KeyboardInterrupt:
            print("\n⚠️ 사용자 중단")
            self.db.update_brand_crawled(self.brand_name)
            return self._get_stats()
            
        except Exception as e:
            print(f"\n❌ 크롤링 오류: {e}")
            import traceback
            traceback.print_exc()
            return self._get_stats()
            
        finally:
            self.close()
    
    def _save_product(self, product_data: dict, idx: int, total: int):
        """단일 상품 DB 저장"""
        # product_id 추출
        product_id = self.db.insert_crawled_product(
            self.brand_name, 
            product_data
        )
        
        self.crawled_count += 1
        
        # 신규 vs 업데이트 판단 (간단한 방법)
        # 실제로는 product_id가 새로 생성되었는지 확인해야 하지만,
        # insert_crawled_product()가 항상 id를 반환하므로
        # 여기서는 카운트만 증가
        
        # 진행률 표시 (10개마다)
        if idx % 10 == 0 or idx == total:
            print(f"  💾 저장 중: {idx}/{total} ({idx/total*100:.1f}%)")
    
    def _print_summary(self, initial_count: int):
        """크롤링 결과 요약"""
        print(f"\n{'='*80}")
        print(f"📊 크롤링 완료 요약")
        print(f"{'='*80}")
        
        final_stats = self.db.get_brand_stats(self.brand_name)
        final_count = final_stats.get('total_products', 0)
        
        print(f"총 크롤링: {self.crawled_count}개")
        print(f"DB 상품 수: {initial_count}개 → {final_count}개")
        
        # 파이프라인 단계별
        by_stage = final_stats.get('by_stage', {})
        print(f"\n파이프라인 단계:")
        for stage, count in by_stage.items():
            emoji = {
                'crawled': '🆕',
                'translated': '📝',
                'matched': '✅',
                'failed': '❌'
            }.get(stage, '❓')
            print(f"  {emoji} {stage}: {count}개")
        
        # 이미지 통계
        if self.image_downloader:
            stats = self.image_downloader.image_download_stats
            print(f"\n이미지 수집:")
            print(f"  📸 성공: {stats['successful_downloads']}개")
            print(f"  ⏭️ 기존: {stats['skipped_existing']}개")
            print(f"  ❌ 실패: {stats['failed_downloads']}개")
    
    def _detect_missing_products(self):
        """사라진 상품 감지"""
        print(f"\n{'='*80}")
        print(f"🔍 사라진 상품 감지")
        print(f"{'='*80}")
        
        missing = self.db.get_missing_products(self.brand_name)
        
        if missing:
            print(f"⚠️ 발견: {len(missing)}개 상품이 최신 크롤링에서 제외됨")
            print(f"\n상품 예시:")
            for product in missing[:5]:
                print(f"  - {product['coupang_product_name'][:50]}...")
            
            if len(missing) > 5:
                print(f"  ... 외 {len(missing) - 5}개")
        else:
            print(f"✓ 모든 상품이 크롤링에 포함됨")
    
    def _get_stats(self) -> dict:
        """통계 반환"""
        return {
            'crawled_count': self.crawled_count,
            'new_products': self.new_products,
            'updated_products': self.updated_products
        }
    
    def close(self):
        """브라우저 종료"""
        self.browser.close()


def main():
    """테스트 실행"""
    print("🧪 DB 연동 쿠팡 크롤러 테스트\n")
    
    # DB 초기화
    db_path = os.path.join(PathConfig.DATA_ROOT, "products.db")
    db = Database(db_path)
    
    # 브랜드 등록
    brand_name = "thorne"
    search_url = "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=14420&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=thorne"
    
    db.upsert_brand(brand_name, search_url)
    
    # 크롤러 실행
    crawler = CoupangCrawlerDB(
        db=db,
        brand_name=brand_name,
        headless=False,
        delay_range=(3, 6),
        download_images=True
    )
    
    try:
        stats = crawler.crawl_and_save(
            search_url=search_url,
            max_pages=2  # 테스트용 2페이지만
        )
        
        print(f"\n{'='*80}")
        print(f"🎉 테스트 완료!")
        print(f"{'='*80}")
        print(f"크롤링: {stats['crawled_count']}개")
        
    except KeyboardInterrupt:
        print("\n⚠️ 테스트 중단")
    finally:
        crawler.close()


if __name__ == "__main__":
    main()