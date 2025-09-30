"""
파이프라인 통합 실행기
쿠팡 크롤링 → 번역 → 아이허브 매칭을 한 번에 실행
"""

import sys
import os
from datetime import datetime

# 경로 설정
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'coupang'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'iherbscraper'))

from db import Database
from config import PathConfig
from coupang.crawler import CoupangCrawlerDB
from coupang.translator import TranslatorDB
from iherbscraper.main import IHerbScraperDB


class Pipeline:
    """전체 파이프라인 통합 실행"""
    
    def __init__(self, db_path=None):
        """
        Args:
            db_path: DB 경로 (기본값: products.db)
        """
        if db_path is None:
            db_path = os.path.join(PathConfig.DATA_ROOT, "products.db")
        
        self.db = Database(db_path)
        self.start_time = None
    
    def run_brand(self, brand_name: str, search_url: str = None, 
                max_pages: int = None, headless: bool = False) -> dict:
        """
        브랜드 전체 파이프라인 실행
        
        Args:
            brand_name: 브랜드명
            search_url: 쿠팡 검색 URL (신규 브랜드만 필수)
            max_pages: 최대 페이지 수
            headless: 헤드리스 모드
        """
        self.start_time = datetime.now()
        
        # 브랜드 확인 및 URL 결정
        brand = self.db.get_brand(brand_name)
        
        if brand:
            # 기존 브랜드 → DB의 URL 사용
            actual_url = brand['coupang_search_url']
            print(f"\n📋 기존 브랜드: {brand_name}")
            print(f"마지막 크롤링: {brand['last_crawled_at'] or '없음'}")
        elif search_url:
            # 신규 브랜드 → URL 등록
            self.db.upsert_brand(brand_name, search_url)
            actual_url = search_url
            print(f"\n🆕 신규 브랜드 등록: {brand_name}")
        else:
            raise ValueError(
                f"브랜드 '{brand_name}'가 DB에 없습니다. "
                f"신규 브랜드는 --url 옵션이 필수입니다."
            )
        
        self.start_time = datetime.now()
        
        print(f"\n{'='*80}")
        print(f"🚀 파이프라인 시작: {brand_name}")
        print(f"{'='*80}")
        print(f"시작 시간: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        
        stats = {}
        
        try:
            # 브랜드 등록
            self.db.upsert_brand(brand_name, search_url)
            
            # 1단계: 쿠팡 크롤링
            print(f"\n{'='*80}")
            print(f"1️⃣ 쿠팡 크롤링")
            print(f"{'='*80}")
            
            crawler = CoupangCrawlerDB(
                db=self.db,
                brand_name=brand_name,
                headless=headless,
                download_images=True
            )
            
            crawl_stats = crawler.crawl_and_save(search_url, max_pages)
            stats['crawl'] = crawl_stats
            
            if crawl_stats['crawled_count'] == 0:
                print("\n⚠️ 크롤링 결과 없음 - 파이프라인 중단")
                return stats
            
            # 2단계: 번역
            print(f"\n{'='*80}")
            print(f"2️⃣ 번역")
            print(f"{'='*80}")
            
            translator = TranslatorDB(self.db)
            translate_stats = translator.translate_brand(brand_name, batch_size=10)
            stats['translate'] = translate_stats
            
            if translate_stats['translated'] == 0:
                print("\n⚠️ 번역 결과 없음 - 파이프라인 중단")
                return stats
            
            # 3단계: 아이허브 매칭
            print(f"\n{'='*80}")
            print(f"3️⃣ 아이허브 매칭")
            print(f"{'='*80}")
            
            scraper = IHerbScraperDB(
                db=self.db,
                brand_name=brand_name,
                headless=headless,
                max_products=4
            )
            
            match_stats = scraper.match_all_products(resume=True)
            stats['match'] = match_stats
            
            # 최종 요약
            self._print_summary(brand_name, stats)
            
            return stats
            
        except KeyboardInterrupt:
            print(f"\n⚠️ 사용자 중단")
            self._print_summary(brand_name, stats)
            return stats
            
        except Exception as e:
            print(f"\n❌ 파이프라인 오류: {e}")
            import traceback
            traceback.print_exc()
            return stats
            
        finally:
            # 리소스 정리
            if 'scraper' in locals():
                scraper.close()
    
    def _print_summary(self, brand_name: str, stats: dict):
        """최종 통계 요약"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        print(f"\n{'='*80}")
        print(f"📊 파이프라인 완료 요약")
        print(f"{'='*80}")
        print(f"브랜드: {brand_name}")
        print(f"실행 시간: {duration.total_seconds()/60:.1f}분")
        
        if 'crawl' in stats:
            print(f"\n1️⃣ 크롤링: {stats['crawl']['crawled_count']}개")
        
        if 'translate' in stats:
            print(f"2️⃣ 번역: {stats['translate']['translated']}개")
        
        if 'match' in stats:
            match = stats['match']
            total = match['matched'] + match['not_found'] + match['error']
            success_rate = (match['matched'] / total * 100) if total > 0 else 0
            print(f"3️⃣ 매칭: {match['matched']}/{total}개 ({success_rate:.1f}%)")
        
        # DB 최종 상태
        brand_stats = self.db.get_brand_stats(brand_name)
        print(f"\n📈 최종 상태:")
        for stage, count in brand_stats['by_stage'].items():
            emoji = {
                'crawled': '🆕',
                'translated': '📝',
                'matched': '✅',
                'failed': '❌'
            }.get(stage, '❓')
            print(f"  {emoji} {stage}: {count}개")
        
        print(f"{'='*80}\n")


def main():
    """CLI 실행"""
    import argparse
    
    parser = argparse.ArgumentParser(description='파이프라인 통합 실행')
    parser.add_argument('--brand', required=True, help='브랜드명')
    parser.add_argument('--url', help='쿠팡 검색 URL (신규 브랜드만 필수)')
    parser.add_argument('--max-pages', type=int, help='최대 페이지 수')
    parser.add_argument('--headless', action='store_true', help='헤드리스 모드')
    parser.add_argument('--list', action='store_true', help='브랜드 목록 보기')
    
    args = parser.parse_args()
    
    pipeline = Pipeline()
    
    # 브랜드 목록
    if args.list:
        pipeline.list_brands()
        return 0
    
    # 실행
    try:
        stats = pipeline.run_brand(
            brand_name=args.brand,
            search_url=args.url,  # None 가능
            max_pages=args.max_pages,
            headless=args.headless
        )
        return 0 if stats else 1
    except ValueError as e:
        print(f"❌ {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())