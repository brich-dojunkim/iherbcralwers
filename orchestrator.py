"""
파이프라인 Orchestrator - 미니멀 버전
DB 상태 기반 자동 실행

핵심: 사용자는 브랜드명만 입력, 시스템이 자동 판단
"""

import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.database import Database
# coupang 폴더 내부에서 상대 import 문제 해결을 위해 경로 추가
coupang_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'coupang')
iherbscraper_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'iherbscraper')
sys.path.insert(0, coupang_path)
sys.path.insert(0, iherbscraper_path)

from coupang.crawler import CoupangCrawlerDB
from coupang.translator import TranslatorDB
from iherbscraper.main import IHerbScraperDB


class PipelineOrchestrator:
    
    def __init__(self, db_path: str = "data/products.db"):
        self.db = Database(db_path)
    
    def run(self, brand_name: str):
        """브랜드 파이프라인 자동 실행"""
        
        # 1. 상태 분석
        mode = self._determine_mode(brand_name)
        print(f"🎯 모드: {mode}")
        
        # 2. 실행
        if mode == 'NEW':
            self._run_new(brand_name)
        elif mode == 'RESUME':
            self._run_resume(brand_name)
        elif mode == 'UPDATE':
            self._run_update(brand_name)
    
    def _determine_mode(self, brand_name: str) -> str:
        """DB 상태 보고 모드 결정"""
        
        # 상품 개수 확인
        stats = self.db.get_brand_stats(brand_name)
        total = stats['total_products']
        
        if total == 0:
            return 'NEW'
        
        # 미완료 작업 확인
        by_stage = stats['by_stage']
        incomplete = by_stage.get('crawled', 0) + by_stage.get('translated', 0)
        
        if incomplete > 0:
            return 'RESUME'
        else:
            return 'UPDATE'
    
    def _run_new(self, brand_name: str):
        """처음 실행: 크롤링 → 번역 → 매칭"""
        
        brand = self.db.get_brand(brand_name)
        url = brand['coupang_search_url']
        
        # 1. 크롤링
        print("[1/3] 크롤링...")
        crawler = CoupangCrawlerDB(self.db, brand_name)
        crawler.start_driver()
        crawler.crawl_and_save(url)
        crawler.close()
        
        # 2. 번역
        print("[2/3] 번역...")
        translator = TranslatorDB(self.db)
        translator.translate_brand(brand_name)
        
        # 3. 매칭
        print("[3/3] 매칭...")
        scraper = IHerbScraperDB(self.db, brand_name)
        scraper.match_all_products()
        scraper.close()
        
        # 완료 시각 업데이트
        now = datetime.now().isoformat()
        self.db.update_brand_timestamps(brand_name, 
                                      last_crawled_at=now, 
                                      last_matched_at=now)
    
    def _run_resume(self, brand_name: str):
        """재시작: 미완료 단계부터 실행"""
        
        # crawled 있으면 번역부터
        crawled = self.db.get_products_by_stage(brand_name, 'crawled')
        if crawled:
            print("[번역] 미완료 상품 처리...")
            translator = TranslatorDB(self.db)
            translator.translate_brand(brand_name)
        
        # translated 있으면 매칭
        translated = self.db.get_products_by_stage(brand_name, 'translated')
        if translated:
            print("[매칭] 미완료 상품 처리...")
            scraper = IHerbScraperDB(self.db, brand_name)
            scraper.match_all_products()
            scraper.close()
        
        # failed 있으면 복구 후 매칭
        failed_count = len(self.db.get_products_by_stage(brand_name, 'failed'))
        if failed_count > 0:
            print(f"[복구] {failed_count}개 실패 상품 재시도...")
            self.db.reset_failed_products(brand_name, 'translated')
            scraper = IHerbScraperDB(self.db, brand_name)
            scraper.match_all_products()
            scraper.close()
        
        # 완료 시각 업데이트
        self.db.update_brand_timestamps(brand_name, 
                                      last_matched_at=datetime.now().isoformat())
    
    def _run_update(self, brand_name: str):
        """업데이트: 새 크롤링 → 신규만 처리"""
        
        brand = self.db.get_brand(brand_name)
        url = brand['coupang_search_url']
        
        # 1. 크롤링 (기존: 가격 업데이트, 신규: 생성)
        print("[크롤링] 업데이트...")
        crawler = CoupangCrawlerDB(self.db, brand_name)
        crawler.start_driver()
        crawler.crawl_and_save(url)
        crawler.close()
        
        # 크롤링 시각 업데이트
        self.db.update_brand_timestamps(brand_name, 
                                      last_crawled_at=datetime.now().isoformat())
        
        # 2. 신규 상품 있으면 자동으로 RESUME
        crawled = self.db.get_products_by_stage(brand_name, 'crawled')
        if crawled:
            print("신규 상품 발견 → 자동 처리")
            self._run_resume(brand_name)
    
    def register_brand(self, brand_name: str, url: str):
        """브랜드 등록"""
        self.db.upsert_brand(brand_name, url)
        print(f"✓ 브랜드 '{brand_name}' 등록 완료")
    
    def list_brands(self):
        """브랜드 목록"""
        brands = self.db.get_all_brands()
        print("등록된 브랜드:")
        for brand in brands:
            print(f"  - {brand['brand_name']}")


def main():
    parser = argparse.ArgumentParser(description='파이프라인 Orchestrator')
    parser.add_argument('--brand', required=True, help='브랜드명')
    parser.add_argument('--register', action='store_true', help='브랜드 등록')
    parser.add_argument('--url', help='쿠팡 검색 URL (등록 시 필요)')
    parser.add_argument('--list', action='store_true', help='브랜드 목록')
    
    args = parser.parse_args()
    
    orchestrator = PipelineOrchestrator()
    
    if args.list:
        orchestrator.list_brands()
    elif args.register:
        if not args.url:
            print("❌ 등록 시 --url 필요")
            return
        orchestrator.register_brand(args.brand, args.url)
    else:
        orchestrator.run(args.brand)


if __name__ == "__main__":
    main()