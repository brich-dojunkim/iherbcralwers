"""
파이프라인 Orchestrator
DB 상태 기반 자동 실행

로직:
1. 상품 없음 → NEW (처음부터)
2. 정상 미완료(crawled/translated) 있음 → RESUME (남은 것만)
3. 완료됨 → UPDATE (크롤링부터)

Failed 처리:
- UPDATE 시 failed → crawled로 리셋 (새 사이클 시도)
- 무한 RESUME 방지
"""

import sys
import os
import argparse
from datetime import datetime
from typing import Tuple, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.database import Database

# Coupang 모듈
from coupang.crawler import CoupangCrawlerDB
from coupang.translator import TranslatorDB

# IHerb 모듈
from iherbscraper.main import IHerbScraperDB


class PipelineOrchestrator:
    """파이프라인 자동 실행 관리"""
    
    def __init__(self, db_path='products.db', headless=False, max_iherb_products=5):
        self.db = Database(db_path)
        self.headless = headless
        self.max_iherb_products = max_iherb_products
        
    def analyze_state(self, brand_name: str) -> Tuple[str, Dict[str, int]]:
        """
        브랜드 상태 분석 및 실행 모드 결정
        
        Returns:
            (mode, stats)
            - mode: 'NEW' | 'RESUME' | 'UPDATE'
            - stats: {'total', 'crawled', 'translated', 'matched', 'failed'}
        """
        stats = {
            'total': 0,
            'crawled': 0,
            'translated': 0,
            'matched': 0,
            'failed': 0
        }
        
        # 각 단계별 상품 수 조회
        with self.db.get_connection() as conn:
            # 전체 상품 수
            result = conn.execute("""
                SELECT COUNT(*) as cnt FROM products WHERE brand_name = ?
            """, (brand_name,)).fetchone()
            stats['total'] = result['cnt']
            
            # 단계별 카운트
            for stage in ['crawled', 'translated', 'matched', 'failed']:
                result = conn.execute("""
                    SELECT COUNT(*) as cnt FROM products 
                    WHERE brand_name = ? AND pipeline_stage = ?
                """, (brand_name, stage)).fetchone()
                stats[stage] = result['cnt']
        
        # 판단 로직
        if stats['total'] == 0:
            mode = 'NEW'
        elif stats['crawled'] + stats['translated'] > 0:
            # 정상 미완료 작업 있음 (failed 제외)
            mode = 'RESUME'
        else:
            # 모두 완료 (matched만 있거나 failed만 있음)
            mode = 'UPDATE'
        
        return mode, stats
    
    def run(self, brand_name: str) -> Dict[str, Any]:
        """
        브랜드 파이프라인 자동 실행
        
        Returns:
            실행 결과 통계
        """
        print(f"\n{'='*80}")
        print(f"🔍 브랜드 상태 분석: {brand_name}")
        print(f"{'='*80}")
        
        # 1. 상태 분석
        mode, stats = self.analyze_state(brand_name)
        
        print(f"\n📊 현재 상태:")
        print(f"   전체 상품: {stats['total']}개")
        if stats['total'] > 0:
            print(f"   - crawled: {stats['crawled']}개")
            print(f"   - translated: {stats['translated']}개")
            print(f"   - matched: {stats['matched']}개")
            print(f"   - failed: {stats['failed']}개")
        
        print(f"\n🎯 결정된 실행 모드: {mode}")
        
        if mode == 'NEW':
            print(f"   이유: 상품 데이터 없음")
            return self._run_new(brand_name)
        elif mode == 'RESUME':
            print(f"   이유: 미완료 작업 {stats['crawled'] + stats['translated']}개 발견")
            return self._run_resume(brand_name, stats)
        else:  # UPDATE
            print(f"   이유: 전체 {stats['matched'] + stats['failed']}개 완료됨, 업데이트 필요")
            return self._run_update(brand_name)
    
    def _run_new(self, brand_name: str) -> Dict[str, Any]:
        """NEW 모드: 처음부터 전체 실행"""
        print(f"\n{'='*80}")
        print(f"🆕 NEW 모드: 전체 파이프라인 실행")
        print(f"{'='*80}")
        
        result = {
            'mode': 'NEW',
            'crawled': 0,
            'translated': 0,
            'matched': 0,
            'failed': 0
        }
        
        # 브랜드 정보 확인
        brand_info = self.db.get_brand(brand_name)
        if not brand_info or not brand_info['coupang_search_url']:
            raise ValueError(f"브랜드 '{brand_name}'이 등록되지 않았거나 URL이 없습니다.")
        
        # 1. 크롤링
        print(f"\n[1/3] 크롤링 시작...")
        crawler = CoupangCrawlerDB(
            db=self.db,
            brand_name=brand_name,
            headless=self.headless
        )
        crawler.start_driver()
        crawler_stats = crawler.crawl_and_save(brand_info['coupang_search_url'])
        crawler.close()
        result['crawled'] = crawler_stats.get('crawled_count', 0)
        print(f"✓ {result['crawled']}개 크롤링 완료")
        
        # 2. 번역
        print(f"\n[2/3] 번역 시작...")
        translator = TranslatorDB(db=self.db)
        trans_stats = translator.translate_brand(brand_name)
        result['translated'] = trans_stats.get('translated', 0)
        print(f"✓ {result['translated']}개 번역 완료")
        
        # 3. 매칭
        print(f"\n[3/3] 매칭 시작...")
        scraper = IHerbScraperDB(
            db=self.db,
            brand_name=brand_name,
            headless=self.headless,
            max_products=self.max_iherb_products
        )
        match_result = scraper.match_all_products()
        scraper.close()
        result['matched'] = match_result.get('matched', 0)
        result['failed'] = match_result.get('error', 0)
        print(f"✓ {result['matched']}개 매칭 성공, {result['failed']}개 실패")
        
        return result
    
    def _run_resume(self, brand_name: str, stats: Dict[str, int]) -> Dict[str, Any]:
        """RESUME 모드: 중단된 작업 이어하기"""
        print(f"\n{'='*80}")
        print(f"▶️ RESUME 모드: 미완료 작업 재개")
        print(f"{'='*80}")
        
        result = {
            'mode': 'RESUME',
            'crawled': 0,
            'translated': 0,
            'matched': 0,
            'failed': 0
        }
        
        # 1. 크롤링 (crawled 상품이 있으면)
        if stats['crawled'] > 0:
            print(f"\n⚠️ crawled 단계 상품 {stats['crawled']}개 발견")
            print(f"   → 크롤링 중단으로 판단, 크롤링부터 재시작")
            
            brand_info = self.db.get_brand(brand_name)
            if not brand_info or not brand_info['coupang_search_url']:
                raise ValueError(f"브랜드 '{brand_name}'의 URL 정보 없음")
            
            crawler = CoupangCrawlerDB(
                db=self.db,
                brand_name=brand_name,
                headless=self.headless
            )
            crawler.start_driver()
            crawler_stats = crawler.crawl_and_save(brand_info['coupang_search_url'])
            crawler.close()
            result['crawled'] = crawler_stats.get('crawled_count', 0)
            print(f"✓ {result['crawled']}개 크롤링 완료")
        else:
            print(f"\n[크롤링] 건너뜀 (crawled 단계 없음)")
        
        # 2. 번역 (crawled 대기 중인 상품만)
        to_translate = len(self.db.get_products_by_stage(brand_name, 'crawled'))
        if to_translate > 0:
            print(f"\n[번역] {to_translate}개 시작...")
            translator = TranslatorDB(db=self.db)
            trans_stats = translator.translate_brand(brand_name)
            result['translated'] = trans_stats.get('translated', 0)
            print(f"✓ {result['translated']}개 번역 완료")
        else:
            print(f"\n[번역] 건너뜀 (crawled 단계 없음)")
        
        # 3. 매칭 (translated 대기 중인 상품만)
        to_match = len(self.db.get_products_by_stage(brand_name, 'translated'))
        if to_match > 0:
            print(f"\n[매칭] {to_match}개 시작...")
            scraper = IHerbScraperDB(
                db=self.db,
                brand_name=brand_name,
                headless=self.headless,
                max_products=self.max_iherb_products
            )
            match_result = scraper.match_all_products()
            scraper.close()
            result['matched'] = match_result.get('matched', 0)
            result['failed'] = match_result.get('error', 0)
            print(f"✓ {result['matched']}개 매칭 성공, {result['failed']}개 실패")
        else:
            print(f"\n[매칭] 건너뜀 (translated 단계 없음)")
        
        return result
    
    def _run_update(self, brand_name: str) -> Dict[str, Any]:
        """UPDATE 모드: 새 크롤링 후 신규 상품만 처리"""
        print(f"\n{'='*80}")
        print(f"🔄 UPDATE 모드: 새 크롤링 시작")
        print(f"{'='*80}")
        
        result = {
            'mode': 'UPDATE',
            'crawled': 0,
            'translated': 0,
            'matched': 0,
            'failed': 0
        }
        
        # 1. 크롤링 (전체)
        print(f"\n[크롤링] 최신 데이터 수집 중...")
        brand_info = self.db.get_brand(brand_name)
        if not brand_info or not brand_info['coupang_search_url']:
            raise ValueError(f"브랜드 '{brand_name}'의 URL 정보 없음")
        
        crawler = CoupangCrawlerDB(
            db=self.db,
            brand_name=brand_name,
            headless=self.headless
        )
        crawler.start_driver()
        crawler_stats = crawler.crawl_and_save(brand_info['coupang_search_url'])
        crawler.close()
        result['crawled'] = crawler_stats.get('crawled_count', 0)
        print(f"✓ {result['crawled']}개 크롤링 완료")
        
        # 크롤링 후 상태 재확인
        print(f"\n[상태 확인] 크롤링 후 분류...")
        with self.db.get_connection() as conn:
            # failed였던 상품이 crawled로 리셋되었는지 확인
            new_crawled = conn.execute("""
                SELECT COUNT(*) as cnt FROM products
                WHERE brand_name = ? AND pipeline_stage = 'crawled'
            """, (brand_name,)).fetchone()['cnt']
            
            matched_count = conn.execute("""
                SELECT COUNT(*) as cnt FROM products
                WHERE brand_name = ? AND pipeline_stage = 'matched'
            """, (brand_name,)).fetchone()['cnt']
        
        print(f"   - 신규/리셋 상품: {new_crawled}개 (crawled)")
        print(f"   - 기존 완료 상품: {matched_count}개 (matched)")
        
        # 신규 상품이 있으면 자동으로 RESUME
        if new_crawled > 0:
            print(f"\n🔄 자동 전환: RESUME 모드 (신규 {new_crawled}개 처리)")
            
            # 2. 번역
            print(f"\n[번역] {new_crawled}개 시작...")
            translator = TranslatorDB(db=self.db)
            trans_stats = translator.translate_brand(brand_name)
            result['translated'] = trans_stats.get('translated', 0)
            print(f"✓ {result['translated']}개 번역 완료")
            
            # 3. 매칭
            to_match = len(self.db.get_products_by_stage(brand_name, 'translated'))
            print(f"\n[매칭] {to_match}개 시작...")
            scraper = IHerbScraperDB(
                db=self.db,
                brand_name=brand_name,
                headless=self.headless,
                max_products=self.max_iherb_products
            )
            match_result = scraper.match_all_products()
            scraper.close()
            result['matched'] = match_result.get('matched', 0)
            result['failed'] = match_result.get('error', 0)
            print(f"✓ {result['matched']}개 매칭 성공, {result['failed']}개 실패")
        else:
            print(f"\n✓ 신규 상품 없음, 가격 업데이트만 완료")
        
        return result
    
    def register_brand(self, brand_name: str, coupang_url: str) -> None:
        """브랜드 등록"""
        self.db.upsert_brand(brand_name, coupang_url)
        print(f"✓ 브랜드 '{brand_name}' 등록 완료")
    
    def list_brands(self) -> None:
        """등록된 브랜드 목록 출력"""
        with self.db.get_connection() as conn:
            brands = conn.execute("""
                SELECT 
                    b.brand_name,
                    b.coupang_search_url,
                    b.last_crawled_at,
                    b.last_matched_at,
                    COUNT(p.id) as product_count,
                    SUM(CASE WHEN p.pipeline_stage = 'matched' THEN 1 ELSE 0 END) as matched_count
                FROM brands b
                LEFT JOIN products p ON b.brand_name = p.brand_name
                GROUP BY b.brand_name
                ORDER BY b.brand_name
            """).fetchall()
            
            if not brands:
                print("등록된 브랜드가 없습니다.")
                return
            
            print(f"\n{'='*80}")
            print(f"등록된 브랜드 목록")
            print(f"{'='*80}")
            
            for brand in brands:
                print(f"\n브랜드: {brand['brand_name']}")
                print(f"  URL: {brand['coupang_search_url']}")
                print(f"  상품 수: {brand['product_count']}개")
                print(f"  매칭 완료: {brand['matched_count']}개")
                print(f"  마지막 크롤링: {brand['last_crawled_at'] or '없음'}")
                print(f"  마지막 매칭: {brand['last_matched_at'] or '없음'}")


def main():
    parser = argparse.ArgumentParser(
        description='쿠팡-아이허브 가격 비교 파이프라인',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 브랜드 등록
  python orchestrator.py --register --brand thorne --url "https://coupang.com/..."
  
  # 실행 (자동 모드 판단)
  python orchestrator.py --brand thorne
  
  # 브랜드 목록
  python orchestrator.py --list
        """
    )
    
    # 명령어 그룹
    parser.add_argument('--brand', type=str, help='처리할 브랜드명')
    parser.add_argument('--register', action='store_true', help='새 브랜드 등록')
    parser.add_argument('--list', action='store_true', help='브랜드 목록 출력')
    
    # 등록 시 필요
    parser.add_argument('--url', type=str, help='쿠팡 검색 URL (등록 시)')
    
    # 옵션
    parser.add_argument('--db', type=str, default='products.db', help='DB 파일 경로')
    parser.add_argument('--headless', action='store_true', help='헤드리스 모드 (기본: 브라우저 표시)')
    parser.add_argument('--max-products', type=int, default=5, 
                       help='아이허브 비교 상품 수 (기본: 5)')
    
    args = parser.parse_args()
    
    # Orchestrator 생성
    orchestrator = PipelineOrchestrator(
        db_path=args.db,
        headless=args.headless,
        max_iherb_products=args.max_products
    )
    
    try:
        # 명령 실행
        if args.list:
            orchestrator.list_brands()
        
        elif args.register:
            if not args.brand or not args.url:
                parser.error("--register는 --brand와 --url이 필요합니다")
            orchestrator.register_brand(args.brand, args.url)
        
        elif args.brand:
            # 자동 실행
            result = orchestrator.run(args.brand)
            
            # 결과 출력
            print(f"\n{'='*80}")
            print(f"✅ 실행 완료")
            print(f"{'='*80}")
            print(f"모드: {result['mode']}")
            print(f"크롤링: {result['crawled']}개")
            print(f"번역: {result['translated']}개")
            print(f"매칭 성공: {result['matched']}개")
            print(f"매칭 실패: {result['failed']}개")
        
        else:
            parser.print_help()
    
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()