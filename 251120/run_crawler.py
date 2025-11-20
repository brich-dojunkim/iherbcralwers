#!/usr/bin/env python3
"""
아이허브 키워드 분석용 쿠팡 크롤러 v2
- 판매량순 정렬
- 실시간 CSV 저장
- 중단 후 재개 기능
"""

import sys
import os
import time
import csv
from datetime import datetime
from typing import List, Dict
import random
import json

# 프로젝트 루트 경로 설정
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR) if '251120' in SCRIPT_DIR else SCRIPT_DIR

# coupang 모듈 경로 추가
COUPANG_MODULE_PATH = os.path.join(PROJECT_ROOT, 'coupang')
if COUPANG_MODULE_PATH not in sys.path:
    sys.path.insert(0, COUPANG_MODULE_PATH)

# 기존 coupang 모듈 import
from coupang_manager import BrowserManager

# scraper import
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
from keyword_scraper import CoupangScraper


# ==================== 설정 ====================
KEYWORDS = [
    "유산균",
    "오메가",
    "액상칼슘",
    "고흡수마그네슘",
    "아스타잔틴",
    "엘더베리",
    "콜라겐업 히알루론산 비타민C파우더",
    "실리마린",
    "베르베린",
    "코큐텐",
    "글루코사민콘드로이틴",
    "옵티MSM",
    "L-아르기닌",
    "루테인",
    "프로폴리스"
]

MAX_PAGE = 10

# 출력 경로
if '251120' in SCRIPT_DIR:
    OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'outputs')
else:
    OUTPUT_DIR = os.path.join(PROJECT_ROOT, '251120', 'outputs')

os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_CSV = os.path.join(OUTPUT_DIR, 'coupang_crawled_data.csv')
PROGRESS_FILE = os.path.join(OUTPUT_DIR, 'crawl_progress.json')

DELAY_RANGE = (3, 6)


# ==================== 진행 상황 관리 ====================
class ProgressManager:
    """크롤링 진행 상황 관리"""
    
    def __init__(self, progress_file: str):
        self.progress_file = progress_file
        self.progress = self.load_progress()
    
    def load_progress(self) -> Dict:
        """저장된 진행 상황 로드"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {'completed': []}
    
    def save_progress(self):
        """진행 상황 저장"""
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.progress, f, ensure_ascii=False, indent=2)
    
    def is_completed(self, keyword: str, page: int) -> bool:
        """특정 키워드-페이지가 완료되었는지 확인"""
        key = f"{keyword}_{page}"
        return key in self.progress['completed']
    
    def mark_completed(self, keyword: str, page: int):
        """특정 키워드-페이지를 완료로 표시"""
        key = f"{keyword}_{page}"
        if key not in self.progress['completed']:
            self.progress['completed'].append(key)
            self.save_progress()
    
    def reset(self):
        """진행 상황 초기화"""
        self.progress = {'completed': []}
        self.save_progress()


# ==================== CSV 실시간 저장 ====================
class RealtimeCSVWriter:
    """실시간 CSV 작성"""
    
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.fieldnames = [
            'keyword', 'page', 'rank', 'product_id', 'product_name', 'brand',
            'price', 'original_price', 'discount_rate', 'review_count', 'rating',
            'is_rocket', 'product_url', 'image_url', 'seller_name', 'crawled_at'
        ]
        self.file_exists = os.path.exists(csv_path)
        
        # 파일이 없으면 헤더 작성
        if not self.file_exists:
            self._write_header()
    
    def _write_header(self):
        """CSV 헤더 작성"""
        with open(self.csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()
        self.file_exists = True
    
    def write_products(self, products: List[Dict]):
        """상품 목록을 즉시 CSV에 추가"""
        if not products:
            return
        
        with open(self.csv_path, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            
            for product in products:
                row = {
                    'keyword': product.get('keyword', ''),
                    'page': product.get('page', ''),
                    'rank': product.get('rank', ''),
                    'product_id': product.get('product_id', ''),
                    'product_name': product.get('name', ''),
                    'brand': product.get('brand', ''),
                    'price': product.get('price', ''),
                    'original_price': product.get('original_price', ''),
                    'discount_rate': product.get('discount_rate', ''),
                    'review_count': product.get('review_count', 0),
                    'rating': product.get('rating', ''),
                    'is_rocket': 'Y' if product.get('is_rocket') else 'N',
                    'product_url': product.get('url', ''),
                    'image_url': product.get('image_url', ''),
                    'seller_name': product.get('seller_name', ''),
                    'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                writer.writerow(row)


# ==================== 크롤러 ====================
class KeywordCrawler:
    """키워드 기반 크롤러"""
    
    def __init__(self):
        self.browser_manager = BrowserManager(headless=False)
        self.scraper = CoupangScraper()
        self.progress_manager = ProgressManager(PROGRESS_FILE)
        self.csv_writer = RealtimeCSVWriter(OUTPUT_CSV)
        self.total_collected = 0
    
    def crawl_keyword(self, keyword: str) -> int:
        """단일 키워드 크롤링 (판매량순)"""
        print(f"\n{'='*60}")
        print(f"키워드: {keyword}")
        print(f"{'='*60}")
        
        keyword_count = 0
        
        for page in range(1, MAX_PAGE + 1):
            # 이미 완료된 페이지는 스킵
            if self.progress_manager.is_completed(keyword, page):
                print(f"\n[페이지 {page}/{MAX_PAGE}] 이미 완료됨 (스킵)")
                continue
            
            try:
                print(f"\n[페이지 {page}/{MAX_PAGE}]", end=" ")
                
                # 검색 URL (판매량순 정렬)
                # sorter=saleCountDesc 파라미터 추가
                search_url = f"https://www.coupang.com/np/search?q={keyword}&page={page}&sorter=saleCountDesc"
                
                # 페이지 이동
                self.browser_manager.get_with_coupang_referrer(search_url, settle_sec=5.0)
                time.sleep(random.uniform(3, 5))
                
                # 스크래핑
                products = self.scraper.scrape_search_results(
                    self.browser_manager.driver,
                    keyword=keyword,
                    page=page
                )
                
                if not products:
                    print("상품 없음 - 중단")
                    break
                
                # 실시간 CSV 저장
                self.csv_writer.write_products(products)
                
                page_count = len(products)
                keyword_count += page_count
                self.total_collected += page_count
                
                print(f"✓ {page_count}개 수집 (누적: {self.total_collected}개)")
                
                # 진행 상황 저장
                self.progress_manager.mark_completed(keyword, page)
                
                # 페이지 간 딜레이
                if page < MAX_PAGE:
                    delay = random.uniform(*DELAY_RANGE)
                    time.sleep(delay)
                
            except Exception as e:
                print(f"오류: {e}")
                continue
        
        print(f"\n→ 키워드 '{keyword}' 총 {keyword_count}개 수집")
        return keyword_count
    
    def crawl_all_keywords(self):
        """모든 키워드 크롤링"""
        print("\n" + "="*60)
        print("쿠팡 키워드 크롤링 시작 v2")
        print(f"키워드: {len(KEYWORDS)}개")
        print(f"페이지: 최대 {MAX_PAGE}페이지")
        print(f"정렬: 판매량순")
        print(f"저장 방식: 실시간 CSV 저장")
        print("="*60)
        
        # 진행 상황 확인
        if self.progress_manager.progress['completed']:
            print(f"\n⚠️  이전 진행 상황 발견: {len(self.progress_manager.progress['completed'])}개 완료됨")
            response = input("처음부터 다시 시작하시겠습니까? (y/N): ")
            if response.lower() == 'y':
                self.progress_manager.reset()
                if os.path.exists(OUTPUT_CSV):
                    os.remove(OUTPUT_CSV)
                self.csv_writer = RealtimeCSVWriter(OUTPUT_CSV)
                print("✓ 진행 상황 초기화 완료")
            else:
                print("✓ 이어서 크롤링 진행")
        
        # 드라이버 시작
        if not self.browser_manager.start_driver():
            print("드라이버 시작 실패")
            return False
        
        try:
            # 쿠팡 홈으로 먼저 이동
            print("\n쿠팡 홈페이지 접속 중...")
            self.browser_manager.driver.get("https://www.coupang.com")
            time.sleep(5)
            print("✓ 접속 완료\n")
            
            # 각 키워드 크롤링
            for idx, keyword in enumerate(KEYWORDS, 1):
                print(f"\n진행: {idx}/{len(KEYWORDS)}")
                
                self.crawl_keyword(keyword)
                
                # 키워드 간 딜레이
                if idx < len(KEYWORDS):
                    delay = random.uniform(5, 8)
                    print(f"\n대기 중... ({delay:.1f}초)")
                    time.sleep(delay)
            
            print("\n" + "="*60)
            print(f"✓ 전체 완료: {self.total_collected}개 상품 수집")
            print(f"✓ CSV 저장: {OUTPUT_CSV}")
            print("="*60)
            
            return True
            
        except KeyboardInterrupt:
            print("\n\n⚠️  사용자에 의해 중단됨")
            print(f"현재까지 {self.total_collected}개 상품 수집됨")
            print(f"CSV 저장: {OUTPUT_CSV}")
            print(f"진행 상황 저장: {PROGRESS_FILE}")
            print("\n다음 실행 시 이어서 크롤링할 수 있습니다.")
            return False
            
        except Exception as e:
            print(f"\n오류 발생: {e}")
            return False
        
        finally:
            if self.browser_manager.driver:
                self.browser_manager.driver.quit()
                print("\n✓ 드라이버 종료")


# ==================== 실행 ====================
def main():
    crawler = KeywordCrawler()
    crawler.crawl_all_keywords()


if __name__ == "__main__":
    main()