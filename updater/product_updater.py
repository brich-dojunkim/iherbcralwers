"""
상품 업데이터 모듈 - 쿠팡 크롤링 + 아이허브 매칭 통합
모듈 이름 충돌 완전 해결 버전 (coupang_browser, iherb_browser)
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
    """쿠팡 크롤링과 아이허브 매칭을 통합한 상품 업데이터"""
    
    def __init__(self, enable_images: bool = True):
        """
        초기화
        
        Args:
            enable_images: 이미지 다운로드 활성화 (아이허브 매칭용)
        """
        self.enable_images = enable_images
        self.coupang_crawler = None
        self.iherb_scraper = None
        self.translator = None
    
    def crawl_coupang_products(self, search_url: str) -> pd.DataFrame:
        """
        쿠팡 상품 크롤링
        
        Args:
            search_url: 쿠팡 검색 결과 URL
            
        Returns:
            크롤링된 상품 DataFrame
        """
        try:
            print(f"   크롤링 시작: {search_url}")
            
            # 크롤러 초기화
            self.coupang_crawler = CoupangCrawlerMacOS(
                headless=False,
                delay_range=(3, 6),
                download_images=self.enable_images,
                image_dir=f"./temp_images"
            )
            
            # 크롤링 실행
            products = self.coupang_crawler.crawl_all_pages(search_url, max_pages=None)
            
            if products:
                # DataFrame 변환
                crawled_df = pd.DataFrame(products)
                
                # 필수 컬럼 확인 및 생성
                required_columns = ['product_id', 'product_name', 'current_price', 'original_price', 'discount_rate']
                for col in required_columns:
                    if col not in crawled_df.columns:
                        crawled_df[col] = ''
                
                print(f"   크롤링 완료: {len(crawled_df)}개 상품")
                return crawled_df
            else:
                print(f"   크롤링 결과 없음")
                # 빈 DataFrame이라도 필수 컬럼은 생성
                empty_df = pd.DataFrame(columns=['product_id', 'product_name', 'current_price', 'original_price', 'discount_rate'])
                return empty_df
                
        except Exception as e:
            print(f"   크롤링 실패: {e}")
            # 상세 오류 정보 출력
            import traceback
            print(f"   상세 오류: {traceback.format_exc()}")
            
            # 오류 발생 시에도 빈 DataFrame 반환 (필수 컬럼 포함)
            empty_df = pd.DataFrame(columns=['product_id', 'product_name', 'current_price', 'original_price', 'discount_rate'])
            return empty_df
    
    def match_iherb_products(self, new_products_df: pd.DataFrame) -> pd.DataFrame:
        """
        신규 상품들을 아이허브와 매칭
        
        Args:
            new_products_df: 신규 쿠팡 상품 DataFrame
            
        Returns:
            매칭 결과 DataFrame
        """
        if len(new_products_df) == 0:
            return pd.DataFrame()
        
        try:
            # 임시 파일 경로 생성
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as temp_file:
                temp_csv_path = temp_file.name
                new_products_df.to_csv(temp_csv_path, index=False, encoding='utf-8-sig')
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as temp_translated_file:
                translated_csv_path = temp_translated_file.name
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as temp_matched_file:
                matched_csv_path = temp_matched_file.name
            
            # 1. 번역 수행
            print(f"   번역 시작: {len(new_products_df)}개 상품")
            self.translator = GeminiCSVTranslator(Config.GEMINI_API_KEY)
            
            translated_df = self.translator.translate_csv(
                input_file=temp_csv_path,
                output_file=translated_csv_path,
                column_name='product_name',
                batch_size=10
            )
            print(f"   번역 완료")
            
            # 2. 아이허브 매칭 수행
            print(f"   아이허브 매칭 시작")
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
            
            # 매칭 결과 로드
            matched_df = pd.read_csv(matched_csv, encoding='utf-8-sig')
            
            # 성공 통계
            success_count = len(matched_df[matched_df['status'] == 'success'])
            success_rate = success_count / len(matched_df) * 100 if len(matched_df) > 0 else 0
            
            print(f"   매칭 성공: {success_count}개 ({success_rate:.1f}%)")
            
            # 임시 파일 정리
            for temp_path in [temp_csv_path, translated_csv_path, matched_csv_path]:
                try:
                    os.unlink(temp_path)
                except:
                    pass
            
            return matched_df
            
        except Exception as e:
            print(f"   매칭 실패: {e}")
            return pd.DataFrame()
    
    def close(self):
        """리소스 정리"""
        if self.coupang_crawler:
            self.coupang_crawler.close()
            print(f"   쿠팡 크롤러 종료")
        
        if self.iherb_scraper:
            self.iherb_scraper.close()
            print(f"   아이허브 스크래퍼 종료")