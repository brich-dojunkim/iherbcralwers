"""
가격 비교 업데이터 - 메인 실행 파일
"""

import os
import pandas as pd
import sys
import time
from datetime import datetime
from typing import Tuple

# 상위 디렉토리와 하위 모듈 경로들을 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
coupang_dir = os.path.join(parent_dir, 'coupang')
iherbscraper_dir = os.path.join(parent_dir, 'iherbscraper')

sys.path.insert(0, parent_dir)
sys.path.insert(0, coupang_dir)
sys.path.insert(0, iherbscraper_dir)

from config import PathConfig
from data_processor import DataProcessor
from product_updater import ProductUpdater


class PriceUpdater:
    """가격 비교 업데이터"""
    
    def __init__(self, output_dir: str = None):
        self.output_dir = output_dir or PathConfig.UNIFIED_OUTPUTS_DIR
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.final_csv = os.path.join(output_dir, f"updated_price_comparison_{self.timestamp}.csv")
        
        # 모듈 초기화
        self.data_processor = DataProcessor()
        self.product_updater = ProductUpdater()
        
        print(f"🚀 가격 비교 업데이터 초기화 완료")
        print(f"   출력 파일: {self.final_csv}")
    
    def run(self, base_csv_path: str, coupang_search_url: str) -> str:
        """
        전체 업데이터 실행
        
        Args:
            base_csv_path: 기존 매칭 결과 CSV 경로
            coupang_search_url: 쿠팡 검색 결과 URL
            
        Returns:
            최종 결과 CSV 경로
        """
        start_time = time.time()
        
        print(f"\n🔄 업데이터 실행 시작")
        print(f"   베이스 데이터: {base_csv_path}")
        print(f"   쿠팡 URL: {coupang_search_url}")
        
        try:
            # 1. 베이스 데이터 로드
            print(f"\n1️⃣ 베이스 데이터 로드")
            base_df = pd.read_csv(base_csv_path, encoding='utf-8-sig')
            print(f"   기존 상품: {len(base_df)}개")
            
            # 2. 쿠팡 최신 데이터 크롤링
            print(f"\n2️⃣ 쿠팡 데이터 크롤링")
            crawled_df = self.product_updater.crawl_coupang_products(coupang_search_url)
            print(f"   크롤링 상품: {len(crawled_df)}개")
            
            # 3. 기존 vs 신규 상품 분류
            print(f"\n3️⃣ 상품 분류")
            existing_df, new_df = self.data_processor.classify_products(base_df, crawled_df)
            print(f"   기존 상품: {len(existing_df)}개")
            print(f"   신규 상품: {len(new_df)}개")
            
            # 4. 기존 상품 가격 업데이트
            print(f"\n4️⃣ 기존 상품 가격 업데이트")
            updated_base_df = self.data_processor.update_existing_prices(base_df, existing_df)
            print(f"   업데이트 완료")
            
            # 5. 신규 상품 아이허브 매칭
            if len(new_df) > 0:
                print(f"\n5️⃣ 신규 상품 아이허브 매칭")
                matched_new_df = self.product_updater.match_iherb_products(new_df)
                print(f"   매칭 완료: {len(matched_new_df)}개")
            else:
                matched_new_df = pd.DataFrame()
                print(f"\n5️⃣ 신규 상품 없음 - 매칭 건너뜀")
            
            # 6. 최종 통합
            print(f"\n6️⃣ 최종 데이터 통합")
            final_df = self.data_processor.integrate_final_data(updated_base_df, matched_new_df)
            
            # 7. 결과 저장
            final_df.to_csv(self.final_csv, index=False, encoding='utf-8-sig')
            
            # 실행 완료
            duration = time.time() - start_time
            print(f"\n✅ 업데이터 실행 완료")
            print(f"   실행 시간: {duration/60:.1f}분")
            print(f"   최종 상품: {len(final_df)}개")
            print(f"   결과 파일: {self.final_csv}")
            
            return self.final_csv
            
        except Exception as e:
            print(f"\n❌ 업데이터 실행 실패: {e}")
            raise
    
    def close(self):
        """리소스 정리"""
        self.product_updater.close()


def main():
    """메인 실행 함수"""
    # 업데이터 실행
    updater = PriceUpdater(output_dir=PathConfig.UNIFIED_OUTPUTS_DIR)
    
    try:
        result_csv = updater.run(
            base_csv_path="/Users/brich/Desktop/iherb_price/iherbscraper/output/thorne_250918.csv",
            coupang_search_url="https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=14420&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=thorne"
        )
        
        print(f"\n🎯 실행 완료!")
        print(f"결과 파일: {result_csv}")
        return 0
        
    except KeyboardInterrupt:
        print(f"\n⚠️ 사용자 중단")
        return 1
    except Exception as e:
        print(f"\n❌ 실행 오류: {e}")
        return 1
    finally:
        updater.close()


if __name__ == "__main__":
    sys.exit(main())