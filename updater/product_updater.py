"""
상품 업데이터 모듈 - 단순화된 버전 (기본 이미지 경로 사용)

이 모듈은 쿠팡 크롤러와 iHerb 스크래퍼를 연계하여 상품 정보를 수집하고 매칭하는 기능을 제공합니다.
기존 코드에서 발생하던 몇 가지 문제를 해결하기 위해 다음과 같이 개선했습니다.

* **필수 컬럼 선 정의** – 크롤링 도중 예외가 발생하거나 사용자가 중단해도 `required_columns`가 정의되어 있지 않아
  `UnboundLocalError`가 발생하는 문제를 수정했습니다. 함수 시작 부분에서 필수 컬럼 목록을 정의하여 항상 사용할 수 있습니다.
* **Graceful 종료 처리** – 크롤링 또는 매칭 과정에서 `KeyboardInterrupt`가 발생하면 현재까지의 진행 상황을
  반환하도록 수정했습니다.
* **지속 가능한 매칭 결과 저장** – `match_iherb_products` 함수에 `output_path` 파라미터를 추가하여,
  매칭 결과를 지정된 경로에 저장하고, 중단 후 재실행 시 이어서 처리할 수 있도록 개선했습니다.
"""

import pandas as pd
import sys
import os
import tempfile
from datetime import datetime

# 상위 디렉토리와 coupang, iherbscraper 경로를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
coupang_dir = os.path.join(parent_dir, 'coupang')
iherbscraper_dir = os.path.join(parent_dir, 'iherbscraper')

sys.path.insert(0, parent_dir)
sys.path.insert(0, coupang_dir)
sys.path.insert(0, iherbscraper_dir)

from config import PathConfig, APIConfig
from iherbscraper.iherb_config import IHerbConfig
from coupang.coupang_config import CoupangConfig
from coupang.crawler import CoupangCrawlerMacOS
from iherbscraper.main import EnglishIHerbScraper
from coupang.translator import GeminiCSVTranslator

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
        # 필수 컬럼을 함수 시작 부분에서 정의하여 예외 상황에서도 참조할 수 있도록 합니다.
        required_columns = CoupangConfig.REQUIRED_COLUMNS[:5]  # 기본 5개만
        try:
            print(f"   🔄 쿠팡 크롤링 시작")
            print(f"   📍 URL: {search_url[:80]}...")

            # 크롤러 초기화 - 기본 이미지 경로 자동 사용
            self.coupang_crawler = CoupangCrawlerMacOS(
                headless=False,
                delay_range=(3, 6),
                download_images=self.enable_images,
                image_dir=None  # None으로 설정하여 기본 경로(coupang/coupang_images) 자동 사용
            )

            if self.enable_images:
                print(f"   🖼️ 이미지 저장 활성화: 자동 경로 사용")
            else:
                print(f"   📝 텍스트 전용 크롤링")

            # 크롤링 실행
            products = self.coupang_crawler.crawl_all_pages(search_url, max_pages=None)

            # products가 None이거나 빈 리스트일 때 처리
            if not products:
                print(f"   ⚠️ 크롤링 결과 없음")
                return pd.DataFrame(columns=required_columns)

            # DataFrame 변환
            crawled_df = pd.DataFrame(products)

            # 필수 컬럼 확인 및 생성
            for col in required_columns:
                if col not in crawled_df.columns:
                    crawled_df[col] = ''

            print(f"   ✅ 크롤링 완료: {len(crawled_df)}개 상품")

            # 이미지 통계 출력
            if self.enable_images and getattr(self.coupang_crawler, 'image_downloader', None):
                stats = self.coupang_crawler.image_downloader.image_download_stats
                successful_images = stats.get('successful_downloads', 0)
                total_attempts = stats.get('total_attempts', 0)
                print(f"   📸 이미지 수집: {successful_images}/{total_attempts}개")

            return crawled_df

        except KeyboardInterrupt:
            # 사용자 중단 시 빈 DataFrame 반환
            print("   ⚠️ 사용자가 크롤링을 중단했습니다.")
            return pd.DataFrame(columns=required_columns)
        except Exception as e:
            print(f"   ❌ 크롤링 실패: {e}")
            import traceback
            print(f"   상세 오류: {traceback.format_exc()}")
            return pd.DataFrame(columns=required_columns)

    def match_iherb_products(self, new_products_df: pd.DataFrame, output_path: str | None = None, brand_name: str = "unknown") -> pd.DataFrame:
        if len(new_products_df) == 0:
            print(f"   📝 매칭할 신규 상품이 없습니다")
            return pd.DataFrame()

        try:
            print(f"   🔄 아이허브 매칭 시작: {len(new_products_df)}개 상품")

            # 1. outputs 폴더 생성
            os.makedirs(PathConfig.UNIFIED_OUTPUTS_DIR, exist_ok=True)
            
            # 2. 출력 경로 설정
            if output_path:
                matched_csv_path = output_path
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                matched_csv_path = os.path.join(PathConfig.UNIFIED_OUTPUTS_DIR, f"matched_{brand_name}_{timestamp}_partial.csv")

            # 3. 중단된 작업 확인 및 이어받기
            existing_results = None
            if os.path.exists(matched_csv_path):
                try:
                    existing_results = pd.read_csv(matched_csv_path, encoding='utf-8-sig')
                    completed_count = len(existing_results)
                    print(f"   📂 기존 진행 상황: {completed_count}개 완료")
                    
                    # 이미 처리된 상품 ID들 확인
                    if 'coupang_product_id' in existing_results.columns:
                        processed_ids = set(existing_results['coupang_product_id'].astype(str))
                        remaining_df = new_products_df[~new_products_df['product_id'].astype(str).isin(processed_ids)]
                        print(f"   ⏭️ 남은 작업: {len(remaining_df)}개")
                        new_products_df = remaining_df
                except Exception as e:
                    print(f"   ⚠️ 기존 결과 파일 읽기 실패: {e}")
                    existing_results = None

            # 4. 입력 CSV를 outputs 폴더에 저장 (아이허브 스크래퍼 입력용)
            input_csv_path = matched_csv_path.replace('.csv', '_input.csv')
            new_products_df.to_csv(input_csv_path, index=False, encoding='utf-8-sig')

            # 5. 번역 (outputs 폴더에 저장)
            print(f"   📝 번역 시작")
            translated_csv_path = matched_csv_path.replace('.csv', '_translated.csv')
            
            self.translator = GeminiCSVTranslator()
            _ = self.translator.translate_csv(
                input_file=input_csv_path,
                output_file=translated_csv_path,
                column_name='product_name',
                batch_size=10
            )
            print(f"   ✅ 번역 완료")

            # 6. 아이허브 매칭 (실시간 저장)
            print(f"   🔄 아이허브 매칭 수행")
            print(f"   💾 실시간 저장: {matched_csv_path}")

            self.iherb_scraper = EnglishIHerbScraper(
                headless=False,
                delay_range=(2, 4),
                max_products_to_compare=4
            )

            # 핵심: 실시간 저장되는 경로로 매칭 수행
            # 중단되어도 matched_csv_path에 지금까지 결과가 저장됨
            matched_csv = self.iherb_scraper.process_products_complete(
                csv_file_path=translated_csv_path,
                output_file_path=matched_csv_path,  # 실시간 저장 경로
                limit=None,
                start_from=None
            )

            # 7. 결과 로드 및 기존 결과와 병합
            if os.path.exists(matched_csv_path):
                matched_df = pd.read_csv(matched_csv_path, encoding='utf-8-sig')
                
                # 기존 결과가 있었다면 병합
                if existing_results is not None:
                    # 새로 매칭된 결과와 기존 결과 병합
                    final_matched_df = pd.concat([existing_results, matched_df], ignore_index=True)
                    # 중복 제거 (product_id 기준)
                    if 'coupang_product_id' in final_matched_df.columns:
                        final_matched_df = final_matched_df.drop_duplicates(subset=['coupang_product_id'], keep='last')
                    
                    # 병합된 결과를 다시 저장
                    final_matched_df.to_csv(matched_csv_path, index=False, encoding='utf-8-sig')
                    matched_df = final_matched_df
                    print(f"   🔗 기존 결과와 병합: 총 {len(matched_df)}개")
                
                # 성공 통계
                if 'status' in matched_df.columns:
                    success_count = len(matched_df[matched_df['status'] == 'success'])
                    success_rate = success_count / len(matched_df) * 100 if len(matched_df) > 0 else 0
                    print(f"   ✅ 매칭 완료: {success_count}/{len(matched_df)}개 ({success_rate:.1f}%)")
                    
                    if hasattr(self.iherb_scraper, 'product_matcher'):
                        api_stats = self.iherb_scraper.product_matcher.get_api_usage_stats()
                        total_calls = api_stats.get('total_calls', 0)
                        vision_calls = api_stats.get('vision_calls', 0)
                        print(f"   🤖 Gemini API: {total_calls}회 (Vision: {vision_calls}회)")
                
                # 임시 파일 정리
                try:
                    os.unlink(input_csv_path)
                    os.unlink(translated_csv_path)
                except:
                    pass
                
                return matched_df
            else:
                print(f"   ❌ 결과 파일이 생성되지 않음")
                return pd.DataFrame()

        except KeyboardInterrupt:
            print(f"\n⚠️ 사용자 중단 - 현재까지 결과가 {matched_csv_path}에 저장됨")
            # 중단되어도 기존 결과 반환
            if os.path.exists(matched_csv_path):
                try:
                    return pd.read_csv(matched_csv_path, encoding='utf-8-sig')
                except Exception:
                    pass
            return pd.DataFrame()
            
        except Exception as e:
            print(f"   ❌ 매칭 실패: {e}")
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
            # 매칭 결과를 임시 경로가 아닌 고정된 경로로 저장해 이어서 실행 가능하도록 테스트
            matched_df = updater.match_iherb_products(test_df, output_path="test_matched_results.csv", brand_name="thorne")
            print(f"매칭 결과: {len(matched_df)}개")

        print("\n✅ 테스트 완료")

    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
    finally:
        updater.close()