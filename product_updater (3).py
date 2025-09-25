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
            크롤링된 상품 DataFrame. 예외 발생 시 또는 중단 시 빈 DataFrame을 반환합니다.
        """
        # 필수 컬럼을 함수 시작 부분에서 정의하여 예외 상황에서도 참조할 수 있도록 합니다.
        required_columns = [
            'product_id', 'product_name', 'current_price',
            'original_price', 'discount_rate'
        ]
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

    def match_iherb_products(self, new_products_df: pd.DataFrame, output_path: str | None = None) -> pd.DataFrame:
        """
        신규 상품들을 아이허브와 매칭합니다.

        Args:
            new_products_df: 신규 쿠팡 상품 DataFrame
            output_path: 매칭 결과를 저장할 CSV 경로. 지정하지 않으면 임시 경로를 사용합니다.

        Returns:
            매칭 결과 DataFrame. 매칭 과정에서 KeyboardInterrupt가 발생하면 중간까지의 결과를 반환합니다.
        """
        if len(new_products_df) == 0:
            print(f"   📝 매칭할 신규 상품이 없습니다")
            return pd.DataFrame()

        # 경로 변수 초기화
        temp_csv_path: str | None = None
        translated_csv_path: str | None = None
        matched_csv_path: str | None = None
        try:
            print(f"   🔄 아이허브 매칭 시작: {len(new_products_df)}개 상품")

            # 입력 CSV를 임시 파일로 저장
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as temp_file:
                temp_csv_path = temp_file.name
                new_products_df.to_csv(temp_csv_path, index=False, encoding='utf-8-sig')

            # 번역 결과도 임시 파일로 저장
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as temp_translated_file:
                translated_csv_path = temp_translated_file.name

            # 출력 경로 설정: 사용자가 제공하면 해당 경로, 아니면 임시 파일 사용
            if output_path:
                matched_csv_path = output_path
            else:
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as temp_matched_file:
                    matched_csv_path = temp_matched_file.name

            # 2. 한국어 → 영어 번역
            print(f"   📝 번역 시작: {len(new_products_df)}개 상품명")
            self.translator = GeminiCSVTranslator(Config.GEMINI_API_KEY)

            _ = self.translator.translate_csv(
                input_file=temp_csv_path,
                output_file=translated_csv_path,
                column_name='product_name',
                batch_size=10
            )
            print(f"   ✅ 번역 완료")

            # 3. 아이허브 매칭 수행
            print(f"   🔄 아이허브 매칭 수행")
            print(f"   💡 기본 이미지 경로 자동 사용 (단순화)")

            # 스크래퍼 초기화
            self.iherb_scraper = EnglishIHerbScraper(
                headless=False,
                delay_range=(2, 4),
                max_products_to_compare=4
            )

            # 매칭 실행. 기존 결과가 있다면 auto_detect_start_point가 이어서 처리함.
            matched_csv = self.iherb_scraper.process_products_complete(
                csv_file_path=translated_csv_path,
                output_file_path=matched_csv_path,
                limit=None,
                start_from=None
            )

            # 매칭 결과 로드
            matched_df = pd.read_csv(matched_csv, encoding='utf-8-sig')

            # 성공 통계 출력
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

            return matched_df

        except KeyboardInterrupt:
            # 사용자 중단 시 현재까지 생성된 결과 반환
            print("\n사용자에 의해 중단되었습니다. 현재까지의 결과를 반환합니다.")
            if matched_csv_path and os.path.exists(matched_csv_path):
                try:
                    return pd.read_csv(matched_csv_path, encoding='utf-8-sig')
                except Exception:
                    pass
            return pd.DataFrame()
        except Exception as e:
            print(f"   ❌ 매칭 실패: {e}")
            import traceback
            print(f"   상세 오류: {traceback.format_exc()}")
            return pd.DataFrame()
        finally:
            # 임시 파일 정리 (단, 사용자가 지정한 출력 파일은 삭제하지 않음)
            for temp_path in [temp_csv_path, translated_csv_path]:
                if temp_path:
                    try:
                        os.unlink(temp_path)
                    except:
                        pass
            # output_path가 없을 때만 임시 매칭 결과 삭제
            if not output_path and matched_csv_path:
                try:
                    os.unlink(matched_csv_path)
                except:
                    pass

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
            matched_df = updater.match_iherb_products(test_df, output_path="test_matched_results.csv")
            print(f"매칭 결과: {len(matched_df)}개")

        print("\n✅ 테스트 완료")

    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
    finally:
        updater.close()