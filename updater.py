"""
통합 파이프라인 updater.py - 루트 폴더 위치
쿠팡/아이허브/업데이터 모듈을 직접 활용하여 완전한 파이프라인 구축
"""

import pandas as pd
import sys
import os
import tempfile
from datetime import datetime

# 세 모듈 경로 추가
current_dir = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(current_dir, 'coupang'))
sys.path.insert(0, os.path.join(current_dir, 'iherbscraper'))
sys.path.insert(0, os.path.join(current_dir, 'updater'))

# 모듈 import
try:
    # 쿠팡 모듈
    from crawler import CoupangCrawlerMacOS
    from translator import GeminiCSVTranslator
    COUPANG_AVAILABLE = True
except ImportError as e:
    print(f"쿠팡 모듈 로드 실패: {e}")
    COUPANG_AVAILABLE = False

try:
    # 아이허브 모듈
    from main import EnglishIHerbScraper
    from config import Config
    IHERB_AVAILABLE = True
except ImportError as e:
    print(f"아이허브 모듈 로드 실패: {e}")
    IHERB_AVAILABLE = False

try:
    # 업데이터 모듈 (데이터 처리 로직만)
    from data_processor import DataProcessor
    UPDATER_AVAILABLE = True
except ImportError as e:
    print(f"업데이터 모듈 로드 실패: {e}")
    UPDATER_AVAILABLE = False


def run_integrated_pipeline(search_url: str, base_csv: str = None, output_file: str = None) -> str:
    """
    통합 파이프라인 - 3개 모듈 직접 활용
    초기값 확보와 업데이트를 하나의 프로세스로 처리
    
    Args:
        search_url: 쿠팡 검색 URL
        base_csv: 기존 CSV (있으면 업데이트 모드)
        output_file: 출력 파일명
        
    Returns:
        결과 CSV 파일 경로
    """
    # 필수 모듈 체크
    if not all([COUPANG_AVAILABLE, IHERB_AVAILABLE, UPDATER_AVAILABLE]):
        print("필요한 모듈이 로드되지 않았습니다")
        return ""
    
    # 출력 파일명 생성
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"integrated_results_{timestamp}.csv"
    
    is_update_mode = base_csv and os.path.exists(base_csv)
    
    print(f"🚀 통합 파이프라인 실행")
    print(f"📍 쿠팡 URL: {search_url[:50]}...")
    print(f"📄 모드: {'업데이트' if is_update_mode else '초기값'}")
    if is_update_mode:
        print(f"📄 기존 파일: {base_csv}")
    print(f"📄 결과 파일: {output_file}")
    
    coupang_crawler = None
    iherb_scraper = None
    
    try:
        # ========================
        # 1단계: 쿠팡 크롤링
        # ========================
        print(f"\n1️⃣ 쿠팡 크롤링 (직접 모듈 활용)")
        coupang_crawler = CoupangCrawlerMacOS(
            headless=False, 
            download_images=True,
            delay_range=(3, 6)
        )
        
        products = coupang_crawler.crawl_all_pages(search_url)
        
        if not products:
            print("❌ 크롤링 결과 없음")
            return ""
        
        coupang_df = pd.DataFrame(products)
        print(f"✅ 쿠팡 크롤링 완료: {len(coupang_df)}개 상품")
        
        # ========================
        # 2단계: 번역
        # ========================
        print(f"\n2️⃣ 번역 (쿠팡 모듈)")
        
        # 임시 파일 생성
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as f:
            temp_coupang_path = f.name
            coupang_df.to_csv(temp_coupang_path, index=False, encoding='utf-8-sig')
        
        # 번역 수행
        translator = GeminiCSVTranslator(Config.GEMINI_API_KEY)
        translated_df = translator.translate_csv(
            input_file=temp_coupang_path,
            output_file=temp_coupang_path,
            column_name='product_name',
            batch_size=10
        )
        print(f"✅ 번역 완료")
        
        # ========================
        # 3단계: 아이허브 매칭
        # ========================
        print(f"\n3️⃣ 아이허브 매칭 (직접 모듈 활용)")
        
        # 임시 매칭 결과 파일
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as f:
            temp_matched_path = f.name
        
        # 아이허브 스크래퍼 실행
        iherb_scraper = EnglishIHerbScraper(
            headless=False,
            delay_range=(2, 4),
            max_products_to_compare=4
        )
        
        matched_csv = iherb_scraper.process_products_complete(
            csv_file_path=temp_coupang_path,
            output_file_path=temp_matched_path,
            limit=None,
            start_from=None
        )
        
        # 매칭 결과 로드
        if os.path.exists(temp_matched_path):
            matched_df = pd.read_csv(temp_matched_path, encoding='utf-8-sig')
            success_count = len(matched_df[matched_df.get('status', '') == 'success'])
            print(f"✅ 아이허브 매칭 완료: {success_count}/{len(matched_df)}개 성공")
        else:
            print("❌ 매칭 결과 파일 생성 실패")
            return ""
        
        # ========================
        # 4단계: 데이터 통합 (업데이터 모듈 활용)
        # ========================
        print(f"\n4️⃣ 데이터 통합 (업데이터 모듈)")
        
        if is_update_mode:
            # 업데이트 모드: 기존 데이터와 통합
            processor = DataProcessor()
            final_df = _integrate_with_existing_data(matched_df, base_csv, processor)
        else:
            # 초기값 모드: 그대로 사용
            final_df = matched_df
        
        # 최종 결과 저장
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        # 임시 파일 정리
        for temp_file in [temp_coupang_path, temp_matched_path]:
            try:
                os.unlink(temp_file)
            except:
                pass
        
        # 최종 결과
        final_success = len(final_df[final_df.get('status', '') == 'success'])
        print(f"✅ 통합 파이프라인 완료: {final_success}/{len(final_df)}개 최종 매칭")
        
        return output_file
        
    except Exception as e:
        print(f"❌ 파이프라인 실행 실패: {e}")
        import traceback
        print(f"상세 오류: {traceback.format_exc()}")
        return ""
    
    finally:
        # 리소스 정리
        if coupang_crawler:
            coupang_crawler.close()
        if iherb_scraper:
            iherb_scraper.close()


def _integrate_with_existing_data(new_df: pd.DataFrame, base_csv: str, processor) -> pd.DataFrame:
    """기존 데이터와 신규 데이터 통합 - 업데이터 모듈 로직 활용"""
    try:
        existing_df = pd.read_csv(base_csv, encoding='utf-8-sig')
        print(f"   기존 데이터: {len(existing_df)}개")
        
        # 상품 ID 기준 비교
        existing_ids = set(existing_df.get('coupang_product_id', []).astype(str))
        new_ids = set(new_df.get('coupang_product_id', []).astype(str))
        
        truly_new = new_ids - existing_ids
        continuing = new_ids & existing_ids
        missing = existing_ids - new_ids
        
        print(f"   신규: {len(truly_new)}, 계속: {len(continuing)}, 사라짐: {len(missing)}")
        
        # 업데이터 모듈의 데이터 통합 로직 활용
        # (여기서는 간단한 버전으로 구현, 필요시 processor의 메서드 활용 가능)
        
        # 기존 데이터 복사
        final_df = existing_df.copy()
        
        # 계속 판매되는 상품의 가격 업데이트
        for idx, row in final_df.iterrows():
            product_id = str(row.get('coupang_product_id', ''))
            if product_id in continuing:
                new_row = new_df[new_df['coupang_product_id'].astype(str) == product_id].iloc[0]
                final_df.at[idx, 'coupang_current_price_krw'] = new_row.get('coupang_current_price_krw', '')
                final_df.at[idx, 'coupang_discount_rate'] = new_row.get('coupang_discount_rate', '')
        
        # 신규 상품 추가
        if truly_new:
            new_products = new_df[new_df['coupang_product_id'].astype(str).isin(truly_new)]
            final_df = pd.concat([final_df, new_products], ignore_index=True)
        
        return final_df
        
    except Exception as e:
        print(f"   데이터 통합 실패: {e}, 신규 데이터만 사용")
        return new_df


def main():
    """메인 실행"""
    if len(sys.argv) < 2:
        print("🎯 통합 파이프라인 실행기")
        print("초기값: python updater.py <쿠팡_URL>")
        print("업데이트: python updater.py <쿠팡_URL> <기존_CSV>")
        print()
        print("예시:")
        print("  python updater.py 'https://www.coupang.com/np/search?q=thorne'")
        print("  python updater.py 'https://www.coupang.com/np/search?q=thorne' results.csv")
        return
    
    search_url = sys.argv[1]
    base_csv = sys.argv[2] if len(sys.argv) > 2 else None
    
    result = run_integrated_pipeline(search_url, base_csv)
    
    if result:
        print(f"\n🎉 완료: {result}")
    else:
        print(f"\n💥 실패")


if __name__ == "__main__":
    main()