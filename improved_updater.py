"""
개선된 통합 파이프라인 스크립트
=================================

이 스크립트는 원래 루트 폴더의 ``updater.py`` 의 간단한 통합 로직을 개선한 버전입니다.  
주요 개선 사항은 다음과 같습니다.

* **업데이터 모듈 재사용** – `DataProcessor` 의 `classify_products()`, `update_existing_prices()` 및 `integrate_final_data()` 를 활용하여
  상품의 상태(신규/계속/사라짐)를 추적하고 가격 업데이트 시점을 기록합니다.
* **신규 상품만 매칭** – 업데이트 모드에서는 기존 상품의 번역 및 iHerb 매칭을 반복하지 않고,
  `ProductUpdater.match_iherb_products()` 를 통해 신규 상품에 대해서만 번역과 매칭을 수행합니다.
* **코드 중복 제거** – 크롤링, 번역, 매칭, 통합 등 핵심 로직을 각 모듈에 위임하여 유지보수를 용이하게 합니다.

실행 방법:

```bash
python improved_updater.py <쿠팡 검색 URL> [기존 CSV 경로] [출력 파일명]
```

* 첫 번째 인자: 쿠팡 검색 결과 URL.  
* 두 번째 인자(선택): 기존 매칭 결과 CSV 파일 경로. 제공하지 않으면 초기값 모드로 처리합니다.  
* 세 번째 인자(선택): 결과를 저장할 파일명. 제공하지 않으면 ``integrated_results_<타임스탬프>.csv`` 형식으로 생성합니다.
"""

import os
import sys
import pandas as pd
import tempfile
from datetime import datetime

# 모듈 경로 설정: 스크립트 위치 기준으로 coupang, iherbscraper, updater 폴더를 sys.path에 추가합니다.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(CURRENT_DIR, 'coupang'))
sys.path.insert(0, os.path.join(CURRENT_DIR, 'iherbscraper'))
sys.path.insert(0, os.path.join(CURRENT_DIR, 'updater'))

try:
    # 상품 업데이트 및 데이터 처리 모듈
    from updater.product_updater import ProductUpdater  # type: ignore
    from updater.data_processor import DataProcessor    # type: ignore
except Exception as e:
    print(f"업데이터 모듈 로드 실패: {e}")
    raise


def run_enhanced_pipeline(search_url: str, base_csv: str | None = None, output_file: str | None = None) -> str:
    """개선된 통합 파이프라인을 실행합니다.

    Args:
        search_url: 쿠팡 검색 URL.
        base_csv: 기존 매칭 결과 CSV 경로 (있으면 업데이트 모드).
        output_file: 출력 파일명. None인 경우 자동으로 생성됩니다.

    Returns:
        생성된 결과 CSV 파일 경로.
    """
    # 출력 파일명 설정
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"integrated_results_{timestamp}.csv"

    # ProductUpdater와 DataProcessor 초기화
    product_updater = ProductUpdater(enable_images=True)
    data_processor = DataProcessor()

    try:
        # 1단계: 쿠팡 데이터 크롤링
        print("\n1️⃣ 쿠팡 데이터 크롤링")
        crawled_df = product_updater.crawl_coupang_products(search_url)
        if len(crawled_df) == 0:
            print("❌ 크롤링된 상품이 없습니다.")
            return ""

        # 업데이트 모드 여부 결정
        is_update_mode = base_csv is not None and os.path.exists(base_csv)

        if is_update_mode:
            # 기존 CSV 로드
            print("\n2️⃣ 기존 데이터 로드 및 상품 분류")
            base_df = pd.read_csv(base_csv, encoding='utf-8-sig')
            # 기존/신규 상품 분류 및 상태 추적
            updated_existing_df, new_products_df = data_processor.classify_products(base_df, crawled_df)
            # 기존 상품 가격 업데이트
            print("\n3️⃣ 기존 상품 가격 업데이트")
            updated_base_df = data_processor.update_existing_prices(base_df, updated_existing_df)
            # 신규 상품 번역 및 iHerb 매칭
            print("\n4️⃣ 신규 상품 번역 및 iHerb 매칭")
            if len(new_products_df) > 0:
                # 매칭 결과를 재시도 가능하도록 고정된 경로에 저장
                # base_csv가 존재하면 해당 파일 이름을 기반으로 매칭 결과 파일명을 생성합니다.
                base_name, _ = os.path.splitext(os.path.basename(base_csv))
                match_output_path = os.path.join(os.path.dirname(base_csv), f"{base_name}_matched.csv")
                matched_new_df = product_updater.match_iherb_products(new_products_df, output_path=match_output_path)
            else:
                matched_new_df = pd.DataFrame()
                print("신규 매칭할 상품이 없습니다.")
            # 최종 데이터 통합
            print("\n5️⃣ 최종 데이터 통합")
            final_df = data_processor.integrate_final_data(updated_base_df, matched_new_df)
        else:
            # 초기값 모드: 모든 상품 번역 및 매칭
            print("\n2️⃣ 초기값 모드 – 번역 및 iHerb 매칭")
            matched_df = product_updater.match_iherb_products(crawled_df)
            final_df = matched_df.copy()

        # 결과 저장
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✅ 완료: 결과 파일 생성 – {output_file} (총 {len(final_df)}개 상품)")
        return output_file
    finally:
        # 리소스 정리
        product_updater.close()


def main() -> int:
    """명령줄 인터페이스: 프로그램을 실행합니다."""
    if len(sys.argv) < 2:
        print("🎯 개선된 통합 파이프라인 실행기")
        print("사용법: python improved_updater.py <쿠팡_URL> [기존_CSV] [출력_CSV]")
        return 1

    search_url = sys.argv[1]
    base_csv = sys.argv[2] if len(sys.argv) > 2 else None
    output_file = sys.argv[3] if len(sys.argv) > 3 else None

    try:
        result_csv = run_enhanced_pipeline(search_url, base_csv, output_file)
        if result_csv:
            print(f"\n🎉 완료! 결과 파일: {result_csv}")
            return 0
        else:
            print("\n💥 실패: 결과 파일이 생성되지 않았습니다.")
            return 1
    except KeyboardInterrupt:
        print("\n⚠️ 사용자 중단")
        return 1
    except Exception as e:
        print(f"\n❌ 실행 오류: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())