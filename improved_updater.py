"""
간소화된 통합 파이프라인 - 핵심 기능만 구현
- outputs 폴더 사용
- 중단 지점부터 재시작 
- 실시간 저장
- 브랜드 기반 자동 감지 및 상태 관리
"""

import os
import sys
import pandas as pd
from datetime import datetime
from config import PathConfig, validate_config

# 모듈 경로 설정
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(CURRENT_DIR, 'coupang'))
sys.path.insert(0, os.path.join(CURRENT_DIR, 'iherbscraper'))
sys.path.insert(0, os.path.join(CURRENT_DIR, 'updater'))

try:
    from updater.product_updater import ProductUpdater
    from updater.data_processor import DataProcessor
except Exception as e:
    print(f"업데이터 모듈 로드 실패: {e}")
    raise

def run_pipeline(search_url: str, base_csv: str = None, brand_name: str = None) -> str:
    # 설정 검증 추가
    validate_config()
    
    # 브랜드명 자동 추출 (URL에서)
    if brand_name is None:
        brand_name = PathConfig.extract_brand_from_url(search_url)
    
    print(f"📋 브랜드: {brand_name}")
    
    # 베이스 CSV 자동 감지
    if base_csv is None:
        # 1순위: 완성된 데이터
        base_csv = PathConfig.find_latest_completed_data(brand_name)
        if base_csv:
            print(f"   기존 완성 데이터 발견: {os.path.basename(base_csv)}")
        else:
            # 2순위: 부분 완성 데이터
            base_csv = PathConfig.find_partial_data(brand_name)
            if base_csv:
                print(f"   부분 완성 데이터 발견: {os.path.basename(base_csv)}")
            else:
                print(f"   기존 데이터 없음 - 신규 생성 모드")
    else:
        print(f"   수동 지정 베이스: {os.path.basename(base_csv)}")
    
    # outputs 디렉토리 생성
    outputs_dir = PathConfig.UNIFIED_OUTPUTS_DIR
    os.makedirs(outputs_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 브랜드명 포함한 파일 경로 정의
    coupang_csv = os.path.join(PathConfig.UNIFIED_OUTPUTS_DIR, f"coupang_{brand_name}_{timestamp}.csv")
    final_csv = os.path.join(PathConfig.UNIFIED_OUTPUTS_DIR, f"final_{brand_name}_{timestamp}_partial.csv")
    
    product_updater = ProductUpdater(enable_images=True)
    data_processor = DataProcessor()
    
    try:
        # 업데이트 모드 여부 확인
        if base_csv and os.path.exists(base_csv):
            print(f"📋 업데이트 모드")
            print(f"   기존 파일: {base_csv}")
            
            # 기존 데이터 로드
            base_df = pd.read_csv(base_csv, encoding='utf-8-sig')
            
            # 1. 쿠팡 크롤링 (항상 수행)
            print(f"\n1️⃣ 쿠팡 최신 데이터 크롤링")
            crawled_df = product_updater.crawl_coupang_products(search_url)
            
            if len(crawled_df) == 0:
                print("❌ 크롤링 실패")
                return ""
            
            # 쿠팡 크롤링 결과 저장 (재사용을 위해)
            crawled_df.to_csv(coupang_csv, index=False, encoding='utf-8-sig')
            print(f"   💾 크롤링 결과 저장: {coupang_csv}")
            
            # 2. 상품 분류
            print(f"\n2️⃣ 상품 분류 (기존 vs 신규)")
            updated_base_df, new_df = data_processor.classify_products(base_df, crawled_df)
            
            # 3. 신규 상품만 매칭 (핵심 개선)
            if len(new_df) > 0:
                print(f"\n3️⃣ 신규 상품 아이허브 매칭 ({len(new_df)}개)")
                # outputs 폴더에 실시간 저장되도록 경로 지정
                match_output = os.path.join(outputs_dir, f"matched_new_{brand_name}_{timestamp}.csv")
                matched_df = product_updater.match_iherb_products(new_df, output_path=match_output, brand_name=brand_name)
            else:
                print(f"\n3️⃣ 신규 상품 없음")
                matched_df = pd.DataFrame()
            
            # 4. 최종 통합
            print(f"\n4️⃣ 최종 데이터 통합")
            final_df = data_processor.integrate_final_data(updated_base_df, matched_df, brand_name)
            
        else:
            print(f"📋 초기 모드 (전체 매칭)")
            
            # 1. 쿠팡 크롤링
            print(f"\n1️⃣ 쿠팡 데이터 크롤링")
            crawled_df = product_updater.crawl_coupang_products(search_url)
            
            if len(crawled_df) == 0:
                return ""
                
            # 크롤링 결과 저장
            crawled_df.to_csv(coupang_csv, index=False, encoding='utf-8-sig')
            print(f"   💾 크롤링 결과 저장: {coupang_csv}")
            
            # 2. 전체 매칭 (outputs 폴더에 실시간 저장)
            print(f"\n2️⃣ 전체 상품 아이허브 매칭")
            match_output = os.path.join(outputs_dir, f"matched_all_{brand_name}_{timestamp}.csv")
            final_df = product_updater.match_iherb_products(crawled_df, output_path=match_output, brand_name=brand_name)

            # 메타데이터 추가 (초기 모드)
            if len(final_df) > 0:
                final_df['data_brand'] = brand_name
                final_df['completion_status'] = 'completed'
                final_df['last_updated'] = timestamp
        
        # 최종 결과 저장
        final_df.to_csv(final_csv, index=False, encoding='utf-8-sig')
        
        # 완료 상태로 변경
        completed_csv = PathConfig.mark_file_completed(final_csv)
        if completed_csv:
            print(f"\n✅ 완료: {os.path.basename(completed_csv)} (총 {len(final_df)}개)")
            return completed_csv
        else:
            print(f"\n✅ 완료: {os.path.basename(final_csv)} (총 {len(final_df)}개)")
            return final_csv
        
    except Exception as e:
        print(f"❌ 오류: {e}")
        return ""
    finally:
        product_updater.close()


def resume_iherb_matching(coupang_csv_path: str) -> str:
    """
    아이허브 매칭만 재시작하는 함수
    
    사용법:
    python improved_updater.py --resume outputs/coupang_crawled_20250924.csv
    """
    if not os.path.exists(coupang_csv_path):
        print(f"❌ 파일이 없습니다: {coupang_csv_path}")
        return ""
    
    print(f"🔄 아이허브 매칭 재시작")
    print(f"   쿠팡 데이터: {coupang_csv_path}")
    
    # 쿠팡 데이터 로드
    crawled_df = pd.read_csv(coupang_csv_path, encoding='utf-8-sig')
    print(f"   상품 수: {len(crawled_df)}개")
    
    # outputs 폴더에 결과 저장
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    match_output = os.path.join(PathConfig.UNIFIED_OUTPUTS_DIR, f"resumed_matching_{timestamp}.csv")
    
    product_updater = ProductUpdater(enable_images=True)
    try:
        # 아이허브 매칭만 수행 (자동 재시작 지원)
        matched_df = product_updater.match_iherb_products(crawled_df, output_path=match_output)
        print(f"✅ 매칭 완료: {match_output}")
        return match_output
    finally:
        product_updater.close()


def main():
    """명령줄 실행"""
    if len(sys.argv) < 2:
        print("사용법:")
        print("  새로운 실행: python improved_updater.py <쿠팡_URL> [기존_CSV]")
        print("  매칭 재시작: python improved_updater.py --resume <쿠팡_크롤링_CSV>")
        return 1
    
    if sys.argv[1] == "--resume":
        if len(sys.argv) < 3:
            print("❌ 재시작할 쿠팡 CSV 파일 경로가 필요합니다")
            return 1
        result = resume_iherb_matching(sys.argv[2])
    else:
        search_url = sys.argv[1]
        base_csv = sys.argv[2] if len(sys.argv) > 2 else None
        result = run_pipeline(search_url, base_csv)
    
    if result:
        print(f"\n🎉 완료: {result}")
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())