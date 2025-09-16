"""
메인 실행 스크립트
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# 현재 디렉토리를 Python 경로에 추가
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from main_updater import CompleteEfficientUpdater
from settings import UPDATER_CONFIG


def main():
    """메인 실행 함수"""
    print("🚀 완전한 재시작 기능이 있는 효율적인 통합 가격 업데이터")
    print("="*60)
    print("🎯 주요 기능:")
    print("- 배치 번역으로 API 효율성 극대화 (90% 절약)")
    print("- 기존 모듈 재사용으로 코드 중복 제거")
    print("- 신규 상품만 선별 처리로 시간 단축")
    print("- 완전한 실시간 저장 및 재시작 기능")
    print("- 번역/매칭 단계별 정밀 재개 시스템")
    print("- 중단 지점부터 정확한 재시작")
    print("="*60)
    
    updater = CompleteEfficientUpdater(headless=False)
    
    try:
        # 실행 파라미터 설정
        input_file = "/Users/brich/Desktop/iherb_price/iherbscraper/output/nowfood_20250915.csv"  # 아이허브 매칭 완료된 파일
        brand = "NOW Foods"
        
        print(f"\n📋 설정:")
        print(f"   - 입력 파일: {input_file}")
        print(f"   - 브랜드: {brand}")
        print(f"   - 배치 크기: {UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']}")
        print(f"   - 체크포인트 간격: {UPDATER_CONFIG['CHECKPOINT_INTERVAL']}")
        
        # 입력 파일 존재 확인
        if not os.path.exists(input_file):
            print(f"❌ 입력 파일을 찾을 수 없습니다: {input_file}")
            print("\n사용 가능한 CSV 파일:")
            csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
            for i, csv_file in enumerate(csv_files[:10], 1):
                print(f"   {i}. {csv_file}")
            return
        
        # 브랜드 지원 확인
        if brand not in UPDATER_CONFIG['BRAND_SEARCH_URLS']:
            print(f"❌ 지원되지 않는 브랜드: {brand}")
            print("지원되는 브랜드:")
            for supported_brand in UPDATER_CONFIG['BRAND_SEARCH_URLS']:
                print(f"   - {supported_brand}")
            return
        
        print(f"\n🎯 작업 시작...")
        print(f"💡 Ctrl+C로 언제든 중단 가능 (재시작 시 중단 지점부터 계속)")
        
        # 메인 업데이트 실행
        result_file = updater.update_prices(
            input_file=input_file,
            brand_name=brand,
            fill_iherb=True  # 아이허브 매칭 활성화
        )
        
        print(f"\n🎉 완전한 효율적인 업데이트 완료!")
        print(f"📁 결과 파일: {result_file}")
        print(f"\n💡 달성된 효율성 개선:")
        print(f"   - 배치 번역: API 호출 {UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']}배 감소")
        print(f"   - 모듈 재사용: 검증된 기존 로직 활용")
        print(f"   - 선별 처리: 신규 상품만 집중 처리")
        print(f"   - 완전한 재시작: 중단 시점부터 정밀 재개")
        print(f"   - 안전 저장: {UPDATER_CONFIG['CHECKPOINT_INTERVAL']}개마다 체크포인트")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ 사용자 중단 감지")
        print(f"💾 현재 진행상황이 자동 저장되었습니다.")
        print(f"🔄 다시 실행하면 중단된 지점부터 정확히 재시작됩니다.")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        print(f"💾 현재까지의 진행상황은 저장되어 있습니다.")
        import traceback
        print("\n상세 오류:")
        traceback.print_exc()
    finally:
        print(f"\n🧹 리소스 정리 중...")
        updater.close()
        print("✅ 완료")


if __name__ == "__main__":
    main()