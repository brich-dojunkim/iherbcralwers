"""
마스터 파일 시스템 실행 스크립트
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
    """마스터 파일 시스템 메인 실행 함수"""
    print("🚀 마스터 파일 시스템 - 통합 가격 업데이터")
    print("="*60)
    print("🎯 마스터 파일 시스템 주요 특징:")
    print("- 단일 파일 관리: master_BRAND.csv 고정 파일명")
    print("- 가격 히스토리: 날짜별 가격 변화 추적")
    print("- 효율적 업데이트: 신규 상품만 추가, 기존은 가격만 갱신")
    print("- 완전한 재시작: 중단 지점부터 정확한 재개")
    print("- 배치 번역: API 효율성 극대화")
    print("- 실시간 저장: 안전한 데이터 보호")
    print("="*60)
    
    updater = CompleteEfficientUpdater(headless=False)
    
    try:
        # 실행 파라미터 설정
        initial_file = "/Users/brich/Desktop/iherb_price/iherbscraper/output/doctorsbest_250915.csv"
        brand = "Doctors Best"
        
        print(f"\n📋 마스터 파일 시스템 설정:")
        print(f"   - 초기 데이터: {initial_file}")
        print(f"   - 브랜드: {brand}")
        print(f"   - 마스터 파일: master_{brand.replace(' ', '_')}.csv")
        print(f"   - 배치 크기: {UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']}")
        print(f"   - 체크포인트 간격: {UPDATER_CONFIG['CHECKPOINT_INTERVAL']}")
        
        # 초기 파일 존재 확인
        if not os.path.exists(initial_file):
            print(f"\n❌ 초기 데이터 파일을 찾을 수 없습니다: {initial_file}")
            print("\n사용 가능한 CSV 파일:")
            
            # 현재 디렉토리와 상위 디렉토리에서 CSV 파일 찾기
            search_dirs = ['.', '..', '../iherbscraper/output']
            csv_files = []
            
            for search_dir in search_dirs:
                if os.path.exists(search_dir):
                    for file in os.listdir(search_dir):
                        if file.endswith('.csv'):
                            csv_files.append(os.path.join(search_dir, file))
            
            for i, csv_file in enumerate(csv_files[:10], 1):
                print(f"   {i}. {csv_file}")
            
            if csv_files:
                print(f"\n💡 위 파일 중 하나를 initial_file로 설정하여 다시 실행하세요.")
            
            return
        
        # 브랜드 지원 확인
        if brand not in UPDATER_CONFIG['BRAND_SEARCH_URLS']:
            print(f"\n❌ 지원되지 않는 브랜드: {brand}")
            print("지원되는 브랜드:")
            for supported_brand in UPDATER_CONFIG['BRAND_SEARCH_URLS']:
                print(f"   - {supported_brand}")
            return
        
        # 마스터 파일 상태 확인
        master_file = f"master_{brand.replace(' ', '_')}.csv"
        if os.path.exists(master_file):
            print(f"\n📂 기존 마스터 파일 발견: {master_file}")
            print(f"   - 기존 데이터를 기반으로 업데이트를 진행합니다.")
            print(f"   - 새로운 가격 정보가 날짜별 컬럼으로 추가됩니다.")
        else:
            print(f"\n🆕 새 마스터 파일 생성 예정: {master_file}")
            print(f"   - 초기 데이터로부터 마스터 파일을 생성합니다.")
            print(f"   - 이후 모든 업데이트는 이 파일을 기준으로 진행됩니다.")
        
        print(f"\n🎯 마스터 파일 시스템 시작...")
        print(f"💡 Ctrl+C로 언제든 중단 가능 (재시작 시 중단 지점부터 계속)")
        print(f"📊 오늘({datetime.now().strftime('%Y%m%d')}) 날짜로 가격 히스토리가 추가됩니다.")
        
        # 메인 업데이트 실행
        result_file = updater.update_prices(
            initial_file=initial_file,
            brand_name=brand,
            fill_iherb=True  # 아이허브 매칭 활성화
        )
        
        print(f"\n🎉 마스터 파일 시스템 업데이트 완료!")
        print(f"📁 마스터 파일: {result_file}")
        
        # 마스터 파일 시스템 장점 요약
        print(f"\n💡 달성된 마스터 파일 시스템 장점:")
        print(f"   - 단일 파일 관리: 모든 데이터가 {result_file}에 통합")
        print(f"   - 가격 히스토리: 날짜별 가격 변화 추적 가능")
        print(f"   - 효율적 업데이트: 신규 상품만 추가, 기존 상품은 가격만 갱신")
        print(f"   - 배치 번역: API 호출 {UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']}배 효율화")
        print(f"   - 완전한 재시작: 중단 시점부터 정밀 재개")
        print(f"   - 안전 저장: {UPDATER_CONFIG['CHECKPOINT_INTERVAL']}개마다 체크포인트")
        
        print(f"\n📈 다음 실행 시:")
        print(f"   - 동일한 {result_file} 파일이 입력으로 사용됩니다.")
        print(f"   - 새로운 날짜 컬럼이 추가되어 가격 변화를 추적할 수 있습니다.")
        print(f"   - 신규 상품만 발견하여 처리하므로 시간이 단축됩니다.")
        
        # 파일 정보 표시
        if os.path.exists(result_file):
            import pandas as pd
            try:
                df = pd.read_csv(result_file, encoding='utf-8-sig')
                price_columns = [col for col in df.columns if col.startswith('쿠팡현재가격_')]
                iherb_columns = [col for col in df.columns if col.startswith('아이허브할인가_')]
                
                print(f"\n📊 마스터 파일 현재 상태:")
                print(f"   - 총 상품: {len(df)}개")
                print(f"   - 쿠팡 가격 히스토리: {len(price_columns)}일")
                print(f"   - 아이허브 가격 히스토리: {len(iherb_columns)}일")
                
                if len(price_columns) > 1:
                    print(f"   - 가격 추적 기간: {price_columns[0]} ~ {price_columns[-1]}")
                
            except Exception as e:
                print(f"   ℹ️ 파일 정보 읽기 실패: {e}")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ 사용자 중단 감지")
        print(f"💾 현재 진행상황이 마스터 파일에 자동 저장되었습니다.")
        print(f"🔄 다시 실행하면 중단된 지점부터 정확히 재시작됩니다.")
        print(f"📁 마스터 파일: master_{brand.replace(' ', '_')}.csv")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        print(f"💾 현재까지의 진행상황은 마스터 파일에 저장되어 있습니다.")
        
        # 상세 오류 정보
        import traceback
        print("\n🔍 상세 오류:")
        traceback.print_exc()
        
        # 복구 안내
        master_file = f"master_{brand.replace(' ', '_')}.csv"
        if os.path.exists(master_file):
            print(f"\n🔧 복구 방법:")
            print(f"   1. {master_file} 파일이 손상되지 않았는지 확인")
            print(f"   2. 스크립트를 다시 실행하여 중단된 지점부터 재시작")
            print(f"   3. 문제가 지속되면 백업 파일에서 복원")
        
    finally:
        print(f"\n🧹 리소스 정리 중...")
        updater.close()
        print("✅ 완료")


def show_master_file_info():
    """마스터 파일 정보 표시 (디버깅용)"""
    print("📊 마스터 파일 정보 확인")
    print("="*40)
    
    # 현재 디렉토리의 마스터 파일들 찾기
    master_files = [f for f in os.listdir('.') if f.startswith('master_') and f.endswith('.csv')]
    
    if not master_files:
        print("📭 마스터 파일이 없습니다.")
        return
    
    for master_file in master_files:
        print(f"\n📁 {master_file}:")
        try:
            import pandas as pd
            df = pd.read_csv(master_file, encoding='utf-8-sig')
            
            # 기본 정보
            print(f"   - 총 상품: {len(df)}개")
            
            # 가격 히스토리
            price_columns = [col for col in df.columns if col.startswith('쿠팡현재가격_')]
            iherb_columns = [col for col in df.columns if col.startswith('아이허브할인가_')]
            
            print(f"   - 쿠팡 가격 히스토리: {len(price_columns)}일")
            print(f"   - 아이허브 가격 히스토리: {len(iherb_columns)}일")
            
            if price_columns:
                print(f"   - 가격 추적 기간: {price_columns[0]} ~ {price_columns[-1]}")
            
            # 매칭 상태
            if 'status' in df.columns:
                success_count = len(df[df['status'] == 'success'])
                print(f"   - 아이허브 매칭 성공: {success_count}개 ({success_count/len(df)*100:.1f}%)")
            
            # 파일 크기
            file_size = os.path.getsize(master_file) / 1024 / 1024  # MB
            print(f"   - 파일 크기: {file_size:.1f}MB")
            
            # 마지막 업데이트
            if 'last_updated' in df.columns:
                last_updated = df['last_updated'].dropna()
                if len(last_updated) > 0:
                    latest_update = last_updated.iloc[-1]
                    print(f"   - 마지막 업데이트: {latest_update}")
            
        except Exception as e:
            print(f"   ❌ 파일 읽기 실패: {e}")


if __name__ == "__main__":
    # 명령행 인수 확인
    if len(sys.argv) > 1 and sys.argv[1] == '--info':
        show_master_file_info()
    else:
        main()