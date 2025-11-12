"""
쿠팡 제품 매칭 시스템 실행 스크립트
"""

import sys
import os

# 프로젝트 루트를 sys.path에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # iherb_price 디렉토리

# 현재 디렉토리를 맨 앞에 추가 (로컬 config.py 우선)
sys.path.insert(0, current_dir)
sys.path.insert(1, project_root)

# config.py 직접 로드
try:
    import importlib.util
    config_path = os.path.join(current_dir, 'config.py')
    spec = importlib.util.spec_from_file_location("matcher_config", config_path)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
except Exception as e:
    print(f"✗ config.py 로드 실패: {e}")
    sys.exit(1)

from product_matcher import ProductMatchingSystem

# coupang_manager import
try:
    from coupang.coupang_manager import BrowserManager
    print("✓ BrowserManager 로드 성공")
except ImportError as e:
    print(f"✗ BrowserManager 로드 실패: {e}")
    print("coupang/coupang_manager.py가 있는지 확인하세요")
    sys.exit(1)


def main():
    """메인 실행 함수"""
    
    # 설정 - 상대 경로로 변경
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    
    # CSV 파일 경로 (coupang2/251111/ 또는 프로젝트 루트에 있을 것으로 예상)
    CSV_PATH = "/Users/brich/Desktop/iherb_price/coupang2/251111/iHerb_쿠팡_추가_가격조사_대조결과_20251112.csv"
    
    # 출력 경로
    output_dir = os.path.join(current_dir, "outputs")
    os.makedirs(output_dir, exist_ok=True)
    OUTPUT_PATH = os.path.join(output_dir, "matching_results.csv")
    
    # 처리 범위 설정
    START_IDX = 0
    END_IDX = None  # None이면 끝까지 전부 처리
    
    print("\n" + "="*60)
    print("쿠팡 제품 자동 매칭 시스템")
    print("="*60)
    print(f"CSV: {CSV_PATH}")
    print(f"출력: {OUTPUT_PATH}")
    if END_IDX:
        print(f"처리 범위: {START_IDX} ~ {END_IDX}")
    else:
        print(f"처리 범위: {START_IDX} ~ 끝까지")
    print("="*60 + "\n")
    
    # CSV 파일 존재 확인
    if not os.path.exists(CSV_PATH):
        print(f"✗ CSV 파일을 찾을 수 없습니다: {CSV_PATH}")
        print("\n다음 경로에 CSV 파일을 배치하세요:")
        print(f"  - {os.path.join(project_root, '251111', 'iHerb_쿠팡 추가 가격조사_20251111.csv')}")
        print(f"  - {os.path.join(project_root, 'iHerb_쿠팡 추가 가격조사_20251111.csv')}")
        return
    
    # 브라우저 시작
    print("1. 브라우저 시작 중...")
    browser = BrowserManager(headless=False)  # GUI 모드로 실행 (디버깅용)
    
    if not browser.start_driver():
        print("✗ 브라우저 시작 실패")
        return
    
    try:
        # 시스템 초기화
        print("\n2. 시스템 초기화 중...")
        system = ProductMatchingSystem(CSV_PATH, None, browser, OUTPUT_PATH)
        
        # 데이터 로드
        print("\n3. 데이터 로드 중...")
        if not system.load_data():
            return
        
        # 처리 시작 (자동으로 기존 결과 로드하여 이어서 진행)
        print("\n4. 제품 매칭 시작...")
        system.process_all(start_idx=START_IDX, end_idx=END_IDX)
        
        print("\n" + "="*60)
        print("✓ 모든 처리 완료!")
        print(f"결과 파일: {OUTPUT_PATH}")
        print(f"총 처리: {len(system.results)}개")
        print("="*60 + "\n")
        
    except KeyboardInterrupt:
        print("\n\n⚠ 사용자에 의해 중단되었습니다")
        print(f"현재까지 결과가 {OUTPUT_PATH}에 저장되었습니다")
        print("다시 실행하면 이어서 진행됩니다")
    except Exception as e:
        print(f"\n✗ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 브라우저 종료
        print("\n5. 브라우저 종료 중...")
        browser.close()


if __name__ == "__main__":
    main()