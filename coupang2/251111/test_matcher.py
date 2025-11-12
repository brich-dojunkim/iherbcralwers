"""
간단한 테스트 스크립트 - 첫 3개 제품만 처리
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
from coupang.coupang_manager import BrowserManager

def test_matching():
    """3개 제품으로 테스트"""
    
    # CSV 파일 경로
    CSV_PATH = os.path.join(project_root, "251111", "iHerb_쿠팡 추가 가격조사_20251111.csv")
    
    # 없으면 다른 경로 시도
    if not os.path.exists(CSV_PATH):
        CSV_PATH = os.path.join(project_root, "iHerb_쿠팡 추가 가격조사_20251111.csv")
    
    # 출력 경로
    output_dir = os.path.join(current_dir, "outputs")
    os.makedirs(output_dir, exist_ok=True)
    OUTPUT_PATH = os.path.join(output_dir, "test_results.csv")
    
    if not os.path.exists(CSV_PATH):
        print(f"✗ CSV 파일을 찾을 수 없습니다: {CSV_PATH}")
        return
    
    print("\n" + "="*60)
    print("테스트 모드: 첫 3개 제품만 처리")
    print(f"CSV: {CSV_PATH}")
    print(f"출력: {OUTPUT_PATH}")
    print("="*60 + "\n")
    
    # 브라우저 시작
    browser = BrowserManager(headless=False)
    if not browser.start_driver():
        print("브라우저 시작 실패")
        return
    
    try:
        # 시스템 초기화
        system = ProductMatchingSystem(CSV_PATH, None, browser, OUTPUT_PATH)
        system.load_data()
        
        # 첫 3개만 처리
        system.process_all(start_idx=0, end_idx=3)
        
        print("\n" + "="*60)
        print("✓ 테스트 완료!")
        print(f"결과: {OUTPUT_PATH}")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        browser.close()

if __name__ == "__main__":
    test_matching()