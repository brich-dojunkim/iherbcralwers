"""
GNC 제품 쿠팡 매칭 실행 스크립트
"""

import sys
import os
import argparse

# 현재 디렉토리
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# GNC 매칭 모듈
from gnc_matcher import GNCMatcher

# BrowserManager import
try:
    # 현재 디렉토리에서 프로젝트 루트 찾기
    # 251111, gnc 등 서브디렉토리에서 실행하는 경우를 고려
    project_root = os.path.dirname(current_dir)  # iherb_price
    coupang_dir = os.path.join(project_root, "coupang")
    
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    if coupang_dir not in sys.path:
        sys.path.insert(0, coupang_dir)
    
    from coupang_manager import BrowserManager
    print("✓ BrowserManager 로드 성공")
except ImportError as e:
    print(f"✗ BrowserManager 로드 실패: {e}")
    print(f"프로젝트 구조:")
    print(f"  현재 디렉토리: {current_dir}")
    print(f"  프로젝트 루트: {project_root}")
    print(f"  coupang 디렉토리: {coupang_dir}")
    print("coupang/coupang_manager.py 경로를 확인하세요")
    sys.exit(1)


def main():
    """메인 실행"""
    
    # 명령행 인자
    parser = argparse.ArgumentParser(description='GNC 제품 쿠팡 매칭')
    parser.add_argument('--xlsx', type=str,
                       default="/Users/brich/Desktop/iherb_price/251111/GNC_상품_리스트_외산.xlsx",
                       help='GNC 엑셀 파일 경로 (기본: 상위 디렉토리)')
    parser.add_argument('--output', type=str,
                       default="./outputs/gnc_matching_results.csv",
                       help='출력 CSV 파일 경로')
    parser.add_argument('--start', type=int, default=0,
                       help='시작 인덱스 (기본: 0)')
    parser.add_argument('--end', type=int, default=None,
                       help='종료 인덱스 (기본: 끝까지)')
    parser.add_argument('--headless', action='store_true',
                       help='헤드리스 모드')
    
    args = parser.parse_args()
    
    # outputs 디렉토리 생성
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # 파일 존재 확인
    if not os.path.exists(args.xlsx):
        print(f"✗ 엑셀 파일을 찾을 수 없습니다: {args.xlsx}")
        return
    
    # 브라우저 시작
    print("\n브라우저 시작 중...")
    browser = BrowserManager(headless=args.headless)
    
    if not browser.start_driver():
        print("✗ 브라우저 시작 실패")
        return
    
    try:
        # 매칭 시스템 초기화
        matcher = GNCMatcher(
            xlsx_path=args.xlsx,
            output_path=args.output,
            browser_manager=browser
        )
        
        # 데이터 로드
        if not matcher.load_data():
            return
        
        # 매칭 실행
        matcher.process_all(args.start, args.end)
        
        # 최종 통계
        matcher.print_summary()
        
        print(f"\n✓ 완료! 결과 파일: {args.output}")
        
    except KeyboardInterrupt:
        print("\n\n⚠ 사용자에 의해 중단되었습니다")
        print(f"현재까지 결과가 {args.output}에 저장되었습니다")
        print("다시 실행하면 이어서 진행됩니다")
        
    except Exception as e:
        print(f"\n✗ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 브라우저 종료
        print("\n브라우저 종료 중...")
        browser.close()


if __name__ == "__main__":
    main()