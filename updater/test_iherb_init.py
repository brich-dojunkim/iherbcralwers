"""
아이허브 스크래퍼 초기화 테스트 - 오류 진단용
"""

import sys
from pathlib import Path
import traceback

# 경로 설정
PROJECT_ROOT = Path(__file__).parent.parent
IHERB_PATH = PROJECT_ROOT / 'iherbscraper'
sys.path.insert(0, str(IHERB_PATH))

def test_iherb_imports():
    """아이허브 모듈 임포트 테스트"""
    print("1. 아이허브 모듈 임포트 테스트...")
    
    try:
        print("   - config 임포트...")
        from config import Config
        print("   ✅ config 임포트 성공")
        
        print("   - BrowserManager 임포트...")
        from browser_manager import BrowserManager
        print("   ✅ BrowserManager 임포트 성공")
        
        print("   - EnglishIHerbScraper 임포트...")
        from main import EnglishIHerbScraper
        print("   ✅ EnglishIHerbScraper 임포트 성공")
        
        return True
        
    except Exception as e:
        print(f"   ❌ 임포트 실패: {e}")
        traceback.print_exc()
        return False

def test_browser_manager_init():
    """BrowserManager 초기화 테스트"""
    print("\n2. BrowserManager 초기화 테스트...")
    
    try:
        from browser_manager import BrowserManager
        
        # 시그니처 확인
        import inspect
        sig = inspect.signature(BrowserManager.__init__)
        print(f"   BrowserManager.__init__ 시그니처: {sig}")
        
        # 실제 초기화 테스트
        print("   - BrowserManager(headless=True) 테스트...")
        browser = BrowserManager(headless=True)
        print("   ✅ BrowserManager 초기화 성공")
        
        # 리소스 정리
        try:
            browser.close()
        except:
            pass
        
        return True
        
    except Exception as e:
        print(f"   ❌ BrowserManager 초기화 실패: {e}")
        traceback.print_exc()
        return False

def test_english_iherb_scraper_init():
    """EnglishIHerbScraper 초기화 테스트"""
    print("\n3. EnglishIHerbScraper 초기화 테스트...")
    
    try:
        from main import EnglishIHerbScraper
        
        # 시그니처 확인
        import inspect
        sig = inspect.signature(EnglishIHerbScraper.__init__)
        print(f"   EnglishIHerbScraper.__init__ 시그니처: {sig}")
        
        # 실제 초기화 테스트 (단계별)
        print("   - headless=True만으로 초기화 테스트...")
        scraper = EnglishIHerbScraper(headless=True)
        print("   ✅ EnglishIHerbScraper 초기화 성공")
        
        # 리소스 정리
        try:
            scraper.close()
        except:
            pass
        
        return True
        
    except Exception as e:
        print(f"   ❌ EnglishIHerbScraper 초기화 실패: {e}")
        traceback.print_exc()
        return False

def test_full_initialization():
    """전체 초기화 과정 테스트"""
    print("\n4. 전체 초기화 과정 테스트 (updater 방식)...")
    
    try:
        from main import EnglishIHerbScraper
        
        # updater에서 사용하는 방식과 동일하게 테스트
        delay_range = (2, 4)
        max_products = 4
        
        print("   - EnglishIHerbScraper 생성 중...")
        scraper = EnglishIHerbScraper(
            headless=True,
            delay_range=delay_range,
            max_products_to_compare=max_products
        )
        print("   ✅ 전체 초기화 성공")
        
        # 리소스 정리
        try:
            scraper.close()
        except:
            pass
        
        return True
        
    except Exception as e:
        print(f"   ❌ 전체 초기화 실패: {e}")
        traceback.print_exc()
        return False

def main():
    """메인 테스트 실행"""
    print("🔍 아이허브 스크래퍼 초기화 진단 테스트")
    print("="*60)
    
    # 각 단계별 테스트
    tests = [
        test_iherb_imports,
        test_browser_manager_init,
        test_english_iherb_scraper_init,
        test_full_initialization
    ]
    
    results = []
    for test_func in tests:
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"   💥 테스트 함수 자체에서 오류: {e}")
            results.append(False)
    
    # 결과 요약
    print("\n" + "="*60)
    print("📊 테스트 결과 요약:")
    
    test_names = [
        "모듈 임포트",
        "BrowserManager 초기화", 
        "EnglishIHerbScraper 초기화",
        "전체 초기화 (updater 방식)"
    ]
    
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = "✅ 성공" if result else "❌ 실패"
        print(f"   {i+1}. {name}: {status}")
    
    # 전체 성공률
    success_count = sum(results)
    print(f"\n📈 전체 성공률: {success_count}/{len(results)} ({success_count/len(results)*100:.1f}%)")
    
    if success_count == len(results):
        print("🎉 모든 테스트 통과! updater에서 정상 작동할 것입니다.")
    else:
        print("⚠️ 일부 테스트 실패. 해당 부분을 수정해야 합니다.")
        print("\n💡 다음 단계:")
        if not results[0]:
            print("   - 아이허브 모듈 경로나 파일 구조 확인")
        if not results[1]:
            print("   - BrowserManager 클래스 정의 확인")
        if not results[2]:
            print("   - EnglishIHerbScraper 내부 로직 확인")
        if not results[3]:
            print("   - updater 매개변수 전달 방식 확인")

if __name__ == "__main__":
    main()