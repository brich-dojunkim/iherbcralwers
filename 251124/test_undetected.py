"""
쿠팡 Access Denied 우회 테스트
undetected-chromedriver 동작 확인
"""

import sys
import os

# 프로젝트 루트 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

try:
    import undetected_chromedriver as uc
    print("✓ undetected-chromedriver 설치 확인")
except ImportError:
    print("✗ undetected-chromedriver 미설치")
    print("  다음 명령어로 설치하세요:")
    print("  pip install undetected-chromedriver")
    sys.exit(1)

import time
import random


def test_coupang_access():
    """쿠팡 접근 테스트"""
    
    print("\n" + "="*60)
    print("쿠팡 봇 차단 우회 테스트")
    print("="*60)
    
    driver = None
    
    try:
        # 1. 브라우저 초기화
        print("\n[1단계] undetected-chromedriver 초기화...")
        
        options = uc.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--window-size=1920,1080')
        
        driver = uc.Chrome(
            options=options,
            use_subprocess=False,
            version_main=None  # 자동 감지
        )
        
        print("  ✓ 브라우저 시작 성공")
        
        # 2. 쿠팡 메인 페이지 접속
        print("\n[2단계] 쿠팡 메인 페이지 접속...")
        driver.get("https://www.coupang.com")
        time.sleep(3)
        
        # Access Denied 확인
        page_source = driver.page_source.lower()
        if 'access denied' in page_source:
            print("  ✗ 메인 페이지에서 Access Denied 발생")
            print("  → IP가 블랙리스트에 있을 수 있습니다")
            print("  → VPN 또는 다른 네트워크 사용 권장")
            return False
        
        print("  ✓ 메인 페이지 접속 성공")
        print(f"  현재 URL: {driver.current_url}")
        
        # 3. 검색창 찾기
        print("\n[3단계] 검색창 확인...")
        
        search_input = None
        search_selectors = [
            "input.headerSearchKeyword",  # 실제 쿠팡 선택자
            "input#headerSearchKeyword",
            "input.search-input",
            "input[name='q']",
            "input[placeholder*='검색']"
        ]
        
        for selector in search_selectors:
            try:
                search_input = driver.find_element("css selector", selector)
                if search_input and search_input.is_displayed():
                    print(f"  ✓ 검색창 발견: {selector}")
                    print(f"    Placeholder: {search_input.get_attribute('placeholder')}")
                    break
            except:
                continue
        
        if not search_input:
            print(f"  ✗ 검색창을 찾을 수 없음")
            return False
        
        # 4. 검색 테스트
        print("\n[4단계] 검색 테스트...")
        test_query = "비타민C"
        
        print(f"  검색어 입력: {test_query}")
        search_input.clear()
        time.sleep(0.5)
        
        # 천천히 입력 (사람처럼)
        for char in test_query:
            search_input.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))
        
        print("  Enter 키 입력...")
        search_input.send_keys("\n")
        time.sleep(5)
        
        # 5. 검색 결과 확인
        print("\n[5단계] 검색 결과 확인...")
        
        current_url = driver.current_url
        print(f"  현재 URL: {current_url}")
        
        # Access Denied 확인
        page_source = driver.page_source.lower()
        if 'access denied' in page_source:
            print("\n  ✗✗✗ 검색 페이지에서 Access Denied 발생 ✗✗✗")
            print("  → undetected-chromedriver로도 차단됨")
            print("\n  추가 해결 방법:")
            print("  1. VPN 사용 (가장 효과적)")
            print("  2. 다른 네트워크 (모바일 핫스팟 등)")
            print("  3. Residential Proxy 사용")
            print("  4. 쿠팡 API 신청 (공식 방법)")
            return False
        
        # 상품 리스트 확인
        try:
            # 실제 쿠팡 상품 선택자
            products = driver.find_elements("css selector", "li.ProductUnit_productUnit__Qd6sv")
            
            if products:
                print(f"  ✓✓✓ 검색 성공! {len(products)}개 상품 발견 ✓✓✓")
                
                # 첫 번째 상품 정보 출력
                if len(products) > 0:
                    print("\n  [첫 번째 상품 샘플]")
                    first = products[0]
                    
                    try:
                        # 실제 쿠팡 상품명 선택자
                        name_elem = first.find_element("css selector", "div.ProductUnit_productNameV2__cV9cw")
                        name = name_elem.text.strip()
                        print(f"  상품명: {name[:50]}...")
                    except Exception as e:
                        print(f"  ⚠ 상품명 추출 실패: {e}")
                    
                    try:
                        # 실제 쿠팡 가격 선택자
                        price_area = first.find_element("css selector", "div.PriceArea_priceArea__NntJz")
                        price_text = price_area.text.strip()
                        # 가격 정보에서 첫 번째 숫자 추출
                        import re
                        price_match = re.search(r'([\d,]+)원', price_text)
                        if price_match:
                            price = price_match.group(1)
                            print(f"  가격: {price}원")
                    except Exception as e:
                        print(f"  ⚠ 가격 추출 실패: {e}")
                
                return True
            else:
                print("  ⚠ 상품 리스트를 찾을 수 없음")
                print("  → 페이지 구조가 변경되었을 수 있음")
                return False
                
        except Exception as e:
            print(f"  ⚠ 상품 파싱 오류: {e}")
            return False
        
    except Exception as e:
        print(f"\n✗ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        if driver:
            print("\n[종료] 브라우저 닫는 중...")
            time.sleep(2)  # 결과 확인 시간
            driver.quit()
            print("  ✓ 브라우저 종료")


def test_detection():
    """봇 감지 테스트"""
    
    print("\n" + "="*60)
    print("봇 감지 속성 테스트")
    print("="*60)
    
    driver = None
    
    try:
        options = uc.ChromeOptions()
        driver = uc.Chrome(options=options, use_subprocess=False)
        
        # JavaScript로 봇 감지 속성 확인
        script = """
        return {
            webdriver: navigator.webdriver,
            chrome: typeof window.chrome !== 'undefined',
            plugins: navigator.plugins.length,
            languages: navigator.languages,
            userAgent: navigator.userAgent
        };
        """
        
        result = driver.execute_script(script)
        
        print("\n브라우저 속성:")
        print(f"  navigator.webdriver: {result['webdriver']}")
        print(f"  window.chrome 존재: {result['chrome']}")
        print(f"  Plugins 개수: {result['plugins']}")
        print(f"  Languages: {result['languages']}")
        print(f"  User-Agent: {result['userAgent'][:80]}...")
        
        if result['webdriver'] is None or result['webdriver'] is False:
            print("\n  ✓✓✓ 봇 감지 우회 성공! ✓✓✓")
            print("  → navigator.webdriver가 undefined/false입니다")
        else:
            print("\n  ✗ 봇 감지 우회 실패")
            print("  → navigator.webdriver가 true입니다")
        
    except Exception as e:
        print(f"\n✗ 테스트 실패: {e}")
    
    finally:
        if driver:
            driver.quit()


def main():
    """메인 실행"""
    
    print("\n쿠팡 Access Denied 해결 테스트")
    print("="*60)
    
    # 1. 봇 감지 테스트
    test_detection()
    
    print("\n" + "="*60)
    input("\n쿠팡 접근 테스트를 시작하려면 Enter 키를 누르세요...")
    
    # 2. 쿠팡 접근 테스트
    success = test_coupang_access()
    
    # 결과
    print("\n" + "="*60)
    print("테스트 결과 요약")
    print("="*60)
    
    if success:
        print("✓✓✓ 성공! undetected-chromedriver가 작동합니다 ✓✓✓")
        print("\n다음 단계:")
        print("1. coupang_manager.py를 coupang_manager_undetected.py로 교체")
        print("2. python main.py 실행")
    else:
        print("✗ 실패: 추가 조치가 필요합니다")
        print("\n권장 해결책:")
        print("1. VPN 사용 (가장 효과적)")
        print("2. 다른 네트워크에서 시도")
        print("3. IP 변경 후 재시도")


if __name__ == '__main__':
    main()