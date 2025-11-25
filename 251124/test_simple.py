"""
쿠팡 Access Denied 간단 테스트
5분 안에 작동 여부 확인
"""

try:
    import undetected_chromedriver as uc
    print("✓ undetected-chromedriver 설치됨")
except ImportError:
    print("✗ 먼저 설치하세요: pip install undetected-chromedriver")
    exit(1)

import time

print("\n브라우저 시작...")
driver = uc.Chrome(use_subprocess=False)

print("쿠팡 메인 페이지 접속...")
driver.get("https://www.coupang.com")
time.sleep(3)

# Access Denied 확인
if 'access denied' in driver.page_source.lower():
    print("✗ 메인 페이지 차단됨 → VPN 필요")
else:
    print("✓ 메인 페이지 성공")
    
    # 검색 시도
    print("\n검색 테스트 중...")
    try:
        search = driver.find_element("css selector", "input#headerSearchKeyword")
        search.send_keys("비타민C")
        search.send_keys("\n")
        time.sleep(5)
        
        if 'access denied' in driver.page_source.lower():
            print("✗ 검색 페이지 차단됨 → VPN 필요")
        else:
            products = driver.find_elements("css selector", "li[class*='product']")
            if products:
                print(f"✓✓✓ 성공! {len(products)}개 상품 발견 ✓✓✓")
            else:
                print("⚠ 상품 없음 (페이지 구조 변경?)")
    except Exception as e:
        print(f"✗ 오류: {e}")

print("\n10초 후 종료...")
time.sleep(10)
driver.quit()