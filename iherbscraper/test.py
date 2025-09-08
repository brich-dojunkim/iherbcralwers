"""
main.py와 동일한 워크플로우로 테스트
"""

import pandas as pd
import tempfile
from main import EnglishIHerbScraper

# 실제 상품 정보로 테스트 데이터 생성 (main.py 입력 형식과 동일)
test_data = {
    'product_name': [
        'NOW Foods 아연 50mg 100정',
        'NOW Foods 리버 리프레쉬 90베지캡슐'
    ],
    'product_name_english': [
        'NOW Foods Zinc 50mg 100 Tablets',
        'NOW Foods Liver Refresh 90 Veg Capsules'
    ],
    'current_price': ['25000', '30000'],
    'original_price': ['28000', '35000'],
    'discount_rate': ['10%', '14%'],
    'product_url': ['https://coupang.com/test1', 'https://coupang.com/test2'],
    'product_id': ['TEST001', 'TEST002']
}

# 임시 CSV 파일 생성 (main.py가 읽는 형식과 동일)
df = pd.DataFrame(test_data)
temp_input_csv = 'test_input.csv'
temp_output_csv = 'test_output.csv'

df.to_csv(temp_input_csv, index=False, encoding='utf-8-sig')

print("테스트 데이터 생성 완료")
print(f"입력 파일: {temp_input_csv}")
print(f"출력 파일: {temp_output_csv}")
print("\n테스트 상품:")
for i, row in df.iterrows():
    print(f"  {i+1}. {row['product_name']} ({row['product_name_english']})")

# main.py의 EnglishIHerbScraper를 그대로 사용
scraper = None
try:
    scraper = EnglishIHerbScraper(headless=False)
    
    # main.py와 동일한 process_products_complete 메서드 사용
    # 검색→매칭→가격추출 전체 워크플로우 실행
    results = scraper.process_products_complete(
        csv_file_path=temp_input_csv,
        output_file_path=temp_output_csv,
        limit=2
    )
    
    print(f"\n✅ 테스트 완료!")
    print(f"결과 파일: {results}")
    
except KeyboardInterrupt:
    print("\n테스트 중단됨")
except Exception as e:
    print(f"테스트 중 오류: {e}")
finally:
    if scraper:
        scraper.close()