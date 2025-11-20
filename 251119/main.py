"""
식품안전나라와 아이허브 제품 이미지 비교 시스템 (Selenium 버전)
"""

import os
import pandas as pd
import time
from typing import List, Dict, Optional, Set
import json
from pathlib import Path
import google.generativeai as genai
from PIL import Image
import io
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

class ProductImageComparator:
    def __init__(self, gemini_api_key: str, headless: bool = False):
        """
        초기화
        Args:
            gemini_api_key: Google Gemini API 키
            headless: False면 브라우저 표시
        """
        self.gemini_api_key = gemini_api_key
        genai.configure(api_key=gemini_api_key)
        self.model = genai.GenerativeModel('gemini-2.5-flash')
        
        # Selenium 설정 - 봇 탐지 우회
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # User-Agent 설정
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = webdriver.Chrome(options=chrome_options)
        
        # 봇 탐지 우회 스크립트
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['ko-KR', 'ko', 'en-US', 'en']
                });
            '''
        })
        
        # 기본 URL
        self.foodsafety_base_url = "https://www.foodsafetykorea.go.kr"
        self.iherb_base_url = "https://kr.iherb.com"
        
        # 결과 저장 디렉토리
        self.output_dir = Path("./comparison_results")
        self.output_dir.mkdir(exist_ok=True)
        
        # 진행 상태 파일
        self.progress_file = self.output_dir / "progress.json"
        self.csv_output = self.output_dir / "comparison_results.csv"
        
    def close_browser(self):
        """브라우저 수동 종료"""
        if hasattr(self, 'driver'):
            self.driver.quit()
            print("✓ 브라우저 종료")
    
    def load_progress(self) -> Set[str]:
        """처리 완료된 제품 코드 목록 로드"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                data = json.load(f)
                return set(data.get('completed_partnos', []))
        return set()
    
    def save_progress(self, completed_partnos: Set[str]):
        """진행 상태 저장"""
        with open(self.progress_file, 'w') as f:
            json.dump({'completed_partnos': list(completed_partnos)}, f)
    
    def clear_progress(self):
        """진행 상태 초기화"""
        if self.progress_file.exists():
            self.progress_file.unlink()
        print("✓ 진행 상태 초기화")
        
    def load_csv_data(self, csv_path: str) -> pd.DataFrame:
        """CSV 파일 로드"""
        df = pd.read_csv(csv_path, encoding='utf-8')
        print(f"✓ CSV 로드 완료: {len(df)} 건")
        return df
    
    def search_foodsafety_product(self, product_name: str) -> Optional[Dict]:
        """
        식품안전나라에서 제품 검색 (Selenium 사용)
        """
        try:
            # 검색 페이지 접속
            search_url = "https://www.foodsafetykorea.go.kr/portal/fooddanger/foodDirectImportBlock.do?menu_grp=MENU_NEW02&menu_no=3594"
            self.driver.get(search_url)
            
            # 검색어 입력
            wait = WebDriverWait(self.driver, 10)
            search_input = wait.until(EC.presence_of_element_located((By.ID, "search_keyword")))
            search_input.clear()
            search_input.send_keys(product_name)
            
            # 검색 실행
            search_btn = self.driver.find_element(By.CSS_SELECTOR, "a.btn.btn-default[href='javascript:setDefault();']")
            search_btn.click()
            
            time.sleep(2)
            
            # 첫 번째 결과 클릭
            try:
                first_result = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#listFrame tr a")))
                first_result.click()
                
                time.sleep(2)
                
                # 페이지 끝까지 스크롤
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                while True:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                
                # 다시 위로 스크롤하여 모든 요소 로드 확인
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                
                # 이미지 URL 추출
                image_elements = self.driver.find_elements(By.CSS_SELECTOR, "img[alt*='위해식품']")
                image_urls = []
                for img in image_elements:
                    img_src = img.get_attribute('src')
                    if img_src and 'commonfileView.do' in img_src:
                        if img_src not in image_urls:  # 중복 제거
                            image_urls.append(img_src)
                
                detail_url = self.driver.current_url
                
                print(f"  ✓ 식품안전나라: {len(image_urls)}개 이미지 발견")
                
                return {
                    'product_name': product_name,
                    'detail_url': detail_url,
                    'image_urls': image_urls
                }
                
            except Exception as e:
                print(f"  ✗ 식품안전나라에서 '{product_name}' 검색 결과 없음")
                return None
                
        except Exception as e:
            print(f"  ✗ 식품안전나라 검색 오류: {str(e)}")
            return None
    
    def get_iherb_product(self, product_partno: str) -> Optional[Dict]:
        """
        아이허브에서 제품 정보 가져오기 (Selenium 사용)
        """
        try:
            product_url = f"{self.iherb_base_url}/pr/{product_partno}"
            self.driver.get(product_url)
            
            time.sleep(2)
            
            # 제품 이미지 URL 추출
            image_urls = []
            
            # 메인 이미지
            try:
                main_img = self.driver.find_element(By.ID, "iherb-product-image")
                img_src = main_img.get_attribute('src')
                if img_src:
                    image_urls.append(img_src)
            except:
                pass
            
            # 썸네일 이미지들
            thumbnails = self.driver.find_elements(By.CSS_SELECTOR, ".thumbnail-item img")
            for thumb in thumbnails[:5]:
                img_src = thumb.get_attribute('src') or thumb.get_attribute('data-lazyload')
                if img_src and img_src not in image_urls:
                    # 더 큰 이미지 URL로 변경
                    img_src = img_src.replace('/s/', '/v/').replace('/r/', '/v/')
                    image_urls.append(img_src)
            
            print(f"  ✓ 아이허브: {len(image_urls)}개 이미지 발견")
            
            return {
                'product_partno': product_partno,
                'product_url': product_url,
                'image_urls': image_urls
            }
            
        except Exception as e:
            print(f"  ✗ 아이허브 접근 오류: {str(e)}")
            return None
    
    def download_image(self, url: str) -> Optional[bytes]:
        """이미지 다운로드"""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"    이미지 다운로드 실패 ({url}): {str(e)}")
            return None
    
    def compare_images_with_gemini(self, foodsafety_images: List[str], 
                                   iherb_images: List[str]) -> Dict:
        """Gemini API를 사용하여 이미지 비교"""
        try:
            # 이미지 다운로드
            fs_image_data = []
            for url in foodsafety_images[:3]:
                img_bytes = self.download_image(url)
                if img_bytes:
                    fs_image_data.append(Image.open(io.BytesIO(img_bytes)))
            
            ih_image_data = []
            for url in iherb_images[:3]:
                img_bytes = self.download_image(url)
                if img_bytes:
                    ih_image_data.append(Image.open(io.BytesIO(img_bytes)))
            
            if not fs_image_data or not ih_image_data:
                return {
                    'match': 'unknown',
                    'confidence': 0,
                    'reason': '이미지 다운로드 실패'
                }
            
            # Gemini 프롬프트
            prompt = """
다음 두 그룹의 이미지를 비교하여 동일한 제품인지 판단해주세요.

첫 번째 그룹: 식품안전나라에서 가져온 위해식품 이미지
두 번째 그룹: 아이허브 온라인 쇼핑몰의 제품 이미지

다음 기준으로 판단해주세요:
1. 제품명, 브랜드명이 일치하는가?
2. 제품 패키징, 라벨 디자인이 유사한가?
3. 제품 용량, 정보가 일치하는가?

응답은 반드시 다음 JSON 형식으로만 작성해주세요:
{
    "match": "yes" 또는 "no" 또는 "uncertain",
    "confidence": 0-100 사이의 숫자,
    "reason": "판단 근거를 한국어로 간단히 설명"
}
"""
            
            # 이미지 결합하여 Gemini에 전송
            content = [prompt]
            content.extend(fs_image_data)
            content.extend(ih_image_data)
            
            response = self.model.generate_content(content)
            result_text = response.text.strip()
            
            # JSON 추출
            import re
            json_match = re.search(r'\{[^}]+\}', result_text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
            else:
                result = {
                    'match': 'uncertain',
                    'confidence': 50,
                    'reason': result_text[:200]
                }
            
            print(f"  ✓ Gemini 분석 완료: {result['match']} (신뢰도: {result['confidence']}%)")
            print(f"     근거: {result['reason']}")
            
            return result
            
        except Exception as e:
            print(f"  ✗ Gemini 비교 오류: {str(e)}")
            return {
                'match': 'error',
                'confidence': 0,
                'reason': str(e)
            }
    
    def process_row(self, row: pd.Series) -> Dict:
        """CSV의 한 행을 처리"""
        result = {
            'product_partno': row['product_partno'],
            'product_id': row['product_id'],
            '위해식품_제품명': row['위해식품_제품명'],
            '아이허브_제품명': row['아이허브_제품명'],
            'foodsafety_data': None,
            'iherb_data': None,
            'comparison_result': None,
            'status': 'pending'
        }
        
        print(f"\n[{row['product_id']}] {row['위해식품_제품명']}")
        
        # 1. 식품안전나라 검색
        foodsafety_data = self.search_foodsafety_product(row['위해식품_제품명'])
        result['foodsafety_data'] = foodsafety_data
        
        if not foodsafety_data or not foodsafety_data.get('image_urls'):
            result['status'] = 'foodsafety_not_found'
            print("  → 식품안전나라 이미지 없음")
            return result
        
        time.sleep(1)
        
        # 2. 아이허브 정보 가져오기
        iherb_data = self.get_iherb_product(row['product_partno'])
        result['iherb_data'] = iherb_data
        
        if not iherb_data or not iherb_data.get('image_urls'):
            result['status'] = 'iherb_not_found'
            print("  → 아이허브 이미지 없음")
            return result
        
        time.sleep(1)
        
        # 3. 이미지 비교
        comparison = self.compare_images_with_gemini(
            foodsafety_data['image_urls'],
            iherb_data['image_urls']
        )
        result['comparison_result'] = comparison
        result['status'] = 'completed'
        
        time.sleep(2)
        
        return result
    
    def process_csv(self, csv_path: str, limit: Optional[int] = None, 
                    product_partnos: Optional[List[str]] = None,
                    resume: bool = False) -> List[Dict]:
        """CSV 파일 전체 처리"""
        df = self.load_csv_data(csv_path)
        
        # 특정 제품만 필터링
        if product_partnos:
            df = df[df['product_partno'].isin(product_partnos)]
            print(f"특정 제품 필터링: {len(df)}건")
        
        # 이어하기 모드
        completed_partnos = set()
        if resume:
            completed_partnos = self.load_progress()
            if completed_partnos:
                print(f"✓ 이어하기: {len(completed_partnos)}건 이미 처리됨")
                df = df[~df['product_partno'].isin(completed_partnos)]
                print(f"남은 처리: {len(df)}건")
        
        if limit:
            df = df.head(limit)
            print(f"처리 제한: {limit}건만 처리")
        
        results = []
        
        for idx, row in df.iterrows():
            try:
                result = self.process_row(row)
                results.append(result)
                
                # 진행 상태 업데이트
                completed_partnos.add(row['product_partno'])
                self.save_progress(completed_partnos)
                
                # CSV 저장
                self.append_to_csv(result)
                
                # 중간 JSON 저장 (10건마다)
                if (idx + 1) % 10 == 0:
                    self.save_results(results, 'intermediate')
                    print(f"\n진행률: {idx + 1}/{len(df)}")
                    
            except Exception as e:
                print(f"  ✗ 처리 오류: {str(e)}")
                error_result = {
                    'product_partno': row['product_partno'],
                    'product_id': row['product_id'],
                    'status': 'error',
                    'error': str(e)
                }
                results.append(error_result)
                self.append_to_csv(error_result)
        
        # 최종 JSON 저장
        self.save_results(results, 'final')
        
        print(f"\n✓ CSV 결과: {self.csv_output}")
        
        return results
    
    def append_to_csv(self, result: Dict):
        """결과를 CSV에 추가"""
        row_data = {
            'product_partno': result.get('product_partno', ''),
            'product_id': result.get('product_id', ''),
            '위해식품_제품명': result.get('위해식품_제품명', ''),
            '아이허브_제품명': result.get('아이허브_제품명', ''),
            'status': result.get('status', ''),
            'match': result.get('comparison_result', {}).get('match', '') if isinstance(result.get('comparison_result'), dict) else '',
            'confidence': result.get('comparison_result', {}).get('confidence', 0) if isinstance(result.get('comparison_result'), dict) else 0,
            'reason': result.get('comparison_result', {}).get('reason', '') if isinstance(result.get('comparison_result'), dict) else '',
            'foodsafety_url': result.get('foodsafety_data', {}).get('detail_url', '') if isinstance(result.get('foodsafety_data'), dict) else '',
            'iherb_url': result.get('iherb_data', {}).get('product_url', '') if isinstance(result.get('iherb_data'), dict) else '',
            'foodsafety_image_count': len(result.get('foodsafety_data', {}).get('image_urls', [])) if isinstance(result.get('foodsafety_data'), dict) else 0,
            'iherb_image_count': len(result.get('iherb_data', {}).get('image_urls', [])) if isinstance(result.get('iherb_data'), dict) else 0
        }
        
        df_row = pd.DataFrame([row_data])
        
        if self.csv_output.exists():
            df_row.to_csv(self.csv_output, mode='a', header=False, index=False, encoding='utf-8-sig')
        else:
            df_row.to_csv(self.csv_output, mode='w', header=True, index=False, encoding='utf-8-sig')
    
    def save_results(self, results: List[Dict], result_type: str = 'final'):
        """결과 저장"""
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        filename = f"comparison_{result_type}_{timestamp}.json"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n결과 저장: {filepath}")
        
        # 요약 통계
        total = len(results)
        completed = sum(1 for r in results if r and r.get('status') == 'completed')
        matched = sum(1 for r in results if r and isinstance(r.get('comparison_result'), dict) and r.get('comparison_result', {}).get('match') == 'yes')
        
        print(f"\n=== 요약 ===")
        print(f"전체: {total}건")
        print(f"완료: {completed}건")
        print(f"매칭: {matched}건")


def main():
    """메인 실행 함수"""
    import sys
    
    # Gemini API 키
    api_key = os.environ.get('GEMINI_API_KEY', '')
    if not api_key:
        api_key = input("Gemini API 키를 입력하세요: ").strip()
    
    if not api_key:
        print("API 키가 필요합니다.")
        return
    
    # 비교 시스템 초기화 (headless=False로 브라우저 표시)
    comparator = ProductImageComparator(api_key, headless=False)
    csv_path = "./위해식품목록_아이허브_비플로우기준.csv"
    
    # 명령행 인자 처리
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        
        if mode == 'clear':
            comparator.clear_progress()
            return
        
        elif mode == 'resume':
            print("\n=== 이어하기 실행 ===")
            results = comparator.process_csv(csv_path, resume=True)
            return
        
        elif mode == 'product':
            if len(sys.argv) < 3:
                print("사용법: python3 main.py product [NOW-01278,CGN-02049,...]")
                return
            product_partnos = [x.strip() for x in sys.argv[2].split(',')]
            print(f"\n=== 특정 제품 실행: {product_partnos} ===")
            results = comparator.process_csv(csv_path, product_partnos=product_partnos)
            return
    
    # 인터랙티브 모드
    print("\n=== 제품 이미지 비교 시스템 ===")
    print("1. 테스트 (3건)")
    print("2. 특정 제품 실행")
    print("3. 전체 실행")
    print("4. 이어하기")
    print("5. 진행 상태 초기화")
    
    choice = input("\n선택: ").strip()
    
    if choice == '1':
        print("\n=== 테스트 실행 (3건) ===")
        results = comparator.process_csv(csv_path, limit=3)
    
    elif choice == '2':
        partnos_input = input("제품 코드를 쉼표로 구분하여 입력 (예: NOW-01278,CGN-02049): ").strip()
        product_partnos = [x.strip() for x in partnos_input.split(',')]
        print(f"\n=== 특정 제품 실행: {product_partnos} ===")
        results = comparator.process_csv(csv_path, product_partnos=product_partnos)
    
    elif choice == '3':
        print("\n=== 전체 실행 ===")
        results = comparator.process_csv(csv_path)
    
    elif choice == '4':
        print("\n=== 이어하기 실행 ===")
        results = comparator.process_csv(csv_path, resume=True)
    
    elif choice == '5':
        comparator.clear_progress()


if __name__ == "__main__":
    main()