import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import time
import json
import csv
import random
import re
from datetime import datetime
import os
import platform
import requests
from PIL import Image

class CoupangCrawlerMacOS:
    def __init__(self, headless=False, delay_range=(2, 5)):
        """
        macOS 최적화 쿠팡 크롤러 - 이미지 수집 기능 추가
        """
        self.headless = headless
        self.delay_range = delay_range
        self.driver = None
        self.products = []
        
        # 이미지 저장 디렉토리 설정
        self.images_dir = "coupang_images"
        os.makedirs(self.images_dir, exist_ok=True)
        self.downloaded_images = {}  # product_id -> image_path
        
        # Chrome 옵션 설정 (macOS 최적화)
        self.options = uc.ChromeOptions()
        
        # macOS에서 안정적으로 작동하는 옵션들만 사용
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_argument('--disable-extensions')
        self.options.add_argument('--disable-plugins-discovery')
        
        # 실험적 옵션 (macOS 호환)
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        
        # User-Agent 설정
        self.options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        if headless:
            self.options.add_argument('--headless')
            
        # 윈도우 크기 설정
        self.options.add_argument('--window-size=1920,1080')
    
    def start_driver(self):
        """Chrome 드라이버 시작 (macOS 최적화)"""
        try:
            print("Chrome 드라이버 시작 중... (macOS)")
            
            # macOS에서는 버전 감지를 수동으로 설정할 수도 있음
            self.driver = uc.Chrome(
                options=self.options,
                version_main=None,  # 자동 감지
                driver_executable_path=None,  # 자동 다운로드
                browser_executable_path=None,  # 시스템 Chrome 사용
            )
            
            # JavaScript로 웹드라이버 속성 숨기기
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['ko-KR', 'ko', 'en-US', 'en']
                });
                
                window.chrome = {
                    runtime: {}
                };
            """)
            
            print("Chrome 드라이버 시작 완료")
            return True
            
        except Exception as e:
            print(f"드라이버 시작 실패: {e}")
            
            # 대체 방법 시도
            try:
                print("대체 방법으로 드라이버 시작 시도...")
                # 더 단순한 옵션으로 재시도
                simple_options = uc.ChromeOptions()
                simple_options.add_argument('--no-sandbox')
                simple_options.add_argument('--disable-dev-shm-usage')
                
                if self.headless:
                    simple_options.add_argument('--headless')
                
                self.driver = uc.Chrome(options=simple_options)
                print("대체 방법으로 드라이버 시작 성공")
                return True
                
            except Exception as e2:
                print(f"대체 방법도 실패: {e2}")
                return False
    
    def human_like_scroll(self):
        """인간처럼 자연스러운 스크롤링"""
        try:
            # 페이지 높이 가져오기
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            viewport_height = self.driver.execute_script("return window.innerHeight")
            
            current_position = 0
            scroll_step = random.randint(200, 400)
            
            # 페이지를 천천히 스크롤 다운
            while current_position < total_height - viewport_height:
                scroll_distance = random.randint(scroll_step - 50, scroll_step + 50)
                current_position += scroll_distance
                
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(0.3, 0.8))
            
            # 잠시 대기 후 상단으로
            time.sleep(random.uniform(1, 2))
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(random.uniform(0.5, 1))
            
        except Exception as e:
            print(f"스크롤 중 오류: {e}")
    
    def wait_for_page_load(self, timeout=20):
        """페이지 로딩 대기 (타임아웃 증가)"""
        try:
            # 여러 조건을 체크해서 페이지 로딩 확인
            wait = WebDriverWait(self.driver, timeout)
            
            # 상품 리스트 또는 에러 메시지 대기
            wait.until(lambda driver: 
                driver.find_elements(By.ID, "product-list") or 
                driver.find_elements(By.CLASS_NAME, "search-no-result") or
                "차단" in driver.title.lower()
            )
            
            # DOM 로딩 완료 대기
            self.driver.execute_script("return document.readyState === 'complete'")
            
            # 추가 로딩 시간
            time.sleep(random.uniform(2, 4))
            
            # 차단 페이지 확인
            if "차단" in self.driver.title.lower() or "blocked" in self.driver.title.lower():
                print("⚠️ 차단 페이지가 감지되었습니다. 잠시 대기해주세요.")
                time.sleep(10)
                return False
            
            return True
            
        except TimeoutException:
            print("페이지 로딩 시간 초과")
            return False
    
    def extract_image_url_from_element(self, product_item):
        """상품 항목에서 이미지 URL 추출"""
        try:
            # 여러 이미지 선택자 시도
            image_selectors = [
                "figure.ProductUnit_productImage__Mqcg1 img",
                ".ProductUnit_productImage img",
                "img[src*='thumbnail']",
                "img[src*='coupangcdn.com']",
                "img"
            ]
            
            for selector in image_selectors:
                try:
                    img_element = product_item.find(selector)
                    if img_element:
                        img_url = img_element.get('src')
                        if img_url and img_url.startswith('http') and 'coupangcdn.com' in img_url:
                            return img_url
                except:
                    continue
            
            return None
            
        except Exception as e:
            print(f"이미지 URL 추출 오류: {e}")
            return None
    
    def download_product_image(self, product_id, image_url):
        """상품 이미지 다운로드"""
        try:
            if not image_url:
                return None
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Referer': 'https://www.coupang.com/'
            }
            
            response = requests.get(image_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # 파일명 생성
            filename = f"coupang_{product_id}.jpg"
            filepath = os.path.join(self.images_dir, filename)
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            # 이미지 유효성 검사
            try:
                with Image.open(filepath) as img:
                    img.verify()
                return filepath
            except:
                # 손상된 파일 삭제
                if os.path.exists(filepath):
                    os.remove(filepath)
                return None
                
        except Exception as e:
            print(f"이미지 다운로드 실패 ({product_id}): {e}")
            return None
    
    def extract_product_info(self, product_item):
        """개별 상품 정보 추출 + 이미지 다운로드"""
        try:
            product = {}
            
            # 상품 ID
            product_id = product_item.get('data-id', '')
            product['product_id'] = product_id
            
            # 상품 링크
            link_element = product_item.find('a')
            if link_element and link_element.get('href'):
                product['product_url'] = 'https://www.coupang.com' + link_element.get('href')
            else:
                product['product_url'] = ''
            
            # 상품명
            name_element = product_item.find('div', class_='ProductUnit_productName__gre7e')
            product['product_name'] = name_element.get_text(strip=True) if name_element else ''
            
            # 가격 정보
            price_area = product_item.find('div', class_='PriceArea_priceArea__NntJz')
            if price_area:
                # 현재 가격
                current_price_elem = price_area.find('strong', class_='Price_priceValue__A4KOr')
                product['current_price'] = current_price_elem.get_text(strip=True) if current_price_elem else ''
                
                # 원래 가격
                original_price_elem = price_area.find('del', class_='PriceInfo_basePrice__8BQ32')
                product['original_price'] = original_price_elem.get_text(strip=True) if original_price_elem else ''
                
                # 할인율
                discount_elem = price_area.find('span', class_='PriceInfo_discountRate__EsQ8I')
                product['discount_rate'] = discount_elem.get_text(strip=True) if discount_elem else ''
            
            # 평점 및 리뷰
            rating_area = product_item.find('div', class_='ProductRating_productRating__jjf7W')
            if rating_area:
                # 평점
                rating_elem = rating_area.find('div', class_='ProductRating_star__RGSlV')
                if rating_elem:
                    width_style = rating_elem.get('style', '')
                    width_match = re.search(r'width:\s*(\d+)%', width_style)
                    if width_match:
                        rating_percent = int(width_match.group(1))
                        product['rating'] = round(rating_percent / 20, 1)
                
                # 리뷰 수
                review_count_elem = rating_area.find('span', class_='ProductRating_ratingCount__R0Vhz')
                if review_count_elem:
                    review_text = review_count_elem.get_text(strip=True)
                    review_number = re.sub(r'[^\d]', '', review_text)
                    product['review_count'] = review_number
            
            # 배송 정보
            delivery_badge = product_item.find('div', class_='TextBadge_delivery__STgTC')
            product['delivery_badge'] = delivery_badge.get_text(strip=True) if delivery_badge else ''
            
            # 로켓직구 여부
            rocket_img = product_item.find('img', alt='로켓직구')
            product['is_rocket'] = rocket_img is not None
            
            # 이미지 URL 추출 및 다운로드
            image_url = self.extract_image_url_from_element(product_item)
            product['image_url'] = image_url if image_url else ''
            
            # 이미지 다운로드 시도
            if image_url and product_id:
                image_path = self.download_product_image(product_id, image_url)
                if image_path:
                    product['image_path'] = image_path
                    self.downloaded_images[product_id] = image_path
                    print(f"    이미지 다운로드 성공: {product_id}")
                else:
                    product['image_path'] = ''
                    print(f"    이미지 다운로드 실패: {product_id}")
            else:
                product['image_path'] = ''
            
            # 크롤링 시간
            product['crawled_at'] = datetime.now().isoformat()
            
            return product
            
        except Exception as e:
            print(f"상품 정보 추출 오류: {e}")
            return {}
    
    def extract_products_from_current_page(self):
        """현재 페이지의 모든 상품 추출 + 이미지 다운로드"""
        try:
            # 차단 확인
            if "차단" in self.driver.title.lower():
                print("⚠️ 차단된 페이지입니다. 크롤링을 중단합니다.")
                return []
            
            # 스크롤링
            self.human_like_scroll()
            
            # HTML 파싱
            html_content = self.driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 상품 리스트 찾기
            product_list = soup.find('ul', id='product-list')
            if not product_list:
                print("상품 리스트를 찾을 수 없습니다.")
                
                # 검색 결과 없음 확인
                no_result = soup.find('div', class_='search-no-result')
                if no_result:
                    print("검색 결과가 없습니다.")
                
                return []
            
            # 상품 추출
            product_items = product_list.find_all('li', class_='ProductUnit_productUnit__Qd6sv')
            page_products = []
            
            print(f"    페이지에서 {len(product_items)}개 상품 발견")
            
            for idx, item in enumerate(product_items):
                product = self.extract_product_info(item)
                if product and product.get('product_name'):
                    page_products.append(product)
                    
                # 이미지 다운로드 간격 조절
                if idx % 5 == 0:
                    time.sleep(random.uniform(0.5, 1.0))
            
            return page_products
            
        except Exception as e:
            print(f"페이지 상품 추출 오류: {e}")
            return []
    
    def has_next_page(self):
        """다음 페이지 확인"""
        try:
            next_button = self.driver.find_element(By.CSS_SELECTOR, 'a.Pagination_nextBtn__TUY5t')
            button_classes = next_button.get_attribute('class')
            return 'disabled' not in button_classes
        except:
            return False
    
    def go_to_next_page(self):
        """다음 페이지 이동"""
        try:
            next_button = self.driver.find_element(By.CSS_SELECTOR, 'a.Pagination_nextBtn__TUY5t')
            
            if 'disabled' in next_button.get_attribute('class'):
                return False
            
            # 버튼 위치로 스크롤
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_button)
            time.sleep(random.uniform(1, 2))
            
            # 클릭
            self.driver.execute_script("arguments[0].click();", next_button)
            
            # 페이지 로딩 대기
            time.sleep(random.uniform(3, 6))
            return True
            
        except Exception as e:
            print(f"다음 페이지 이동 오류: {e}")
            return False
    
    def crawl_all_pages(self, start_url, max_pages=None):
        """모든 페이지 크롤링 + 이미지 수집"""
        if not self.start_driver():
            return []
        
        try:
            print(f"크롤링 시작: {start_url}")
            print("(macOS에서는 약간의 추가 대기시간이 필요할 수 있습니다)")
            print(f"이미지 저장 폴더: {self.images_dir}")
            
            # 첫 페이지 로드
            self.driver.get(start_url)
            
            if not self.wait_for_page_load():
                print("첫 페이지 로딩 실패")
                return []
            
            page_count = 0
            total_images_downloaded = 0
            
            while True:
                page_count += 1
                print(f"\n=== 페이지 {page_count} 크롤링 중 ===")
                
                # 현재 페이지 상품 추출 (이미지 포함)
                page_products = self.extract_products_from_current_page()
                
                if not page_products and page_count == 1:
                    print("⚠️ 첫 페이지에서 상품을 찾을 수 없습니다.")
                    print("현재 페이지 제목:", self.driver.title)
                    print("브라우저에서 수동으로 확인해주세요.")
                    
                    # 10초 대기 (수동 처리 시간)
                    time.sleep(10)
                    page_products = self.extract_products_from_current_page()
                
                self.products.extend(page_products)
                
                # 이미지 다운로드 통계
                page_images = len([p for p in page_products if p.get('image_path')])
                total_images_downloaded += page_images
                
                print(f"페이지 {page_count}에서 {len(page_products)}개 상품 추출 (이미지: {page_images}개)")
                print(f"총 누적 상품 수: {len(self.products)}개 (이미지: {total_images_downloaded}개)")
                
                # 최대 페이지 확인
                if max_pages and page_count >= max_pages:
                    print(f"최대 페이지 수({max_pages}) 도달")
                    break
                
                # 다음 페이지 확인
                if not self.has_next_page():
                    print("마지막 페이지 도달")
                    break
                
                # 다음 페이지로 이동
                if not self.go_to_next_page():
                    print("다음 페이지 이동 실패")
                    break
                
                # 페이지 로딩 대기
                if not self.wait_for_page_load():
                    print("페이지 로딩 실패")
                    break
                
                # 랜덤 대기 (macOS에서는 조금 더 길게)
                wait_time = random.uniform(self.delay_range[0] + 1, self.delay_range[1] + 2)
                print(f"{wait_time:.1f}초 대기 중...")
                time.sleep(wait_time)
            
            print(f"\n크롤링 완료!")
            print(f"총 {len(self.products)}개 상품 수집")
            print(f"총 {total_images_downloaded}개 이미지 다운로드")
            
            return self.products
            
        except KeyboardInterrupt:
            print("\n사용자가 크롤링을 중단했습니다.")
            return self.products
            
        except Exception as e:
            print(f"크롤링 중 오류 발생: {e}")
            return self.products
        
        finally:
            try:
                if self.driver:
                    print("브라우저 종료 중...")
                    self.driver.quit()
            except:
                pass
    
    def save_to_csv(self, filename=None):
        """CSV 저장 - 이미지 정보 포함"""
        if not self.products:
            print("저장할 상품이 없습니다.")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f'coupang_products_{timestamp}.csv'
        
        fieldnames = [
            'product_id', 'product_name', 'product_url',
            'current_price', 'original_price', 'discount_rate',
            'rating', 'review_count', 'delivery_badge',
            'is_rocket', 'image_url', 'image_path', 'crawled_at'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for product in self.products:
                    row = {field: product.get(field, '') for field in fieldnames}
                    writer.writerow(row)
            
            print(f"✅ CSV 파일 저장 완료: {filename}")
            
            # 이미지 매핑 JSON 저장
            if self.downloaded_images:
                mapping_filename = f'coupang_image_mapping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
                with open(mapping_filename, 'w', encoding='utf-8') as f:
                    json.dump(self.downloaded_images, f, ensure_ascii=False, indent=2)
                print(f"✅ 이미지 매핑 저장 완료: {mapping_filename}")
            
            return filename
            
        except Exception as e:
            print(f"CSV 저장 오류: {e}")
            return None
    
    def print_summary(self):
        """결과 요약 - 이미지 정보 포함"""
        if not self.products:
            print("수집된 상품이 없습니다.")
            return
        
        print(f"\n=== 크롤링 결과 요약 ===")
        print(f"총 상품 수: {len(self.products)}개")
        
        # 이미지 다운로드 통계
        products_with_images = len([p for p in self.products if p.get('image_path')])
        print(f"이미지 다운로드 성공: {products_with_images}개 ({products_with_images/len(self.products)*100:.1f}%)")
        
        # 평점 통계
        rated_products = [p for p in self.products if p.get('rating') and isinstance(p.get('rating'), (int, float))]
        if rated_products:
            avg_rating = sum(p['rating'] for p in rated_products) / len(rated_products)
            print(f"평균 평점: {avg_rating:.2f}점")
        
        # 로켓직구 상품
        rocket_count = sum(1 for p in self.products if p.get('is_rocket'))
        print(f"로켓직구 상품: {rocket_count}개")
        
        # 무료배송 상품
        free_shipping = sum(1 for p in self.products if '무료배송' in str(p.get('delivery_badge', '')))
        print(f"무료배송 상품: {free_shipping}개")
        
        print(f"이미지 저장 폴더: {self.images_dir}")


# 실행 부분
if __name__ == "__main__":
    print("🍎 macOS용 쿠팡 크롤러 (이미지 수집 포함)를 시작합니다...")
    
    # 크롤러 생성
    crawler = CoupangCrawlerMacOS(
        headless=False,  # macOS에서는 처음에는 False 권장
        delay_range=(3, 6)  # macOS에서는 조금 더 보수적으로
    )
    
    # 검색 URL
    search_url = "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=%EB%82%98%EC%9A%B0%ED%91%B8%EB%93%9C"
    
    print("브라우저가 열리면 필요시 수동으로 처리해주세요.")
    print("Ctrl+C로 언제든지 중단할 수 있습니다.")
    
    try:
        # 크롤링 실행 (이미지 수집 포함)
        products = crawler.crawl_all_pages(search_url, max_pages=None)
        
        # 결과 저장
        if products:
            csv_filename = crawler.save_to_csv()
            crawler.print_summary()
            
            print(f"\n🎉 작업 완료!")
            print(f"CSV 파일: {csv_filename}")
            print(f"이미지 폴더: {crawler.images_dir}")
            print("이제 image_experiment_runner.py를 실행할 수 있습니다.")
        else:
            print("❌ 크롤링된 상품이 없습니다.")
            print("브라우저에서 페이지 상태를 확인해주세요.")
    
    except KeyboardInterrupt:
        print("\n👋 크롤링을 중단했습니다.")
        # 중단된 상태에서도 지금까지 수집한 데이터 저장
        if crawler.products:
            crawler.save_to_csv()
            print("지금까지 수집한 데이터를 저장했습니다.")
    
    print("🎉 작업 완료!")