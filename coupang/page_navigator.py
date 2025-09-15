import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

class PageNavigator:
    def __init__(self, browser_manager, scraper, image_downloader):
        self.browser = browser_manager
        self.scraper = scraper
        self.image_downloader = image_downloader
        self.products = []
    
    def wait_for_page_load(self, timeout=20):
        """페이지 로딩 대기"""
        try:
            wait = WebDriverWait(self.browser.driver, timeout)
            
            # 상품 리스트 또는 에러 메시지 대기
            wait.until(lambda driver: 
                driver.find_elements(By.ID, "product-list") or 
                driver.find_elements(By.CLASS_NAME, "search-no-result") or
                "차단" in driver.title.lower()
            )
            
            # DOM 로딩 완료 대기
            self.browser.driver.execute_script("return document.readyState === 'complete'")
            
            # 추가 로딩 시간
            time.sleep(random.uniform(2, 4))
            
            # 차단 페이지 확인
            if "차단" in self.browser.driver.title.lower() or "blocked" in self.browser.driver.title.lower():
                print("⚠️ 차단 페이지가 감지되었습니다. 잠시 대기해주세요.")
                time.sleep(10)
                return False
            
            return True
            
        except TimeoutException:
            print("페이지 로딩 시간 초과")
            return False
    
    def human_like_scroll(self):
        """인간처럼 자연스러운 스크롤링"""
        try:
            # 페이지 높이 가져오기
            total_height = self.browser.driver.execute_script("return document.body.scrollHeight")
            viewport_height = self.browser.driver.execute_script("return window.innerHeight")
            
            current_position = 0
            scroll_step = random.randint(200, 400)
            
            # 페이지를 천천히 스크롤 다운
            while current_position < total_height - viewport_height:
                scroll_distance = random.randint(scroll_step - 50, scroll_step + 50)
                current_position += scroll_distance
                
                self.browser.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(0.3, 0.8))
            
            # 잠시 대기 후 상단으로
            time.sleep(random.uniform(1, 2))
            self.browser.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(random.uniform(0.5, 1))
            
        except Exception as e:
            print(f"스크롤 중 오류: {e}")
    
    def extract_products_from_current_page(self):
        """현재 페이지의 모든 상품 추출 - 새로운 HTML 구조 대응"""
        try:
            # 차단 확인
            if "차단" in self.browser.driver.title.lower():
                print("⚠️ 차단된 페이지입니다. 크롤링을 중단합니다.")
                return []
            
            # 스크롤링
            self.human_like_scroll()
            
            # HTML 파싱
            html_content = self.browser.driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 상품 리스트 찾기
            product_list = soup.find('ul', id='product-list')
            if not product_list:
                product_list = soup.select_one('ul[id="product-list"], [id*="product"], ul[class*="product"]')
                
            if not product_list:
                print("상품 리스트를 찾을 수 없습니다.")
                no_result = soup.find('div', class_='search-no-result')
                if no_result:
                    print("검색 결과가 없습니다.")
                return []
            
            # 상품 추출
            product_items = product_list.select('li.ProductUnit_productUnit__Qd6sv, li[class*="ProductUnit"], li[data-id]')
            page_products = []
            
            print(f"    페이지에서 {len(product_items)}개 상품 발견")
            
            for idx, item in enumerate(product_items):
                try:
                    product = self.scraper.extract_product_info(item)
                    if product and product.get('product_name'):
                        # 이미지 처리
                        product_id = product.get('product_id')
                        
                        if self.image_downloader and product_id:
                            # 이미지 URL 추출
                            image_url = self.image_downloader.extract_image_url_from_element(item)
                            product['image_url'] = image_url if image_url else ''
                            
                            # 이미지 다운로드
                            if image_url:
                                download_result = self.image_downloader.download_image(image_url, product_id)
                                product['image_download_result'] = download_result
                                product['image_local_path'] = download_result.get('filepath', '') if download_result.get('success') else ''
                                product['image_filename'] = download_result.get('filename', '') if download_result.get('success') else ''
                            else:
                                product['image_download_result'] = {'success': False, 'reason': 'no_url'}
                                product['image_local_path'] = ''
                                product['image_filename'] = ''
                        else:
                            product['image_url'] = ''
                            product['image_download_result'] = {'success': False, 'reason': 'not_attempted'}
                            product['image_local_path'] = ''
                            product['image_filename'] = ''
                        
                        page_products.append(product)
                        
                        # 처리 결과 로깅 (처음 3개만)
                        if idx < 3:
                            product_name = product.get('product_name', 'N/A')[:50]
                            current_price = product.get('current_price', 'N/A')
                            discount_rate = product.get('discount_rate', 'N/A')
                            print(f"      상품 {idx+1}: {product_name}... ({current_price}, 할인: {discount_rate})")
                    
                except Exception as e:
                    print(f"      상품 {idx+1} 처리 중 오류: {e}")
                    
                # 처리 간격 조절
                if idx % 10 == 0:
                    time.sleep(random.uniform(0.1, 0.3))
            
            print(f"    실제 추출된 상품 수: {len(page_products)}개")
            if self.image_downloader:
                print(f"    이미지 다운로드 통계: 시도 {self.image_downloader.image_download_stats['total_attempts']}개, 성공 {self.image_downloader.image_download_stats['successful_downloads']}개")
            
            return page_products
            
        except Exception as e:
            print(f"페이지 상품 추출 오류: {e}")
            import traceback
            print(f"상세 오류: {traceback.format_exc()}")
            return []
    
    def has_next_page(self):
        """다음 페이지 확인"""
        try:
            next_button = self.browser.driver.find_element(By.CSS_SELECTOR, 'a.Pagination_nextBtn__TUY5t')
            button_classes = next_button.get_attribute('class')
            return 'disabled' not in button_classes
        except:
            return False
    
    def go_to_next_page(self):
        """다음 페이지 이동"""
        try:
            next_button = self.browser.driver.find_element(By.CSS_SELECTOR, 'a.Pagination_nextBtn__TUY5t')
            
            if 'disabled' in next_button.get_attribute('class'):
                return False
            
            # 버튼 위치로 스크롤
            self.browser.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_button)
            time.sleep(random.uniform(1, 2))
            
            # 클릭
            self.browser.driver.execute_script("arguments[0].click();", next_button)
            
            # 페이지 로딩 대기
            time.sleep(random.uniform(3, 6))
            return True
            
        except Exception as e:
            print(f"다음 페이지 이동 오류: {e}")
            return False
    
    def crawl_all_pages(self, start_url, max_pages=None, delay_range=(2, 5)):
        """모든 페이지 크롤링 - 새로운 HTML 구조 대응"""
        try:
            print(f"크롤링 시작: {start_url}")
            if self.image_downloader:
                print(f"이미지 다운로드 활성화: {self.image_downloader.image_dir}")
                print("(Gemini 이미지 매칭을 위한 고품질 이미지 수집)")
            print("(macOS에서는 약간의 추가 대기시간이 필요할 수 있습니다)")
            print("🔧 새로운 HTML 구조 대응 완료 - 가격/할인율 정상 추출")
            
            # 첫 페이지 로드
            self.browser.driver.get(start_url)
            
            if not self.wait_for_page_load():
                print("첫 페이지 로딩 실패")
                return []
            
            page_count = 0
            
            while True:
                page_count += 1
                print(f"\n=== 페이지 {page_count} 크롤링 중 ===")
                
                # 현재 페이지 상품 추출
                page_products = self.extract_products_from_current_page()
                
                if not page_products and page_count == 1:
                    print("⚠️ 첫 페이지에서 상품을 찾을 수 없습니다.")
                    print("현재 페이지 제목:", self.browser.driver.title)
                    print("브라우저에서 수동으로 확인해주세요.")
                    
                    # 10초 대기 (수동 처리 시간)
                    time.sleep(10)
                    page_products = self.extract_products_from_current_page()
                
                self.products.extend(page_products)
                
                print(f"페이지 {page_count}에서 {len(page_products)}개 상품 추출")
                print(f"총 누적 상품 수: {len(self.products)}개")
                
                # 데이터 품질 확인 (처음 몇 개 상품)
                if page_products and len(page_products) > 0:
                    sample_product = page_products[0]
                    print(f"샘플 상품 품질 확인:")
                    print(f"  - 상품명: {sample_product.get('product_name', 'N/A')[:50]}...")
                    print(f"  - 가격: {sample_product.get('current_price', 'N/A')}")
                    print(f"  - 할인율: {sample_product.get('discount_rate', 'N/A')}")
                    print(f"  - 리뷰수: {sample_product.get('review_count', 'N/A')}")
                
                # 이미지 다운로드 누적 통계
                if self.image_downloader:
                    stats = self.image_downloader.image_download_stats
                    print(f"이미지 다운로드 누적 통계:")
                    print(f"  - 시도: {stats['total_attempts']}개")
                    print(f"  - 성공: {stats['successful_downloads']}개")
                    print(f"  - 실패: {stats['failed_downloads']}개")
                    print(f"  - 기존파일: {stats['skipped_existing']}개")
                
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
                wait_time = random.uniform(delay_range[0] + 1, delay_range[1] + 2)
                print(f"{wait_time:.1f}초 대기 중...")
                time.sleep(wait_time)
            
            print(f"\n크롤링 완료!")
            print(f"총 {len(self.products)}개 상품 수집")
            
            # 이미지 다운로드 최종 통계
            if self.image_downloader:
                self.image_downloader.print_image_download_summary()
            
            return self.products
            
        except KeyboardInterrupt:
            print("\n사용자가 크롤링을 중단했습니다.")
            if self.image_downloader:
                print("현재까지 다운로드된 이미지:")
                self.image_downloader.print_image_download_summary()
            return self.products
            
        except Exception as e:
            print(f"크롤링 중 오류 발생: {e}")
            if self.image_downloader:
                print("현재까지 다운로드된 이미지:")
                self.image_downloader.print_image_download_summary()
            return self.products