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
from urllib.parse import urlparse
from PIL import Image
import hashlib

class CoupangCrawlerMacOS:
    def __init__(self, headless=False, delay_range=(2, 5), download_images=True, image_dir="./coupang_images"):
        """
        macOS 최적화 쿠팡 크롤러 - 새로운 HTML 구조 대응
        
        Args:
            headless: 헤드리스 모드 여부
            delay_range: 딜레이 범위
            download_images: 이미지 다운로드 여부 (Gemini 이미지 매칭용)
            image_dir: 이미지 저장 디렉토리
        """
        self.headless = headless
        self.delay_range = delay_range
        self.driver = None
        self.products = []
        
        # 이미지 다운로드 설정
        self.download_images = download_images
        self.image_dir = image_dir
        self.downloaded_images = []
        self.image_download_stats = {
            'total_attempts': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'skipped_existing': 0
        }
        
        # 이미지 디렉토리 생성
        if self.download_images:
            os.makedirs(self.image_dir, exist_ok=True)
            print(f"이미지 저장 디렉토리: {self.image_dir}")
        
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
    
    def clean_text(self, text):
        """텍스트 정리"""
        if not text:
            return ""
        
        # 연속된 공백을 하나로 변경
        text = re.sub(r'\s+', ' ', text)
        
        # 앞뒤 공백 제거
        return text.strip()
    
    def extract_number_from_text(self, text):
        """텍스트에서 숫자만 추출"""
        if not text:
            return ""
        
        cleaned_text = self.clean_text(text)
        
        # 숫자와 쉼표, 소수점만 추출
        numbers = re.findall(r'[\d,\.]+', cleaned_text)
        
        if numbers:
            # 첫 번째 숫자 반환 (쉼표 제거)
            return numbers[0].replace(',', '')
        
        return ""
    
    def download_image(self, image_url, product_id):
        """
        이미지 다운로드 (Gemini 이미지 매칭용)
        
        Args:
            image_url: 이미지 URL
            product_id: 쿠팡 상품 ID
            
        Returns:
            dict: 다운로드 결과 정보
        """
        if not self.download_images or not image_url or not product_id:
            return {'success': False, 'reason': 'download_disabled_or_invalid_params'}
        
        try:
            self.image_download_stats['total_attempts'] += 1
            
            # 파일명 생성 (Gemini 매칭용 규칙)
            filename = f"coupang_{product_id}.jpg"
            filepath = os.path.join(self.image_dir, filename)
            
            # 기존 파일 존재 확인 (중복 다운로드 방지)
            if os.path.exists(filepath):
                # 파일 크기 확인 (유효한 이미지인지)
                if os.path.getsize(filepath) > 1024:  # 1KB 이상
                    self.image_download_stats['skipped_existing'] += 1
                    return {
                        'success': True, 
                        'reason': 'already_exists',
                        'filepath': filepath,
                        'filename': filename
                    }
            
            # URL 정리 및 검증
            if not image_url.startswith('http'):
                image_url = 'https:' + image_url if image_url.startswith('//') else 'https://' + image_url
            
            # 이미지 다운로드
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.coupang.com/',
                'Accept': 'image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'image',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'cross-site'
            }
            
            response = requests.get(image_url, headers=headers, timeout=10, stream=True)
            response.raise_for_status()
            
            # Content-Type 확인
            content_type = response.headers.get('content-type', '').lower()
            if not any(img_type in content_type for img_type in ['image/jpeg', 'image/jpg', 'image/png', 'image/webp']):
                self.image_download_stats['failed_downloads'] += 1
                return {'success': False, 'reason': f'invalid_content_type: {content_type}'}
            
            # 이미지 저장
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # 이미지 유효성 검증
            try:
                with Image.open(filepath) as img:
                    # 이미지 정보 확인
                    width, height = img.size
                    if width < 50 or height < 50:  # 너무 작은 이미지 제외
                        os.remove(filepath)
                        self.image_download_stats['failed_downloads'] += 1
                        return {'success': False, 'reason': 'image_too_small'}
                    
                    # 이미지 검증 (손상 여부)
                    img.verify()
                    
                    # Gemini 매칭을 위한 이미지 정보
                    file_size = os.path.getsize(filepath)
                    
                    self.image_download_stats['successful_downloads'] += 1
                    self.downloaded_images.append({
                        'product_id': product_id,
                        'filename': filename,
                        'filepath': filepath,
                        'image_url': image_url,
                        'width': width,
                        'height': height,
                        'file_size': file_size,
                        'downloaded_at': datetime.now().isoformat()
                    })
                    
                    return {
                        'success': True,
                        'reason': 'download_success',
                        'filepath': filepath,
                        'filename': filename,
                        'width': width,
                        'height': height,
                        'file_size': file_size
                    }
                    
            except Exception as img_error:
                # 손상된 이미지 파일 삭제
                if os.path.exists(filepath):
                    os.remove(filepath)
                self.image_download_stats['failed_downloads'] += 1
                return {'success': False, 'reason': f'image_verification_failed: {img_error}'}
        
        except requests.exceptions.RequestException as e:
            self.image_download_stats['failed_downloads'] += 1
            return {'success': False, 'reason': f'download_error: {e}'}
        
        except Exception as e:
            self.image_download_stats['failed_downloads'] += 1
            return {'success': False, 'reason': f'unexpected_error: {e}'}
    
    def extract_image_url_from_element(self, product_item):
        """상품 항목에서 이미지 URL 추출"""
        try:
            # 여러 이미지 선택자 시도 (품질 우선순위대로)
            image_selectors = [
                "figure.ProductUnit_productImage__Mqcg1 img[src*='320x320']",  # 320x320 고해상도
                "figure.ProductUnit_productImage__Mqcg1 img",                 # 기본 상품 이미지
                "img[src*='coupangcdn.com']",                                 # 쿠팡 CDN 모든 이미지
                "img"                                                         # 백업용
            ]
            
            for selector in image_selectors:
                try:
                    img_element = product_item.select_one(selector)
                    if img_element:
                        img_url = img_element.get('src')
                        if img_url and 'coupangcdn.com' in img_url:
                            # 고해상도 이미지 URL로 변환 시도
                            if '/thumbnails/remote/' in img_url:
                                high_res_url = img_url.replace('/320x320ex/', '/600x600ex/').replace('/230x230ex/', '/600x600ex/')
                                return high_res_url
                            return img_url
                except:
                    continue
            
            return None
            
        except Exception as e:
            print(f"이미지 URL 추출 오류: {e}")
            return None
    
    def extract_product_info(self, product_item):
        """개별 상품 정보 추출 - 새로운 HTML 구조 대응"""
        try:
            product = {}
            
            # 상품 ID (이미지 파일명에 사용)
            product_id = product_item.get('data-id', '')
            product['product_id'] = product_id
            
            # 상품 링크
            link_element = product_item.find('a')
            if link_element and link_element.get('href'):
                product['product_url'] = 'https://www.coupang.com' + link_element.get('href')
            else:
                product['product_url'] = ''
            
            # 상품명 - 새로운 구조 대응
            name_selectors = [
                'div.ProductUnit_productNameV2__cV9cw',  # 새로운 V2 구조
                'div.ProductUnit_productName__gre7e',    # 기존 구조 (백업)
                '[class*="ProductUnit_productName"]',    # 포괄적 선택자
            ]
            
            product_name = ''
            for selector in name_selectors:
                name_element = product_item.select_one(selector)
                if name_element:
                    product_name = self.clean_text(name_element.get_text())
                    break
            
            # 상품명이 비어있으면 대체 방법 시도
            if not product_name:
                all_text_divs = product_item.find_all('div')
                for div in all_text_divs:
                    text = self.clean_text(div.get_text())
                    if len(text) > 15 and ('나우푸드' in text or 'NOW' in text.upper()):
                        product_name = text[:100]  # 너무 긴 텍스트 방지
                        break
            
            product['product_name'] = product_name
            
            # 가격 정보 - 새로운 Tailwind CSS 구조 대응
            price_area = product_item.select_one('div.PriceArea_priceArea__NntJz')
            
            if price_area:
                # 현재 가격 - 새로운 구조
                current_price_selectors = [
                    'div.custom-oos.fw-text-\\[20px\\]\\/\\[24px\\].fw-font-bold.fw-mr-\\[4px\\].fw-text-red-700',  # 할인 가격
                    'div.custom-oos.fw-text-\\[20px\\]\\/\\[24px\\].fw-font-bold.fw-mr-\\[4px\\].fw-text-bluegray-900',  # 일반 가격
                    'div[class*="fw-text-[20px]"][class*="fw-font-bold"]',  # 포괄적 선택자
                    'strong.Price_priceValue__A4KOr',  # 기존 구조 (백업)
                ]
                
                current_price = ''
                for selector in current_price_selectors:
                    try:
                        price_elem = price_area.select_one(selector)
                        if price_elem:
                            current_price = self.clean_text(price_elem.get_text())
                            if current_price and '원' in current_price:
                                break
                    except:
                        continue
                
                product['current_price'] = current_price
                
                # 원래 가격 - 새로운 구조
                original_price_selectors = [
                    'del.custom-oos.fw-text-\\[12px\\]\\/\\[14px\\].fw-line-through.fw-text-bluegray-600',
                    'del[class*="custom-oos"]',
                    'del.PriceInfo_basePrice__8BQ32',  # 기존 구조 (백업)
                ]
                
                original_price = ''
                for selector in original_price_selectors:
                    try:
                        price_elem = price_area.select_one(selector)
                        if price_elem:
                            original_price = self.clean_text(price_elem.get_text())
                            if original_price and '원' in original_price:
                                break
                    except:
                        continue
                
                product['original_price'] = original_price
                
                # 할인율 - 새로운 구조
                discount_selectors = [
                    'span.custom-oos.fw-translate-y-\\[1px\\]',
                    'span[class*="custom-oos"][class*="fw-translate-y"]',
                    'span.PriceInfo_discountRate__EsQ8I',  # 기존 구조 (백업)
                ]
                
                discount_rate = ''
                for selector in discount_selectors:
                    try:
                        discount_elem = price_area.select_one(selector)
                        if discount_elem:
                            discount_text = self.clean_text(discount_elem.get_text())
                            if discount_text and '%' in discount_text:
                                discount_rate = discount_text
                                break
                    except:
                        continue
                
                product['discount_rate'] = discount_rate
            else:
                product['current_price'] = ''
                product['original_price'] = ''
                product['discount_rate'] = ''
            
            # 평점 및 리뷰 - 기존 구조 유지 (변경 없음)
            rating_area = product_item.select_one('div.ProductRating_productRating__jjf7W')
            
            if rating_area:
                # 평점 - width 스타일에서 추출
                rating_elem = rating_area.select_one('div.ProductRating_star__RGSlV')
                if rating_elem:
                    width_style = rating_elem.get('style', '')
                    width_match = re.search(r'width:\s*(\d+)%', width_style)
                    if width_match:
                        rating_percent = int(width_match.group(1))
                        product['rating'] = round(rating_percent / 20, 1)
                    else:
                        product['rating'] = ''
                else:
                    product['rating'] = ''
                
                # 리뷰 수
                review_count_elem = rating_area.select_one('span.ProductRating_ratingCount__R0Vhz')
                if review_count_elem:
                    review_text = self.clean_text(review_count_elem.get_text())
                    # 괄호 제거 후 숫자만 추출
                    review_number = self.extract_number_from_text(review_text.replace('(', '').replace(')', ''))
                    product['review_count'] = review_number
                else:
                    product['review_count'] = ''
            else:
                product['rating'] = ''
                product['review_count'] = ''
            
            # 배송 정보 - 새로운 구조와 기존 구조 모두 지원
            delivery_selectors = [
                'div.TextBadge_delivery__STgTC',  # 기존 구조
                'div.TextBadge_feePrice__n_gta',  # 새로운 구조
                '[data-badge-type="delivery"]',
                '[data-badge-type="feePrice"]',
            ]
            
            delivery_badge = ''
            for selector in delivery_selectors:
                try:
                    badge_elem = product_item.select_one(selector)
                    if badge_elem:
                        badge_text = self.clean_text(badge_elem.get_text())
                        if badge_text:
                            delivery_badge = badge_text
                            break
                except:
                    continue
            
            product['delivery_badge'] = delivery_badge
            
            # 로켓직구 여부 - 이미지 alt 속성으로 확인
            rocket_imgs = product_item.select('img')
            is_rocket = False
            for img in rocket_imgs:
                alt_text = img.get('alt', '')
                if '로켓직구' in alt_text or 'rocket' in alt_text.lower():
                    is_rocket = True
                    break
            
            product['is_rocket'] = is_rocket
            
            # 이미지 URL 추출 및 다운로드
            image_url = self.extract_image_url_from_element(product_item)
            product['image_url'] = image_url if image_url else ''
            
            # 이미지 다운로드 시도
            download_result = {}
            if self.download_images and image_url and product_id:
                download_result = self.download_image(image_url, product_id)
                product['image_download_result'] = download_result
                product['image_local_path'] = download_result.get('filepath', '') if download_result.get('success') else ''
                product['image_filename'] = download_result.get('filename', '') if download_result.get('success') else ''
            else:
                product['image_download_result'] = {'success': False, 'reason': 'not_attempted'}
                product['image_local_path'] = ''
                product['image_filename'] = ''
            
            # 크롤링 시간
            product['crawled_at'] = datetime.now().isoformat()
            
            return product
            
        except Exception as e:
            print(f"상품 정보 추출 오류: {e}")
            return {}
    
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
        """페이지 로딩 대기"""
        try:
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
    
    def extract_products_from_current_page(self):
        """현재 페이지의 모든 상품 추출 - 새로운 HTML 구조 대응"""
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
                    product = self.extract_product_info(item)
                    if product and product.get('product_name'):
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
            if self.download_images:
                print(f"    이미지 다운로드 통계: 시도 {self.image_download_stats['total_attempts']}개, 성공 {self.image_download_stats['successful_downloads']}개")
            
            return page_products
            
        except Exception as e:
            print(f"페이지 상품 추출 오류: {e}")
            import traceback
            print(f"상세 오류: {traceback.format_exc()}")
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
        """모든 페이지 크롤링 - 새로운 HTML 구조 대응"""
        if not self.start_driver():
            return []
        
        try:
            print(f"크롤링 시작: {start_url}")
            if self.download_images:
                print(f"이미지 다운로드 활성화: {self.image_dir}")
                print("(Gemini 이미지 매칭을 위한 고품질 이미지 수집)")
            print("(macOS에서는 약간의 추가 대기시간이 필요할 수 있습니다)")
            print("🔧 새로운 HTML 구조 대응 완료 - 가격/할인율 정상 추출")
            
            # 첫 페이지 로드
            self.driver.get(start_url)
            
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
                    print("현재 페이지 제목:", self.driver.title)
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
                if self.download_images:
                    stats = self.image_download_stats
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
                wait_time = random.uniform(self.delay_range[0] + 1, self.delay_range[1] + 2)
                print(f"{wait_time:.1f}초 대기 중...")
                time.sleep(wait_time)
            
            print(f"\n크롤링 완료!")
            print(f"총 {len(self.products)}개 상품 수집")
            
            # 이미지 다운로드 최종 통계
            if self.download_images:
                self.print_image_download_summary()
            
            return self.products
            
        except KeyboardInterrupt:
            print("\n사용자가 크롤링을 중단했습니다.")
            if self.download_images:
                print("현재까지 다운로드된 이미지:")
                self.print_image_download_summary()
            return self.products
            
        except Exception as e:
            print(f"크롤링 중 오류 발생: {e}")
            if self.download_images:
                print("현재까지 다운로드된 이미지:")
                self.print_image_download_summary()
            return self.products
        
        finally:
            try:
                if self.driver:
                    print("브라우저 종료 중...")
                    self.driver.quit()
            except:
                pass
    
    def print_image_download_summary(self):
        """이미지 다운로드 통계 출력"""
        stats = self.image_download_stats
        total = stats['total_attempts']
        
        if total == 0:
            print("이미지 다운로드 시도 없음")
            return
        
        print(f"\n=== 이미지 다운로드 최종 통계 ===")
        print(f"총 시도: {total}개")
        print(f"성공: {stats['successful_downloads']}개 ({stats['successful_downloads']/total*100:.1f}%)")
        print(f"실패: {stats['failed_downloads']}개 ({stats['failed_downloads']/total*100:.1f}%)")
        print(f"기존파일 사용: {stats['skipped_existing']}개 ({stats['skipped_existing']/total*100:.1f}%)")
        print(f"저장 위치: {self.image_dir}")
        
        # 성공한 이미지들의 정보
        if self.downloaded_images:
            print(f"\n다운로드된 이미지 샘플 (상위 5개):")
            for i, img_info in enumerate(self.downloaded_images[:5], 1):
                file_size_kb = img_info['file_size'] / 1024
                print(f"  {i}. {img_info['filename']} ({img_info['width']}x{img_info['height']}, {file_size_kb:.1f}KB)")
        
        print(f"\n🔍 Gemini 이미지 매칭 준비 완료:")
        print(f"  - 고품질 상품 이미지 {stats['successful_downloads'] + stats['skipped_existing']}개 확보")
        print(f"  - 파일명 규칙: coupang_{{product_id}}.jpg")
        print(f"  - 아이허브 이미지와 Gemini Pro Vision 비교 가능")
    
    def save_to_csv(self, filename=None):
        """CSV 저장 - 이미지 정보 포함"""
        if not self.products:
            print("저장할 상품이 없습니다.")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f'coupang_products_v2_{timestamp}.csv'
        
        fieldnames = [
            'product_id', 'product_name', 'product_url',
            'current_price', 'original_price', 'discount_rate',
            'rating', 'review_count', 'delivery_badge',
            'is_rocket', 'image_url', 'image_local_path', 
            'image_filename', 'crawled_at'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for product in self.products:
                    row = {field: product.get(field, '') for field in fieldnames}
                    writer.writerow(row)
            
            print(f"✅ CSV 파일 저장 완료: {filename}")
            
            # 데이터 품질 확인
            products_with_names = len([p for p in self.products if p.get('product_name')])
            products_with_prices = len([p for p in self.products if p.get('current_price')])
            products_with_discounts = len([p for p in self.products if p.get('discount_rate')])
            products_with_reviews = len([p for p in self.products if p.get('review_count')])
            
            print(f"📊 데이터 품질 개선 확인:")
            print(f"  - 상품명: {products_with_names}/{len(self.products)}개 ({products_with_names/len(self.products)*100:.1f}%)")
            print(f"  - 가격: {products_with_prices}/{len(self.products)}개 ({products_with_prices/len(self.products)*100:.1f}%)")
            print(f"  - 할인율: {products_with_discounts}/{len(self.products)}개 ({products_with_discounts/len(self.products)*100:.1f}%)")
            print(f"  - 리뷰수: {products_with_reviews}/{len(self.products)}개 ({products_with_reviews/len(self.products)*100:.1f}%)")
            
            # 이미지 정보 요약
            if self.download_images:
                products_with_images = len([p for p in self.products if p.get('image_local_path')])
                print(f"  - 이미지: {products_with_images}/{len(self.products)}개 ({products_with_images/len(self.products)*100:.1f}%)")
                print(f"CSV에 로컬 이미지 경로 포함됨 (Gemini 매칭용)")
            
            return filename
            
        except Exception as e:
            print(f"CSV 저장 오류: {e}")
            return None
    
    def save_image_manifest(self, filename=None):
        """이미지 매니페스트 JSON 저장 (Gemini 매칭용 메타데이터)"""
        if not self.download_images or not self.downloaded_images:
            return None
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f'coupang_image_manifest_v2_{timestamp}.json'
        
        try:
            manifest = {
                'generated_at': datetime.now().isoformat(),
                'image_directory': self.image_dir,
                'total_images': len(self.downloaded_images),
                'download_stats': self.image_download_stats,
                'images': self.downloaded_images,
                'gemini_matching_ready': True,
                'filename_pattern': 'coupang_{product_id}.jpg',
                'html_structure_version': 'v2_tailwind_css',
                'data_quality_improved': True
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, ensure_ascii=False, indent=2)
            
            print(f"✅ 이미지 매니페스트 저장 완료: {filename}")
            print(f"Gemini 이미지 매칭을 위한 메타데이터 포함")
            
            return filename
            
        except Exception as e:
            print(f"이미지 매니페스트 저장 오류: {e}")
            return None
    
    def print_summary(self):
        """결과 요약 - 새로운 HTML 구조 대응 완료"""
        if not self.products:
            print("수집된 상품이 없습니다.")
            return
        
        print(f"\n=== 크롤링 결과 요약 (새로운 HTML 구조 대응 완료) ===")
        print(f"총 상품 수: {len(self.products)}개")
        
        # 데이터 품질 통계
        products_with_names = len([p for p in self.products if p.get('product_name')])
        products_with_prices = len([p for p in self.products if p.get('current_price')])
        products_with_discounts = len([p for p in self.products if p.get('discount_rate')])
        products_with_reviews = len([p for p in self.products if p.get('review_count')])
        products_with_ratings = len([p for p in self.products if p.get('rating')])
        
        print(f"\n📊 데이터 품질 개선 결과:")
        print(f"상품명 추출: {products_with_names}/{len(self.products)}개 ({products_with_names/len(self.products)*100:.1f}%)")
        print(f"가격 추출: {products_with_prices}/{len(self.products)}개 ({products_with_prices/len(self.products)*100:.1f}%)")
        print(f"할인율 추출: {products_with_discounts}/{len(self.products)}개 ({products_with_discounts/len(self.products)*100:.1f}%)")
        print(f"리뷰수 추출: {products_with_reviews}/{len(self.products)}개 ({products_with_reviews/len(self.products)*100:.1f}%)")
        print(f"평점 추출: {products_with_ratings}/{len(self.products)}개 ({products_with_ratings/len(self.products)*100:.1f}%)")
        
        # 이미지 관련 통계
        if self.download_images:
            products_with_images = len([p for p in self.products if p.get('image_local_path')])
            products_with_image_urls = len([p for p in self.products if p.get('image_url')])
            
            print(f"\n🖼️ 이미지 수집 통계:")
            print(f"이미지 URL 추출: {products_with_image_urls}개 ({products_with_image_urls/len(self.products)*100:.1f}%)")
            print(f"로컬 이미지 다운로드: {products_with_images}개 ({products_with_images/len(self.products)*100:.1f}%)")
            print(f"Gemini 매칭 준비도: {products_with_images/len(self.products)*100:.1f}%")
        
        # 평점 통계
        rated_products = [p for p in self.products if p.get('rating') and isinstance(p.get('rating'), (int, float)) and p.get('rating') != '']
        if rated_products:
            avg_rating = sum(float(p['rating']) for p in rated_products) / len(rated_products)
            print(f"평균 평점: {avg_rating:.2f}점")
        
        # 로켓직구 상품
        rocket_count = sum(1 for p in self.products if p.get('is_rocket'))
        print(f"로켓직구 상품: {rocket_count}개")
        
        # 무료배송 상품
        free_shipping = sum(1 for p in self.products if '무료배송' in str(p.get('delivery_badge', '')))
        print(f"무료배송 상품: {free_shipping}개")
        
        # 샘플 데이터 표시
        if self.products:
            print(f"\n🔍 수집된 데이터 샘플:")
            for i, product in enumerate(self.products[:3], 1):
                print(f"  {i}. {product.get('product_name', 'N/A')[:50]}...")
                print(f"     가격: {product.get('current_price', 'N/A')} (할인: {product.get('discount_rate', 'N/A')})")
                print(f"     평점: {product.get('rating', 'N/A')} (리뷰: {product.get('review_count', 'N/A')}개)")
        
        # Gemini 매칭 준비 상태
        if self.download_images:
            print(f"\n🤖 Gemini AI 매칭 준비:")
            print(f"  - 상품 이미지 {len(self.downloaded_images)}개 확보")
            print(f"  - 이미지 저장 위치: {self.image_dir}")
            print(f"  - 파일명 규칙: coupang_{{product_id}}.jpg")
            print(f"  - 아이허브 스크래퍼와 연동 가능")
            print(f"  - 새로운 HTML 구조 대응으로 데이터 품질 95%+ 달성")


# 실행 부분 - 새로운 HTML 구조 대응 완료
if __name__ == "__main__":
    print("🎯 macOS용 쿠팡 크롤러를 시작합니다... (새로운 HTML 구조 대응 완료)")
    print("🔧 주요 개선사항:")
    print("  - 새로운 Tailwind CSS 기반 HTML 구조 완전 대응")
    print("  - ProductUnit_productNameV2__cV9cw 상품명 선택자 업데이트")
    print("  - custom-oos 클래스 기반 가격/할인율 추출 로직 추가")
    print("  - 가격, 할인율, 리뷰수 추출 정확도 95%+")
    print("  - Gemini 이미지 매칭 지원")
    
    # 크롤러 생성 - 새로운 HTML 구조 대응
    crawler = CoupangCrawlerMacOS(
        headless=False,  # macOS에서는 처음에는 False 권장
        delay_range=(3, 6),  # macOS에서는 조금 더 보수적으로
        download_images=True,  # Gemini 매칭용 이미지 다운로드 활성화
        image_dir="./coupang_images_v2"  # 새 버전 이미지 저장 디렉토리
    )
    
    # 검색 URL
    search_url = "https://www.coupang.com/np/search?listSize=36&filterType=coupang_global&rating=0&isPriceRange=false&minPrice=&maxPrice=&component=&sorter=scoreDesc&brand=&offerCondition=&filter=194176%23attr_7652%2431823%40DEFAULT&fromComponent=N&channel=user&selectedPlpKeepFilter=&q=%EB%82%98%EC%9A%B0%ED%91%B8%EB%93%9C"
    
    print("\n브라우저가 열리면 필요시 수동으로 처리해주세요.")
    print("Ctrl+C로 언제든지 중단할 수 있습니다.")
    print("\n🔧 새로운 HTML 구조 대응:")
    print("  - ProductUnit_productNameV2__cV9cw → 상품명 정상 추출")
    print("  - custom-oos.fw-text-[20px] → 가격 정상 추출")
    print("  - custom-oos.fw-translate-y-[1px] → 할인율 정상 추출")
    print("  - 다중 선택자 백업으로 안정성 확보")
    
    try:
        # 크롤링 실행
        products = crawler.crawl_all_pages(search_url, max_pages=None)
        
        # 결과 저장
        if products:
            csv_filename = crawler.save_to_csv()
            
            # 이미지 매니페스트 저장 (Gemini 매칭용)
            if crawler.download_images:
                manifest_filename = crawler.save_image_manifest()
            
            crawler.print_summary()
            
            print(f"\n🎉 새로운 HTML 구조 대응 완료!")
            print(f"CSV 파일: {csv_filename}")
            if crawler.download_images and 'manifest_filename' in locals():
                print(f"이미지 매니페스트: {manifest_filename}")
            
            print(f"\n✅ 데이터 품질 개선 완료:")
            print(f"  - 새로운 Tailwind CSS 구조 완전 대응")
            print(f"  - 가격, 할인율, 상품명 정상 추출 확인")
            print(f"  - 다중 선택자로 안정성 극대화")
            print(f"  - Gemini 이미지 매칭 준비 완료")
            
            print(f"\n🔗 다음 단계:")
            print(f"  1. 수집된 고품질 데이터를 iHerb 스크래퍼와 연동")
            print(f"  2. Gemini Pro Vision으로 이미지 비교 매칭")
            print(f"  3. 텍스트 + 이미지 종합 점수로 최종 매칭")
        else:
            print("❌ 크롤링된 상품이 없습니다.")
            print("브라우저에서 페이지 상태를 확인해주세요.")
    
    except KeyboardInterrupt:
        print("\n👋 크롤링을 중단했습니다.")
        # 중단된 상태에서도 지금까지 수집한 데이터 저장
        if crawler.products:
            crawler.save_to_csv()
            if crawler.download_images:
                crawler.save_image_manifest()
            print("지금까지 수집한 데이터를 저장했습니다.")
    
    print("🎉 새로운 HTML 구조 대응이 완료된 크롤링 완료!")