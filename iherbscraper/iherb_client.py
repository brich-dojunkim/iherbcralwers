"""
Modular IHerb Client - Config 통합 버전 대응
Config 클래스에 패턴 매칭이 통합됨에 따른 업데이트
"""

import time
import os
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from PIL import Image


class IHerbClient:
    """아이허브 브라우저 조작 전용 클래스 - Config 통합 대응"""
    
    def __init__(self, browser_manager, config):
        """의존성 주입 - config만 받음 (패턴 매칭 포함)"""
        self.browser = browser_manager
        self.driver = browser_manager.driver
        self.config = config
    
    # ========== 언어 설정 관련 ==========
    
    def set_language_to_english(self):
        """언어를 영어로 설정"""
        try:
            print("  아이허브 언어 설정 (자동)...")
            
            if not self.browser.safe_get(self.config.KOREA_URL):
                print("  아이허브 접속 실패 - 기본 설정으로 진행")
                return False
            
            # 설정 버튼 클릭
            if not self._click_element('.selected-country-wrapper', "설정 버튼", 3):
                return self._manual_language_setting()
            
            time.sleep(0.5)
            
            # 모달 열림 확인
            if not self._wait_for_element('.selection-list-wrapper', 2):
                return self._manual_language_setting()
            
            # 언어 드롭다운 클릭
            if not self._click_element('.select-language', "언어 드롭다운", 2):
                return self._manual_language_setting()
            
            time.sleep(0.3)
            
            # English 옵션 선택
            if not self._click_element('[data-val="en-US"]', "English 옵션", 2):
                return self._manual_language_setting()
            
            time.sleep(0.3)
            
            # 저장 버튼 클릭
            if not self._click_element('.save-selection', "저장 버튼", 2):
                return self._manual_language_setting()
            
            print("  페이지 변경 대기 중...")
            time.sleep(3)
            
            print("  언어 설정 완료")
            return True
            
        except Exception as e:
            print(f"  언어 설정 중 오류: {e}")
            return self._manual_language_setting()
    
    def _click_element(self, selector, element_name, timeout=5):
        """요소 클릭 헬퍼 메서드"""
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            element.click()
            print(f"    {element_name} 클릭 완료")
            return True
        except TimeoutException:
            print(f"  {element_name}을 찾을 수 없음")
            return False
    
    def _wait_for_element(self, selector, timeout=5):
        """요소 존재 확인 헬퍼 메서드"""
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return True
        except TimeoutException:
            return False
    
    def _manual_language_setting(self):
        """수동 언어 설정 가이드"""
        print("  수동 언어 설정이 필요합니다.")
        print("  브라우저에서 직접 언어를 영어로 변경해주세요.")
        return True
    
    # ========== 검색 관련 ==========
    
    def get_multiple_products(self, search_url):
        """검색 결과에서 여러 상품 정보 추출"""
        try:
            if not self.browser.safe_get(search_url):
                return []
            
            products = []
            
            # 검색 결과 로딩 대기
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, self.config.SELECTORS['product_containers']))
                )
            except TimeoutException:
                print("    검색 결과 로딩 실패")
                return []
            
            product_elements = self.driver.find_elements(
                By.CSS_SELECTOR, 
                self.config.SELECTORS['product_containers']
            )[:4]  # 최대 4개
            
            for i, element in enumerate(product_elements):
                try:
                    link_elem = element.find_element(By.CSS_SELECTOR, self.config.SELECTORS['product_link'])
                    product_url = link_elem.get_attribute("href")
                    
                    title_elem = element.find_element(By.CSS_SELECTOR, self.config.SELECTORS['product_title'])
                    product_title = title_elem.text.strip()
                    
                    if product_url and "/pr/" in product_url and product_title:
                        products.append({
                            'url': product_url,
                            'title': product_title,
                            'rank': i + 1
                        })
                
                except Exception:
                    continue
            
            return products
            
        except Exception as e:
            print(f"    검색 결과 추출 실패: {e}")
            return []
    
    # ========== 상품 페이지 정보 추출 ==========
    
    def extract_product_name(self):
        """상품명 추출 - DOM 조작"""
        for selector in self.config.SELECTORS['product_name']:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, selector)
                name = element.text.strip()
                if name and len(name) > 5:
                    return name
            except:
                continue
        return None
    
    def extract_product_code(self, product_url):
        """상품코드 추출 - URL 우선, HTML 백업"""
        # Config의 패턴 매칭 사용
        html_content = self.driver.page_source
        return self.config.extract_product_code(html_content)
    
    def extract_price_info(self):
        """가격 정보 추출 - Config의 패턴 매칭 사용"""
        try:
            html_content = self.driver.page_source
            
            # Config의 통합 패턴 매칭 사용
            price_info = self.config.extract_all_price_info(html_content)
            stock_info = self.config.extract_all_stock_info(html_content)
            
            # 형식 변환 (기존 호환성 유지)
            result = {
                'discount_price': str(price_info['current_price_krw']) if price_info['current_price_krw'] else '',
                'list_price': str(price_info['list_price_krw']) if price_info['list_price_krw'] else '',
                'discount_percent': str(price_info['discount_percent']) if price_info['discount_percent'] else '',
                'subscription_discount': str(price_info['subscription_discount']) if price_info['subscription_discount'] else '',
                'price_per_unit': price_info['price_per_unit'],
                'is_in_stock': stock_info['is_in_stock'],
                'stock_message': stock_info['stock_message'],
                'back_in_stock_date': stock_info['back_in_stock_date']
            }
            
            print(f"    가격 정보: ✓ Config 패턴 매칭 사용")
            return result
            
        except Exception as e:
            print(f"    가격 정보 추출 중 오류: {e}")
            return {}
    
    def extract_product_info_with_price(self, product_url):
        """상품 정보와 가격 정보를 함께 추출 - 메인 API"""
        try:
            print("  상품 정보 추출 중...")
            
            if not self.browser.safe_get(product_url):
                print("    상품 페이지 로딩 실패")
                return None, None, {}
            
            # 페이지 로딩 대기
            try:
                WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
                )
            except TimeoutException:
                print("    페이지 로딩 타임아웃")
            
            # 상품명 추출 (DOM)
            iherb_product_name = self.extract_product_name()
            if iherb_product_name:
                print(f"    상품명: ✓")
            else:
                print(f"    상품명: ✗ 추출 실패")
            
            # 상품코드 추출 (Config 패턴 매칭)
            product_code = self.extract_product_code(product_url)
            if product_code:
                print(f"    상품코드: ✓ {product_code}")
            else:
                print(f"    상품코드: ✗ 추출 실패")
            
            # 가격 정보 추출 (Config 패턴 매칭)
            price_info = self.extract_price_info()
            
            return product_code, iherb_product_name, price_info
            
        except Exception as e:
            print(f"    상품 정보 추출 중 오류: {e}")
            return None, None, {}
    
    # ========== 이미지 관련 ==========
    
    def extract_product_image_url(self, product_url=None):
        """상품 이미지 URL 추출"""
        try:
            if product_url:
                if not self.browser.safe_get(product_url):
                    return None
                time.sleep(2)
            
            image_selectors = [
                "#iherb-product-image",
                ".product-summary-image img",
                "img[src*='cloudinary.images-iherb.com']",
                ".product-easyzoom img",
                "img[width='400'][height='400']"
            ]
            
            for selector in image_selectors:
                try:
                    img_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    img_url = img_element.get_attribute("src")
                    if img_url and img_url.startswith("http") and "iherb.com" in img_url:
                        print(f"    이미지 URL 추출 성공: {selector}")
                        return img_url
                except:
                    continue
            
            print("    이미지 URL 추출 실패")
            return None
            
        except Exception as e:
            print(f"    이미지 URL 추출 오류: {e}")
            return None
    
    def download_product_image(self, image_url, output_path):
        """상품 이미지 다운로드"""
        try:
            if not image_url:
                return False
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Referer': 'https://www.iherb.com/'
            }
            
            response = requests.get(image_url, headers=headers, timeout=self.config.IMAGE_DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            
            # 디렉토리 생성
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            # 이미지 유효성 검사
            try:
                with Image.open(output_path) as img:
                    img.verify()
                print(f"    이미지 다운로드 성공: {os.path.basename(output_path)}")
                return True
            except:
                if os.path.exists(output_path):
                    os.remove(output_path)
                print(f"    이미지 파일 손상: {os.path.basename(output_path)}")
                return False
                
        except Exception as e:
            print(f"    이미지 다운로드 실패: {e}")
            return False
    
    def extract_and_download_image(self, product_url, output_path):
        """이미지 URL 추출 + 다운로드 통합 메서드"""
        try:
            # 이미지 URL 추출
            image_url = self.extract_product_image_url(product_url)
            if not image_url:
                return False
            
            # 이미지 다운로드
            return self.download_product_image(image_url, output_path)
            
        except Exception as e:
            print(f"    이미지 추출/다운로드 실패: {e}")
            return False