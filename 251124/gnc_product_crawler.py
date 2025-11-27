"""
GNC 전체 상품 크롤러
- undetected-chromedriver 사용
- 메인 카테고리별 URL 수집
- Load More 버튼 클릭으로 모든 상품 수집
"""

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time
import random
from typing import List, Dict
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GNCCrawler:
    def __init__(self, headless=False):
        """크롤러 초기화"""
        options = uc.ChromeOptions()
        if headless:
            options.add_argument('--headless')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--window-size=1920,1080')
        
        self.driver = uc.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 10)
        self.all_products = []
        
    def get_category_urls(self) -> List[Dict[str, str]]:
        """메인 카테고리 URL 추출 - 프로젝트 파일에서 추출"""
        logger.info("메인 카테고리 URL 수집 중...")
        
        categories = []
        
        # HTML 파일에서 추출한 주요 카테고리 URL
        # lvl2-nav-link 클래스에서 추출
        category_data = [
            {'name': 'Vitamins & Supplements', 'url': 'https://www.gnc.com/vitamins-supplements/'},
            {'name': 'Protein & Fitness', 'url': 'https://www.gnc.com/protein-fitness/'},
            {'name': 'Weight Management', 'url': 'https://www.gnc.com/weight-management/'},
            {'name': 'Healthy Aging', 'url': 'https://www.gnc.com/healthy-aging/'},
            {'name': 'Superfoods & Greens', 'url': 'https://www.gnc.com/superfoods-greens/'},
            {'name': 'Herbs', 'url': 'https://www.gnc.com/herbs/'},
            {'name': 'Food & Drink', 'url': 'https://www.gnc.com/food-drink/'},
            {'name': 'Beauty & Personal Care', 'url': 'https://www.gnc.com/beauty-personal-care/'},
            {'name': 'Clothing & Accessories', 'url': 'https://www.gnc.com/clothing-accessories/'},
            {'name': 'Sale', 'url': 'https://www.gnc.com/sale/'},
        ]
        
        for cat in category_data:
            categories.append(cat)
            logger.info(f"카테고리 추가: {cat['name']} - {cat['url']}")
        
        logger.info(f"총 {len(categories)}개 카테고리 준비")
        return categories
    
    def scroll_and_click_load_more(self) -> bool:
        """Load More 버튼 클릭"""
        try:
            # 페이지 하단으로 스크롤
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(1.5, 2.5))
            
            # Load More 버튼 찾기
            load_more_btn = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.load-more-btn'))
            )
            
            # 버튼이 보이도록 스크롤
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more_btn)
            time.sleep(1)
            
            # 클릭
            load_more_btn.click()
            logger.info("Load More 버튼 클릭 성공")
            time.sleep(random.uniform(2, 3))
            
            return True
            
        except TimeoutException:
            logger.info("더 이상 Load More 버튼이 없습니다")
            return False
        except Exception as e:
            logger.error(f"Load More 클릭 오류: {e}")
            return False
    
    def extract_products_from_page(self) -> List[Dict]:
        """현재 페이지의 모든 상품 정보 추출"""
        products = []
        
        try:
            # 모든 상품 타일 찾기
            product_tiles = self.driver.find_elements(By.CSS_SELECTOR, 'div.product-tile')
            logger.info(f"현재 페이지에서 {len(product_tiles)}개 상품 발견")
            
            for tile in product_tiles:
                try:
                    product = {}
                    
                    # 상품 ID
                    try:
                        product['product_id'] = tile.get_attribute('data-itemid')
                    except:
                        product['product_id'] = None
                    
                    # 브랜드
                    try:
                        brand = tile.find_element(By.CSS_SELECTOR, 'span.tile-brand-name')
                        product['brand'] = brand.text.strip()
                    except:
                        product['brand'] = None
                    
                    # 상품명
                    try:
                        name = tile.find_element(By.CSS_SELECTOR, 'span.tile-product-name')
                        product['product_name'] = name.text.strip()
                    except:
                        product['product_name'] = None
                    
                    # 상품 URL
                    try:
                        url_link = tile.find_element(By.CSS_SELECTOR, 'a.name-link')
                        product['product_url'] = url_link.get_attribute('href')
                    except:
                        product['product_url'] = None
                    
                    # 가격
                    try:
                        price = tile.find_element(By.CSS_SELECTOR, 'span.product-standard-price')
                        product['price'] = price.text.strip()
                    except:
                        product['price'] = None
                    
                    # 서빙 수
                    try:
                        servings = tile.find_element(By.CSS_SELECTOR, 'div.atc-servings span')
                        product['servings'] = servings.text.strip()
                    except:
                        product['servings'] = None
                    
                    # 리뷰 수
                    try:
                        review_count = tile.find_element(By.CSS_SELECTOR, 'div.product-review span')
                        product['review_count'] = review_count.text.strip()
                    except:
                        product['review_count'] = None
                    
                    # 평점
                    try:
                        rating_box = tile.find_element(By.CSS_SELECTOR, 'div[class*="TTrating"]')
                        rating_class = rating_box.get_attribute('class')
                        # TTrating-4-5 형태에서 평점 추출
                        if 'TTrating-' in rating_class:
                            rating_str = rating_class.split('TTrating-')[1].split()[0]
                            product['rating'] = rating_str.replace('-', '.')
                        else:
                            product['rating'] = None
                    except:
                        product['rating'] = None
                    
                    # 프로모션 정보
                    try:
                        promo = tile.find_element(By.CSS_SELECTOR, 'div.promotional-message')
                        product['promotion'] = promo.text.strip()
                    except:
                        product['promotion'] = None
                    
                    # 이미지 URL
                    try:
                        img = tile.find_element(By.CSS_SELECTOR, 'img.product-tile-img')
                        product['image_url'] = img.get_attribute('src') or img.get_attribute('data-src')
                    except:
                        product['image_url'] = None
                    
                    products.append(product)
                    
                except Exception as e:
                    logger.error(f"개별 상품 추출 오류: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"상품 목록 추출 오류: {e}")
        
        return products
    
    def crawl_category(self, category_name: str, category_url: str) -> List[Dict]:
        """특정 카테고리의 모든 상품 크롤링"""
        logger.info(f"\n{'='*60}")
        logger.info(f"카테고리 크롤링 시작: {category_name}")
        logger.info(f"URL: {category_url}")
        logger.info(f"{'='*60}")
        
        self.driver.get(category_url)
        time.sleep(random.uniform(3, 5))
        
        category_products = []
        previous_count = 0
        no_change_count = 0
        
        # 첫 페이지 상품 수집
        products = self.extract_products_from_page()
        for product in products:
            product['category'] = category_name
        category_products.extend(products)
        logger.info(f"초기 {len(products)}개 상품 수집")
        
        # Load More 버튼 반복 클릭
        while True:
            current_count = len(category_products)
            
            # Load More 클릭
            if not self.scroll_and_click_load_more():
                break
            
            # 새로 로드된 상품 추출
            all_current_products = self.extract_products_from_page()
            
            # 새로운 상품만 추가 (중복 제거)
            existing_ids = {p['product_id'] for p in category_products}
            new_products = [
                p for p in all_current_products 
                if p['product_id'] not in existing_ids
            ]
            
            for product in new_products:
                product['category'] = category_name
            
            category_products.extend(new_products)
            logger.info(f"추가 {len(new_products)}개 상품 수집 (총: {len(category_products)}개)")
            
            # 더 이상 새로운 상품이 없으면 종료
            if current_count == len(category_products):
                no_change_count += 1
                if no_change_count >= 2:
                    logger.info("더 이상 새로운 상품이 없습니다")
                    break
            else:
                no_change_count = 0
            
            # 과도한 반복 방지
            if len(category_products) > 5000:
                logger.warning("상품 수가 5000개를 초과했습니다")
                break
        
        logger.info(f"카테고리 '{category_name}' 크롤링 완료: {len(category_products)}개 상품")
        return category_products
    
    def crawl_all_categories(self) -> pd.DataFrame:
        """모든 카테고리 크롤링"""
        logger.info("\n" + "="*60)
        logger.info("GNC 전체 카테고리 크롤링 시작")
        logger.info("="*60 + "\n")
        
        categories = self.get_category_urls()
        
        for i, category in enumerate(categories, 1):
            logger.info(f"\n진행 상황: [{i}/{len(categories)}]")
            
            try:
                products = self.crawl_category(category['name'], category['url'])
                self.all_products.extend(products)
                
                # 카테고리 간 대기
                if i < len(categories):
                    wait_time = random.uniform(5, 10)
                    logger.info(f"{wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)
                    
            except Exception as e:
                logger.error(f"카테고리 '{category['name']}' 크롤링 실패: {e}")
                continue
        
        logger.info(f"\n{'='*60}")
        logger.info(f"전체 크롤링 완료: 총 {len(self.all_products)}개 상품")
        logger.info(f"{'='*60}\n")
        
        # DataFrame 생성
        df = pd.DataFrame(self.all_products)
        return df
    
    def save_to_csv(self, filename: str = 'gnc_products.csv'):
        """결과를 CSV로 저장"""
        if not self.all_products:
            logger.warning("저장할 데이터가 없습니다")
            return
        
        df = pd.DataFrame(self.all_products)
        
        # 컬럼 순서 정리
        column_order = [
            'category',
            'product_id',
            'brand',
            'product_name',
            'price',
            'servings',
            'rating',
            'review_count',
            'promotion',
            'product_url',
            'image_url'
        ]
        
        # 존재하는 컬럼만 선택
        available_columns = [col for col in column_order if col in df.columns]
        df = df[available_columns]
        
        # 중복 제거 (product_id 기준)
        df = df.drop_duplicates(subset=['product_id'], keep='first')
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logger.info(f"CSV 파일 저장 완료: {filename}")
        logger.info(f"총 {len(df)}개 상품 저장")
        
        return df
    
    def close(self):
        """브라우저 종료"""
        if self.driver:
            self.driver.quit()
            logger.info("브라우저 종료")


def main():
    """메인 실행 함수"""
    crawler = GNCCrawler(headless=False)
    
    try:
        # 모든 카테고리 크롤링
        df = crawler.crawl_all_categories()
        
        # CSV 저장
        output_file = '/Users/brich/Desktop/iherb_price/251124'
        crawler.save_to_csv(output_file)
        
        # 결과 요약
        print(f"\n{'='*60}")
        print("크롤링 결과 요약")
        print(f"{'='*60}")
        print(f"총 상품 수: {len(df)}")
        print(f"\n카테고리별 상품 수:")
        print(df['category'].value_counts())
        print(f"\n브랜드별 상품 수 (상위 10개):")
        print(df['brand'].value_counts().head(10))
        
    except Exception as e:
        logger.error(f"크롤링 중 오류 발생: {e}")
        
    finally:
        crawler.close()


if __name__ == "__main__":
    main()