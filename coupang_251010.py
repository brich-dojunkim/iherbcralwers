import sys
import os
import time
import random
import csv
import re
import shutil
from datetime import datetime

# 기존 모듈 임포트
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'coupang'))
sys.path.insert(0, os.path.join(current_dir, 'iherbscraper'))

from coupang.coupang_manager import BrowserManager as CoupangBrowser
from iherbscraper.iherb_manager import BrowserManager as IHerbBrowser
from iherbscraper.iherb_client import IHerbClient
from iherbscraper.product_matcher import ProductMatcher

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class InfiniteScrollCrawler:
    """무한 스크롤 크롤러 - 디버깅 포함"""
    
    def __init__(self, browser, existing_ids, global_new_ids):
        self.browser = browser
        self.existing_ids = existing_ids
        self.global_new_ids = global_new_ids
        self.products = []
    
    def infinite_scroll_crawl(self, url, target_count=40):
        """무한 스크롤로 신규 상품만 수집"""
        try:
            print(f"\n{'='*80}")
            print(f"🎯 신규 상품 크롤링: {target_count}개 목표")
            print(f"{'='*80}\n")
            
            self.browser.driver.get(url)
            time.sleep(random.uniform(3, 5))
            
            self._click_sales_filter()
            
            no_new_products_count = 0
            max_no_new_attempts = 15
            category_seen_ids = set()
            scroll_count = 0
            max_scrolls = 200
            consecutive_no_height_change = 0
            
            while len(self.products) < target_count and scroll_count < max_scrolls:
                scroll_count += 1
                
                collected = self._extract_new_products(category_seen_ids)
                
                progress = (len(self.products) / target_count) * 100
                print(f"[스크롤 {scroll_count}회] 신규: {collected}개 | 누적: {len(self.products)}/{target_count}개 ({progress:.1f}%)")
                
                if collected > 0:
                    no_new_products_count = 0
                    consecutive_no_height_change = 0
                else:
                    no_new_products_count += 1
                
                # 목표 달성
                if len(self.products) >= target_count:
                    print(f"🎉 목표 달성: {len(self.products)}개!")
                    break
                
                # 스크롤
                height_changed = self._scroll_to_bottom()
                
                if not height_changed:
                    consecutive_no_height_change += 1
                    print(f"  ⚠️ 높이 변화 없음 (연속 {consecutive_no_height_change}회)")
                    
                    # 5회 연속 높이 변화 없으면서 신규도 없으면 종료
                    if consecutive_no_height_change >= 5 and no_new_products_count >= 5:
                        print(f"⚠️ 더 이상 상품을 찾을 수 없습니다")
                        break
                else:
                    consecutive_no_height_change = 0
                
                # 오래 신규 없으면 경고
                if no_new_products_count >= max_no_new_attempts:
                    print(f"⚠️ {max_no_new_attempts}회 연속 신규 0개 (계속 시도 중...)")
                
                time.sleep(random.uniform(1, 2))
            
            print(f"\n✅ 크롤링 완료: {len(self.products)}개 수집")
            if len(self.products) < target_count:
                print(f"⚠️ 목표 미달: {target_count - len(self.products)}개 부족")
                print(f"   → 해당 카테고리에 신규 상품이 없을 수 있습니다\n")
            else:
                print()
            
            return self.products
            
        except Exception as e:
            print(f"\n❌ 크롤링 오류: {e}")
            return self.products
    
    def _click_sales_filter(self):
        """판매량순 필터 클릭"""
        try:
            print("🔍 판매량순 필터 찾는 중...")
            time.sleep(2)
            
            filter_buttons = self.browser.driver.find_elements(By.CSS_SELECTOR, 'li.sortkey')
            
            for button in filter_buttons:
                if '판매량순' in button.text:
                    button.click()
                    print("✅ 판매량순 필터 적용")
                    time.sleep(3)
                    return
            
            print("⚠️ 판매량순 필터 없음 (기본 정렬로 진행)")
        except Exception as e:
            print(f"⚠️ 판매량순 필터 실패: {e}")
    
    def _extract_new_products(self, category_seen_ids):
        """신규 상품만 추출 - 디버깅 포함"""
        try:
            collected = 0
            product_elements = self.browser.driver.find_elements(By.CSS_SELECTOR, 'li.product-wrap')
            
            print(f"  🔍 페이지에서 발견한 상품: {len(product_elements)}개")
            
            duplicates = {'existing': 0, 'global': 0, 'category': 0}
            
            for element in product_elements:
                try:
                    link = element.find_element(By.CSS_SELECTOR, 'a.product-wrapper')
                    product_url = link.get_attribute('href')
                    
                    match = re.search(r'itemId=(\d+)', product_url)
                    if not match:
                        continue
                    
                    product_id = match.group(1)
                    
                    # 중복 체크 (디버깅)
                    if product_id in self.existing_ids:
                        duplicates['existing'] += 1
                        continue
                    
                    if product_id in self.global_new_ids:
                        duplicates['global'] += 1
                        continue
                    
                    if product_id in category_seen_ids:
                        duplicates['category'] += 1
                        continue
                    
                    # 상품명
                    try:
                        name_elem = element.find_element(By.CSS_SELECTOR, 'div.name')
                        product_name = name_elem.text.strip()
                    except:
                        continue
                    
                    # 가격
                    try:
                        price_elem = element.find_element(By.CSS_SELECTOR, 'strong.price-value')
                        price = price_elem.text.strip()
                    except:
                        price = ""
                    
                    if not product_name:
                        continue
                    
                    # 신규 상품 추가
                    self.products.append({
                        'product_id': product_id,
                        'product_name': product_name,
                        'product_url': product_url,
                        'current_price': price
                    })
                    
                    category_seen_ids.add(product_id)
                    self.global_new_ids.add(product_id)
                    collected += 1
                    
                    # 첫 신규 상품 로그
                    if collected == 1:
                        print(f"  ✅ 첫 신규 상품: {product_name[:30]}... (ID: {product_id})")
                    
                except:
                    continue
            
            # 중복 통계
            if duplicates['existing'] > 0 or duplicates['global'] > 0 or duplicates['category'] > 0:
                print(f"  ⚠️ 중복: 캐시 {duplicates['existing']}개 | 전역 {duplicates['global']}개 | 카테고리 {duplicates['category']}개")
            
            return collected
            
        except Exception as e:
            print(f"  ❌ 추출 오류: {e}")
            return 0
    
    def _scroll_to_bottom(self):
        """스크롤 & 새 콘텐츠 확인"""
        try:
            last_height = self.browser.driver.execute_script("return document.body.scrollHeight")
            
            # 천천히 단계적으로 스크롤
            scroll_steps = random.randint(3, 5)
            for i in range(scroll_steps):
                scroll_amount = (last_height / scroll_steps) * (i + 1)
                self.browser.driver.execute_script(f"window.scrollTo(0, {scroll_amount});")
                time.sleep(0.3)
            
            # 완전히 끝까지
            self.browser.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # 새 콘텐츠 로딩 대기 (최대 5초)
            for _ in range(5):
                time.sleep(1)
                new_height = self.browser.driver.execute_script("return document.body.scrollHeight")
                if new_height > last_height:
                    return True
            
            return False
            
        except:
            return False


class CoupangIHerbCrawler:
    """쿠팡 -> 아이허브 크롤러"""
    
    def __init__(self, 
                 output_file="coupang_iherb_products.csv",
                 cache_file="coupang_products_cache.csv",
                 restart_interval=10):
        """
        Args:
            output_file: 매칭 결과 CSV
            cache_file: 쿠팡 상품 캐시
            restart_interval: N개마다 브라우저 재시작
        """
        self.output_file = output_file
        self.cache_file = cache_file
        self.restart_interval = restart_interval
        
        self.csv_file = None
        self.csv_writer = None
        
        # 데이터
        self.cache_product_ids = set()
        self.matched_product_ids = set()
        self.all_cache_products = []
        self.new_products = []
        self.global_new_ids = set()
        
        # 브라우저
        self.coupang_browser = None
        self.iherb_browser = None
        self.iherb_client = None
        self.product_matcher = None
        
        self._load_data()
        self._init_csv()
    
    def _load_data(self):
        """데이터 로드"""
        # 캐시 로드
        if os.path.exists(self.cache_file):
            print(f"📂 캐시 로드: {self.cache_file}")
            try:
                with open(self.cache_file, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        product_id = row.get('상품ID', '')
                        if product_id:
                            self.cache_product_ids.add(product_id)
                        self.all_cache_products.append(row)
                
                total_rows = len(self.all_cache_products)
                unique_ids = len(self.cache_product_ids)
                duplicates = total_rows - unique_ids
                
                print(f"  ✅ 캐시 전체: {total_rows}개")
                print(f"  ✅ 유니크 상품: {unique_ids}개")
                if duplicates > 0:
                    print(f"  ⚠️ 중복: {duplicates}개")
            except Exception as e:
                print(f"  ⚠️ 캐시 로드 실패: {e}")
        
        # 매칭 결과 로드
        if os.path.exists(self.output_file):
            print(f"📂 매칭 결과 로드: {self.output_file}")
            try:
                with open(self.output_file, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        url = row.get('쿠팡_상품URL', '')
                        match = re.search(r'itemId=(\d+)', url)
                        if match:
                            self.matched_product_ids.add(match.group(1))
                
                print(f"  ✅ 매칭 완료: {len(self.matched_product_ids)}개\n")
            except Exception as e:
                print(f"  ⚠️ 매칭 결과 로드 실패: {e}\n")
    
    def _init_csv(self):
        """CSV 초기화"""
        file_exists = os.path.exists(self.output_file)
        
        if file_exists:
            self.csv_file = open(self.output_file, 'a', newline='', encoding='utf-8-sig')
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=[
                '수집순서', '카테고리', '쿠팡_상품URL', '쿠팡_제품명',
                '쿠팡_비회원가격', '아이허브_UPC', '아이허브_파트넘버', '수집시간'
            ])
            print(f"📝 CSV 파일 열기: {self.output_file}\n")
        else:
            self.csv_file = open(self.output_file, 'w', newline='', encoding='utf-8-sig')
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=[
                '수집순서', '카테고리', '쿠팡_상품URL', '쿠팡_제품명',
                '쿠팡_비회원가격', '아이허브_UPC', '아이허브_파트넘버', '수집시간'
            ])
            self.csv_writer.writeheader()
            print(f"✅ CSV 파일 생성: {self.output_file}\n")
        
        self.csv_file.flush()
    
    def crawl_new_products(self, categories):
        """신규 쿠팡 상품 크롤링"""
        print("\n" + "="*80)
        print("📦 쿠팡 크롤링")
        print("="*80)
        
        self.coupang_browser = CoupangBrowser(headless=False)
        if not self.coupang_browser.start_driver():
            raise Exception("브라우저 시작 실패")
        
        crawler = InfiniteScrollCrawler(
            self.coupang_browser,
            self.cache_product_ids,
            self.global_new_ids
        )
        
        for category in categories:
            print(f"\n📂 {category['name']}: {category['count']}개 목표")
            
            crawler.products = []
            products = crawler.infinite_scroll_crawl(category['url'], category['count'])
            
            if products:
                for p in products:
                    p['category'] = category['name']
                self.new_products.extend(products)
                print(f"✅ {len(products)}개 수집")
            else:
                print(f"⚠️ 수집 실패 (신규 상품 없음)")
        
        self.coupang_browser.close()
        print(f"\n✅ 총 {len(self.new_products)}개 신규 수집\n")
        
        self._update_cache()
    
    def _update_cache(self):
        """캐시 업데이트"""
        if not self.new_products:
            return
        
        # 백업
        if os.path.exists(self.cache_file):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup = f"{self.cache_file}.{timestamp}.backup"
            shutil.copy(self.cache_file, backup)
            print(f"💾 백업: {backup}")
        
        # 추가
        for p in self.new_products:
            self.all_cache_products.append({
                '카테고리': p['category'],
                '상품ID': p['product_id'],
                '상품명': p['product_name'],
                '상품URL': p['product_url'],
                '가격': p['current_price']
            })
        
        # 저장
        try:
            with open(self.cache_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=['카테고리', '상품ID', '상품명', '상품URL', '가격'])
                writer.writeheader()
                
                # 기존 데이터 정리 (필요한 컬럼만)
                clean_rows = []
                for row in self.all_cache_products:
                    clean_row = {
                        '카테고리': row.get('카테고리', ''),
                        '상품ID': row.get('상품ID', ''),
                        '상품명': row.get('상품명', ''),
                        '상품URL': row.get('상품URL', ''),
                        '가격': row.get('가격', '')
                    }
                    clean_rows.append(clean_row)
                
                writer.writerows(clean_rows)
            
            print(f"💾 캐시 업데이트: {len(clean_rows)}개\n")
        except Exception as e:
            print(f"⚠️ 캐시 저장 실패: {e}\n")
    
    def match_iherb_products(self):
        """아이허브 매칭"""
        print("\n" + "="*80)
        print("🔍 아이허브 매칭")
        print("="*80 + "\n")
        
        self.iherb_browser = IHerbBrowser(headless=False)
        self.iherb_client = IHerbClient(self.iherb_browser)
        self.product_matcher = ProductMatcher(self.iherb_client)
        print("✅ 아이허브 브라우저 준비\n")
        
        start_time = time.time()
        completed = 0
        restart_counter = 0
        skipped = 0
        
        for idx, product in enumerate(self.all_cache_products, 1):
            product_id = product.get('상품ID', '')
            
            # 이미 매칭 완료
            if product_id in self.matched_product_ids:
                skipped += 1
                if skipped <= 3:
                    print(f"[{idx}/{len(self.all_cache_products)}] ⏭️ 스킵: {product_id}")
                elif skipped == 4:
                    print(f"... (이후 스킵 로그 생략)")
                continue
            
            # 진행률
            if completed > 0:
                elapsed = time.time() - start_time
                avg = elapsed / completed
                remaining = (len(self.all_cache_products) - idx + 1) * avg
                h = int(remaining // 3600)
                m = int((remaining % 3600) // 60)
                print(f"\n⏱️ 예상 잔여: {h}h {m}m | 재시작까지: {self.restart_interval - restart_counter}개")
            
            try:
                success = self._match_and_save(
                    idx,
                    product.get('카테고리', ''),
                    product.get('상품URL', ''),
                    product.get('상품명', ''),
                    product.get('가격', ''),
                    product_id
                )
                
                if success:
                    completed += 1
                    restart_counter += 1
                
                # 브라우저 재시작
                if restart_counter >= self.restart_interval:
                    print("\n" + "="*80)
                    print("🔄 브라우저 재시작")
                    print("="*80)
                    
                    try:
                        self.iherb_browser.close()
                    except:
                        pass
                    
                    time.sleep(3)
                    
                    self.iherb_browser = IHerbBrowser(headless=False)
                    self.iherb_client = IHerbClient(self.iherb_browser)
                    self.product_matcher = ProductMatcher(self.iherb_client)
                    print("✅ 재시작 완료\n")
                    
                    restart_counter = 0
                    
            except KeyboardInterrupt:
                print(f"\n{'='*80}")
                print(f"⚠️ 중단")
                print(f"{'='*80}")
                print(f"처리: {completed}개 | 스킵: {skipped}개")
                print(f"💾 진행사항 저장됨")
                print(f"{'='*80}")
                return
        
        self.iherb_browser.close()
        
        print(f"\n{'='*80}")
        print(f"🎉 매칭 완료!")
        print(f"{'='*80}")
        print(f"처리: {completed}개 | 스킵: {skipped}개")
        print(f"{'='*80}")

    def _match_and_save(self, idx, category, url, name, price, product_id):
        """매칭 & 저장"""
        print(f"\n[{idx}/{len(self.all_cache_products)}] {name[:50]}...")
        
        upc = ""
        part_number = ""
        
        try:
            result = self.product_matcher.search_product_enhanced(name, product_id)
            
            if result and len(result) == 3:
                matched_url, score, details = result
                
                if matched_url:
                    print(f"  ✅ 매칭: {matched_url[:50]}...")
                    
                    code, iherb_name, price_info = \
                        self.iherb_client.extract_product_info_with_price(matched_url)
                    
                    if code:
                        part_number = code
                        print(f"  📦 파트넘버: {part_number}")
                        
                        upc = self._extract_upc()
                        if upc:
                            print(f"  🔢 UPC: {upc}")
                else:
                    reason = details.get('reason', 'unknown') if isinstance(details, dict) else 'unknown'
                    print(f"  ❌ 실패: {reason}")
        
        except Exception as e:
            error = str(e)
            
            if "quota" in error.lower() or "limit" in error.lower():
                print(f"\n⚠️ API 할당량 초과!")
                raise KeyboardInterrupt("API 제한")
            
            print(f"  ❌ 오류: {error[:50]}...")
        
        # 저장
        row = {
            '수집순서': idx,
            '카테고리': category,
            '쿠팡_상품URL': url,
            '쿠팡_제품명': name,
            '쿠팡_비회원가격': price,
            '아이허브_UPC': upc,
            '아이허브_파트넘버': part_number,
            '수집시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        try:
            self.csv_writer.writerow(row)
            self.csv_file.flush()
            os.fsync(self.csv_file.fileno())
        except Exception as e:
            print(f"  ⚠️ 저장 실패: {e}")
        
        self.matched_product_ids.add(product_id)
        time.sleep(random.uniform(1, 2))
        
        return True
    
    def _extract_upc(self):
        """UPC 추출"""
        try:
            elements = self.iherb_browser.driver.find_elements(
                By.XPATH, 
                "//ul[@id='product-specs-list']//li[contains(text(), 'UPC')]"
            )
            
            for elem in elements:
                match = re.search(r'UPC[^:]*:\s*([0-9]{12,13})', elem.text, re.IGNORECASE)
                if match:
                    upc = match.group(1)
                    if len(upc) in [12, 13]:
                        return upc
            
            return ""
        except:
            return ""
    
    def run(self, categories):
        """
        실행
        
        Args:
            categories: [
                {'name': '헬스/건강식품', 'url': '...', 'count': 20},
            ]
        """
        try:
            # 상태 확인
            cache_total = len(self.cache_product_ids)
            matched_total = len(self.matched_product_ids)
            pending = cache_total - matched_total
            
            print("\n" + "="*80)
            print("📊 현재 상태")
            print("="*80)
            print(f"유니크 상품: {cache_total}개")
            print(f"매칭 완료: {matched_total}개")
            print(f"매칭 대기: {pending}개")
            print("="*80)
            
            # 로직: 매칭 완료 → 크롤링 / 미완료 → 매칭
            if pending == 0:
                print(f"\n✅ 모든 매칭 완료 → 신규 크롤링 시작")
                total_target = sum(c['count'] for c in categories)
                print(f"   목표: {total_target}개 추가")
                self.crawl_new_products(categories)
                
                # 크롤링 후 매칭
                if self.new_products:
                    print(f"\n→ 신규 상품 매칭 시작")
                    self.match_iherb_products()
                else:
                    print(f"\n⚠️ 신규 상품이 없어 매칭 스킵")
            else:
                print(f"\n⏭️ 크롤링 스킵 → 기존 {pending}개 매칭 우선")
                self.match_iherb_products()
            
        except KeyboardInterrupt:
            print("\n⚠️ 중단")
        except Exception as e:
            print(f"\n❌ 오류: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()
        
    def cleanup(self):
        """정리"""
        print("\n🧹 정리 중...")
        
        if self.csv_file:
            self.csv_file.close()
            print("  ✅ CSV 닫기")
        
        if self.coupang_browser:
            try:
                self.coupang_browser.close()
            except:
                pass
        
        if self.iherb_browser:
            try:
                self.iherb_browser.close()
            except:
                pass


if __name__ == "__main__":
    crawler = CoupangIHerbCrawler(
        output_file="coupang_iherb_products.csv",
        cache_file="coupang_products_cache.csv",
        restart_interval=10
    )
    
    # 여기만 수정!
    categories = [
        {
            'name': '헬스/건강식품',
            'url': 'https://shop.coupang.com/coupangus/74511?category=305433&platform=p&brandId=0',
            'count': 200
        },
        {
            'name': '출산유아동',
            'url': 'https://shop.coupang.com/coupangus/74511?category=219079&platform=p&brandId=0',
            'count': 200
        },
        {
            'name': '스포츠레저',
            'url': 'https://shop.coupang.com/coupangus/74511?category=317675&platform=p&brandId=0',
            'count': 200
        }
    ]
    
    crawler.run(categories)