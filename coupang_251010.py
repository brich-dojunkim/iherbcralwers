
import sys
import os
import time
import random
import csv
import re
import shutil
from datetime import datetime

# 기존 모듈 임포트 - 경로 설정
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
    """무한 스크롤 크롤러 - 신규 상품만 수집"""
    
    def __init__(self, browser, existing_ids, global_new_ids):
        self.browser = browser
        self.existing_ids = existing_ids  # 기존 774개 블랙리스트
        self.global_new_ids = global_new_ids  # 전체 신규 수집 ID
        self.products = []
    
    def infinite_scroll_crawl(self, url, target_count=300):
        """무한 스크롤로 신규 상품만 수집"""
        try:
            print(f"\n{'='*80}")
            print(f"🎯 신규 상품 크롤링 시작: {target_count}개 목표")
            print(f"   기존 {len(self.existing_ids)}개 제외")
            print(f"{'='*80}\n")
            
            # 페이지 로드
            self.browser.driver.get(url)
            time.sleep(random.uniform(3, 5))
            
            # 판매량순 필터 클릭
            self._click_sales_filter()
            
            no_new_products_count = 0
            max_no_new_attempts = 8  # 8번 연속 새 상품 없으면 종료
            category_seen_ids = set()  # 이 카테고리에서 수집한 ID
            scroll_count = 0
            max_scrolls = 100  # 최대 100번 스크롤
            
            # 누적 통계
            total_stats = {
                'total_found': 0,
                'skipped_existing': 0,
                'skipped_already_collected': 0,
                'skipped_duplicate_in_page': 0,
                'collected': 0
            }
            
            while len(self.products) < target_count and scroll_count < max_scrolls:
                scroll_count += 1
                
                # 현재 페이지 상품 추출 (통계 포함)
                stats = self._extract_new_products(category_seen_ids)
                
                # 누적 통계 업데이트
                for key in total_stats:
                    total_stats[key] += stats[key]
                
                # 상세 출력
                print(f"\n{'─'*60}")
                print(f"[스크롤 {scroll_count}회]")
                print(f"{'─'*60}")
                print(f"페이지 전체: {stats['total_found']}개 (DOM에 로드된 총 상품)")
                
                if stats['skipped_existing'] > 0 or stats['skipped_already_collected'] > 0 or stats['skipped_duplicate_in_page'] > 0 or stats['collected'] > 0:
                    print(f"이번 라운드 처리:")
                
                if stats['skipped_existing'] > 0:
                    print(f"  ├─ 기존 776개 중복: {stats['skipped_existing']}개 ❌")
                if stats['skipped_already_collected'] > 0:
                    print(f"  ├─ 이미 수집한 상품: {stats['skipped_already_collected']}개 ❌")
                if stats['skipped_duplicate_in_page'] > 0:
                    print(f"  ├─ 페이지 내 중복: {stats['skipped_duplicate_in_page']}개 ❌")
                
                if stats['collected'] > 0:
                    print(f"  └─ 🎯 신규 수집: {stats['collected']}개 ✅")
                    no_new_products_count = 0
                else:
                    print(f"  └─ 🎯 신규 수집: 0개 ⚠️")
                    no_new_products_count += 1
                
                # 진행률
                progress = (len(self.products) / target_count) * 100
                print(f"\n📊 누적 진행: {len(self.products)}/{target_count}개 ({progress:.1f}%)")
                print(f"{'─'*60}")
                
                # 종료 조건 체크
                if stats['collected'] == 0:
                    if no_new_products_count >= max_no_new_attempts:
                        print(f"\n{'='*60}")
                        print(f"⚠️  더 이상 새 상품이 없습니다.")
                        print(f"   ({no_new_products_count}번 연속 신규 상품 0개)")
                        print(f"{'='*60}")
                        break
                
                # 목표 달성 체크
                if len(self.products) >= target_count:
                    print(f"\n{'='*60}")
                    print(f"🎉 목표 달성: {len(self.products)}개 수집!")
                    print(f"{'='*60}")
                    break
                
                # 스크롤 (새 콘텐츠 로드 확인)
                has_new_content = self._scroll_to_bottom()
                
                # 페이지 끝에 도달했는데 목표 미달성
                if not has_new_content:
                    print(f"\n{'='*60}")
                    print(f"⚠️  페이지 끝 도달")
                    print(f"   최종 수집: {len(self.products)}개 (목표: {target_count}개)")
                    print(f"{'='*60}")
                    break
                
                # 추가 대기 (무한 스크롤 안정화)
                time.sleep(random.uniform(1, 2))
            
            # 최종 통계
            print(f"\n{'='*80}")
            print(f"📊 크롤링 완료 - 누적 통계")
            print(f"{'='*80}")
            print(f"총 스크롤: {scroll_count}회")
            print(f"발견한 상품: {total_stats['total_found']}개")
            print(f"")
            print(f"필터링 내역:")
            print(f"  ├─ 기존 776개 중복: {total_stats['skipped_existing']}개")
            print(f"  ├─ 이미 수집한 상품: {total_stats['skipped_already_collected']}개")
            print(f"  └─ 페이지 내 중복: {total_stats['skipped_duplicate_in_page']}개")
            print(f"")
            print(f"✅ 신규 수집: {total_stats['collected']}개")
            print(f"{'='*80}\n")
            
            if scroll_count >= max_scrolls:
                print(f"⚠️ 최대 스크롤 횟수({max_scrolls}회) 도달")
            
            return self.products
            
        except Exception as e:
            print(f"\n❌ 크롤링 오류: {e}")
            return self.products
    
    def _click_sales_filter(self):
        """판매량순 필터 클릭"""
        try:
            print("🔍 판매량순 필터 찾는 중...")
            time.sleep(2)
            
            # 판매량순 버튼 찾기
            filter_button = WebDriverWait(self.browser.driver, 10).until(
                EC.presence_of_element_located((
                    By.XPATH, 
                    "//button[contains(text(), '판매량순') or contains(@class, 'sales')]"
                ))
            )
            
            filter_button.click()
            print("✅ 판매량순 필터 클릭")
            time.sleep(3)
            
        except Exception as e:
            print(f"⚠️ 판매량순 필터 찾기 실패: {e}")
    
    def _extract_new_products(self, category_seen_ids):
        """현재 화면에서 신규 상품만 추출 - 상세 통계 포함"""
        try:
            # 통계 변수
            stats = {
                'total_found': 0,                # 화면에서 발견한 총 상품 수
                'skipped_existing': 0,            # 기존 776개 중복
                'skipped_already_collected': 0,   # 이번 크롤링에서 이미 수집한 상품 (수정!)
                'skipped_duplicate_in_page': 0,   # 같은 페이지 내 중복
                'collected': 0                    # 신규 수집
            }
            
            # 상품 요소 찾기
            product_elements = self.browser.driver.find_elements(
                By.CSS_SELECTOR,
                'li.product-wrap'
            )
            
            stats['total_found'] = len(product_elements)
            seen_this_round = set()  # 이번 추출에서 본 ID들
            
            for element in product_elements:
                try:
                    # 상품 링크
                    link_element = element.find_element(By.CSS_SELECTOR, 'a.product-wrapper')
                    product_url = link_element.get_attribute('href')
                    
                    # Product ID 추출
                    match = re.search(r'itemId=(\d+)', product_url)
                    if not match:
                        continue
                    
                    product_id = match.group(1)
                    
                    # 이번 라운드에서 이미 처리했는가? (같은 페이지 내 중복)
                    if product_id in seen_this_round:
                        stats['skipped_duplicate_in_page'] += 1
                        continue
                    seen_this_round.add(product_id)
                    
                    # 🔥 필터링 로직 (통계 수집)
                    
                    # 1. 기존 776개에 있는가?
                    if product_id in self.existing_ids:
                        stats['skipped_existing'] += 1
                        continue
                    
                    # 2. 이번 크롤링에서 이미 수집했는가? (전체 카테고리 포함)
                    if product_id in self.global_new_ids:
                        stats['skipped_already_collected'] += 1
                        continue
                    
                    # 3. 이 카테고리에서 이미 수집했는가? (중복 체크, 위에서 걸러져야 함)
                    if product_id in category_seen_ids:
                        stats['skipped_already_collected'] += 1
                        continue
                    
                    # ✅ 완전히 새로운 상품!
                    
                    # 상품명
                    try:
                        name_element = element.find_element(By.CSS_SELECTOR, 'div[data-v-1acd0e2a].name')
                        product_name = name_element.text.strip()
                    except:
                        try:
                            name_element = element.find_element(By.CSS_SELECTOR, 'div.name')
                            product_name = name_element.text.strip()
                        except:
                            continue
                    
                    # 가격
                    try:
                        price_element = element.find_element(By.CSS_SELECTOR, 'strong.price-value')
                        price = price_element.text.strip()
                    except:
                        price = ""
                    
                    if not product_name:
                        continue
                    
                    # 신규 상품 추가
                    product = {
                        'product_id': product_id,
                        'product_name': product_name,
                        'product_url': product_url,
                        'current_price': price
                    }
                    
                    self.products.append(product)
                    category_seen_ids.add(product_id)
                    self.global_new_ids.add(product_id)
                    stats['collected'] += 1
                    
                except Exception as e:
                    continue
            
            return stats
            
        except Exception as e:
            print(f"  ⚠️ 상품 추출 오류: {e}")
            return {
                'total_found': 0,
                'skipped_existing': 0,
                'skipped_already_collected': 0,
                'skipped_duplicate_in_page': 0,
                'collected': 0
            }
    
    def _scroll_to_bottom(self):
        """페이지 끝까지 천천히 스크롤 (무한 스크롤 대응)"""
        try:
            # 현재 페이지 높이
            last_height = self.browser.driver.execute_script("return document.body.scrollHeight")
            
            # 단계적으로 스크롤 (3-5단계)
            scroll_steps = random.randint(3, 5)
            scroll_pause = 0.5  # 각 단계마다 0.5초 대기
            
            for i in range(scroll_steps):
                # 일부만 스크롤
                scroll_amount = (last_height / scroll_steps) * (i + 1)
                self.browser.driver.execute_script(f"window.scrollTo(0, {scroll_amount});")
                time.sleep(scroll_pause)
            
            # 마지막으로 완전히 끝까지
            self.browser.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # 새 콘텐츠 로딩 대기 (최대 5초)
            wait_time = 0
            max_wait = 5
            
            while wait_time < max_wait:
                time.sleep(1)
                wait_time += 1
                
                # 새 높이 확인
                new_height = self.browser.driver.execute_script("return document.body.scrollHeight")
                
                # 높이가 증가했으면 (새 콘텐츠 로드됨)
                if new_height > last_height:
                    print(f"  📥 새 콘텐츠 로드됨 ({last_height} → {new_height})")
                    return True
            
            # 높이 변화 없음 (더 이상 로드할 콘텐츠 없음)
            print(f"  ⚠️ 페이지 끝 도달 (높이 변화 없음)")
            return False
            
        except Exception as e:
            print(f"  ⚠️ 스크롤 오류: {e}")
            return False


class CoupangIHerbAdd900Crawler:
    """쿠팡 -> 아이허브 크롤러: 신규 900개 추가"""
    
    def __init__(self, 
                 output_file="coupang_iherb_products.csv",
                 cache_file="coupang_products_cache.csv"):
        self.output_file = output_file
        self.cache_file = cache_file
        
        self.csv_file = None
        self.csv_writer = None
        
        # 기존 데이터
        self.existing_product_ids = set()  # 기존 774개 ID
        self.matched_product_ids = set()   # 이미 매칭 완료된 ID
        
        # 신규 데이터
        self.new_products = []  # 신규 수집할 900개
        self.global_new_ids = set()  # 전체 신규 ID (카테고리 간 중복 방지)
        
        # 전체 상품 (기존 + 신규)
        self.all_products = []
        
        # 브라우저
        self.coupang_browser = None
        self.iherb_browser = None
        self.iherb_client = None
        self.product_matcher = None
        
        self._load_existing_data()
        self._init_csv()
    
    def _load_existing_data(self):
        """기존 데이터 로드"""
        # 1. 기존 캐시에서 상품 ID 로드 (블랙리스트)
        if os.path.exists(self.cache_file):
            print(f"📂 기존 캐시 로드: {self.cache_file}")
            try:
                with open(self.cache_file, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        product_id = row.get('상품ID', '')
                        if product_id:
                            self.existing_product_ids.add(product_id)
                        self.all_products.append(row)
                
                print(f"  ✅ 기존 상품: {len(self.existing_product_ids)}개")
            except Exception as e:
                print(f"  ⚠️ 캐시 로드 실패: {e}")
        
        # 2. CSV에서 이미 매칭 완료된 ID 로드
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
                print(f"  ⚠️ CSV 로드 실패: {e}\n")
    
    def _init_csv(self):
        """CSV 파일 초기화"""
        file_exists = os.path.exists(self.output_file)
        
        if file_exists:
            # Append 모드
            self.csv_file = open(self.output_file, 'a', newline='', encoding='utf-8-sig')
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=[
                '수집순서', '카테고리', '쿠팡_상품URL', '쿠팡_제품명',
                '쿠팡_비회원가격', '아이허브_UPC', '아이허브_파트넘버', '수집시간'
            ])
            print(f"📝 CSV 파일 열기 (Append): {self.output_file}\n")
        else:
            # 새로 생성
            self.csv_file = open(self.output_file, 'w', newline='', encoding='utf-8-sig')
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=[
                '수집순서', '카테고리', '쿠팡_상품URL', '쿠팡_제품명',
                '쿠팡_비회원가격', '아이허브_UPC', '아이허브_파트넘버', '수집시간'
            ])
            self.csv_writer.writeheader()
            print(f"✅ CSV 파일 생성: {self.output_file}\n")
        
        self.csv_file.flush()
    
    def crawl_new_coupang_products(self, categories):
        """신규 쿠팡 상품 900개 크롤링"""
        
        # ✅ 이미 충분한 신규 상품이 수집되었는지 확인
        expected_total = len(self.existing_product_ids) + 900
        current_total = len(self.all_products)
        
        if current_total >= expected_total:
            print("\n" + "="*80)
            print("✅ 쿠팡 크롤링 이미 완료됨")
            print(f"   캐시 상품: {current_total}개")
            print(f"   기존: {len(self.existing_product_ids)}개")
            print(f"   신규: {current_total - len(self.existing_product_ids)}개")
            print("="*80)
            print("\n➡️  아이허브 매칭 단계로 이동...\n")
            return
        
        print("\n" + "="*80)
        print("📦 1단계: 신규 쿠팡 상품 수집")
        print(f"   기존 {len(self.existing_product_ids)}개 제외")
        print("="*80)
        
        # 쿠팡 브라우저 시작
        print("\n🚀 쿠팡 브라우저 시작")
        self.coupang_browser = CoupangBrowser(headless=False)
        if not self.coupang_browser.start_driver():
            raise Exception("쿠팡 브라우저 시작 실패")
        
        scroll_crawler = InfiniteScrollCrawler(
            self.coupang_browser,
            self.existing_product_ids,
            self.global_new_ids
        )
        
        # 각 카테고리별 크롤링
        for category in categories:
            print("\n" + "="*80)
            print(f"📦 카테고리: {category['name']}")
            print(f"🎯 목표: 신규 {category['count']}개")
            print("="*80)
            
            scroll_crawler.products = []  # 카테고리마다 초기화
            
            products = scroll_crawler.infinite_scroll_crawl(
                category['url'],
                category['count']
            )
            
            if products:
                # 카테고리 정보 추가
                for product in products:
                    product['category'] = category['name']
                
                self.new_products.extend(products)
                print(f"✅ {category['name']}: {len(products)}개 신규 수집")
            else:
                print(f"⚠️ {category['name']}: 수집 실패")
        
        # 쿠팡 브라우저 종료
        self.coupang_browser.close()
        print(f"\n{'='*80}")
        print(f"✅ 신규 크롤링 완료: 총 {len(self.new_products)}개")
        print(f"{'='*80}\n")
        
        # 캐시 업데이트
        self._update_cache()
    
    def _update_cache(self):
        """캐시 파일 업데이트 (기존 + 신규 = 1,674개)"""
        if not self.new_products:
            return
        
        # 백업 생성
        if os.path.exists(self.cache_file):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = f"coupang_products_cache_{timestamp}.backup"
            shutil.copy(self.cache_file, backup_file)
            print(f"💾 백업 생성: {backup_file}")
        
        # 신규 상품을 all_products에 추가
        for product in self.new_products:
            self.all_products.append({
                '카테고리': product['category'],
                '상품ID': product['product_id'],
                '상품명': product['product_name'],
                '상품URL': product['product_url'],
                '가격': product['current_price']
            })
        
        # 캐시 파일 덮어쓰기
        try:
            with open(self.cache_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    '카테고리', '상품ID', '상품명', '상품URL', '가격'
                ])
                writer.writeheader()
                writer.writerows(self.all_products)
            
            print(f"💾 캐시 업데이트: {len(self.all_products)}개 저장\n")
        except Exception as e:
            print(f"⚠️ 캐시 저장 실패: {e}\n")
    
    def match_iherb_products(self):
        """아이허브 매칭 (신규 900개만)"""
        print("\n" + "="*80)
        print(f"📦 2단계: 아이허브 매칭")
        print(f"   전체 {len(self.all_products)}개 중")
        print(f"   신규 {len(self.new_products)}개만 매칭")
        print("="*80 + "\n")
        
        # 아이허브 브라우저 시작
        print("🚀 아이허브 브라우저 시작")
        self.iherb_browser = IHerbBrowser(headless=False)
        self.iherb_client = IHerbClient(self.iherb_browser)
        self.product_matcher = ProductMatcher(self.iherb_client)
        print("✅ 아이허브 브라우저 준비 완료 (한글 검색)\n")
        
        # 매칭 시작
        start_time = time.time()
        completed = 0
        restart_counter = 0  # 브라우저 재시작 카운터
        
        for idx, product_data in enumerate(self.all_products, 1):
            product_id = product_data.get('상품ID', '')
            
            # 이미 매칭 완료된 상품 스킵
            if product_id in self.matched_product_ids:
                print(f"[{idx}/{len(self.all_products)}] ⏭️ 스킵 (이미 매칭 완료)")
                continue
            
            # 신규 상품이 아니면 스킵 (안전장치)
            if product_id in self.existing_product_ids and product_id not in self.global_new_ids:
                continue
            
            # 예상 잔여 시간 계산
            if completed > 0:
                elapsed = time.time() - start_time
                avg_time = elapsed / completed
                remaining = (len(self.new_products) - completed) * avg_time
                hours = int(remaining // 3600)
                minutes = int((remaining % 3600) // 60)
                
                if hours > 0:
                    time_str = f"{hours}시간 {minutes}분"
                else:
                    time_str = f"{minutes}분"
                
                print(f"\n⏱️  예상 잔여 시간: {time_str} | 재시작까지: {10 - restart_counter}개")
            
            # 매칭 실행
            try:
                success = self._match_and_save(
                    idx,
                    product_data.get('카테고리', ''),
                    product_data.get('상품URL', ''),
                    product_data.get('상품명', ''),
                    product_data.get('가격', ''),
                    product_id
                )
                
                if success:
                    completed += 1
                    restart_counter += 1
                
                # 🔄 10개마다 브라우저 재시작
                if restart_counter >= 10:
                    print("\n" + "="*80)
                    print("🔄 브라우저 재시작 (메모리 관리 & 차단 방지)")
                    print("="*80)
                    
                    # 기존 브라우저 종료
                    try:
                        self.iherb_browser.close()
                        print("  ✅ 기존 브라우저 종료")
                    except:
                        pass
                    
                    # 대기
                    print("  ⏳ 3초 대기 중...")
                    time.sleep(3)
                    
                    # 새 브라우저 시작
                    print("  🚀 새 브라우저 시작 중...")
                    self.iherb_browser = IHerbBrowser(headless=False)
                    self.iherb_client = IHerbClient(self.iherb_browser)
                    self.product_matcher = ProductMatcher(self.iherb_client)
                    print("  ✅ 브라우저 재시작 완료\n")
                    
                    restart_counter = 0
                    
            except KeyboardInterrupt:
                elapsed_total = time.time() - start_time
                hours = int(elapsed_total // 3600)
                minutes = int((elapsed_total % 3600) // 60)
                
                print(f"\n{'='*80}")
                print(f"⚠️  작업 중단!")
                print(f"📊 처리 완료: {completed}개 / {len(self.new_products)}개")
                print(f"⏱️  소요 시간: {hours}시간 {minutes}분")
                print(f"💾 진행사항 저장됨: {self.output_file}")
                print(f"🔄 다시 실행하면 이어서 진행됩니다.")
                print(f"{'='*80}")
                raise
        
        # 완료
        self.iherb_browser.close()
        
        elapsed = time.time() - start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        
        print(f"\n{'='*80}")
        print(f"🎉 매칭 완료!")
        print(f"📊 신규 매칭: {completed}개")
        print(f"⏱️  소요 시간: {hours}시간 {minutes}분")
        print(f"{'='*80}")
    
    def _match_and_save(self, idx, category, product_url, product_name, price, product_id):
        """아이허브 매칭 및 저장"""
        print(f"\n[{idx}/{len(self.all_products)}] 🔍 {product_name[:50]}...")
        
        upc = ""
        part_number = ""
        
        try:
            # 아이허브 검색 및 매칭
            result = self.product_matcher.search_product_enhanced(product_name)
            
            if result and result.get('matched_url'):
                matched_url = result['matched_url']
                print(f"  ✅ 매칭 성공: {matched_url}")
                
                # 상품 정보 추출
                info = self.iherb_client.extract_product_info_with_price(matched_url)
                if info:
                    part_number = info.get('product_id', '')
                    upc = self._extract_upc_from_page()
                    print(f"  📦 파트넘버: {part_number}")
                    if upc:
                        print(f"  🔢 UPC: {upc}")
            else:
                print(f"  ❌ 매칭 실패")
        
        except Exception as e:
            error_msg = str(e)
            
            # API 쿼터 초과 감지
            if "quota" in error_msg.lower() or "limit" in error_msg.lower():
                print(f"\n{'='*80}")
                print("⚠️  Gemini API 할당량 초과!")
                print(f"📊 처리 완료: {idx - len(self.existing_product_ids)}개 / {len(self.new_products)}개")
                print(f"💾 진행사항 저장됨: {self.output_file}")
                print(f"🔄 다시 실행하면 이어서 진행됩니다.")
                print(f"{'='*80}")
                raise KeyboardInterrupt("API 할당량 초과")
            
            print(f"  ❌ 오류: {error_msg[:100]}")
        
        # CSV에 저장
        row = {
            '수집순서': idx,
            '카테고리': category,
            '쿠팡_상품URL': product_url,
            '쿠팡_제품명': product_name,
            '쿠팡_비회원가격': price,
            '아이허브_UPC': upc,
            '아이허브_파트넘버': part_number,
            '수집시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        self.csv_writer.writerow(row)
        self.csv_file.flush()
        
        # 처리 완료 ID에 추가
        self.matched_product_ids.add(product_id)
        
        # 딜레이
        time.sleep(random.uniform(1, 2))
        
        return True
    
    def _extract_upc_from_page(self):
        """현재 아이허브 페이지에서 UPC 추출"""
        try:
            # XPath로 정확히 찾기
            upc_elements = self.iherb_browser.driver.find_elements(
                By.XPATH, 
                "//ul[@id='product-specs-list']//li[contains(text(), 'UPC')]"
            )
            
            for element in upc_elements:
                text = element.text
                # "UPC 코드: 898220021055" 형식
                match = re.search(r'UPC[^:]*:\s*([0-9]{12,13})', text, re.IGNORECASE)
                if match:
                    upc = match.group(1)
                    if len(upc) in [12, 13]:
                        return upc
                
                # span 태그 안에 있을 수도
                try:
                    span = element.find_element(By.TAG_NAME, 'span')
                    upc = span.text.strip()
                    if upc.isdigit() and len(upc) in [12, 13]:
                        return upc
                except:
                    pass
            
            return ""
            
        except Exception as e:
            return ""
    
    def run(self):
        """전체 실행"""
        categories = [
            {
                'name': '헬스/건강식품',
                'url': 'https://shop.coupang.com/coupangus/74511?category=305433&platform=p&brandId=0',
                'count': 300
            },
            {
                'name': '출산유아동',
                'url': 'https://shop.coupang.com/coupangus/74511?category=219079&platform=p&brandId=0',
                'count': 300
            },
            {
                'name': '스포츠레저',
                'url': 'https://shop.coupang.com/coupangus/74511?category=317675&platform=p&brandId=0',
                'count': 300
            }
        ]
        
        try:
            # 1단계: 신규 쿠팡 크롤링 (900개)
            self.crawl_new_coupang_products(categories)
            
            # 2단계: 신규 상품만 아이허브 매칭
            self.match_iherb_products()
            
        except KeyboardInterrupt:
            print("\n⚠️ 사용자 중단 또는 API 제한")
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """리소스 정리"""
        print("\n🧹 정리 중...")
        
        if self.csv_file:
            self.csv_file.close()
            print("  ✅ CSV 파일 닫기 완료")
        
        if self.coupang_browser:
            try:
                self.coupang_browser.close()
                print("  ✅ 쿠팡 브라우저 종료")
            except:
                pass
        
        if self.iherb_browser:
            try:
                self.iherb_browser.close()
                print("  ✅ 아이허브 브라우저 종료")
            except:
                pass


if __name__ == "__main__":
    crawler = CoupangIHerbAdd900Crawler(
        output_file="coupang_iherb_products.csv",
        cache_file="coupang_products_cache.csv"
    )
    crawler.run()