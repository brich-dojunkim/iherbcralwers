"""
페이지 모니터링 시스템
쿠팡 페이지의 상품 순위, 가격 등을 지속적으로 추적
"""

import sys
import os
import time
import re
import sqlite3
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# 기존 모듈 임포트 - 경로 수정
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)  # iherb_price 디렉토리
sys.path.insert(0, parent_dir)
sys.path.insert(0, os.path.join(parent_dir, 'coupang'))

from coupang.coupang_manager import BrowserManager
from coupang.scraper import ProductScraper


class MonitoringDatabase:
    """모니터링 전용 데이터베이스"""
    
    def __init__(self, db_path="page_monitoring.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """데이터베이스 테이블 초기화"""
        conn = sqlite3.connect(self.db_path)
        
        # 매칭 참조 테이블 (CSV에서 한번만 로드)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS matching_reference (
                coupang_product_id TEXT PRIMARY KEY,
                coupang_product_name TEXT,
                coupang_product_url TEXT,
                iherb_upc TEXT,
                iherb_part_number TEXT,
                created_from_csv BOOLEAN DEFAULT TRUE,
                created_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 페이지 스냅샷 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS page_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_url TEXT NOT NULL,
                snapshot_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_products INTEGER,
                crawl_duration_seconds REAL,
                status TEXT DEFAULT 'completed'
            )
        """)
        
        # 상품 상태 테이블 (메인 데이터)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS product_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL,
                coupang_product_id TEXT NOT NULL,
                rank_position INTEGER,
                
                -- 쿠팡 실시간 정보
                product_name TEXT,
                product_url TEXT,
                current_price INTEGER,
                original_price INTEGER,
                discount_rate INTEGER,
                review_count INTEGER,
                rating_score REAL,
                is_rocket_delivery BOOLEAN,
                is_free_shipping BOOLEAN,
                cashback_amount INTEGER,
                delivery_info TEXT,
                
                -- 아이허브 매칭 정보 (베이스라인에서)
                iherb_upc TEXT,
                iherb_part_number TEXT,
                matching_status TEXT DEFAULT 'unknown',
                
                -- 메타 정보
                is_baseline_matched BOOLEAN DEFAULT FALSE,
                
                FOREIGN KEY (snapshot_id) REFERENCES page_snapshots(id)
            )
        """)
        
        # 변화 이벤트 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS change_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coupang_product_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                change_magnitude INTEGER,
                change_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                description TEXT
            )
        """)
        
        # 인덱스 생성
        conn.execute("CREATE INDEX IF NOT EXISTS idx_product_states_snapshot ON product_states(snapshot_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_product_states_product ON product_states(coupang_product_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_change_events_product ON change_events(coupang_product_id)")
        
        conn.commit()
        conn.close()
        print(f"데이터베이스 초기화 완료: {self.db_path}")
    
    def load_csv_baseline(self, csv_path):
        """CSV 매칭 정보를 베이스라인으로 로드 (1회만)"""
        if self.has_baseline_data():
            print("베이스라인 데이터가 이미 존재합니다. 스킵합니다.")
            return
        
        print(f"CSV 베이스라인 로딩 시작: {csv_path}")
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        
        conn = sqlite3.connect(self.db_path)
        
        loaded_count = 0
        for _, row in df.iterrows():
            try:
                # 상품 ID 추출
                product_id = self.extract_product_id(row['쿠팡_상품URL'])
                if not product_id:
                    continue
                
                # 매칭 정보 저장
                conn.execute("""
                    INSERT OR REPLACE INTO matching_reference 
                    (coupang_product_id, coupang_product_name, coupang_product_url, 
                     iherb_upc, iherb_part_number, created_from_csv)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    product_id,
                    row['쿠팡_제품명'],
                    row['쿠팡_상품URL'],
                    row['아이허브_UPC'] if pd.notna(row['아이허브_UPC']) else None,
                    row['아이허브_파트넘버'] if pd.notna(row['아이허브_파트넘버']) else None,
                    True
                ])
                loaded_count += 1
                
            except Exception as e:
                print(f"CSV 로딩 오류 (행 {_ + 1}): {e}")
                continue
        
        conn.commit()
        conn.close()
        
        print(f"베이스라인 로딩 완료: {loaded_count}개 상품")
    
    def has_baseline_data(self):
        """베이스라인 데이터 존재 여부 확인"""
        conn = sqlite3.connect(self.db_path)
        count = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
        conn.close()
        return count > 0
    
    def extract_product_id(self, url):
        """URL에서 상품 ID 추출"""
        if not url:
            return None
        match = re.search(r'itemId=(\d+)', url)
        return match.group(1) if match else None
    
    def save_snapshot(self, page_url, products, crawl_duration):
        """페이지 스냅샷 저장"""
        conn = sqlite3.connect(self.db_path)
        
        # 스냅샷 기록
        cursor = conn.execute("""
            INSERT INTO page_snapshots (page_url, total_products, crawl_duration_seconds)
            VALUES (?, ?, ?)
        """, [page_url, len(products), crawl_duration])
        
        snapshot_id = cursor.lastrowid
        
        # 상품 상태 저장
        for rank, product in enumerate(products, 1):
            # 매칭 정보 조회
            matching_info = conn.execute("""
                SELECT iherb_upc, iherb_part_number 
                FROM matching_reference 
                WHERE coupang_product_id = ?
            """, [product['product_id']]).fetchone()
            
            iherb_upc = matching_info[0] if matching_info else None
            iherb_part_number = matching_info[1] if matching_info else None
            is_baseline_matched = bool(matching_info)
            matching_status = 'matched' if iherb_upc else 'not_matched'
            
            conn.execute("""
                INSERT INTO product_states 
                (snapshot_id, coupang_product_id, rank_position, product_name, product_url,
                 current_price, original_price, discount_rate, review_count, rating_score,
                 is_rocket_delivery, is_free_shipping, cashback_amount, delivery_info,
                 iherb_upc, iherb_part_number, matching_status, is_baseline_matched)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                snapshot_id, product['product_id'], rank, product['product_name'],
                product['product_url'], product['current_price'], product['original_price'],
                product['discount_rate'], product['review_count'], product['rating_score'],
                product['is_rocket_delivery'], product['is_free_shipping'],
                product['cashback_amount'], product['delivery_info'],
                iherb_upc, iherb_part_number, matching_status, is_baseline_matched
            ])
        
        conn.commit()
        conn.close()
        
        print(f"스냅샷 저장 완료: ID {snapshot_id}, {len(products)}개 상품")
        return snapshot_id
    
    def get_latest_snapshot_data(self, page_url):
        """최신 스냅샷 데이터 조회"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        # 최신 스냅샷 ID 조회
        snapshot_row = conn.execute("""
            SELECT id FROM page_snapshots 
            WHERE page_url = ? 
            ORDER BY snapshot_time DESC 
            LIMIT 1
        """, [page_url]).fetchone()
        
        if not snapshot_row:
            conn.close()
            return []
        
        # 상품 데이터 조회
        products = conn.execute("""
            SELECT * FROM product_states 
            WHERE snapshot_id = ? 
            ORDER BY rank_position
        """, [snapshot_row['id']]).fetchall()
        
        conn.close()
        return [dict(row) for row in products]
    
    def log_change_event(self, product_id, event_type, old_value, new_value, description=""):
        """변화 이벤트 로깅"""
        conn = sqlite3.connect(self.db_path)
        
        # 변화 크기 계산
        change_magnitude = 0
        if event_type == 'rank_change' and old_value and new_value:
            change_magnitude = int(old_value) - int(new_value)  # 양수면 순위 상승
        elif event_type == 'price_change' and old_value and new_value:
            change_magnitude = int(new_value) - int(old_value)  # 가격 변화폭
        
        conn.execute("""
            INSERT INTO change_events 
            (coupang_product_id, event_type, old_value, new_value, change_magnitude, description)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [product_id, event_type, str(old_value), str(new_value), change_magnitude, description])
        
        conn.commit()
        conn.close()


class ProductExtractor:
    """상품 정보 추출기"""
    
    def __init__(self):
        self.scraper = ProductScraper()
    
    def extract_products_from_page(self, soup):
        """페이지에서 상품 목록 추출"""
        products = []
        product_items = soup.select('ul.products-list li.product-wrap')
        
        print(f"페이지에서 {len(product_items)}개 상품 발견")
        
        for item in product_items:
            try:
                product = self.extract_single_product(item)
                if product and product['product_id']:
                    products.append(product)
            except Exception as e:
                print(f"상품 추출 오류: {e}")
                continue
        
        return products
    
    def extract_single_product(self, item):
        """개별 상품 정보 추출"""
        # 상품 URL 및 ID
        link = item.select_one('a')
        if not link:
            return None
        
        product_url = link.get('href', '')
        if product_url.startswith('/'):
            product_url = 'https://www.coupang.com' + product_url
        
        product_id = self.extract_product_id(product_url)
        if not product_id:
            return None
        
        # 상품명
        name_elem = item.select_one('.name')
        product_name = name_elem.get_text(strip=True) if name_elem else ''
        
        # 가격 정보
        current_price = self.extract_price(item, 'strong.price-value')
        original_price = self.extract_price(item, 'del.base-price')
        
        # 할인율
        discount_elem = item.select_one('.discount-percentage')
        discount_rate = self.extract_number(discount_elem.get_text() if discount_elem else '0')
        
        # 리뷰 정보
        review_elem = item.select_one('.rating-total-count')
        review_count = self.extract_review_count(review_elem.get_text() if review_elem else '0')
        
        # 평점
        rating_elem = item.select_one('[data-rating]')
        rating_score = float(rating_elem.get('data-rating', 0)) if rating_elem else 0.0
        
        # 배송 정보
        is_rocket = bool(item.select_one('.badge.rocket'))
        is_free_shipping = bool(item.select_one('.shippingtype'))
        
        # 적립금
        cashback_elem = item.select_one('.reward-cash .txt')
        cashback_amount = self.extract_cashback(cashback_elem.get_text() if cashback_elem else '0')
        
        # 배송 정보
        delivery_elem = item.select_one('.delivery')
        delivery_info = delivery_elem.get_text(strip=True) if delivery_elem else ''
        
        return {
            'product_id': product_id,
            'product_name': product_name,
            'product_url': product_url,
            'current_price': current_price,
            'original_price': original_price,
            'discount_rate': discount_rate,
            'review_count': review_count,
            'rating_score': rating_score,
            'is_rocket_delivery': is_rocket,
            'is_free_shipping': is_free_shipping,
            'cashback_amount': cashback_amount,
            'delivery_info': delivery_info
        }
    
    def extract_product_id(self, url):
        """URL에서 상품 ID 추출"""
        match = re.search(r'itemId=(\d+)', url)
        return match.group(1) if match else None
    
    def extract_price(self, item, selector):
        """가격 추출"""
        elem = item.select_one(selector)
        if not elem:
            return 0
        
        price_text = elem.get_text(strip=True)
        return self.extract_number(price_text)
    
    def extract_number(self, text):
        """텍스트에서 숫자 추출"""
        if not text:
            return 0
        
        # 숫자와 쉼표만 추출
        numbers = re.findall(r'[\d,]+', text)
        if numbers:
            return int(numbers[0].replace(',', ''))
        return 0
    
    def extract_review_count(self, text):
        """리뷰 수 추출 - (170) 형태"""
        match = re.search(r'\((\d+)\)', text)
        return int(match.group(1)) if match else 0
    
    def extract_cashback(self, text):
        """적립금 추출 - '최대 429원 적립' 형태"""
        match = re.search(r'(\d+)원', text)
        return int(match.group(1)) if match else 0


class ChangeDetector:
    """변화 감지 엔진"""
    
    def detect_changes(self, old_products, new_products):
        """변화 감지"""
        if not old_products:
            # 첫 크롤링인 경우
            return {
                'new_products': new_products,
                'rank_changes': [],
                'price_changes': [],
                'removed_products': []
            }
        
        # 상품 ID별 매핑
        old_map = {p['coupang_product_id']: p for p in old_products}
        new_map = {p['product_id']: p for p in new_products}
        
        changes = {
            'new_products': [],
            'rank_changes': [],
            'price_changes': [],
            'removed_products': []
        }
        
        # 신규 상품
        new_ids = set(new_map.keys()) - set(old_map.keys())
        changes['new_products'] = [new_map[pid] for pid in new_ids]
        
        # 제거된 상품
        removed_ids = set(old_map.keys()) - set(new_map.keys())
        changes['removed_products'] = [old_map[pid] for pid in removed_ids]
        
        # 기존 상품의 변화
        common_ids = set(old_map.keys()) & set(new_map.keys())
        
        for pid in common_ids:
            old_product = old_map[pid]
            new_product = new_map[pid]
            
            # 순위 변화
            old_rank = old_product['rank_position']
            new_rank = new_products.index(new_product) + 1
            
            if old_rank != new_rank:
                changes['rank_changes'].append({
                    'product_id': pid,
                    'product_name': new_product['product_name'],
                    'old_rank': old_rank,
                    'new_rank': new_rank,
                    'rank_change': old_rank - new_rank
                })
            
            # 가격 변화
            if old_product['current_price'] != new_product['current_price']:
                changes['price_changes'].append({
                    'product_id': pid,
                    'product_name': new_product['product_name'],
                    'old_price': old_product['current_price'],
                    'new_price': new_product['current_price'],
                    'price_change': new_product['current_price'] - old_product['current_price']
                })
        
        return changes


class PageMonitor:
    """페이지 모니터링 메인 클래스"""
    
    def __init__(self, page_url, csv_baseline_path=None, db_path="page_monitoring.db", headless=True):
        self.page_url = page_url
        self.db = MonitoringDatabase(db_path)
        self.browser = BrowserManager(headless=headless)
        self.extractor = ProductExtractor()
        self.change_detector = ChangeDetector()
        
        # CSV 베이스라인 로드 (초기 1회만)
        if csv_baseline_path:
            self.db.load_csv_baseline(csv_baseline_path)
        
        print(f"페이지 모니터링 시스템 초기화 완료")
        print(f"대상 URL: {page_url}")
    
    def start_driver(self):
        """브라우저 드라이버 시작"""
        return self.browser.start_driver()
    
    def run_monitoring_cycle(self):
        """모니터링 사이클 실행"""
        print(f"\n{'='*60}")
        print(f"모니터링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        start_time = time.time()
        
        try:
            # 1. 페이지 크롤링
            current_products = self.crawl_page()
            
            # 2. 이전 데이터와 비교
            previous_products = self.db.get_latest_snapshot_data(self.page_url)
            changes = self.change_detector.detect_changes(previous_products, current_products)
            
            # 3. 변화 보고
            self.report_changes(changes)
            
            # 4. 변화 이벤트 로깅
            self.log_changes(changes)
            
            # 5. 스냅샷 저장
            crawl_duration = time.time() - start_time
            snapshot_id = self.db.save_snapshot(self.page_url, current_products, crawl_duration)
            
            print(f"모니터링 완료: 스냅샷 ID {snapshot_id}")
            return changes
            
        except Exception as e:
            print(f"모니터링 오류: {e}")
            return None
    
    def crawl_page(self):
        """페이지 크롤링"""
        print("페이지 크롤링 중...")
        
        self.browser.driver.get(self.page_url)
        time.sleep(3)
        
        soup = BeautifulSoup(self.browser.driver.page_source, 'html.parser')
        products = self.extractor.extract_products_from_page(soup)
        
        print(f"크롤링 완료: {len(products)}개 상품")
        return products
    
    def report_changes(self, changes):
        """변화 보고"""
        print(f"\n변화 감지 결과:")
        print(f"  신규 상품: {len(changes['new_products'])}개")
        print(f"  제거된 상품: {len(changes['removed_products'])}개")
        print(f"  순위 변화: {len(changes['rank_changes'])}개")
        print(f"  가격 변화: {len(changes['price_changes'])}개")
        
        # 주요 변화 상세 출력
        if changes['rank_changes']:
            print(f"\n주요 순위 변화:")
            for change in changes['rank_changes'][:5]:
                direction = "상승" if change['rank_change'] > 0 else "하락"
                print(f"  {change['product_name'][:30]}: {change['old_rank']}위 → {change['new_rank']}위 ({abs(change['rank_change'])}단계 {direction})")
        
        if changes['price_changes']:
            print(f"\n주요 가격 변화:")
            for change in changes['price_changes'][:5]:
                direction = "인상" if change['price_change'] > 0 else "인하"
                print(f"  {change['product_name'][:30]}: {change['old_price']:,}원 → {change['new_price']:,}원 ({abs(change['price_change']):,}원 {direction})")
    
    def log_changes(self, changes):
        """변화 이벤트 로깅"""
        # 순위 변화 로깅
        for change in changes['rank_changes']:
            self.db.log_change_event(
                change['product_id'], 'rank_change',
                change['old_rank'], change['new_rank'],
                f"순위 {abs(change['rank_change'])}단계 {'상승' if change['rank_change'] > 0 else '하락'}"
            )
        
        # 가격 변화 로깅
        for change in changes['price_changes']:
            self.db.log_change_event(
                change['product_id'], 'price_change',
                change['old_price'], change['new_price'],
                f"가격 {abs(change['price_change']):,}원 {'인상' if change['price_change'] > 0 else '인하'}"
            )
        
        # 신규 상품 로깅
        for product in changes['new_products']:
            self.db.log_change_event(
                product['product_id'], 'new_product',
                None, product['product_name'],
                f"신규 상품 등장"
            )
    
    def close(self):
        """리소스 정리"""
        if self.browser:
            self.browser.close()


def main():
    """테스트 실행"""
    # 설정 - 파일 경로 수정
    page_url = "https://shop.coupang.com/coupangus/74511?category=305433&platform=p&brandId=0"
    csv_baseline = "coupang_iherb_products.csv"  # 현재 디렉토리의 CSV 파일
    
    # 모니터링 시스템 초기화
    monitor = PageMonitor(
        page_url=page_url,
        csv_baseline_path=csv_baseline,
        headless=False  # 테스트용으로 브라우저 표시
    )
    
    try:
        # 브라우저 시작
        if not monitor.start_driver():
            print("브라우저 시작 실패")
            return
        
        # 모니터링 실행
        changes = monitor.run_monitoring_cycle()
        
        if changes:
            print(f"\n첫 모니터링 완료!")
            
            # 추가 모니터링 (테스트용)
            print(f"\n10초 후 두 번째 모니터링...")
            time.sleep(10)
            
            changes2 = monitor.run_monitoring_cycle()
            print(f"\n두 번째 모니터링 완료!")
        
    except KeyboardInterrupt:
        print("\n모니터링 중단")
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        monitor.close()


if __name__ == "__main__":
    main()