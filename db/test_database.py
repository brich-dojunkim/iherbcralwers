"""
DB 모듈 종합 테스트
모든 CRUD 작업과 엣지 케이스를 검증
"""

import os
import sys
import sqlite3
from datetime import datetime

# 프로젝트 루트 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import Database
from config import PathConfig


class DatabaseTester:
    """DB 테스트 러너"""
    
    def __init__(self):
        self.test_db_path = os.path.join(PathConfig.DATA_ROOT, "test.db")
        self.db = None
        self.passed = 0
        self.failed = 0
    
    def setup(self):
        """테스트 환경 설정"""
        # 기존 테스트 DB 삭제
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
            print(f"✓ 기존 테스트 DB 삭제")
        
        # 새 DB 생성
        self.db = Database(self.test_db_path)
        print(f"✓ 테스트 DB 생성: {self.test_db_path}\n")
    
    def teardown(self):
        """테스트 정리"""
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
            print(f"\n✓ 테스트 DB 정리 완료")
    
    def assert_equal(self, actual, expected, test_name):
        """검증 헬퍼"""
        if actual == expected:
            print(f"  ✓ {test_name}")
            self.passed += 1
        else:
            print(f"  ✗ {test_name}")
            print(f"    예상: {expected}")
            print(f"    실제: {actual}")
            self.failed += 1
    
    def assert_true(self, condition, test_name):
        """조건 검증"""
        if condition:
            print(f"  ✓ {test_name}")
            self.passed += 1
        else:
            print(f"  ✗ {test_name}")
            self.failed += 1
    
    def assert_not_none(self, value, test_name):
        """None 아님 검증"""
        self.assert_true(value is not None, test_name)
    
    # ========== 테스트 케이스 ==========
    
    def test_01_schema_creation(self):
        """1. 스키마 생성 테스트"""
        print("="*80)
        print("TEST 1: 스키마 생성")
        print("-"*80)
        
        with self.db.get_connection() as conn:
            # 테이블 존재 확인
            tables = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """).fetchall()
            table_names = [t['name'] for t in tables]
            
            expected_tables = [
                'brands', 'products', 'coupang_details', 'iherb_details',
                'price_history', 'pipeline_errors', 'product_images'
            ]
            
            for table in expected_tables:
                self.assert_true(
                    table in table_names,
                    f"테이블 '{table}' 존재"
                )
            
            # 뷰 확인
            views = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='view'
            """).fetchall()
            view_names = [v['name'] for v in views]
            
            self.assert_true(
                'v_products_full' in view_names,
                "뷰 'v_products_full' 존재"
            )
            self.assert_true(
                'v_price_comparison' in view_names,
                "뷰 'v_price_comparison' 존재"
            )
    
    def test_02_brand_operations(self):
        """2. 브랜드 CRUD 테스트"""
        print("\n" + "="*80)
        print("TEST 2: 브랜드 관리")
        print("-"*80)
        
        # 브랜드 추가
        self.db.upsert_brand("test_brand", "https://test.url")
        brand = self.db.get_brand("test_brand")
        
        self.assert_not_none(brand, "브랜드 추가")
        self.assert_equal(
            brand['coupang_search_url'],
            "https://test.url",
            "브랜드 URL 저장"
        )
        
        # 브랜드 업데이트 (URL 변경)
        self.db.upsert_brand("test_brand", "https://new.url")
        brand = self.db.get_brand("test_brand")
        
        self.assert_equal(
            brand['coupang_search_url'],
            "https://new.url",
            "브랜드 URL 업데이트"
        )
        
        # 크롤링 시간 업데이트
        self.db.update_brand_crawled("test_brand")
        brand = self.db.get_brand("test_brand")
        
        self.assert_not_none(
            brand['last_crawled_at'],
            "크롤링 시간 기록"
        )
        
        # 매칭 시간 업데이트
        self.db.update_brand_matched("test_brand")
        brand = self.db.get_brand("test_brand")
        
        self.assert_not_none(
            brand['last_matched_at'],
            "매칭 시간 기록"
        )
    
    def test_03_product_insert_new(self):
        """3. 신규 상품 추가 테스트"""
        print("\n" + "="*80)
        print("TEST 3: 신규 상품 추가")
        print("-"*80)
        
        coupang_data = {
            'product_id': 'CP001',
            'product_name': '쏜리서치 비타민 D3',
            'product_url': 'https://coupang.com/products/CP001',
            'current_price': '25,000원',
            'original_price': '30,000원',
            'discount_rate': '17%',
            'stock_status': 'in_stock',
            'delivery_badge': '로켓배송',
            'origin_country': '미국',
            'unit_price': '500원/개',
            'rating': '4.5',
            'review_count': '1,234',
            'is_rocket': True
        }
        
        product_id = self.db.insert_crawled_product("test_brand", coupang_data)
        
        self.assert_true(product_id > 0, "상품 ID 생성")
        
        # 상품 정보 확인
        product = self.db.get_product_full(product_id)
        
        self.assert_not_none(product, "상품 조회")
        self.assert_equal(
            product['coupang_product_name'],
            '쏜리서치 비타민 D3',
            "상품명 저장"
        )
        self.assert_equal(
            product['coupang_current_price'],
            25000,
            "가격 파싱 (쉼표/원 제거)"
        )
        self.assert_equal(
            product['pipeline_stage'],
            'crawled',
            "파이프라인 단계 초기화"
        )
        self.assert_equal(
            product['matching_status'],
            'pending',
            "매칭 상태 초기화"
        )
        
        # coupang_details 확인
        self.assert_equal(
            product['stock_status'],
            'in_stock',
            "재고 상태 저장"
        )
        self.assert_equal(
            product['rating'],
            4.5,
            "평점 저장"
        )
        self.assert_equal(
            product['review_count'],
            1234,
            "리뷰 수 파싱"
        )
    
    def test_04_product_update_existing(self):
        """4. 기존 상품 업데이트 테스트"""
        print("\n" + "="*80)
        print("TEST 4: 기존 상품 업데이트")
        print("-"*80)
        
        # 첫 번째 크롤링
        data1 = {
            'product_id': 'CP002',
            'product_name': '나우푸드 비타민 C',
            'current_price': '15000',
            'original_price': '18000'
        }
        product_id = self.db.insert_crawled_product("test_brand", data1)
        
        product = self.db.get_product_full(product_id)
        first_price = product['coupang_current_price']
        
        self.assert_equal(first_price, 15000, "초기 가격 저장")
        
        # 두 번째 크롤링 (가격 변동)
        data2 = {
            'product_id': 'CP002',
            'product_name': '나우푸드 비타민 C',
            'current_price': '12000',  # 가격 하락
            'original_price': '18000'
        }
        product_id2 = self.db.insert_crawled_product("test_brand", data2)
        
        self.assert_equal(
            product_id,
            product_id2,
            "기존 상품 ID 유지"
        )
        
        product = self.db.get_product_full(product_id)
        
        self.assert_equal(
            product['coupang_current_price'],
            12000,
            "가격 업데이트"
        )
        self.assert_not_none(
            product['price_updated_at'],
            "가격 업데이트 시간 기록"
        )
    
    def test_05_translation_workflow(self):
        """5. 번역 워크플로우 테스트"""
        print("\n" + "="*80)
        print("TEST 5: 번역 워크플로우")
        print("-"*80)
        
        # 상품 추가
        data = {
            'product_id': 'CP003',
            'product_name': '닥터스베스트 마그네슘'
        }
        product_id = self.db.insert_crawled_product("test_brand", data)
        
        # 번역 전 상태 확인
        products = self.db.get_products_by_stage("test_brand", "crawled")
        self.assert_true(
            len(products) >= 1,
            "crawled 단계 상품 조회"
        )
        
        # 번역 업데이트
        self.db.update_translation(product_id, "Doctor's Best Magnesium")
        
        product = self.db.get_product_full(product_id)
        
        self.assert_equal(
            product['coupang_product_name_english'],
            "Doctor's Best Magnesium",
            "영어명 저장"
        )
        self.assert_equal(
            product['pipeline_stage'],
            'translated',
            "파이프라인 단계 업데이트"
        )
        
        # 번역 완료 상품 조회
        products = self.db.get_products_by_stage("test_brand", "translated")
        self.assert_true(
            any(p['id'] == product_id for p in products),
            "translated 단계에서 조회 가능"
        )
    
    def test_06_matching_workflow(self):
        """6. 매칭 워크플로우 테스트"""
        print("\n" + "="*80)
        print("TEST 6: 매칭 워크플로우")
        print("-"*80)
        
        # 상품 추가 + 번역
        data = {
            'product_id': 'CP004',
            'product_name': '솔가 비타민 E'
        }
        product_id = self.db.insert_crawled_product("test_brand", data)
        self.db.update_translation(product_id, "Solgar Vitamin E")
        
        # 매칭 성공 케이스
        iherb_data = {
            'product_code': 'SLG-12345',
            'product_name': 'Solgar, Vitamin E, 400 IU',
            'product_url': 'https://iherb.com/pr/SLG-12345',
            'discount_price': '18000',
            'list_price': '22000',
            'discount_percent': '18',
            'subscription_discount': '5',
            'price_per_unit': '₩360/softgel',
            'is_in_stock': True,
            'status': 'success'
        }
        
        self.db.update_matching_result(product_id, iherb_data)
        
        product = self.db.get_product_full(product_id)
        
        self.assert_equal(
            product['iherb_product_code'],
            'SLG-12345',
            "아이허브 상품 코드 저장"
        )
        self.assert_equal(
            product['iherb_discount_price'],
            18000,
            "아이허브 가격 저장"
        )
        self.assert_equal(
            product['pipeline_stage'],
            'matched',
            "파이프라인 단계: matched"
        )
        self.assert_equal(
            product['matching_status'],
            'success',
            "매칭 상태: success"
        )
        self.assert_equal(
            product['is_in_stock'],
            1,
            "재고 상태 저장"
        )
        
        # 매칭 실패 케이스
        data2 = {
            'product_id': 'CP005',
            'product_name': '알 수 없는 상품'
        }
        product_id2 = self.db.insert_crawled_product("test_brand", data2)
        self.db.update_translation(product_id2, "Unknown Product")
        
        iherb_data_fail = {
            'status': 'not_found'
        }
        self.db.update_matching_result(product_id2, iherb_data_fail)
        
        product2 = self.db.get_product_full(product_id2)
        
        self.assert_equal(
            product2['matching_status'],
            'not_found',
            "매칭 실패 상태 저장"
        )
    
    def test_07_lock_mechanism(self):
        """7. 락 메커니즘 테스트"""
        print("\n" + "="*80)
        print("TEST 7: 락 메커니즘")
        print("-"*80)
        
        data = {
            'product_id': 'CP006',
            'product_name': '테스트 상품'
        }
        product_id = self.db.insert_crawled_product("test_brand", data)
        
        # 첫 번째 락 획득
        lock1 = self.db.acquire_lock(product_id, "process_1")
        self.assert_true(lock1, "첫 번째 락 획득 성공")
        
        # 두 번째 락 획득 시도 (실패해야 함)
        lock2 = self.db.acquire_lock(product_id, "process_2")
        self.assert_true(not lock2, "두 번째 락 획득 실패 (이미 락 걸림)")
        
        # 락 해제
        self.db.release_lock(product_id)
        
        # 락 해제 후 다시 획득
        lock3 = self.db.acquire_lock(product_id, "process_3")
        self.assert_true(lock3, "락 해제 후 재획득 성공")
        
        self.db.release_lock(product_id)
    
    def test_08_error_logging(self):
        """8. 에러 로깅 테스트"""
        print("\n" + "="*80)
        print("TEST 8: 에러 로깅")
        print("-"*80)
        
        data = {
            'product_id': 'CP007',
            'product_name': '에러 테스트 상품'
        }
        product_id = self.db.insert_crawled_product("test_brand", data)
        
        # 에러 로깅
        self.db.log_error(
            product_id,
            'match',
            'TIMEOUT_ERROR',
            'Connection timeout after 30 seconds'
        )
        
        product = self.db.get_product_full(product_id)
        
        self.assert_equal(
            product['pipeline_stage'],
            'failed',
            "에러 발생 시 파이프라인 단계: failed"
        )
        self.assert_not_none(
            product['last_error'],
            "마지막 에러 메시지 저장"
        )
        
        # 에러 로그 확인
        with self.db.get_connection() as conn:
            errors = conn.execute("""
                SELECT * FROM pipeline_errors WHERE product_id = ?
            """, (product_id,)).fetchall()
            
            self.assert_equal(
                len(errors),
                1,
                "에러 로그 테이블 기록"
            )
    
    def test_09_price_history(self):
        """9. 가격 이력 테스트"""
        print("\n" + "="*80)
        print("TEST 9: 가격 이력")
        print("-"*80)
        
        data = {
            'product_id': 'CP008',
            'product_name': '가격 변동 상품',
            'current_price': '10000'
        }
        product_id = self.db.insert_crawled_product("test_brand", data)
        
        # 가격 변동 기록
        self.db.record_price_change(product_id, 'coupang', 10000, 8000)
        
        with self.db.get_connection() as conn:
            history = conn.execute("""
                SELECT * FROM price_history WHERE product_id = ?
            """, (product_id,)).fetchall()
            
            self.assert_equal(
                len(history),
                1,
                "가격 이력 기록"
            )
            
            if len(history) > 0:
                self.assert_equal(
                    history[0]['old_price'],
                    10000,
                    "변경 전 가격"
                )
                self.assert_equal(
                    history[0]['new_price'],
                    8000,
                    "변경 후 가격"
                )
    
    def test_10_statistics(self):
        """10. 통계 조회 테스트"""
        print("\n" + "="*80)
        print("TEST 10: 통계 조회")
        print("-"*80)
        
        # 여러 상품 추가 (다양한 상태)
        for i in range(5):
            data = {
                'product_id': f'CP10{i}',
                'product_name': f'통계 테스트 상품 {i}'
            }
            product_id = self.db.insert_crawled_product("test_brand", data)
            
            if i < 2:
                self.db.update_translation(product_id, f"Stats Test {i}")
            if i < 1:
                self.db.update_matching_result(product_id, {'status': 'success'})
        
        # 브랜드 통계
        stats = self.db.get_brand_stats("test_brand")
        
        self.assert_true(
            stats['total_products'] >= 5,
            f"전체 상품 수: {stats['total_products']}"
        )
        self.assert_true(
            'by_stage' in stats,
            "파이프라인 단계별 통계"
        )
        self.assert_true(
            'by_matching' in stats,
            "매칭 상태별 통계"
        )
        
        print(f"  📊 통계 상세:")
        print(f"    - 전체: {stats['total_products']}개")
        print(f"    - 단계별: {stats['by_stage']}")
        print(f"    - 매칭별: {stats['by_matching']}")
    
    def test_11_edge_cases(self):
        """11. 엣지 케이스 테스트"""
        print("\n" + "="*80)
        print("TEST 11: 엣지 케이스")
        print("-"*80)
        
        # 빈 가격
        data1 = {
            'product_id': 'CP111',
            'product_name': '가격 없는 상품',
            'current_price': '',
            'original_price': None
        }
        product_id1 = self.db.insert_crawled_product("test_brand", data1)
        product1 = self.db.get_product_full(product_id1)
        
        self.assert_true(
            product1['coupang_current_price'] is None,
            "빈 가격 처리"
        )
        
        # 비정상 가격 형식
        data2 = {
            'product_id': 'CP112',
            'product_name': '비정상 가격',
            'current_price': '12,345.67원',  # 소수점 포함
            'rating': 'N/A',  # 숫자 아님
            'review_count': 'unknown'
        }
        product_id2 = self.db.insert_crawled_product("test_brand", data2)
        product2 = self.db.get_product_full(product_id2)
        
        self.assert_equal(
            product2['coupang_current_price'],
            12345,
            "소수점 가격 파싱 (정수 변환)"
        )
        self.assert_true(
            product2['rating'] is None,
            "비정상 평점 처리"
        )
        
        # 중복 product_id (같은 브랜드)
        data3 = {'product_id': 'CP113', 'product_name': '원본'}
        pid1 = self.db.insert_crawled_product("test_brand", data3)
        
        data3['product_name'] = '업데이트'
        pid2 = self.db.insert_crawled_product("test_brand", data3)
        
        self.assert_equal(
            pid1, pid2,
            "중복 product_id는 UPDATE (INSERT 아님)"
        )
        
        product = self.db.get_product_full(pid1)
        self.assert_equal(
            product['coupang_product_name'],
            '업데이트',
            "상품명 업데이트 확인"
        )
    
    def test_12_concurrent_scenario(self):
        """12. 동시 실행 시나리오 시뮬레이션"""
        print("\n" + "="*80)
        print("TEST 12: 동시 실행 시나리오")
        print("-"*80)
        
        # 10개 상품 추가
        product_ids = []
        for i in range(10):
            data = {
                'product_id': f'CP12{i}',
                'product_name': f'동시 실행 테스트 {i}'
            }
            pid = self.db.insert_crawled_product("test_brand", data)
            product_ids.append(pid)
        
        # Process 1: 처음 5개 락 획득
        locked_by_p1 = []
        for pid in product_ids[:5]:
            if self.db.acquire_lock(pid, "process_1"):
                locked_by_p1.append(pid)
        
        self.assert_equal(
            len(locked_by_p1),
            5,
            "Process 1: 5개 락 획득"
        )
        
        # Process 2: 전체 조회 시도 (unlocked만)
        available = self.db.get_products_by_stage("test_brand", "crawled", unlocked_only=True)
        
        self.assert_equal(
            len(available),
            5,
            "Process 2: 락 안 걸린 5개만 조회 가능"
        )
        
        # Process 1: 작업 완료 후 락 해제
        for pid in locked_by_p1:
            self.db.update_translation(pid, f"Translated {pid}")
            self.db.release_lock(pid)
        
        # Process 2: 이제 translated 단계 조회 가능
        translated = self.db.get_products_by_stage("test_brand", "translated")
        
        self.assert_equal(
            len(translated),
            5,
            "Process 1 완료 후 translated 상품 조회"
        )
    
    def test_13_view_queries(self):
        """13. 뷰 조회 테스트"""
        print("\n" + "="*80)
        print("TEST 13: 뷰 조회")
        print("-"*80)
        
        # 완전한 매칭 상품 추가
        data = {
            'product_id': 'CP131',
            'product_name': '뷰 테스트 상품',
            'current_price': '20000'
        }
        product_id = self.db.insert_crawled_product("test_brand", data)
        self.db.update_translation(product_id, "View Test Product")
        self.db.update_matching_result(product_id, {
            'product_code': 'IH-131',
            'discount_price': '15000',
            'status': 'success'
        })
        
        # v_products_full 조회
        full = self.db.get_product_full(product_id)
        
        self.assert_not_none(full, "v_products_full 뷰 조회")
        self.assert_true(
            'stock_status' in full,
            "JOIN된 coupang_details 컬럼 포함"
        )
        
        # v_price_comparison 조회
        comparison = self.db.get_price_comparison("test_brand", limit=10)
        
        self.assert_true(
            len(comparison) >= 1,
            "가격 비교 뷰 조회"
        )
        
        if len(comparison) > 0:
            item = comparison[0]
            self.assert_true(
                'cheaper_platform' in item,
                "가격 비교 계산 결과 포함"
            )
            self.assert_true(
                'price_difference' in item,
                "가격 차이 계산"
            )
    
    def test_14_full_pipeline(self):
        """14. 전체 파이프라인 통합 테스트"""
        print("\n" + "="*80)
        print("TEST 14: 전체 파이프라인 통합")
        print("-"*80)
        
        # 브랜드 생성
        self.db.upsert_brand("full_test", "https://test.url")
        
        # Step 1: 쿠팡 크롤링
        coupang_data = {
            'product_id': 'FT001',
            'product_name': '통합 테스트 상품',
            'current_price': '30000',
            'stock_status': 'in_stock'
        }
        product_id = self.db.insert_crawled_product("full_test", coupang_data)
        self.db.update_brand_crawled("full_test")
        
        # Step 2: 번역
        crawled = self.db.get_products_by_stage("full_test", "crawled")
        self.assert_equal(len(crawled), 1, "Step 1: 크롤링 완료")
        
        self.db.update_translation(product_id, "Integration Test Product")
        
        translated = self.db.get_products_by_stage("full_test", "translated")
        self.assert_equal(len(translated), 1, "Step 2: 번역 완료")
        
        # Step 3: 매칭
        iherb_data = {
            'product_code': 'IH-FT001',
            'product_name': 'Integration Test Product, 100 Tablets',
            'product_url': 'https://iherb.com/pr/IH-FT001',
            'discount_price': '25000',
            'list_price': '35000',
            'status': 'success'
        }
        self.db.update_matching_result(product_id, iherb_data)
        self.db.update_brand_matched("full_test")
        
        matched = self.db.get_products_by_stage("full_test", "matched")
        self.assert_equal(len(matched), 1, "Step 3: 매칭 완료")
        
        # 최종 결과 검증
        final = self.db.get_product_full(product_id)
        
        self.assert_equal(
            final['pipeline_stage'],
            'matched',
            "최종 파이프라인 단계"
        )
        self.assert_equal(
            final['matching_status'],
            'success',
            "최종 매칭 상태"
        )
        self.assert_not_none(
            final['coupang_product_name_english'],
            "최종: 영어명 존재"
        )
        self.assert_not_none(
            final['iherb_product_code'],
            "최종: 아이허브 코드 존재"
        )
        
        # 브랜드 메타데이터 확인
        brand = self.db.get_brand("full_test")
        self.assert_not_none(
            brand['last_crawled_at'],
            "브랜드: 크롤링 시간 기록"
        )
        self.assert_not_none(
            brand['last_matched_at'],
            "브랜드: 매칭 시간 기록"
        )
        
        print(f"  ✓ 전체 파이프라인 검증 완료")
        print(f"    crawled → translated → matched")
    
    # ========== 메인 실행 ==========
    
    def run_all_tests(self):
        """모든 테스트 실행"""
        print("\n" + "="*80)
        print("🧪 DB 모듈 종합 테스트 시작")
        print("="*80 + "\n")
        
        self.setup()
        
        try:
            # 테스트 실행
            self.test_01_schema_creation()
            self.test_02_brand_operations()
            self.test_03_product_insert_new()
            self.test_04_product_update_existing()
            self.test_05_translation_workflow()
            self.test_06_matching_workflow()
            self.test_07_lock_mechanism()
            self.test_08_error_logging()
            self.test_09_price_history()
            self.test_10_statistics()
            self.test_11_edge_cases()
            self.test_12_concurrent_scenario()
            self.test_13_view_queries()
            self.test_14_full_pipeline()
            
        finally:
            self.teardown()
        
        # 결과 요약
        print("\n" + "="*80)
        print("📊 테스트 결과 요약")
        print("="*80)
        print(f"✓ 통과: {self.passed}개")
        print(f"✗ 실패: {self.failed}개")
        
        if self.failed == 0:
            print("\n🎉 모든 테스트 통과!")
            return 0
        else:
            print(f"\n⚠️ {self.failed}개 테스트 실패")
            return 1


def main():
    """메인 함수"""
    tester = DatabaseTester()
    exit_code = tester.run_all_tests()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()