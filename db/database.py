"""
Database 접근 레이어 - SQLite 기반 상태 관리
"""

import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager


class Database:
    """통합 데이터베이스 접근 클래스"""
    
    def __init__(self, db_path: str):
        """
        Args:
            db_path: SQLite DB 파일 경로
        """
        self.db_path = db_path
        self._ensure_db_exists()
        self._init_schema()
    
    def _ensure_db_exists(self):
        """DB 파일이 있는 디렉토리 생성"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
    
    def _init_schema(self):
        """스키마 초기화"""
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        if os.path.exists(schema_path):
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            with self.get_connection() as conn:
                conn.executescript(schema_sql)
    
    @contextmanager
    def get_connection(self):
        """Connection 컨텍스트 매니저"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    # ========== Brand 관련 ==========
    
    def upsert_brand(self, brand_name: str, coupang_url: str) -> None:
        """브랜드 추가 또는 업데이트"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO brands (brand_name, coupang_search_url, created_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(brand_name) DO UPDATE SET
                    coupang_search_url = excluded.coupang_search_url
            """, (brand_name, coupang_url))
    
    def get_brand(self, brand_name: str) -> Optional[Dict[str, Any]]:
        """브랜드 정보 조회"""
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM brands WHERE brand_name = ?",
                (brand_name,)
            ).fetchone()
            return dict(row) if row else None
    
    def update_brand_crawled(self, brand_name: str) -> None:
        """브랜드 크롤링 시간 업데이트"""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE brands 
                SET last_crawled_at = datetime('now')
                WHERE brand_name = ?
            """, (brand_name,))
    
    def update_brand_matched(self, brand_name: str) -> None:
        """브랜드 매칭 시간 업데이트"""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE brands 
                SET last_matched_at = datetime('now')
                WHERE brand_name = ?
            """, (brand_name,))
    
    # ========== Product 관련 ==========
    
    def insert_crawled_product(self, brand_name: str, coupang_data: Dict[str, Any]) -> int:
        """
        쿠팡 크롤링 결과 저장
        
        Returns:
            product_id (생성 또는 기존)
        """
        now = datetime.now().isoformat()
        
        with self.get_connection() as conn:
            # 기존 상품 확인 (매칭 정보 포함)
            existing = conn.execute("""
                SELECT id, iherb_product_code, pipeline_stage FROM products 
                WHERE brand_name = ? AND coupang_product_id = ?
            """, (brand_name, coupang_data['product_id'])).fetchone()
            
            if existing:
                product_id = existing['id']
                
                # 이미 매칭 완료된 상품인지 확인
                has_matching = existing['iherb_product_code'] is not None
                
                if has_matching:
                    # 매칭 완료 상품: pipeline_stage 유지, 가격/URL만 업데이트
                    conn.execute("""
                        UPDATE products SET
                            coupang_product_name = ?,
                            coupang_url = ?,
                            coupang_current_price = ?,
                            coupang_original_price = ?,
                            coupang_discount_rate = ?,
                            last_crawled_at = ?,
                            price_updated_at = ?
                        WHERE id = ?
                    """, (
                        coupang_data.get('product_name', ''),
                        coupang_data.get('product_url', ''),
                        self._parse_price(coupang_data.get('current_price')),
                        self._parse_price(coupang_data.get('original_price')),
                        coupang_data.get('discount_rate', ''),
                        now,
                        now,
                        product_id
                    ))
                else:
                    # 미매칭 상품: pipeline_stage도 'crawled'로 재설정
                    conn.execute("""
                        UPDATE products SET
                            coupang_product_name = ?,
                            coupang_url = ?,
                            coupang_current_price = ?,
                            coupang_original_price = ?,
                            coupang_discount_rate = ?,
                            last_crawled_at = ?,
                            price_updated_at = ?,
                            pipeline_stage = 'crawled'
                        WHERE id = ?
                    """, (
                        coupang_data.get('product_name', ''),
                        coupang_data.get('product_url', ''),
                        self._parse_price(coupang_data.get('current_price')),
                        self._parse_price(coupang_data.get('original_price')),
                        coupang_data.get('discount_rate', ''),
                        now,
                        now,
                        product_id
                    ))
            else:
                # 신규 상품 생성
                cursor = conn.execute("""
                    INSERT INTO products (
                        brand_name, coupang_product_id, coupang_product_name,
                        coupang_url, coupang_current_price, coupang_original_price,
                        coupang_discount_rate, first_seen_at, last_crawled_at,
                        price_updated_at, pipeline_stage
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'crawled')
                """, (
                    brand_name,
                    coupang_data['product_id'],
                    coupang_data.get('product_name', ''),
                    coupang_data.get('product_url', ''),
                    self._parse_price(coupang_data.get('current_price')),
                    self._parse_price(coupang_data.get('original_price')),
                    coupang_data.get('discount_rate', ''),
                    now, now, now
                ))
                product_id = cursor.lastrowid
            
            # coupang_details 업데이트
            self._upsert_coupang_details(conn, product_id, coupang_data)
            
            return product_id
    
    def _upsert_coupang_details(self, conn, product_id: int, data: Dict[str, Any]):
        """쿠팡 상세 정보 업데이트"""
        conn.execute("""
            INSERT INTO coupang_details (
                product_id, stock_status, delivery_badge, origin_country,
                unit_price, rating, review_count, is_rocket, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(product_id) DO UPDATE SET
                stock_status = excluded.stock_status,
                delivery_badge = excluded.delivery_badge,
                origin_country = excluded.origin_country,
                unit_price = excluded.unit_price,
                rating = excluded.rating,
                review_count = excluded.review_count,
                is_rocket = excluded.is_rocket,
                updated_at = datetime('now')
        """, (
            product_id,
            data.get('stock_status', ''),
            data.get('delivery_badge', ''),
            data.get('origin_country', ''),
            data.get('unit_price', ''),
            self._parse_float(data.get('rating')),
            self._parse_int(data.get('review_count')),
            1 if data.get('is_rocket') else 0
        ))
    
    def update_translation(self, product_id: int, english_name: str) -> None:
        """번역 결과 업데이트"""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE products SET
                    coupang_product_name_english = ?,
                    pipeline_stage = 'translated'
                WHERE id = ?
            """, (english_name, product_id))
    
    def update_matching_result(self, product_id: int, iherb_data: Dict[str, Any]) -> None:
        """아이허브 매칭 결과 업데이트"""
        now = datetime.now().isoformat()
                
        status = iherb_data.get('status', 'success')
        if status not in {'pending', 'success', 'not_found', 'error'}:
            status = 'error'  # 알 수 없는 값은 error로
            
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE products SET
                    iherb_product_code = ?,
                    iherb_product_name = ?,
                    iherb_product_url = ?,
                    iherb_discount_price = ?,
                    iherb_list_price = ?,
                    pipeline_stage = 'matched',
                    matching_status = ?,
                    last_matched_at = ?
                WHERE id = ?
            """, (
                iherb_data.get('product_code'),
                iherb_data.get('product_name'),
                iherb_data.get('product_url'),
                self._parse_price(iherb_data.get('discount_price')),
                self._parse_price(iherb_data.get('list_price')),
                iherb_data.get('status', 'success'),
                now,
                product_id
            ))
            
            # iherb_details 업데이트
            if iherb_data.get('product_code'):
                self._upsert_iherb_details(conn, product_id, iherb_data)
    
    def _upsert_iherb_details(self, conn, product_id: int, data: Dict[str, Any]):
        """아이허브 상세 정보 업데이트"""
        conn.execute("""
            INSERT INTO iherb_details (
                product_id, discount_percent, subscription_discount,
                price_per_unit, is_in_stock, stock_message,
                back_in_stock_date, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(product_id) DO UPDATE SET
                discount_percent = excluded.discount_percent,
                subscription_discount = excluded.subscription_discount,
                price_per_unit = excluded.price_per_unit,
                is_in_stock = excluded.is_in_stock,
                stock_message = excluded.stock_message,
                back_in_stock_date = excluded.back_in_stock_date,
                updated_at = datetime('now')
        """, (
            product_id,
            data.get('discount_percent', ''),
            data.get('subscription_discount', ''),
            data.get('price_per_unit', ''),
            1 if data.get('is_in_stock', True) else 0,
            data.get('stock_message', ''),
            data.get('back_in_stock_date', '')
        ))
    
    def get_products_by_stage(self, brand_name: str, stage: str, 
                             unlocked_only: bool = True) -> List[Dict[str, Any]]:
        """
        특정 파이프라인 단계의 상품 조회
        
        Args:
            brand_name: 브랜드명
            stage: 'crawled', 'translated', 'matched', 'failed'
            unlocked_only: 락 걸리지 않은 상품만 조회
        """
        with self.get_connection() as conn:
            query = """
                SELECT * FROM products
                WHERE brand_name = ? AND pipeline_stage = ?
            """
            params = [brand_name, stage]
            
            if unlocked_only:
                query += " AND (processing_lock IS NULL OR processing_lock < datetime('now', '-10 minutes'))"
            
            query += " ORDER BY id"
            
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
    
    def get_product_full(self, product_id: int) -> Optional[Dict[str, Any]]:
        """전체 상품 정보 조회 (JOIN)"""
        with self.get_connection() as conn:
            row = conn.execute("""
                SELECT * FROM v_products_full WHERE id = ?
            """, (product_id,)).fetchone()
            return dict(row) if row else None
    
    # ========== 락 관리 ==========
    
    def acquire_lock(self, product_id: int, process_id: str) -> bool:
        """
        상품 처리 락 획득
        
        Returns:
            락 획득 성공 여부
        """
        now = datetime.now().isoformat()
        with self.get_connection() as conn:
            # stale lock 해제
            conn.execute("""
                UPDATE products 
                SET processing_lock = NULL 
                WHERE processing_lock < datetime('now', '-10 minutes')
            """)
            
            # 락 시도
            result = conn.execute("""
                UPDATE products 
                SET processing_lock = ?
                WHERE id = ? AND processing_lock IS NULL
            """, (now, product_id))
            
            return result.rowcount > 0
    
    def release_lock(self, product_id: int) -> None:
        """락 해제"""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE products 
                SET processing_lock = NULL 
                WHERE id = ?
            """, (product_id,))
    
    # ========== 에러 로깅 ==========
    
    def log_error(self, product_id: int, stage: str, 
                  error_type: str, error_message: str) -> None:
        """에러 로깅"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO pipeline_errors (
                    product_id, stage, error_type, error_message
                ) VALUES (?, ?, ?, ?)
            """, (product_id, stage, error_type, error_message))
            
            # products 테이블에도 마지막 에러 기록
            conn.execute("""
                UPDATE products 
                SET last_error = ?, pipeline_stage = 'failed'
                WHERE id = ?
            """, (f"[{stage}] {error_type}: {error_message[:100]}", product_id))
    
    # ========== 가격 이력 ==========
    
    def record_price_change(self, product_id: int, price_type: str,
                           old_price: Optional[int], new_price: int) -> None:
        """가격 변동 기록"""
        if old_price is not None and old_price != new_price:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO price_history (
                        product_id, price_type, old_price, new_price
                    ) VALUES (?, ?, ?, ?)
                """, (product_id, price_type, old_price, new_price))
    
    # ========== 사라진 상품 감지 ==========
    
    def get_missing_products(self, brand_name: str) -> List[Dict[str, Any]]:
        """
        최신 크롤링에서 발견되지 않은 상품 조회
        
        Returns:
            사라진 상품 리스트
        """
        brand = self.get_brand(brand_name)
        if not brand or not brand['last_crawled_at']:
            return []
        
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM products
                WHERE brand_name = ?
                AND last_crawled_at < ?
            """, (brand_name, brand['last_crawled_at'])).fetchall()
            return [dict(row) for row in rows]
    
    def reset_failed_products(self, brand_name: str, 
                             stage: str = 'translated') -> int:
        """
        failed 상품을 재시도 가능 상태로 변경
        
        Returns:
            변경된 상품 수
        """
        with self.get_connection() as conn:
            result = conn.execute("""
                UPDATE products SET
                    pipeline_stage = ?,
                    last_error = NULL,
                    processing_lock = NULL
                WHERE brand_name = ? AND pipeline_stage = 'failed'
            """, (stage, brand_name))
            return result.rowcount
    
    # ========== 통계 조회 ==========
    
    def get_brand_stats(self, brand_name: str) -> Dict[str, Any]:
        """브랜드 통계"""
        with self.get_connection() as conn:
            stats = {}
            
            # 전체 상품 수
            row = conn.execute("""
                SELECT COUNT(*) as total FROM products WHERE brand_name = ?
            """, (brand_name,)).fetchone()
            stats['total_products'] = row['total']
            
            # 파이프라인 단계별
            rows = conn.execute("""
                SELECT pipeline_stage, COUNT(*) as cnt
                FROM products WHERE brand_name = ?
                GROUP BY pipeline_stage
            """, (brand_name,)).fetchall()
            stats['by_stage'] = {row['pipeline_stage']: row['cnt'] for row in rows}
            
            # 매칭 상태별
            rows = conn.execute("""
                SELECT matching_status, COUNT(*) as cnt
                FROM products WHERE brand_name = ?
                GROUP BY matching_status
            """, (brand_name,)).fetchall()
            stats['by_matching'] = {row['matching_status']: row['cnt'] for row in rows}
            
            return stats
    
    def get_price_comparison(self, brand_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """가격 비교 결과 조회"""
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM v_price_comparison 
                WHERE brand_name = ?
                ORDER BY price_difference DESC
                LIMIT ?
            """, (brand_name, limit)).fetchall()
            return [dict(row) for row in rows]
    
    # ========== 유틸리티 ==========
    
    @staticmethod
    def _parse_price(value: Any) -> Optional[int]:
        """가격 파싱"""
        if value is None or value == '':
            return None
        try:
            # "12,345원" -> 12345
            cleaned = str(value).replace(',', '').replace('원', '').strip()
            return int(float(cleaned)) if cleaned else None
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _parse_int(value: Any) -> Optional[int]:
        """정수 파싱"""
        if value is None or value == '':
            return None
        try:
            return int(str(value).replace(',', ''))
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _parse_float(value: Any) -> Optional[float]:
        """실수 파싱"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None