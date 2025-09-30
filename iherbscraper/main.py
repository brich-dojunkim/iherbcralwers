"""
IHerb 스크래퍼 - DB 버전
CSV 대신 DB에서 읽고 저장
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import Database
from config import PathConfig
from iherb_manager import BrowserManager
from iherb_client import IHerbClient
from product_matcher import ProductMatcher


class IHerbScraperDB:
    """DB 연동 아이허브 스크래퍼"""
    
    def __init__(self, db: Database, brand_name: str,
                 headless: bool = False,
                 max_products: int = 4):
        """
        Args:
            db: Database 인스턴스
            brand_name: 브랜드명
            headless: 헤드리스 모드
            max_products: 비교할 최대 상품 수
        """
        self.db = db
        self.brand_name = brand_name
        self.headless = headless
        self.max_products = max_products
        
        # 기존 모듈 초기화
        self.browser = BrowserManager(headless)
        self.iherb_client = IHerbClient(self.browser)
        self.product_matcher = ProductMatcher(self.iherb_client)
        
        # 언어 설정
        self.iherb_client.set_language_to_english()
        
        self.matched_count = 0
        self.not_found_count = 0
        self.error_count = 0
    
    def match_all_products(self, resume: bool = True) -> dict:
        """
        브랜드의 translated 상품들을 매칭
        
        Args:
            resume: 중단된 작업 이어서 실행
            
        Returns:
            통계 딕셔너리
        """
        print(f"\n{'='*80}")
        print(f"🔍 아이허브 매칭 시작: {self.brand_name}")
        print(f"{'='*80}\n")
        
        # translated 상품 조회 (락 안 걸린 것만)
        products = self.db.get_products_by_stage(
            self.brand_name, 
            'translated',
            unlocked_only=resume
        )
        
        if not products:
            print("✓ 매칭할 상품이 없습니다 (모두 완료)")
            return self._get_stats()
        
        print(f"매칭 대상: {len(products)}개\n")
        
        total = len(products)
        for idx, product in enumerate(products, 1):
            try:
                self._match_single_product(product, idx, total)
            except KeyboardInterrupt:
                print(f"\n⚠️ 사용자 중단")
                self.db.update_brand_matched(self.brand_name)
                return self._get_stats()
            except Exception as e:
                print(f"  ❌ 매칭 오류: {e}")
                self.error_count += 1
                self.db.log_error(
                    product['id'],
                    'match',
                    'PROCESSING_ERROR',
                    str(e)
                )
        
        # 브랜드 매칭 시간 업데이트
        self.db.update_brand_matched(self.brand_name)
        
        # 최종 통계
        self._print_summary()
        
        return self._get_stats()
    
    def _match_single_product(self, product: dict, idx: int, total: int):
        """단일 상품 매칭"""
        product_id = product['id']
        english_name = product['coupang_product_name_english']
        coupang_product_id = product['coupang_product_id']
        
        print(f"[{idx}/{total}] {english_name[:50]}...")
        
        # 락 획득
        if not self.db.acquire_lock(product_id, 'matcher'):
            print(f"  ⏭️ 다른 프로세스가 처리 중")
            return
        
        try:
            # 아이허브 검색 및 매칭
            search_result = self.product_matcher.search_product_enhanced(
                english_name,
                coupang_product_id
            )
            
            if len(search_result) != 3:
                raise Exception("검색 결과 형식 오류")
            
            product_url, similarity_score, match_details = search_result
            
            if product_url:
                # 매칭 성공 - 상품 정보 추출
                product_code, iherb_name, price_info = \
                    self.iherb_client.extract_product_info_with_price(product_url)
                
                if product_code:
                    # DB 업데이트
                    self.db.update_matching_result(product_id, {
                        'product_code': product_code,
                        'product_name': iherb_name,
                        'product_url': product_url,
                        'discount_price': price_info.get('discount_price'),
                        'list_price': price_info.get('list_price'),
                        'discount_percent': price_info.get('discount_percent'),
                        'subscription_discount': price_info.get('subscription_discount'),
                        'price_per_unit': price_info.get('price_per_unit'),
                        'is_in_stock': price_info.get('is_in_stock', True),
                        'stock_message': price_info.get('stock_message', ''),
                        'back_in_stock_date': price_info.get('back_in_stock_date', ''),
                        'status': 'success'
                    })
                    
                    self.matched_count += 1
                    print(f"  ✅ 매칭 성공: {product_code}")
                    
                    # 가격 정보 출력
                    if price_info.get('discount_price'):
                        print(f"     아이허브: {int(price_info['discount_price']):,}원")
                else:
                    # URL은 찾았으나 코드 추출 실패
                    self.db.update_matching_result(product_id, {
                        'product_url': product_url,
                        'status': 'code_not_found'
                    })
                    self.not_found_count += 1
                    print(f"  ⚠️ 상품코드 추출 실패")
            else:
                # 매칭 실패
                reason = match_details.get('reason', 'unknown')
                
                self.db.update_matching_result(product_id, {
                    'status': 'not_found'
                })
                
                self.not_found_count += 1
                print(f"  ❌ 매칭 실패: {reason}")
        
        except Exception as e:
            error_msg = str(e)
            
            # Gemini API 할당량 초과 시 즉시 중단
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                print(f"  ⚠️ Gemini API 할당량 초과")
                raise KeyboardInterrupt("API 할당량 초과")
            
            # 일반 오류
            self.db.log_error(
                product_id,
                'match',
                'MATCHING_ERROR',
                error_msg[:200]
            )
            
            self.error_count += 1
            print(f"  ❌ 오류: {error_msg[:50]}...")
        
        finally:
            # 락 해제
            self.db.release_lock(product_id)
    
    def _print_summary(self):
        """매칭 결과 요약"""
        print(f"\n{'='*80}")
        print(f"📊 매칭 완료 요약")
        print(f"{'='*80}")
        
        total = self.matched_count + self.not_found_count + self.error_count
        
        print(f"총 처리: {total}개")
        print(f"✅ 성공: {self.matched_count}개 ({self.matched_count/total*100:.1f}%)")
        print(f"❌ 실패: {self.not_found_count}개")
        print(f"💥 오류: {self.error_count}개")
        
        # 브랜드 통계
        stats = self.db.get_brand_stats(self.brand_name)
        by_stage = stats.get('by_stage', {})
        
        print(f"\n파이프라인 단계:")
        for stage, count in by_stage.items():
            emoji = {
                'crawled': '🆕',
                'translated': '📝',
                'matched': '✅',
                'failed': '❌'
            }.get(stage, '❓')
            print(f"  {emoji} {stage}: {count}개")
    
    def _get_stats(self) -> dict:
        """통계 반환"""
        return {
            'matched': self.matched_count,
            'not_found': self.not_found_count,
            'error': self.error_count
        }
    
    def close(self):
        """브라우저 종료"""
        self.browser.close()


def main():
    """테스트 실행"""
    print("🧪 DB 연동 아이허브 스크래퍼 테스트\n")
    
    # DB 연결
    db_path = os.path.join(PathConfig.DATA_ROOT, "products.db")
    db = Database(db_path)
    
    # 브랜드 확인
    brand_name = "thorne"
    brand = db.get_brand(brand_name)
    
    if not brand:
        print(f"❌ 브랜드 '{brand_name}'가 없습니다")
        return
    
    # 매칭 대상 확인
    products = db.get_products_by_stage(brand_name, 'translated')
    print(f"매칭 대상: {len(products)}개\n")
    
    if not products:
        print("✓ 매칭할 상품이 없습니다")
        return
    
    # 매칭 실행
    scraper = IHerbScraperDB(
        db=db,
        brand_name=brand_name,
        headless=False,
        max_products=4
    )
    
    try:
        stats = scraper.match_all_products(resume=True)
        
        print(f"\n{'='*80}")
        print(f"🎉 테스트 완료!")
        print(f"{'='*80}")
        print(f"매칭: {stats['matched']}개")
        print(f"실패: {stats['not_found']}개")
        print(f"오류: {stats['error']}개")
        
    except KeyboardInterrupt:
        print("\n⚠️ 테스트 중단")
    finally:
        scraper.close()


if __name__ == "__main__":
    main()