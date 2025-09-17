"""
공통 패턴, 컬럼명, 날짜 형식 정의
모든 매니저에서 이 모듈을 import해서 사용
"""

from datetime import datetime

class MasterFilePatterns:
    """마스터 파일 관련 모든 패턴 정의"""
    
    @staticmethod
    def get_today_suffix():
        """오늘 날짜 suffix 반환: 20250917"""
        return datetime.now().strftime("%Y%m%d")
    
    @staticmethod
    def get_new_product_status():
        """신규 상품 상태 패턴: NEW_PRODUCT__20250917"""
        return f"NEW_PRODUCT__{MasterFilePatterns.get_today_suffix()}"
    
    @staticmethod
    def get_daily_coupang_columns():
        """오늘 날짜 쿠팡 컬럼들"""
        suffix = MasterFilePatterns.get_today_suffix()
        return {
            'current_price': f'쿠팡현재가격_{suffix}',
            'original_price': f'쿠팡정가_{suffix}',
            'discount_rate': f'쿠팡할인율_{suffix}',
            'review_count': f'쿠팡리뷰수_{suffix}',
            'rating': f'쿠팡평점_{suffix}',
            'crawled_at': f'쿠팡크롤링시간_{suffix}'
        }
    
    @staticmethod
    def get_daily_iherb_columns():
        """오늘 날짜 아이허브 컬럼들"""
        suffix = MasterFilePatterns.get_today_suffix()
        return {
            'list_price': f'아이허브정가_{suffix}',
            'discount_price': f'아이허브할인가_{suffix}',
            'discount_percent': f'아이허브할인율_{suffix}',
            'subscription_discount': f'아이허브구독할인_{suffix}',
            'price_per_unit': f'아이허브단위가격_{suffix}',
            'stock_status': f'재고상태_{suffix}',
            'stock_message': f'재고메시지_{suffix}',
            'matching_status': f'아이허브매칭상태_{suffix}',
            'matching_reason': f'아이허브매칭사유_{suffix}',
            'matched_at': f'아이허브매칭일시_{suffix}',
            'price_difference': f'가격차이_{suffix}',
            'cheaper_platform': f'저렴한플랫폼_{suffix}',
            'savings_amount': f'절약금액_{suffix}',
            'savings_percentage': f'절약비율_{suffix}',
            'price_difference_note': f'가격차이메모_{suffix}'
        }

class UpdateStatus:
    """업데이트 상태 상수"""
    UPDATED = "UPDATED"
    NOT_FOUND = "NOT_FOUND"
    NEW_PRODUCT = MasterFilePatterns.get_new_product_status()
    ERROR = f"ERROR_{MasterFilePatterns.get_today_suffix()}"

# 편의 함수들
def get_new_products_filter(df):
    """신규 상품 필터링"""
    pattern = MasterFilePatterns.get_new_product_status()
    return df['update_status'] == pattern

def get_today_columns():
    """오늘 날짜 모든 컬럼"""
    coupang_cols = MasterFilePatterns.get_daily_coupang_columns()
    iherb_cols = MasterFilePatterns.get_daily_iherb_columns()
    return {**coupang_cols, **iherb_cols}

def debug_patterns():
    """패턴 디버깅용 함수"""
    print(f"🔍 패턴 정보:")
    print(f"  - 오늘 날짜: {MasterFilePatterns.get_today_suffix()}")
    print(f"  - 신규 상품 패턴: '{MasterFilePatterns.get_new_product_status()}'")
    print(f"  - 쿠팡 컬럼 예시: {list(MasterFilePatterns.get_daily_coupang_columns().values())[:3]}")
    print(f"  - 아이허브 컬럼 예시: {list(MasterFilePatterns.get_daily_iherb_columns().values())[:3]}")