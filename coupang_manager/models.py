"""
쿠팡 데이터 모델
"""

from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class CoupangProduct:
    """쿠팡 상품 정보"""
    rank: int
    name: str
    price: int
    shipping_fee: int
    final_price: int
    url: str
    thumbnail_url: Optional[str] = None
    count: Optional[int] = None
    brand: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    seller_name: Optional[str] = None
    
    def to_dict(self):
        """딕셔너리로 변환"""
        return asdict(self)
    
    def __str__(self):
        return f"[{self.rank}] {self.name[:30]}... - {self.final_price:,}원"
