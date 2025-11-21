#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
아이허브 vs 국내 리딩 브랜드 시장 분석 시스템 (리뷰 기반 버전)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
목적:
- 아이허브 제품이 국내에서 더 잘 판매되기 위한 솔루션 제안
- 키워드별 국내 리딩 브랜드(누적 판매량 ≒ 리뷰수 기준) 및 아이허브 제품 포지셔닝 분석

핵심 로직:
- 쿠팡 데이터: '판매량순' 정렬 결과를 크롤링한 데이터 사용
- rank: 페이지별 rank → 키워드 전체 기준 연속 rank 로 재계산 (1,2,3,...)
- 브랜드 리더 판정 기준:
  - 가격(판매가/정가)은 신뢰도 낮아 완전히 무시
  - total_reviews(누적 리뷰수)를 기반으로 연간 판매량(개수)을 추정
  - estimated_annual_sales = 리뷰 기반 '연간 판매량(개수)' 상대 지표
  - market_share = 이 판매량 기준 점유율
  - 브랜드 랭킹(rank) = estimated_annual_sales 내림차순
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ==================== 전역 설정 ====================
TARGET_KEYWORDS = [
    '유산균', '오메가', '액상칼슘', '고흡수마그네슘', '아스타잔틴',
    '엘더베리', '콜라겐업 히알루론산 비타민C파우더', '실리마린', '베르베린', '코큐텐',
    '글루코사민콘드로이틴', '옵티MSM', 'L-아르기닌', '루테인', '프로폴리스'
]

# 매출(판매량) 추정 계수 (가격은 사용하지 않음)
REVIEW_CONVERSION_RATE = 0.03  # 구매자 중 리뷰 작성 비율 (리뷰 1개 ≈ 33.3건 구매)
REPURCHASE_MULTIPLIER = 1.5    # 재구매 계수
ANNUAL_MULTIPLIER = 1.0        # 연간 환산 (현재 시점 스냅샷이므로 1로 둠)


# ==================== 1. 데이터 로더 ====================
class DataLoader:
    """데이터 로드 및 기본 전처리"""
    
    def __init__(self):
        self.coupang = None
        self.iherb_inventory = None
        self.iherb_sales = None
    
    def load_all(self):
        """모든 데이터 로드"""
        print("=" * 80)
        print("1. 데이터 로드")
        print("=" * 80)
        
        base_dir = "/Users/brich/Desktop/iherb_price/251120"
        
        # 쿠팡 크롤링 데이터 (판매량순 정렬 결과 크롤링)
        self.coupang = pd.read_csv(f"{base_dir}/coupang_crawled_data.csv")
        print(f"✓ 쿠팡 크롤링: {len(self.coupang):,}개 제품")
        
        # 아이허브 상품 목록
        self.iherb_inventory = pd.read_excel(
            f"{base_dir}/price_inventory_251120.xlsx",
            skiprows=2
        )
        print(f"✓ 아이허브 상품 목록: {len(self.iherb_inventory):,}개")
        
        # 아이허브 판매 내역
        self.iherb_sales = pd.read_excel(
            f"{base_dir}/SELLER_INSIGHTS_VENDOR_ITEM_METRICS_(0) (17).xlsx"
        )
        print(f"✓ 아이허브 판매 내역: {len(self.iherb_sales):,}개\n")
        
        return self
    
    def preprocess(self):
        """데이터 전처리"""
        print("=" * 80)
        print("2. 데이터 전처리")
        print("=" * 80)
        
        # 쿠팡 데이터
        self.coupang = self._preprocess_coupang(self.coupang)
        print(f"✓ 쿠팡: {len(self.coupang):,}개 (중복 제거 후)")
        
        # 아이허브 상품 목록
        self.iherb_inventory = self._preprocess_iherb_inventory(self.iherb_inventory)
        print(f"✓ 아이허브 상품: {len(self.iherb_inventory):,}개")
        
        # 아이허브 판매 내역
        self.iherb_sales = self._preprocess_iherb_sales(self.iherb_sales)
        print(f"✓ 아이허브 판매: {len(self.iherb_sales):,}개\n")
        
        return self
    
    def _preprocess_coupang(self, df):
        """쿠팡 데이터 전처리
        
        - brand 비어있으면 product_name 첫 단어로 대체
        - (product_id, keyword) 기준 중복 제거
        - rank: 페이지별 rank → 키워드 전체 기준 연속 rank 로 재계산
        - 가격 관련 값은 분석/랭킹에 사용하지 않지만 numeric 캐스팅은 해 둠
        """
        df = df.copy()
        
        # 브랜드 추출 (기존 brand 비어있으면 상품명 첫 단어 사용)
        df['brand_extracted'] = df['product_name'].str.split().str[0]
        df['brand'] = df.apply(
            lambda x: x['brand_extracted'] if (pd.isna(x['brand']) or x['brand'] == '') else x['brand'],
            axis=1
        )
        df.drop(columns=['brand_extracted'], inplace=True)
        
        # 숫자형 변환
        df['review_count'] = pd.to_numeric(df['review_count'], errors='coerce').fillna(0).astype(int)
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')              # 사용은 안 하지만 캐스팅
        df['original_price'] = pd.to_numeric(df['original_price'], errors='coerce')
        df['page'] = pd.to_numeric(df['page'], errors='coerce').fillna(1).astype(int)
        df['rank'] = pd.to_numeric(df['rank'], errors='coerce').fillna(0).astype(int)
        
        # 페이지 내 rank 보존
        df['page_rank'] = df['rank']
        
        # (product_id, keyword) 기준 중복 제거
        df = df.drop_duplicates(subset=['product_id', 'keyword'], keep='first')
        
        # 키워드별로 page, page_rank 기준 정렬 후 연속 rank 재계산
        df = df.sort_values(['keyword', 'page', 'page_rank', 'product_id'], kind='mergesort')
        df['rank'] = df.groupby('keyword').cumcount() + 1
        
        return df
    
    def _preprocess_iherb_inventory(self, df):
        """아이허브 상품 목록 전처리"""
        df = df.copy()
        
        df['Product ID'] = pd.to_numeric(df['Product ID'], errors='coerce')
        df = df.dropna(subset=['Product ID'])
        df['Product ID'] = df['Product ID'].astype(np.int64)
        df['옵션 ID'] = pd.to_numeric(df['옵션 ID'], errors='coerce')
        df['판매가격'] = pd.to_numeric(df['판매가격'], errors='coerce')
        
        return df
    
    def _preprocess_iherb_sales(self, df):
        """아이허브 판매 내역 전처리"""
        df = df.copy()
        
        df['옵션 ID'] = pd.to_numeric(df['옵션 ID'], errors='coerce')
        df['매출(원)'] = pd.to_numeric(df['매출(원)'], errors='coerce').fillna(0)
        df['판매량'] = pd.to_numeric(df['판매량'], errors='coerce').fillna(0)
        
        return df


# ==================== 2. 시장 분석기 ====================
class MarketAnalyzer:
    """키워드별 시장 구조 분석 (브랜드 판매량 기준 리더 산출)"""
    
    def __init__(self, coupang_data):
        self.coupang = coupang_data
    
    def analyze_keyword(self, keyword):
        """단일 키워드 시장 분석"""
        
        keyword_data = self.coupang[self.coupang['keyword'] == keyword].copy()
        
        if len(keyword_data) == 0:
            return None
        
        # 브랜드별 집계
        brand_stats = self._aggregate_by_brand(keyword_data)
        
        # 연간 판매량(개수) 추정 (리뷰 기반, 가격 무시)
        brand_stats['estimated_annual_sales'] = self._estimate_annual_sales(brand_stats)
        
        # 판매량 기준 시장 점유율
        total_sales = brand_stats['estimated_annual_sales'].sum()
        if total_sales > 0:
            brand_stats['market_share'] = (
                brand_stats['estimated_annual_sales'] / total_sales * 100
            ).round(2)
        else:
            brand_stats['market_share'] = 0.0
        
        # 판매량 퍼센타일 (참고용)
        brand_stats['sales_percentile'] = (
            brand_stats['estimated_annual_sales'].rank(pct=True) * 100
        )
        
        # 성공 요인 분석 (판매량/리뷰/가격/노출/로켓 관점)
        brand_stats['success_factors'] = brand_stats.apply(
            lambda x: self._identify_success_factors(x, brand_stats),
            axis=1
        )
        
        # 브랜드 랭킹: 추정 판매량(개수) 기준 내림차순
        brand_stats = brand_stats.sort_values(
            ['estimated_annual_sales', 'total_reviews'],
            ascending=False
        ).reset_index(drop=True)
        brand_stats['rank'] = range(1, len(brand_stats) + 1)
        
        return brand_stats
    
    def _aggregate_by_brand(self, keyword_data):
        """브랜드별 집계
        
        여기서는 기본 집계 + 퍼센타일까지만 계산하고
        market_share는 analyze_keyword에서 'estimated_annual_sales' 기준으로 계산한다.
        """
        
        brand_stats = keyword_data.groupby('brand').agg({
            'product_id': 'count',
            'review_count': ['sum', 'mean'],
            'rating': 'mean',
            'price': 'mean',   # 참고용
            'rank': 'mean',
            'page': 'mean'
        }).reset_index()
        
        brand_stats.columns = [
            'brand', 'product_count', 'total_reviews', 'avg_reviews',
            'avg_rating', 'avg_price', 'avg_rank', 'avg_page'
        ]
        
        # 로켓배송 비율
        rocket_ratio = keyword_data.groupby('brand')['is_rocket'].apply(
            lambda x: (x == 'Y').mean() * 100
        ).reset_index()
        rocket_ratio.columns = ['brand', 'rocket_delivery_ratio']
        brand_stats = brand_stats.merge(rocket_ratio, on='brand')
        
        # 리뷰 / 평점 / 랭크 백분위 (참고용)
        brand_stats['review_percentile'] = brand_stats['total_reviews'].rank(pct=True) * 100
        brand_stats['rating_percentile'] = brand_stats['avg_rating'].rank(pct=True) * 100
        brand_stats['rank_percentile'] = (1 - brand_stats['avg_rank'].rank(pct=True)) * 100
        
        return brand_stats
    
    def _estimate_annual_sales(self, brand_stats):
        """연간 판매량(개수) 추정 (가격 완전 무시, 리뷰 기반)
        
        - 판매량 ≈ total_reviews / REVIEW_CONVERSION_RATE
        - 연간 판매량 ≈ 판매량 × 재구매계수 × 연간 계수
        """
        # 기본 판매량 = 리뷰수 / 리뷰작성비율
        base_sales_volume = brand_stats['total_reviews'] / REVIEW_CONVERSION_RATE
        
        # 재구매/연간 계수 반영
        annual_sales_volume = base_sales_volume * REPURCHASE_MULTIPLIER * ANNUAL_MULTIPLIER
        
        # NaN 처리
        annual_sales_volume = annual_sales_volume.fillna(0)
        
        # 정수(개수 느낌)로 반환
        return annual_sales_volume.round(0).astype(int)
    
    def _identify_success_factors(self, row, all_brands):
        """성공 요인 식별 (판매량/리뷰/가격/노출/로켓 관점)"""
        
        factors = []
        
        median_reviews = all_brands['total_reviews'].median()
        
        # 압도적 리뷰수
        if row['total_reviews'] > median_reviews * 2:
            factors.append('압도적_리뷰수')
        
        # 높은 평점
        if row['avg_rating'] >= 4.5:
            factors.append('높은_평점')
        
        # 가격 경쟁력 (참고용: avg_price < 중앙값의 90%)
        median_price = all_brands['avg_price'].median()
        if pd.notna(row['avg_price']) and pd.notna(median_price):
            if row['avg_price'] < median_price * 0.9:
                factors.append('가격_경쟁력')
        
        # 상위 노출 (판매량순 정렬 결과에서 상위 노출)
        if row['avg_rank'] <= 10:
            factors.append('상위_노출')
        
        # 로켓배송
        if row['rocket_delivery_ratio'] >= 80:
            factors.append('로켓배송')
        
        # 시장 점유율 (판매량 기준)
        if row.get('market_share', 0) >= 20:
            factors.append('높은_시장점유율')
        
        return ', '.join(factors) if factors else '복합_요인'
    
    def get_top_product(self, keyword, brand):
        """브랜드의 대표 상품 추출 (리뷰수 기준)"""
        
        products = self.coupang[
            (self.coupang['keyword'] == keyword) &
            (self.coupang['brand'] == brand)
        ].copy()
        
        if len(products) == 0:
            return None
        
        top = products.nlargest(1, 'review_count').iloc[0]
        
        return {
            'product_name': top['product_name'],
            'review_count': top['review_count'],
            'rating': top['rating'],
            'price': top['price'],
            'rank': top['rank']
        }


# ==================== 3. 아이허브 경쟁력 분석기 ====================
class IherbCompetitiveAnalyzer:
    """아이허브 제품 경쟁력 분석"""
    
    def __init__(self, coupang_data, iherb_inventory, iherb_sales):
        self.coupang = coupang_data
        self.iherb_inventory = iherb_inventory
        self.iherb_sales = iherb_sales
    
    def analyze_keyword(self, keyword, market_leader_info):
        """키워드별 아이허브 경쟁력 분석"""
        
        keyword_data = self.coupang[self.coupang['keyword'] == keyword].copy()
        
        if len(keyword_data) == 0:
            return None
        
        # 쿠팡 Product ID 목록
        coupang_product_ids = keyword_data['product_id'].unique()
        
        # 아이허브 제품 중 쿠팡에 있는 것 찾기
        iherb_in_coupang = self.iherb_inventory[
            self.iherb_inventory['Product ID'].isin(coupang_product_ids)
        ].copy()
        
        if len(iherb_in_coupang) == 0:
            return None
        
        # 판매 데이터 결합
        iherb_in_coupang = iherb_in_coupang.merge(
            self.iherb_sales[['옵션 ID', '매출(원)', '판매량', '아이템위너 비율(%)']],
            on='옵션 ID',
            how='left'
        )
        
        # 각 제품별 경쟁 위치 분석
        results = []
        
        for _, iherb_prod in iherb_in_coupang.iterrows():
            product_id = int(iherb_prod['Product ID'])
            
            # 쿠팡에서의 데이터
            coupang_matches = keyword_data[keyword_data['product_id'] == product_id]
            
            if len(coupang_matches) == 0:
                continue
            
            coupang_avg = coupang_matches.agg({
                'review_count': 'mean',
                'rating': 'mean',
                'price': 'mean',
                'rank': 'mean'
            })
            
            # 시장 내 순위 (리뷰수 기준)
            all_reviews = keyword_data.groupby('product_id')['review_count'].mean().sort_values(ascending=False)
            rank_in_market = list(all_reviews.index).index(product_id) + 1
            
            # 리더 대비 격차
            leader_reviews = market_leader_info['leader_product_reviews']
            leader_price = market_leader_info['leader_avg_price']
            
            review_gap_pct = ((leader_reviews - coupang_avg['review_count']) / leader_reviews * 100) if leader_reviews > 0 else 0
            price_gap_pct = (
                (coupang_avg['price'] - leader_price) / leader_price * 100
                if (leader_price > 0 and pd.notna(coupang_avg['price']))
                else 0
            )
            
            # 경쟁 상태 판단
            if rank_in_market <= 10:
                competitive_status = '강세'
            elif rank_in_market <= 30:
                competitive_status = '중립'
            else:
                competitive_status = '약세'
            
            results.append({
                'keyword': keyword,
                'iherb_product_id': product_id,
                'iherb_product_name': iherb_prod['쿠팡 노출 상품명'],
                'iherb_vendor_item_id': iherb_prod['업체상품 ID'],
                'iherb_price': iherb_prod['판매가격'],
                'iherb_sales_amount': iherb_prod['매출(원)'],
                'iherb_sales_volume': iherb_prod['판매량'],
                'iherb_winner_rate': iherb_prod['아이템위너 비율(%)'],
                'coupang_reviews': int(coupang_avg['review_count']),
                'coupang_rating': round(coupang_avg['rating'], 2),
                'coupang_price': coupang_avg['price'],
                'coupang_rank': round(coupang_avg['rank'], 1),
                'rank_in_market': rank_in_market,
                'total_products': len(all_reviews),
                'leader_brand': market_leader_info['leader_brand'],
                'leader_reviews': leader_reviews,
                'leader_price': leader_price,
                'review_gap_pct': round(review_gap_pct, 1),
                'price_gap_pct': round(price_gap_pct, 1),
                'competitive_status': competitive_status
            })
        
        return pd.DataFrame(results) if results else None


# ==================== 4. 전략 제안 엔진 ====================
class StrategyRecommender:
    """전략 제안 생성"""
    
    def generate_recommendations(self, keyword, market_structure, iherb_position):
        """키워드별 전략 제안"""
        
        if market_structure is None or len(market_structure) == 0:
            return None
        
        leader = market_structure.iloc[0]
        
        recommendation = {
            'keyword': keyword,
            'market_leader': leader['brand'],
            'leader_market_share': leader['market_share'],
            'leader_estimated_sales': leader['estimated_annual_sales'],
            'key_success_factors': leader['success_factors'],
        }
        
        # 아이허브 상태 판단
        if iherb_position is None or len(iherb_position) == 0:
            # 미진입
            recommendation['iherb_status'] = '미진입'
            recommendation['primary_gap'] = '시장_진입'
            
            # 시장 규모 기반 진입 여부
            if leader['estimated_annual_sales'] > 1_000_000:  # 개수 기준이라 숫자는 상대적 의미
                recommendation['entry_recommendation'] = '적극_진입_권장'
            else:
                recommendation['entry_recommendation'] = '진입_검토'
            
            recommendation['recommendation_priority_1'] = f"{leader['brand']} 벤치마킹"
            recommendation['recommendation_priority_2'] = "리뷰 확보 전략 (샘플링/프로모션)"
            recommendation['recommendation_priority_3'] = "검색 최적화 (키워드/상품명)"
            recommendation['estimated_investment_level'] = '상'
            
        else:
            # 진입 상태
            best_iherb = iherb_position.nlargest(1, 'coupang_reviews').iloc[0]
            
            if best_iherb['competitive_status'] == '강세':
                recommendation['iherb_status'] = '선전'
                recommendation['primary_gap'] = '1위_도약'
                
                recommendation['recommendation_priority_1'] = "시장 점유율 확대 (프로모션 강화)"
                recommendation['recommendation_priority_2'] = "브랜드 인지도 제고"
                recommendation['recommendation_priority_3'] = "상품 라인업 확대"
                recommendation['estimated_investment_level'] = '중'
                
            elif best_iherb['competitive_status'] == '중립':
                recommendation['iherb_status'] = '중립'
                
                gaps = {
                    '리뷰': best_iherb['review_gap_pct'],
                    '가격': abs(best_iherb['price_gap_pct']),
                    '노출': (best_iherb['rank_in_market'] / best_iherb['total_products'] * 100)
                }
                recommendation['primary_gap'] = max(gaps, key=gaps.get)
                
                if gaps['리뷰'] > 70:
                    recommendation['recommendation_priority_1'] = "리뷰 확보 집중 (초기 할인/샘플링)"
                elif gaps['가격'] > 20:
                    if best_iherb['price_gap_pct'] > 0:
                        recommendation['recommendation_priority_1'] = "가격 최적화 (현재 너무 높음)"
                    else:
                        recommendation['recommendation_priority_1'] = "가격 우위 마케팅"
                else:
                    recommendation['recommendation_priority_1'] = "검색 순위 개선 (SEO)"
                
                recommendation['recommendation_priority_2'] = "로켓배송 확대"
                recommendation['recommendation_priority_3'] = "상품 상세페이지 개선"
                recommendation['estimated_investment_level'] = '중'
                
            else:  # 약세
                recommendation['iherb_status'] = '약세'
                
                gaps = {
                    '리뷰': best_iherb['review_gap_pct'],
                    '가격': abs(best_iherb['price_gap_pct']),
                    '노출': (best_iherb['rank_in_market'] / best_iherb['total_products'] * 100)
                }
                recommendation['primary_gap'] = max(gaps, key=gaps.get)
                
                recommendation['recommendation_priority_1'] = "전면 리뷰 확보 전략"
                recommendation['recommendation_priority_2'] = "가격 경쟁력 강화"
                recommendation['recommendation_priority_3'] = "검색광고 투자"
                recommendation['estimated_investment_level'] = '상'
            
            # 아이허브 제품 정보 추가
            recommendation['best_iherb_product'] = best_iherb['iherb_product_name']
            recommendation['best_iherb_rank'] = best_iherb['rank_in_market']
            recommendation['best_iherb_reviews'] = best_iherb['coupang_reviews']
            recommendation['review_gap_pct'] = best_iherb['review_gap_pct']
            recommendation['price_gap_pct'] = best_iherb['price_gap_pct']
        
        return recommendation


# ==================== 5. 메인 실행기 ====================
class MarketIntelligenceSystem:
    """시장 분석 시스템 통합"""
    
    def __init__(self):
        self.data_loader = DataLoader()
        self.market_analyzer = None
        self.iherb_analyzer = None
        self.strategy_recommender = StrategyRecommender()
        
        self.market_structures = []
        self.iherb_positions = []
        self.recommendations = []
    
    def run(self):
        """전체 분석 실행"""
        
        print("\n" + "=" * 80)
        print("아이허브 시장 분석 시스템")
        print("=" * 80 + "\n")
        
        # 데이터 로드 및 전처리
        self.data_loader.load_all().preprocess()
        
        # 분석기 초기화
        self.market_analyzer = MarketAnalyzer(self.data_loader.coupang)
        self.iherb_analyzer = IherbCompetitiveAnalyzer(
            self.data_loader.coupang,
            self.data_loader.iherb_inventory,
            self.data_loader.iherb_sales
        )
        
        # 키워드별 분석
        print("=" * 80)
        print("3. 키워드별 시장 분석")
        print("=" * 80 + "\n")
        
        for idx, keyword in enumerate(TARGET_KEYWORDS, 1):
            print(f"[{idx}/{len(TARGET_KEYWORDS)}] {keyword}")
            
            # 시장 구조 분석
            market_structure = self.market_analyzer.analyze_keyword(keyword)
            
            if market_structure is None or len(market_structure) == 0:
                print(f"  ✗ 데이터 없음\n")
                continue
            
            # 상위 5개 브랜드만 저장
            market_structure_top5 = market_structure.head(5).copy()
            market_structure_top5['keyword'] = keyword
            
            # 1위 브랜드 대표 상품
            leader = market_structure.iloc[0]
            top_product = self.market_analyzer.get_top_product(keyword, leader['brand'])
            
            if top_product:
                market_structure_top5.loc[market_structure_top5['rank'] == 1, 'representative_product'] = top_product['product_name']
                market_structure_top5.loc[market_structure_top5['rank'] == 1, 'leader_product_reviews'] = top_product['review_count']
            
            self.market_structures.append(market_structure_top5)
            
            print(f"  ✓ 시장 리더: {leader['brand']} (점유율 {leader['market_share']:.1f}%)")
            
            # 아이허브 경쟁력 분석
            leader_info = {
                'leader_brand': leader['brand'],
                'leader_avg_price': leader['avg_price'],
                'leader_product_reviews': top_product['review_count'] if top_product else leader['avg_reviews']
            }
            
            iherb_position = self.iherb_analyzer.analyze_keyword(keyword, leader_info)
            
            if iherb_position is not None and len(iherb_position) > 0:
                self.iherb_positions.append(iherb_position)
                best = iherb_position.nlargest(1, 'coupang_reviews').iloc[0]
                print(f"  ✓ 아이허브: {len(iherb_position)}개 제품, 최고 순위 {best['rank_in_market']}위 ({best['competitive_status']})")
            else:
                print(f"  ✗ 아이허브 제품 없음")
            
            # 전략 제안
            recommendation = self.strategy_recommender.generate_recommendations(
                keyword, market_structure, iherb_position
            )
            
            if recommendation:
                self.recommendations.append(recommendation)
                print(f"  → 전략: {recommendation['iherb_status']} / 우선순위: {recommendation['recommendation_priority_1']}")
            
            print()
        
        # 결과 저장
        self._save_results()
        
        print("=" * 80)
        print("분석 완료")
        print("=" * 80)
    
    def _save_results(self):
        """결과 저장"""
        
        print("\n" + "=" * 80)
        print("4. 결과 저장")
        print("=" * 80 + "\n")
        
        output_dir = '/Users/brich/Desktop/iherb_price/251120/outputs'
        
        # 1) 키워드별 시장 구조
        if self.market_structures:
            df = pd.concat(self.market_structures, ignore_index=True)
            
            # 컬럼 정리 (market_score 제거됨)
            cols = [
                'keyword', 'rank', 'brand', 'representative_product',
                'total_reviews', 'market_share', 'avg_reviews', 'avg_rating',
                'avg_price', 'avg_rank', 'rocket_delivery_ratio',
                'estimated_annual_sales', 'success_factors'
            ]
            df = df[[col for col in cols if col in df.columns]]
            
            path = f"{output_dir}/01_keyword_market_structure.csv"
            df.to_csv(path, index=False, encoding='utf-8-sig')
            print(f"✓ 시장 구조: {path}")
            print(f"  → {len(df)}개 브랜드 분석")
        
        # 2) 아이허브 경쟁 위치
        if self.iherb_positions:
            df = pd.concat(self.iherb_positions, ignore_index=True)
            
            path = f"{output_dir}/02_iherb_competitive_position.csv"
            df.to_csv(path, index=False, encoding='utf-8-sig')
            print(f"✓ 아이허브 경쟁 위치: {path}")
            print(f"  → {len(df)}개 제품 분석")
        
        # 3) 전략 제안
        if self.recommendations:
            df = pd.DataFrame(self.recommendations)
            
            path = f"{output_dir}/03_strategic_recommendations.csv"
            df.to_csv(path, index=False, encoding='utf-8-sig')
            print(f"✓ 전략 제안: {path}")
            print(f"  → {len(df)}개 키워드 전략")


# ==================== 실행 ====================
if __name__ == "__main__":
    system = MarketIntelligenceSystem()
    system.run()
