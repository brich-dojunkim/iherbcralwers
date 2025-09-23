"""
데이터 처리 모듈 - 향상된 상품 상태 추적 (이중 시점 추적)
"""

import pandas as pd
from datetime import datetime
from typing import Tuple


class DataProcessor:
    """데이터 분류 및 업데이트 처리 - 향상된 상품 상태 추적"""
    
    def __init__(self):
        self.timestamp = datetime.now().isoformat()
        self.date_only = datetime.now().strftime('%Y-%m-%d')
    
    def classify_products(self, base_df: pd.DataFrame, crawled_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        상품 분류 + 향상된 상태 추적 (이중 시점 추적)
        
        Args:
            base_df: 기존 매칭 결과 데이터
            crawled_df: 크롤링된 최신 쿠팡 데이터
            
        Returns:
            (업데이트된 기존 상품 DataFrame, 신규 상품 DataFrame)
        """
        # 크롤링 데이터가 비어있는 경우 처리
        if len(crawled_df) == 0 or 'product_id' not in crawled_df.columns:
            print("   크롤링 데이터가 없어서 분류를 건너뜀")
            empty_df = pd.DataFrame(columns=['product_id', 'product_name', 'current_price', 'original_price', 'discount_rate'])
            return base_df, empty_df
        
        # 상품 ID 집합 생성
        existing_ids = set(base_df['coupang_product_id'].dropna().astype(str))
        crawled_ids = set(crawled_df['product_id'].astype(str))
        
        # 상태 분류
        active_ids = existing_ids & crawled_ids      # 계속 활성
        missing_ids = existing_ids - crawled_ids     # 사라짐  
        new_ids = crawled_ids - existing_ids         # 신규 발견
        
        print(f"   🟢 계속 활성: {len(active_ids)}개")
        print(f"   🔴 사라진 상품: {len(missing_ids)}개") 
        print(f"   🆕 신규 발견: {len(new_ids)}개")
        
        # 1. 기존 데이터 상태 업데이트
        updated_base_df = self._update_existing_status_enhanced(base_df, active_ids, missing_ids, crawled_df)
        
        # 2. 신규 상품 데이터 생성  
        new_products_df = self._create_new_products_data_enhanced(crawled_df, new_ids)
        
        return updated_base_df, new_products_df
    
    def _update_existing_status_enhanced(self, base_df: pd.DataFrame, active_ids: set, missing_ids: set, crawled_df: pd.DataFrame) -> pd.DataFrame:
        """기존 상품 상태 업데이트 - 향상된 이중 시점 추적"""
        updated_df = base_df.copy()
        
        # 상태 추적 컬럼 초기화 (없으면 생성)
        if 'product_status' not in updated_df.columns:
            updated_df['product_status'] = 'active'
        if 'status_changed_at' not in updated_df.columns:
            updated_df['status_changed_at'] = ''
        if 'last_status_change' not in updated_df.columns:
            updated_df['last_status_change'] = ''
        
        # 가격 업데이트 준비
        if len(crawled_df) > 0:
            crawled_clean = crawled_df.drop_duplicates(subset=['product_id'], keep='first')
            price_updates = crawled_clean.set_index('product_id').to_dict('index')
        else:
            price_updates = {}
        
        status_changes = 0
        price_updates_count = 0
        reverted_products = 0  # 복귀한 상품 수
        newly_missing = 0      # 새로 사라진 상품 수
        
        for idx, row in updated_df.iterrows():
            product_id = str(row['coupang_product_id'])
            current_status = row.get('product_status', 'active')
            status_changed = False
            
            # 상태 변화 확인 및 업데이트
            if product_id in missing_ids:
                if current_status != 'missing':
                    # active/new → missing 전환
                    updated_df.at[idx, 'product_status'] = 'missing'
                    updated_df.at[idx, 'status_changed_at'] = self.timestamp
                    updated_df.at[idx, 'last_status_change'] = self.timestamp
                    status_changes += 1
                    newly_missing += 1
                    status_changed = True
                    
            elif product_id in active_ids:
                # missing → active 복귀
                if current_status == 'missing':
                    updated_df.at[idx, 'product_status'] = 'active'
                    updated_df.at[idx, 'status_changed_at'] = self.timestamp
                    updated_df.at[idx, 'last_status_change'] = self.timestamp
                    status_changes += 1
                    reverted_products += 1
                    status_changed = True
                
                # new → active 전환 (매칭 완료된 신규 상품)
                elif current_status == 'new':
                    updated_df.at[idx, 'product_status'] = 'active'
                    updated_df.at[idx, 'status_changed_at'] = self.timestamp
                    updated_df.at[idx, 'last_status_change'] = self.timestamp
                    status_changes += 1
                    status_changed = True
                
                # 가격 정보 업데이트 (active 상품만)
                if product_id in price_updates:
                    new_data = price_updates[product_id]
                    
                    price_fields = {
                        'coupang_current_price_krw': 'current_price',
                        'coupang_original_price_krw': 'original_price', 
                        'coupang_discount_rate': 'discount_rate'
                    }
                    
                    for base_field, crawled_field in price_fields.items():
                        if crawled_field in new_data and new_data[crawled_field]:
                            updated_df.at[idx, base_field] = new_data[crawled_field]
                    
                    updated_df.at[idx, 'price_updated_at'] = self.timestamp
                    price_updates_count += 1
                    
                    # 가격 업데이트는 상태 변경이 아니므로 last_status_change는 업데이트하지 않음
                    # 단, status_changed_at과 last_status_change가 비어있으면 초기값 설정
                    if not updated_df.at[idx, 'status_changed_at']:
                        updated_df.at[idx, 'status_changed_at'] = self.timestamp
                    if not updated_df.at[idx, 'last_status_change']:
                        updated_df.at[idx, 'last_status_change'] = self.timestamp
        
        # 상세 통계 출력
        print(f"   📈 상태 변화: {status_changes}개")
        if newly_missing > 0:
            print(f"      └─ 🔴 새로 사라짐: {newly_missing}개")
        if reverted_products > 0:
            print(f"      └─ 🔄 복귀한 상품: {reverted_products}개")
        print(f"   💰 가격 업데이트: {price_updates_count}개")
        
        return updated_df
    
    def _create_new_products_data_enhanced(self, crawled_df: pd.DataFrame, new_ids: set) -> pd.DataFrame:
        """신규 상품 데이터 생성 - 향상된 이중 시점 추적"""
        if len(new_ids) == 0:
            return pd.DataFrame()
        
        # 신규 상품들만 필터링
        new_products_df = crawled_df[crawled_df['product_id'].isin(new_ids)].copy()
        
        # 상태 추적 컬럼 추가 (신규 상품은 두 시점이 동일)
        new_products_df['product_status'] = 'new'
        new_products_df['status_changed_at'] = self.timestamp
        new_products_df['last_status_change'] = self.timestamp
        
        # 컬럼명 매핑 (coupang 크롤링 → 표준 형식)
        column_mapping = {
            'product_id': 'coupang_product_id',
            'product_name': 'coupang_product_name',
            'current_price': 'coupang_current_price_krw',
            'original_price': 'coupang_original_price_krw',
            'discount_rate': 'coupang_discount_rate'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in new_products_df.columns:
                new_products_df[new_col] = new_products_df[old_col]
        
        return new_products_df
    
    def update_existing_prices(self, base_df: pd.DataFrame, existing_products_df: pd.DataFrame) -> pd.DataFrame:
        """
        기존 상품의 쿠팡 가격 정보 업데이트
        (deprecated - classify_products에서 통합 처리됨)
        """
        # 이제 classify_products에서 가격 업데이트도 함께 처리하므로 
        # 이 메서드는 호환성을 위해 유지하되 실제로는 사용하지 않음
        return base_df
    
    def integrate_final_data(self, updated_base_df: pd.DataFrame, matched_new_df: pd.DataFrame) -> pd.DataFrame:
        """최종 데이터 통합 - 향상된 상태 추적"""
        
        # 신규 상품이 매칭 완료되면 상태를 'active'로 변경
        if len(matched_new_df) > 0:
            matched_new_df = matched_new_df.copy()
            
            # 매칭 성공한 상품들의 상태 업데이트
            success_mask = matched_new_df['status'] == 'success'
            matched_new_df.loc[success_mask, 'product_status'] = 'active'
            matched_new_df.loc[success_mask, 'status_changed_at'] = self.timestamp
            matched_new_df.loc[success_mask, 'last_status_change'] = self.timestamp
            
            # 통합
            final_df = pd.concat([updated_base_df, matched_new_df], ignore_index=True)
        else:
            final_df = updated_base_df.copy()
        
        # 메타데이터 추가
        final_df['last_updated'] = self.timestamp
        
        # 상태별 요약 및 분석
        if 'product_status' in final_df.columns:
            self._print_enhanced_status_summary(final_df)
        
        print(f"   통합 완료: {len(final_df)}개 상품")
        
        return final_df
    
    def _print_enhanced_status_summary(self, df: pd.DataFrame):
        """향상된 상태 요약 출력"""
        print(f"\n📋 최종 상품 상태 요약:")
        
        # 기본 상태별 분포
        status_summary = df['product_status'].value_counts()
        for status, count in status_summary.items():
            emoji = {'active': '🟢', 'missing': '🔴', 'new': '🆕'}.get(status, '❓')
            print(f"   {emoji} {status}: {count}개")
        
        # 오늘 상태 변경된 상품들
        today_changes = 0
        if 'last_status_change' in df.columns:
            today_changes = len(df[
                (df['last_status_change'].astype(str).str.contains(self.date_only, na=False))
            ])
        
        if today_changes > 0:
            print(f"\n📊 금일 상태 변경: {today_changes}개")
        
        # 최근 활동 상품들 (지난 7일)
        recent_active = 0
        if 'last_status_change' in df.columns:
            # 간단한 최근 활동 계산 (실제로는 더 정교한 날짜 비교 필요)
            recent_active = len(df[df['last_status_change'].astype(str) != ''])
        
        if recent_active > 0:
            print(f"📈 최근 활동 상품: {recent_active}개")
    
    def get_status_analytics(self, df: pd.DataFrame) -> dict:
        """상품 상태 분석 리포트"""
        analytics = {
            'total_products': len(df),
            'status_distribution': {},
            'recent_changes': 0,
            'long_term_missing': 0,
            'stable_active': 0
        }
        
        if 'product_status' in df.columns:
            analytics['status_distribution'] = df['product_status'].value_counts().to_dict()
        
        if 'last_status_change' in df.columns:
            # 오늘 변경된 상품
            analytics['recent_changes'] = len(df[
                (df['last_status_change'].astype(str).str.contains(self.date_only, na=False))
            ])
            
            # 장기 missing 상품 (임시 구현)
            missing_products = df[df['product_status'] == 'missing']
            analytics['long_term_missing'] = len(missing_products)
            
            # 안정적인 active 상품
            active_products = df[df['product_status'] == 'active']
            analytics['stable_active'] = len(active_products)
        
        return analytics