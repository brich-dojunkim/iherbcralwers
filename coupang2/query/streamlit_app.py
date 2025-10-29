#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Streamlit 순위 비교 분석 대시보드
- 기준 소스의 상위 제품과 매칭된 제품 비교
- 문제점 자동 진단
"""

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# 페이지 설정
st.set_page_config(
    page_title="쿠팡 순위 비교 분석",
    page_icon="🔍",
    layout="wide"
)

DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/monitoring.db"


@st.cache_data
def get_available_dates():
    """수집된 날짜 목록 조회"""
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT DISTINCT DATE(snapshot_time) as date
    FROM snapshots
    ORDER BY date DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['date'].tolist()


@st.cache_data
def get_categories():
    """카테고리 목록 조회"""
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT DISTINCT name FROM categories ORDER BY name"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['name'].tolist()


@st.cache_data
def get_products_by_rank(date, category, source_type, rank_from, rank_to):
    """특정 날짜/카테고리/소스의 순위별 제품 조회"""
    
    conn = sqlite3.connect(DB_PATH)
    
    source_map = {
        '로켓직구': 'rocket_direct',
        '아이허브': 'iherb_official'
    }
    
    query = """
    SELECT 
        ps.vendor_item_id,
        ps.product_name,
        ps.product_url,
        ps.category_rank,
        ps.current_price,
        ps.discount_rate,
        ps.review_count,
        ps.rating_score,
        mr.iherb_upc,
        mr.iherb_part_number
    FROM product_states ps
    JOIN snapshots snap ON ps.snapshot_id = snap.id
    JOIN sources s ON snap.source_id = s.id
    JOIN categories c ON snap.category_id = c.id
    LEFT JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
    WHERE DATE(snap.snapshot_time) = ?
      AND c.name = ?
      AND s.source_type = ?
      AND ps.category_rank >= ?
      AND ps.category_rank <= ?
    ORDER BY ps.category_rank
    """
    
    df = pd.read_sql_query(
        query, 
        conn, 
        params=(date, category, source_map[source_type], rank_from, rank_to)
    )
    conn.close()
    
    return df


@st.cache_data
def get_matched_product(upc, date, category, source_type):
    """UPC로 매칭된 제품 조회"""
    
    if pd.isna(upc) or upc is None:
        return None
    
    conn = sqlite3.connect(DB_PATH)
    
    source_map = {
        '로켓직구': 'rocket_direct',
        '아이허브': 'iherb_official'
    }
    
    query = """
    SELECT 
        ps.vendor_item_id,
        ps.product_name,
        ps.product_url,
        ps.category_rank,
        ps.current_price,
        ps.discount_rate,
        ps.review_count,
        ps.rating_score
    FROM product_states ps
    JOIN snapshots snap ON ps.snapshot_id = snap.id
    JOIN sources s ON snap.source_id = s.id
    JOIN categories c ON snap.category_id = c.id
    JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
    WHERE DATE(snap.snapshot_time) = ?
      AND c.name = ?
      AND s.source_type = ?
      AND mr.iherb_upc = ?
    LIMIT 1
    """
    
    df = pd.read_sql_query(
        query, 
        conn, 
        params=(date, category, source_map[source_type], upc)
    )
    conn.close()
    
    if df.empty:
        return None
    
    return df.iloc[0].to_dict()


@st.cache_data
def get_rank_history(vendor_item_id, category, source_type, days=7):
    """제품의 최근 순위 변화 조회"""
    
    conn = sqlite3.connect(DB_PATH)
    
    source_map = {
        '로켓직구': 'rocket_direct',
        '아이허브': 'iherb_official'
    }
    
    query = """
    SELECT 
        DATE(snap.snapshot_time) as date,
        ps.category_rank
    FROM product_states ps
    JOIN snapshots snap ON ps.snapshot_id = snap.id
    JOIN sources s ON snap.source_id = s.id
    JOIN categories c ON snap.category_id = c.id
    WHERE ps.vendor_item_id = ?
      AND c.name = ?
      AND s.source_type = ?
    ORDER BY snap.snapshot_time DESC
    LIMIT ?
    """
    
    df = pd.read_sql_query(
        query, 
        conn, 
        params=(vendor_item_id, category, source_map[source_type], days)
    )
    conn.close()
    
    return df


def diagnose_problems(base_product, matched_product):
    """두 제품 비교 후 문제점 진단"""
    
    problems = []
    
    # 가격 차이
    price_diff = matched_product['current_price'] - base_product['current_price']
    price_diff_pct = (price_diff / base_product['current_price']) * 100
    
    if abs(price_diff_pct) > 10:
        if price_diff > 0:
            problems.append({
                'type': '가격',
                'severity': 'high',
                'message': f"가격이 {abs(price_diff):,.0f}원 ({abs(price_diff_pct):.1f}%) 더 비쌈",
                'icon': '💰'
            })
        else:
            problems.append({
                'type': '가격',
                'severity': 'low',
                'message': f"가격이 {abs(price_diff):,.0f}원 ({abs(price_diff_pct):.1f}%) 더 저렴",
                'icon': '✅'
            })
    
    # 할인율 차이
    discount_diff = matched_product['discount_rate'] - base_product['discount_rate']
    
    if abs(discount_diff) > 5:
        if discount_diff < 0:
            problems.append({
                'type': '할인율',
                'severity': 'medium',
                'message': f"할인율이 {abs(discount_diff)}%p 낮음",
                'icon': '⚠️'
            })
        else:
            problems.append({
                'type': '할인율',
                'severity': 'low',
                'message': f"할인율이 {discount_diff}%p 높음",
                'icon': '✅'
            })
    
    # 리뷰 수 차이
    review_diff = matched_product['review_count'] - base_product['review_count']
    
    if abs(review_diff) > 100:
        if review_diff < 0:
            problems.append({
                'type': '리뷰',
                'severity': 'medium',
                'message': f"리뷰가 {abs(review_diff):,}개 적음",
                'icon': '⚠️'
            })
        else:
            problems.append({
                'type': '리뷰',
                'severity': 'low',
                'message': f"리뷰가 {review_diff:,}개 많음",
                'icon': '✅'
            })
    
    # 순위 차이
    rank_diff = matched_product['category_rank'] - base_product['category_rank']
    
    if abs(rank_diff) > 20:
        if rank_diff > 0:
            problems.append({
                'type': '순위',
                'severity': 'high',
                'message': f"순위가 {abs(rank_diff)}위 낮음",
                'icon': '🔴'
            })
        else:
            problems.append({
                'type': '순위',
                'severity': 'low',
                'message': f"순위가 {abs(rank_diff)}위 높음",
                'icon': '✅'
            })
    
    # 평점 차이
    rating_diff = matched_product['rating_score'] - base_product['rating_score']
    
    if abs(rating_diff) > 0.3:
        if rating_diff < 0:
            problems.append({
                'type': '평점',
                'severity': 'low',
                'message': f"평점이 {abs(rating_diff):.1f}점 낮음",
                'icon': '⚠️'
            })
        else:
            problems.append({
                'type': '평점',
                'severity': 'low',
                'message': f"평점이 {rating_diff:.1f}점 높음",
                'icon': '✅'
            })
    
    return problems


def display_product_comparison(base_product, matched_product, base_source, matched_source, category):
    """제품 비교 카드 표시"""
    
    # 순위 표시
    rank = base_product['category_rank']
    st.markdown(f"## {rank}위. [{base_source}] {base_product['product_name'][:80]}")
    
    # 기준 제품 정보
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.markdown(f"**💰 가격**")
        st.markdown(f"{base_product['current_price']:,}원")
        st.markdown(f"할인 {base_product['discount_rate']}%")
    
    with col2:
        st.markdown(f"**⭐ 평점**")
        st.markdown(f"{base_product['rating_score']:.1f}점")
    
    with col3:
        st.markdown(f"**💬 리뷰**")
        st.markdown(f"{base_product['review_count']:,}개")
    
    with col4:
        st.markdown(f"**📦 UPC**")
        st.markdown(f"{base_product['iherb_upc']}")
    
    with col5:
        st.markdown(f"**🔗 링크**")
        st.markdown(f"[상품 보기]({base_product['product_url']})")
    
    # 매칭된 제품이 있는 경우
    if matched_product:
        st.markdown("---")
        
        # 매칭 제품 정보 박스
        with st.container():
            st.markdown(f"### 📊 매칭 비교: [{matched_source}] {matched_product['product_name'][:80]}")
            
            # 비교 정보
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                price_diff = matched_product['current_price'] - base_product['current_price']
                price_color = "red" if price_diff > 0 else "green" if price_diff < 0 else "gray"
                st.markdown(f"**💰 가격**")
                st.markdown(f"{matched_product['current_price']:,}원")
                st.markdown(f":{price_color}[{'⬆' if price_diff > 0 else '⬇' if price_diff < 0 else '='} {abs(price_diff):,}원]")
            
            with col2:
                st.markdown(f"**⭐ 평점**")
                st.markdown(f"{matched_product['rating_score']:.1f}점")
                rating_diff = matched_product['rating_score'] - base_product['rating_score']
                if rating_diff != 0:
                    st.markdown(f"{'⬆' if rating_diff > 0 else '⬇'} {abs(rating_diff):.1f}점")
            
            with col3:
                st.markdown(f"**💬 리뷰**")
                st.markdown(f"{matched_product['review_count']:,}개")
                review_diff = matched_product['review_count'] - base_product['review_count']
                if review_diff != 0:
                    st.markdown(f"{'⬆' if review_diff > 0 else '⬇'} {abs(review_diff):,}개")
            
            with col4:
                rank_diff = matched_product['category_rank'] - base_product['category_rank']
                rank_color = "red" if rank_diff > 0 else "green" if rank_diff < 0 else "gray"
                st.markdown(f"**📊 순위**")
                st.markdown(f"{matched_product['category_rank']}위")
                st.markdown(f":{rank_color}[{'⬇' if rank_diff > 0 else '⬆' if rank_diff < 0 else '='} {abs(rank_diff)}위 차이]")
            
            with col5:
                discount_diff = matched_product['discount_rate'] - base_product['discount_rate']
                st.markdown(f"**🏷️ 할인율**")
                st.markdown(f"{matched_product['discount_rate']}%")
                if discount_diff != 0:
                    st.markdown(f"{'⬆' if discount_diff > 0 else '⬇'} {abs(discount_diff)}%p")
            
            # 문제 진단
            problems = diagnose_problems(base_product, matched_product)
            
            if problems:
                st.markdown("#### 💡 문제 진단")
                
                high_problems = [p for p in problems if p['severity'] == 'high']
                medium_problems = [p for p in problems if p['severity'] == 'medium']
                
                if high_problems:
                    for prob in high_problems:
                        st.error(f"{prob['icon']} **{prob['type']}**: {prob['message']}")
                
                if medium_problems:
                    for prob in medium_problems:
                        st.warning(f"{prob['icon']} **{prob['type']}**: {prob['message']}")
            
            # 순위 변화 추이
            history = get_rank_history(matched_product['vendor_item_id'], category, matched_source, days=7)
            
            if not history.empty and len(history) > 1:
                st.markdown("#### 📈 순위 변화 추이 (최근 7일)")
                
                # 날짜순으로 정렬 (오래된 것부터)
                history = history.sort_values('date')
                
                trend_text = " → ".join([f"{row['date'][5:]}: {row['category_rank']}위" for _, row in history.iterrows()])
                st.text(trend_text)
                
                # 추세 분석
                first_rank = history.iloc[0]['category_rank']
                last_rank = history.iloc[-1]['category_rank']
                rank_change = first_rank - last_rank
                
                if rank_change > 0:
                    st.success(f"⬆ 상승 추세 ({len(history)}일간 +{rank_change}위)")
                elif rank_change < 0:
                    st.error(f"⬇ 하락 추세 ({len(history)}일간 {rank_change}위)")
                else:
                    st.info(f"→ 유지 ({len(history)}일간 변동 없음)")
    
    else:
        st.warning("❌ 매칭된 제품이 없습니다")
    
    st.markdown("---")


def main():
    """메인 함수"""
    
    st.title("🔍 쿠팡 순위 비교 분석 대시보드")
    st.caption("기준 소스의 상위 제품과 매칭된 제품을 비교하여 문제점을 진단합니다")
    
    # 데이터 로드
    dates = get_available_dates()
    categories = get_categories()
    
    if not dates or not categories:
        st.error("데이터가 없습니다.")
        return
    
    # 필터 영역
    st.markdown("## 🔍 필터 설정")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_date = st.selectbox('📅 날짜', dates, index=0)
    
    with col2:
        selected_category = st.selectbox('📂 카테고리', categories)
    
    with col3:
        base_source = st.radio('🏪 기준 소스', ['로켓직구', '아이허브'], horizontal=True)
    
    # 순위 범위 설정
    st.markdown("### 📊 순위 범위")
    rank_range = st.slider('순위', min_value=1, max_value=100, value=(1, 20))
    rank_from, rank_to = rank_range
    
    # 검색
    search_term = st.text_input('🔍 상품명 검색 (선택)')
    
    st.markdown("---")
    
    # 데이터 로드
    matched_source = '아이허브' if base_source == '로켓직구' else '로켓직구'
    
    try:
        with st.spinner('데이터 로딩 중...'):
            products = get_products_by_rank(
                selected_date, 
                selected_category, 
                base_source, 
                rank_from, 
                rank_to
            )
        
        if products.empty:
            st.warning("조건에 맞는 제품이 없습니다.")
            return
        
        # 검색 필터 적용
        if search_term:
            products = products[products['product_name'].str.contains(search_term, case=False, na=False)]
            
            if products.empty:
                st.warning(f"'{search_term}'에 해당하는 제품이 없습니다.")
                return
        
        st.markdown(f"## 📋 [{base_source}] 상위 제품 ({len(products)}개)")
        st.caption(f"날짜: {selected_date} | 카테고리: {selected_category} | 순위: {rank_from}~{rank_to}위")
        
        # 각 제품 표시
        for idx, row in products.iterrows():
            base_product = row.to_dict()
            
            # 매칭된 제품 찾기
            matched_product = get_matched_product(
                base_product['iherb_upc'],
                selected_date,
                selected_category,
                matched_source
            )
            
            # 비교 카드 표시
            display_product_comparison(
                base_product,
                matched_product,
                base_source,
                matched_source,
                selected_category
            )
    
    except Exception as e:
        st.error(f"오류 발생: {e}")
        import traceback
        st.code(traceback.format_exc())


if __name__ == "__main__":
    main()