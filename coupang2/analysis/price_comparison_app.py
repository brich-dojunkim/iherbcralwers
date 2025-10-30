#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
로켓직구 vs 아이허브 가격 비교 Streamlit 대시보드
- 로켓직구 전체 상품 기준
- rocket.csv 기반 매칭
- 인터랙티브 필터링 및 정렬
"""

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import re
import sys
from datetime import datetime
from pathlib import Path

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_manager import DataManager
from config.settings import Config

# 페이지 설정
st.set_page_config(
    page_title="로켓직구 vs 아이허브 가격 비교",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_data
def get_available_dates():
    """사용 가능한 날짜 목록"""
    conn = sqlite3.connect(str(Config.DB_PATH))
    query = "SELECT DISTINCT DATE(snapshot_time) as date FROM snapshots ORDER BY date DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['date'].tolist()


@st.cache_data
def get_categories():
    """카테고리 목록"""
    conn = sqlite3.connect(str(Config.DB_PATH))
    query = "SELECT DISTINCT name FROM categories ORDER BY name"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['name'].tolist()


@st.cache_data
def load_comparison_data(target_date):
    """가격 비교 데이터 로드 (rocket.csv 기반)"""
    
    # DataManager를 사용하여 통합 데이터 로드
    manager = DataManager(
        db_path=str(Config.DB_PATH),
        rocket_csv_path=str(Config.MATCHING_CSV_PATH),
        excel_dir=str(Config.IHERB_EXCEL_DIR)
    )
    
    df = manager.get_integrated_df(target_date=target_date)
    
    if df.empty:
        return df
    
    # 컬럼 이름 변환 (기존 코드와 호환성)
    df = df.rename(columns={
        'rocket_vendor_id': 'vendor_item_id',
        'rocket_product_name': 'product_name',
        'rocket_category': 'category',
        'rocket_rank': 'category_rank',
        'rocket_price': 'current_price',
        'rocket_original_price': 'original_price',
        'rocket_discount_rate': 'discount_rate',
        'rocket_rating': 'rating_score',
        'rocket_reviews': 'review_count',
        'rocket_url': 'product_url',
        'part_number': 'iherb_part_number',
        'upc': 'iherb_upc',
    })
    
    return df


def format_currency(value):
    """통화 포맷"""
    if pd.isna(value):
        return "-"
    try:
        return f"{int(float(value)):,}원"
    except Exception:
        return "-"


def format_percentage(value):
    """퍼센트 포맷"""
    if pd.isna(value):
        return "-"
    try:
        return f"{int(float(value))}%"
    except Exception:
        return "-"


def format_rating(value):
    """평점 포맷"""
    if pd.isna(value):
        return "-"
    try:
        return f"⭐ {float(value):.1f}"
    except Exception:
        return "-"


def format_count(value):
    """정수 카운트 포맷(리뷰수 등)"""
    if pd.isna(value):
        return "-"
    try:
        return f"{int(float(value)):,}개"
    except Exception:
        return "-"


def format_rank(value):
    """순위 포맷"""
    if pd.isna(value):
        return "-"
    try:
        return f"{int(float(value))}위"
    except Exception:
        return "-"


def main():
    """메인 함수"""

    st.title("💰 로켓직구 vs 아이허브 가격 비교")
    st.caption("로켓직구 전체 상품을 기준으로 매칭되는 아이허브 상품과 가격을 비교합니다 (rocket.csv 기반)")

    # 사이드바 필터
    st.sidebar.header("🔍 필터 설정")

    # 날짜 선택
    dates = get_available_dates()
    if not dates:
        st.error("데이터가 없습니다.")
        return

    selected_date = st.sidebar.selectbox("📅 날짜", dates, index=0)

    # 데이터 로드
    with st.spinner("데이터 로딩 중..."):
        df = load_comparison_data(selected_date)

    if df.empty:
        st.warning("선택한 날짜에 데이터가 없습니다.")
        return

    # 카테고리 필터
    categories = ['전체'] + sorted(df['category'].dropna().unique().tolist())
    selected_category = st.sidebar.selectbox("📂 카테고리", categories)

    if selected_category != '전체':
        df = df[df['category'] == selected_category]

    # 매칭 상태 필터
    match_options = {
        '전체': None,
        '매칭됨 (아이허브 상품 있음)': True,
        '미매칭 (아이허브 상품 없음)': False
    }
    selected_match = st.sidebar.selectbox("🔗 매칭 상태", list(match_options.keys()))

    if match_options[selected_match] is not None:
        if match_options[selected_match]:
            df = df[df['iherb_vendor_id'].notna()]
        else:
            df = df[df['iherb_vendor_id'].isna()]

    # 순위 범위
    st.sidebar.markdown("---")
    st.sidebar.markdown("**📊 로켓직구 순위 범위**")

    # NaN 방지: 순위 존재하는 행만 사용
    rank_series = df['category_rank'].dropna().astype(float)
    if len(rank_series) == 0:
        st.warning("표시할 로켓직구 순위 데이터가 없습니다.")
        return
    rank_min = int(rank_series.min())
    rank_max = int(rank_series.max())

    rank_range = st.sidebar.slider(
        "순위",
        min_value=rank_min,
        max_value=rank_max,
        value=(rank_min, min(20, rank_max)),
        step=1
    )

    df = df[(df['category_rank'] >= rank_range[0]) & (df['category_rank'] <= rank_range[1])]

    # 가격 차이 필터 (매칭된 제품만)
    if df['iherb_vendor_id'].notna().any():
        st.sidebar.markdown("---")
        st.sidebar.markdown("**💰 가격 경쟁력**")

        price_filter = st.sidebar.radio(
            "가격 비교",
            ['전체', '아이허브가 저렴', '로켓직구가 저렴', '동일 가격'],
            index=0
        )

        if price_filter != '전체':
            if price_filter == '아이허브가 저렴':
                df = df[df['cheaper_source'] == '아이허브']
            elif price_filter == '로켓직구가 저렴':
                df = df[df['cheaper_source'] == '로켓직구']
            else:
                df = df[df['cheaper_source'] == '동일']

    # 검색
    st.sidebar.markdown("---")
    search_term = st.sidebar.text_input("🔍 상품명 검색")

    if search_term:
        df = df[df['product_name'].fillna("").str.contains(search_term, case=False, na=False)]

    # 정렬
    st.sidebar.markdown("---")
    sort_options = {
        '로켓 순위 (오름차순)': ('category_rank', True),
        '로켓 가격 (낮은순)': ('current_price', True),
        '로켓 가격 (높은순)': ('current_price', False),
        '가격 차이 (큰순)': ('price_diff', False),
        '가격 차이 (작은순)': ('price_diff', True),
    }

    selected_sort = st.sidebar.selectbox("📊 정렬", list(sort_options.keys()))
    sort_col, sort_asc = sort_options[selected_sort]

    if sort_col in df.columns:
        df = df.sort_values(by=sort_col, ascending=sort_asc, na_position="last")

    # 메인 영역
    st.markdown("---")

    # 요약 통계
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("전체 상품", f"{len(df):,}개")

    with col2:
        matched_count = df['iherb_vendor_id'].notna().sum()
        match_rate = (matched_count / len(df) * 100) if len(df) > 0 else 0
        st.metric("매칭된 상품", f"{matched_count:,}개", f"{match_rate:.1f}%")

    with col3:
        if matched_count > 0:
            iherb_cheaper = (df['cheaper_source'] == '아이허브').sum()
            st.metric("아이허브가 저렴", f"{iherb_cheaper:,}개",
                      f"{(iherb_cheaper / matched_count * 100):.1f}%")

    with col4:
        if matched_count > 0 and df['price_diff'].notna().any():
            avg_diff = df['price_diff'].dropna().mean()
            st.metric("평균 가격 차이", f"{avg_diff:,.0f}원")

    st.markdown("---")

    # 상품 리스트
    st.markdown(f"### 📦 상품 목록 ({len(df)}개)")

    for _, row in df.iterrows():
        rank_label = int(row['category_rank']) if pd.notna(row['category_rank']) else 0
        title = row['product_name'] if pd.notna(row['product_name']) else ""
        display_title = f"**{rank_label}위** | {title[:60]}..." if len(title) > 60 else f"**{rank_label}위** | {title}"

        with st.expander(display_title):
            # 로켓직구 정보
            st.markdown("#### 🚀 로켓직구")

            col1, col2, col3, col4, col5 = st.columns(5)

            with col1:
                st.markdown("**가격**")
                st.markdown(format_currency(row['current_price']))
                if pd.notna(row['discount_rate']) and row['discount_rate'] > 0:
                    st.caption(f"할인 {format_percentage(row['discount_rate'])}")

            with col2:
                st.markdown("**평점**")
                st.markdown(format_rating(row['rating_score']))

            with col3:
                st.markdown("**리뷰**")
                st.markdown(format_count(row['review_count']))

            with col4:
                st.markdown("**UPC**")
                st.markdown(row['iherb_upc'] if pd.notna(row['iherb_upc']) else "-")

            with col5:
                st.markdown("**링크**")
                if pd.notna(row['product_url']) and str(row['product_url']).strip():
                    st.markdown(f"[바로가기]({row['product_url']})")

            # 아이허브 정보
            if pd.notna(row['iherb_vendor_id']):
                st.markdown("---")
                st.markdown("#### 🌿 아이허브 (매칭)")

                col1, col2, col3, col4, col5 = st.columns(5)

                with col1:
                    st.markdown("**가격**")
                    st.markdown(format_currency(row['iherb_price']))
                    if pd.notna(row.get('iherb_discount_rate')) and row.get('iherb_discount_rate', 0) > 0:
                        st.caption(f"할인 {format_percentage(row['iherb_discount_rate'])}")

                with col2:
                    st.markdown("**평점**")
                    st.markdown(format_rating(row.get('iherb_rating')))

                with col3:
                    st.markdown("**리뷰**")
                    st.markdown(format_count(row.get('iherb_reviews')))

                with col4:
                    st.markdown("**재고**")
                    stock_status = row.get('iherb_stock_status', '-')
                    if stock_status == '재고있음':
                        st.success(stock_status)
                    else:
                        st.error(stock_status)

                with col5:
                    st.markdown("**링크**")
                    if pd.notna(row.get('iherb_url')) and str(row.get('iherb_url')).strip():
                        st.markdown(f"[바로가기]({row['iherb_url']})")

                # 비교
                st.markdown("---")
                st.markdown("#### 📊 비교")

                col1, col2, col3 = st.columns(3)

                with col1:
                    price_diff = row['price_diff']
                    if pd.notna(price_diff):
                        if price_diff < 0:
                            st.success(f"💚 아이허브가 {abs(price_diff):,.0f}원 저렴")
                        elif price_diff > 0:
                            st.error(f"💔 로켓직구가 {abs(price_diff):,.0f}원 저렴")
                        else:
                            st.info("💙 가격 동일")

                with col2:
                    if pd.notna(row['price_diff_pct']):
                        st.metric("가격 차이", f"{float(row['price_diff_pct']):+.1f}%")

                with col3:
                    st.markdown(f"**제품명**")
                    st.caption(row['iherb_product_name'] if pd.notna(row.get('iherb_product_name')) else "-")

            else:
                st.info("ℹ️ 매칭되는 아이허브 상품이 없습니다")


if __name__ == "__main__":
    main()