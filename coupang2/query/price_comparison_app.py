#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
로켓직구 vs 아이허브 가격 비교 Streamlit 대시보드
- 로켓직구 전체 상품 기준
- 매칭된 아이허브 상품 정보 표시
- 인터랙티브 필터링 및 정렬
"""

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import re
from datetime import datetime

# 페이지 설정
st.set_page_config(
    page_title="로켓직구 vs 아이허브 가격 비교",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# DB 경로 (실제 경로로 수정 필요)
DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/monitoring.db"


@st.cache_data
def extract_quantity_from_product_name(product_name):
    """상품명에서 개수 추출"""
    if not product_name:
        return None
    match = re.search(r'(\d+)개\s*$', str(product_name).strip())
    if match:
        return int(match.group(1))
    return None


@st.cache_data
def get_available_dates():
    """사용 가능한 날짜 목록"""
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT DISTINCT DATE(snapshot_time) as date FROM snapshots ORDER BY date DESC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['date'].tolist()


@st.cache_data
def get_categories():
    """카테고리 목록"""
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT DISTINCT name FROM categories ORDER BY name"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df['name'].tolist()


@st.cache_data
def load_comparison_data(target_date):
    """가격 비교 데이터 로드"""

    conn = sqlite3.connect(DB_PATH)

    query = """
    WITH latest_snapshots AS (
        SELECT 
            snap.id as snapshot_id,
            s.source_type,
            c.name as category
        FROM snapshots snap
        JOIN sources s ON snap.source_id = s.id
        JOIN categories c ON snap.category_id = c.id
        WHERE DATE(snap.snapshot_time) = ?
    ),
    rocket_products AS (
        SELECT 
            ps.vendor_item_id,
            ps.product_name,
            ps.current_price,
            ps.original_price,
            ps.discount_rate,
            ps.category_rank,
            ps.rating_score,
            ps.review_count,
            ps.product_url,
            mr.iherb_upc as upc,
            mr.iherb_part_number,
            ls.category
        FROM product_states ps
        JOIN latest_snapshots ls ON ps.snapshot_id = ls.snapshot_id
        LEFT JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE ls.source_type = 'rocket_direct'
    ),
    iherb_products AS (
        SELECT 
            ps.vendor_item_id,
            ps.product_name,
            ps.current_price,
            ps.original_price,
            ps.discount_rate,
            ps.category_rank,
            ps.rating_score,
            ps.review_count,
            ps.product_url,
            mr.iherb_upc as upc,
            ls.category
        FROM product_states ps
        JOIN latest_snapshots ls ON ps.snapshot_id = ls.snapshot_id
        LEFT JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE ls.source_type = 'iherb_official'
    )
    SELECT 
        r.category,
        r.category_rank as rocket_rank,
        r.upc,
        r.iherb_part_number,
        
        r.vendor_item_id as rocket_vendor_id,
        r.product_name as rocket_product_name,
        r.current_price as rocket_price,
        r.original_price as rocket_original_price,
        r.discount_rate as rocket_discount_rate,
        r.rating_score as rocket_rating,
        r.review_count as rocket_reviews,
        r.product_url as rocket_url,
        
        i.vendor_item_id as iherb_vendor_id,
        i.product_name as iherb_product_name,
        i.current_price as iherb_price,
        i.original_price as iherb_original_price,
        i.discount_rate as iherb_discount_rate,
        i.category_rank as iherb_rank,
        i.rating_score as iherb_rating,
        i.review_count as iherb_reviews,
        i.product_url as iherb_url
        
    FROM rocket_products r
    LEFT JOIN iherb_products i ON r.upc = i.upc AND r.category = i.category
    ORDER BY r.category, r.category_rank
    """

    df = pd.read_sql_query(query, conn, params=(target_date,))
    conn.close()

    # ─────────────────────────────────────────────────────────
    # 숫자형 강제 변환 (문자/공백/기호 섞여도 안전)
    # ─────────────────────────────────────────────────────────
    def to_num(s):
        # 문자열로 들어온 경우 쉼표/원/공백 제거 후 숫자화
        if s is None:
            return pd.Series([], dtype="float64")
        if isinstance(s, pd.Series):
            cleaned = s.astype(str).str.replace(",", "", regex=False)\
                                   .str.replace("원", "", regex=False)\
                                   .str.strip()
            return pd.to_numeric(cleaned, errors="coerce")
        return pd.to_numeric(s, errors="coerce")

    numeric_cols = [
        "rocket_rank", "rocket_price", "rocket_original_price", "rocket_discount_rate",
        "rocket_rating", "rocket_reviews",
        "iherb_price", "iherb_original_price", "iherb_discount_rate",
        "iherb_rank", "iherb_rating", "iherb_reviews"
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = to_num(df[c]).astype("float64")

    # 개수 추출
    df['rocket_quantity'] = df['rocket_product_name'].apply(extract_quantity_from_product_name)
    df['iherb_quantity']  = df['iherb_product_name'].apply(extract_quantity_from_product_name)

    # 매칭 조건
    def should_match(row):
        if pd.isna(row['upc']) or pd.isna(row['iherb_vendor_id']):
            return False
        if pd.notna(row['rocket_quantity']) and pd.notna(row['iherb_quantity']):
            return row['rocket_quantity'] == row['iherb_quantity']
        if pd.notna(row['rocket_quantity']) or pd.notna(row['iherb_quantity']):
            return False
        return True

    df['is_matched'] = df.apply(should_match, axis=1)

    # 매칭 안 된 행: 아이허브 정보 제거
    iherb_columns = [
        'iherb_vendor_id', 'iherb_product_name', 'iherb_price', 
        'iherb_original_price', 'iherb_discount_rate', 'iherb_rank',
        'iherb_rating', 'iherb_reviews', 'iherb_url'
    ]
    for col in iherb_columns:
        df.loc[~df['is_matched'], col] = None

    # ─────────────────────────────────────────────────────────
    # 비교 지표 계산 (dtype을 끝까지 float로 유지)
    # ─────────────────────────────────────────────────────────
    df['price_diff'] = np.nan
    df['price_diff_pct'] = np.nan
    df['cheaper_source'] = pd.NA

    matched_mask = df['is_matched'].fillna(False)
    valid_mask = matched_mask & df['rocket_price'].notna() & df['iherb_price'].notna() & (df['rocket_price'] > 0)

    # 값 뽑아서 float로 계산
    rprice = df.loc[valid_mask, 'rocket_price'].astype(float)
    iprice = df.loc[valid_mask, 'iherb_price'].astype(float)

    price_diff_vals = iprice.values - rprice.values
    df.loc[valid_mask, 'price_diff'] = price_diff_vals

    pct_vals = (price_diff_vals / rprice.values) * 100.0
    df.loc[valid_mask, 'price_diff_pct'] = pct_vals

    # 마지막에 dtype을 확실히 float로 고정 후 반올림
    df['price_diff'] = pd.to_numeric(df['price_diff'], errors='coerce')
    df['price_diff_pct'] = pd.to_numeric(df['price_diff_pct'], errors='coerce').round(1)

    df.loc[valid_mask, 'cheaper_source'] = df.loc[valid_mask, 'price_diff'].apply(
        lambda x: '로켓직구' if x > 0 else ('아이허브' if x < 0 else '동일')
    )

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
    st.caption("로켓직구 전체 상품을 기준으로 매칭되는 아이허브 상품과 가격을 비교합니다")

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
    rank_series = df['rocket_rank'].dropna().astype(float)
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

    df = df[(df['rocket_rank'] >= rank_range[0]) & (df['rocket_rank'] <= rank_range[1])]

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
        df = df[df['rocket_product_name'].fillna("").str.contains(search_term, case=False, na=False)]

    # 정렬
    st.sidebar.markdown("---")
    sort_options = {
        '로켓 순위 (오름차순)': ('rocket_rank', True),
        '로켓 가격 (낮은순)': ('rocket_price', True),
        '로켓 가격 (높은순)': ('rocket_price', False),
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
        rank_label = int(row['rocket_rank']) if pd.notna(row['rocket_rank']) else 0
        title = row['rocket_product_name'] if pd.notna(row['rocket_product_name']) else ""
        display_title = f"**{rank_label}위** | {title[:60]}..." if len(title) > 60 else f"**{rank_label}위** | {title}"

        with st.expander(display_title):
            # 로켓직구 정보
            st.markdown("#### 🚀 로켓직구")

            col1, col2, col3, col4, col5 = st.columns(5)

            with col1:
                st.markdown("**가격**")
                st.markdown(format_currency(row['rocket_price']))
                if pd.notna(row['rocket_discount_rate']) and row['rocket_discount_rate'] > 0:
                    st.caption(f"할인 {format_percentage(row['rocket_discount_rate'])}")

            with col2:
                st.markdown("**평점**")
                st.markdown(format_rating(row['rocket_rating']))

            with col3:
                st.markdown("**리뷰**")
                st.markdown(format_count(row['rocket_reviews']))

            with col4:
                st.markdown("**UPC**")
                st.markdown(row['upc'] if pd.notna(row['upc']) else "-")

            with col5:
                st.markdown("**링크**")
                if pd.notna(row['rocket_url']) and str(row['rocket_url']).strip():
                    st.markdown(f"[바로가기]({row['rocket_url']})")

            # 아이허브 정보
            if pd.notna(row['iherb_vendor_id']):
                st.markdown("---")
                st.markdown("#### 🌿 아이허브 (매칭)")

                col1, col2, col3, col4, col5 = st.columns(5)

                with col1:
                    st.markdown("**가격**")
                    st.markdown(format_currency(row['iherb_price']))
                    if pd.notna(row['iherb_discount_rate']) and row['iherb_discount_rate'] > 0:
                        st.caption(f"할인 {format_percentage(row['iherb_discount_rate'])}")

                with col2:
                    st.markdown("**평점**")
                    st.markdown(format_rating(row['iherb_rating']))

                with col3:
                    st.markdown("**리뷰**")
                    st.markdown(format_count(row['iherb_reviews']))

                with col4:
                    st.markdown("**순위**")
                    st.markdown(format_rank(row['iherb_rank']))

                with col5:
                    st.markdown("**링크**")
                    if pd.notna(row['iherb_url']) and str(row['iherb_url']).strip():
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
                    st.caption(row['iherb_product_name'] if pd.notna(row['iherb_product_name']) else "-")

            else:
                st.info("ℹ️ 매칭되는 아이허브 상품이 없습니다")


if __name__ == "__main__":
    main()
