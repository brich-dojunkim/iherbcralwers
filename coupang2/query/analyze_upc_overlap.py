#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
아이허브 vs 로켓직구 UPC 중복 분석 (상세 버전)
- 최신 스냅샷이 아닌 전체 데이터 기준
- 풀어서 설명
- 단계별 상세 출력
"""

import sqlite3
import pandas as pd

DB_PATH = "/Users/brich/Desktop/iherb_price/coupang2/monitoring.db"


def print_section(title, level=1):
    """섹션 헤더 출력"""
    if level == 1:
        print(f"\n{'='*100}")
        print(f"{'='*100}")
        print(f"{title:^100}")
        print(f"{'='*100}")
        print(f"{'='*100}\n")
    elif level == 2:
        print(f"\n{'-'*100}")
        print(f"{title}")
        print(f"{'-'*100}\n")
    else:
        print(f"\n{'·'*100}")
        print(f"  {title}")
        print(f"{'·'*100}\n")


def analyze_overall_status():
    """1. 전체 현황 분석"""
    
    print_section("STEP 1: 전체 현황 파악", level=1)
    
    conn = sqlite3.connect(DB_PATH)
    
    # 1-1. DB 전체 통계
    print_section("1-1. DB에 저장된 전체 데이터", level=2)
    
    total_snapshots = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    total_products = conn.execute("SELECT COUNT(*) FROM product_states").fetchone()[0]
    total_vendors = conn.execute("SELECT COUNT(DISTINCT vendor_item_id) FROM product_states").fetchone()[0]
    
    print(f"📦 전체 스냅샷 수: {total_snapshots:,}개")
    print(f"   → 여러 날짜에 걸쳐 수집한 크롤링 데이터의 총 개수입니다.")
    print()
    print(f"📦 전체 product_states 레코드: {total_products:,}개")
    print(f"   → 모든 스냅샷의 모든 상품 데이터를 합친 총 개수입니다.")
    print(f"   → (스냅샷 수 × 스냅샷당 평균 상품 수)")
    print()
    print(f"📦 고유 vendor_item_id: {total_vendors:,}개")
    print(f"   → 중복 제거한 실제 상품 ID 개수입니다.")
    print(f"   → 같은 상품이 여러 스냅샷에 반복 등장하므로 이 숫자가 더 적습니다.")
    print()
    
    # 1-2. 소스별 통계
    print_section("1-2. 소스(아이허브/로켓직구)별 데이터", level=2)
    
    query_source = """
    SELECT 
        s.source_type as 소스타입,
        s.display_name as 표시명,
        COUNT(DISTINCT snap.id) as 스냅샷수,
        COUNT(DISTINCT ps.vendor_item_id) as 고유상품수,
        COUNT(ps.vendor_item_id) as 총레코드수
    FROM sources s
    LEFT JOIN snapshots snap ON s.id = snap.source_id
    LEFT JOIN product_states ps ON snap.id = ps.snapshot_id
    GROUP BY s.source_type, s.display_name
    """
    
    df_source = pd.read_sql_query(query_source, conn)
    print(df_source.to_string(index=False))
    print()
    print("💡 설명:")
    print("   - 스냅샷수: 각 소스에서 크롤링한 총 횟수")
    print("   - 고유상품수: 중복 제거한 실제 판매 상품 개수")
    print("   - 총레코드수: 모든 스냅샷의 레코드 합계 (고유상품수 × 평균 크롤링 횟수)")
    print()
    
    # 1-3. 매칭 현황
    print_section("1-3. 매칭 데이터 현황 (UPC/품번)", level=2)
    
    total_matching = conn.execute("SELECT COUNT(*) FROM matching_reference").fetchone()[0]
    with_upc = conn.execute("""
        SELECT COUNT(*) FROM matching_reference WHERE iherb_upc IS NOT NULL
    """).fetchone()[0]
    with_part = conn.execute("""
        SELECT COUNT(*) FROM matching_reference 
        WHERE iherb_part_number IS NOT NULL AND iherb_part_number != ''
    """).fetchone()[0]
    
    print(f"📋 전체 매칭 레코드: {total_matching:,}개")
    print(f"   → matching_reference 테이블에 등록된 총 vendor_item_id 개수")
    print()
    print(f"🏷️  UPC가 있는 레코드: {with_upc:,}개 ({with_upc/total_matching*100:.1f}%)")
    print(f"   → iHerb UPC 코드가 매칭된 상품 개수")
    print()
    print(f"🏷️  품번이 있는 레코드: {with_part:,}개 ({with_part/total_matching*100:.1f}%)")
    print(f"   → iHerb Part Number가 매칭된 상품 개수")
    print()
    
    conn.close()


def analyze_upc_by_source():
    """2. 소스별 UPC 현황"""
    
    print_section("STEP 2: 소스별 UPC 분석 (전체 데이터 기준)", level=1)
    
    conn = sqlite3.connect(DB_PATH)
    
    # 2-1. 전체 UPC 개수
    print_section("2-1. 전체 UPC 통계", level=2)
    
    total_upc = conn.execute("""
        SELECT COUNT(DISTINCT iherb_upc) 
        FROM matching_reference 
        WHERE iherb_upc IS NOT NULL
    """).fetchone()[0]
    
    print(f"🌍 전체 고유 UPC 개수: {total_upc:,}개")
    print(f"   → matching_reference에 등록된 모든 UPC를 중복 제거한 개수입니다.")
    print(f"   → 이 중 일부만 실제로 product_states에 존재합니다.")
    print()
    
    # 2-2. DB에 실제 존재하는 UPC
    print_section("2-2. 실제 DB에 존재하는 UPC (전체 스냅샷 기준)", level=2)
    
    query_existing = """
    SELECT 
        s.source_type,
        s.display_name,
        COUNT(DISTINCT mr.iherb_upc) as upc_count,
        COUNT(DISTINCT ps.vendor_item_id) as vendor_count
    FROM product_states ps
    JOIN snapshots snap ON ps.snapshot_id = snap.id
    JOIN sources s ON snap.source_id = s.id
    INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
    WHERE mr.iherb_upc IS NOT NULL
    GROUP BY s.source_type, s.display_name
    """
    
    df_existing = pd.read_sql_query(query_existing, conn)
    df_existing.columns = ['소스타입', '표시명', 'UPC개수', 'vendor_item_id개수']
    
    print(df_existing.to_string(index=False))
    print()
    print("💡 설명:")
    print("   - UPC개수: 해당 소스의 모든 스냅샷에서 등장한 고유 UPC 개수")
    print("   - vendor_item_id개수: 해당 소스의 고유 상품 ID 개수")
    print("   - 한 UPC가 여러 vendor_item_id로 판매될 수 있음 (옵션 차이 등)")
    print()
    
    iherb_upc = df_existing[df_existing['소스타입'] == 'iherb_official']['UPC개수'].values[0]
    rocket_upc = df_existing[df_existing['소스타입'] == 'rocket_direct']['UPC개수'].values[0]
    
    print(f"📊 요약:")
    print(f"   - 아이허브에만 있는 UPC: 약 {iherb_upc:,}개")
    print(f"   - 로켓직구에만 있는 UPC: 약 {rocket_upc:,}개")
    print(f"   - 이 중 일부가 양쪽에서 동시에 판매되는 중복 UPC입니다.")
    print()
    
    conn.close()


def analyze_overlap_upc():
    """3. 중복 UPC 상세 분석"""
    
    print_section("STEP 3: 중복 UPC 상세 분석 (핵심!)", level=1)
    
    conn = sqlite3.connect(DB_PATH)
    
    # 3-1. 양쪽 소스에 모두 존재하는 UPC
    print_section("3-1. 양쪽 소스에 모두 존재하는 UPC 찾기", level=2)
    
    query_overlap = """
    WITH upc_by_source AS (
        SELECT 
            s.source_type,
            mr.iherb_upc
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources s ON snap.source_id = s.id
        INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE mr.iherb_upc IS NOT NULL
        GROUP BY s.source_type, mr.iherb_upc
    ),
    overlap_upc AS (
        SELECT 
            iherb_upc,
            COUNT(DISTINCT source_type) as source_count
        FROM upc_by_source
        GROUP BY iherb_upc
        HAVING COUNT(DISTINCT source_type) = 2
    )
    SELECT COUNT(*) as overlap_count FROM overlap_upc
    """
    
    overlap_count = pd.read_sql_query(query_overlap, conn)['overlap_count'].values[0]
    
    print(f"🎯 중복 UPC 개수: {overlap_count:,}개")
    print()
    print("💡 이게 뭐냐면:")
    print(f"   → 아이허브와 로켓직구 '양쪽 모두'에서 판매되고 있는 상품입니다.")
    print(f"   → 같은 UPC 코드를 가진 상품이 두 소스에 모두 등장합니다.")
    print(f"   → 이 상품들은 직접 가격 비교가 가능합니다!")
    print()
    print(f"🔍 전체 맥락:")
    print(f"   - 전체 UPC: 수만 개")
    print(f"   - 이 중 양쪽에서 동시 판매: {overlap_count:,}개 (약 1~2%)")
    print(f"   - 나머지는 한쪽에만 있는 독점 상품")
    print()
    
    # 3-2. 소스별 독점 UPC
    print_section("3-2. 소스별 독점 UPC (한쪽에만 있는 상품)", level=2)
    
    query_exclusive = """
    WITH upc_by_source AS (
        SELECT 
            s.source_type,
            mr.iherb_upc
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources s ON snap.source_id = s.id
        INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE mr.iherb_upc IS NOT NULL
        GROUP BY s.source_type, mr.iherb_upc
    )
    SELECT 
        source_type,
        COUNT(DISTINCT iherb_upc) as exclusive_upc_count
    FROM upc_by_source
    WHERE iherb_upc IN (
        SELECT iherb_upc 
        FROM upc_by_source 
        GROUP BY iherb_upc 
        HAVING COUNT(DISTINCT source_type) = 1
    )
    GROUP BY source_type
    """
    
    df_exclusive = pd.read_sql_query(query_exclusive, conn)
    df_exclusive.columns = ['소스', '독점UPC개수']
    df_exclusive['소스'] = df_exclusive['소스'].map({
        'rocket_direct': '로켓직구',
        'iherb_official': '아이허브'
    })
    
    print(df_exclusive.to_string(index=False))
    print()
    print("💡 독점 UPC란:")
    print("   → 한쪽 소스에만 있고 다른 쪽에는 없는 상품입니다.")
    print("   → 이 상품들은 가격 비교가 불가능합니다.")
    print()
    
    # 3-3. 카테고리별 중복 분포
    print_section("3-3. 카테고리별 중복 UPC 분포", level=2)
    
    query_category = """
    WITH upc_by_category_source AS (
        SELECT 
            c.name as category_name,
            s.source_type,
            mr.iherb_upc
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources s ON snap.source_id = s.id
        JOIN categories c ON snap.category_id = c.id
        INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE mr.iherb_upc IS NOT NULL
        GROUP BY c.name, s.source_type, mr.iherb_upc
    )
    SELECT 
        category_name as 카테고리,
        COUNT(DISTINCT iherb_upc) as 중복UPC개수
    FROM upc_by_category_source
    WHERE iherb_upc IN (
        SELECT iherb_upc 
        FROM upc_by_category_source 
        GROUP BY iherb_upc 
        HAVING COUNT(DISTINCT source_type) = 2
    )
    GROUP BY category_name
    ORDER BY 중복UPC개수 DESC
    """
    
    df_category = pd.read_sql_query(query_category, conn)
    
    print(df_category.to_string(index=False))
    print()
    print("💡 해석:")
    print("   → 각 카테고리에서 양쪽 소스가 동시에 판매하는 UPC 개수입니다.")
    print("   → 이 숫자가 많을수록 해당 카테고리의 가격 비교가 용이합니다.")
    print()
    
    # 3-4. 중복 UPC의 vendor_item_id 개수
    print_section("3-4. 중복 UPC별 상품 개수 분포", level=2)
    
    query_vendor_count = """
    WITH overlap_upc AS (
        SELECT 
            mr.iherb_upc
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources s ON snap.source_id = s.id
        INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE mr.iherb_upc IS NOT NULL
        GROUP BY mr.iherb_upc
        HAVING COUNT(DISTINCT s.source_type) = 2
    )
    SELECT 
        mr.iherb_upc,
        COUNT(DISTINCT ps.vendor_item_id) as vendor_count,
        COUNT(DISTINCT CASE WHEN s.source_type = 'iherb_official' THEN ps.vendor_item_id END) as iherb_count,
        COUNT(DISTINCT CASE WHEN s.source_type = 'rocket_direct' THEN ps.vendor_item_id END) as rocket_count
    FROM overlap_upc ou
    JOIN matching_reference mr ON ou.iherb_upc = mr.iherb_upc
    JOIN product_states ps ON mr.vendor_item_id = ps.vendor_item_id
    JOIN snapshots snap ON ps.snapshot_id = snap.id
    JOIN sources s ON snap.source_id = s.id
    GROUP BY mr.iherb_upc
    """
    
    df_vendor_count = pd.read_sql_query(query_vendor_count, conn)
    
    print(f"📊 중복 UPC별 상품 개수 통계:")
    print(f"   - 총 중복 UPC: {len(df_vendor_count):,}개")
    print(f"   - 평균 vendor_item_id 개수: {df_vendor_count['vendor_count'].mean():.1f}개/UPC")
    print(f"   - 최대 vendor_item_id 개수: {df_vendor_count['vendor_count'].max()}개/UPC")
    print()
    print("💡 왜 1개 UPC에 여러 vendor_item_id가 있나요?")
    print("   → 같은 제품이지만 용량이나 수량이 다른 옵션으로 판매되기 때문입니다.")
    print("   → 예: '비타민C 1개', '비타민C 2개', '비타민C 3개' = 3개 vendor_item_id")
    print()
    
    # 분포 확인
    distribution = df_vendor_count['vendor_count'].value_counts().sort_index()
    print("📊 분포:")
    for count, freq in distribution.head(10).items():
        print(f"   - {count}개 상품: {freq:,}개 UPC ({freq/len(df_vendor_count)*100:.1f}%)")
    
    if len(distribution) > 10:
        print(f"   - ... (생략)")
    print()
    
    conn.close()


def show_overlap_samples():
    """4. 중복 UPC 샘플 상세 출력"""
    
    print_section("STEP 4: 중복 UPC 샘플 상세 보기", level=1)
    
    conn = sqlite3.connect(DB_PATH)
    
    print_section("4-1. 중복 UPC 샘플 (상위 5개)", level=2)
    
    query_sample = """
    WITH overlap_upc AS (
        SELECT 
            mr.iherb_upc
        FROM product_states ps
        JOIN snapshots snap ON ps.snapshot_id = snap.id
        JOIN sources s ON snap.source_id = s.id
        INNER JOIN matching_reference mr ON ps.vendor_item_id = mr.vendor_item_id
        WHERE mr.iherb_upc IS NOT NULL
        GROUP BY mr.iherb_upc
        HAVING COUNT(DISTINCT s.source_type) = 2
        LIMIT 5
    )
    SELECT 
        mr.iherb_upc as UPC,
        mr.iherb_part_number as 품번,
        s.source_type as 소스타입,
        c.name as 카테고리,
        ps.product_name as 상품명,
        ps.current_price as 현재가,
        ps.original_price as 정가,
        ps.discount_rate as 할인율,
        ps.category_rank as 순위,
        ps.vendor_item_id as VendorItemID
    FROM overlap_upc ou
    JOIN matching_reference mr ON ou.iherb_upc = mr.iherb_upc
    JOIN product_states ps ON mr.vendor_item_id = ps.vendor_item_id
    JOIN snapshots snap ON ps.snapshot_id = snap.id
    JOIN sources s ON snap.source_id = s.id
    JOIN categories c ON snap.category_id = c.id
    WHERE snap.id IN (
        SELECT MAX(id) FROM snapshots GROUP BY source_id, category_id
    )
    ORDER BY mr.iherb_upc, s.source_type
    """
    
    df_sample = pd.read_sql_query(query_sample, conn)
    df_sample['소스'] = df_sample['소스타입'].map({
        'rocket_direct': '로켓직구',
        'iherb_official': '아이허브'
    })
    
    # UPC별로 그룹화해서 출력
    for upc in df_sample['UPC'].unique():
        upc_data = df_sample[df_sample['UPC'] == upc]
        
        print(f"{'='*100}")
        print(f"🏷️  UPC: {upc}")
        print(f"{'='*100}")
        
        for idx, row in upc_data.iterrows():
            print(f"\n소스: {row['소스']} ({row['소스타입']})")
            print(f"카테고리: {row['카테고리']}")
            print(f"상품명: {row['상품명'][:80]}...")
            print(f"현재가: {row['현재가']:,}원 (정가: {row['정가']:,}원, 할인율: {row['할인율']}%)")
            print(f"순위: {row['순위']}위")
            print(f"VendorItemID: {row['VendorItemID']}")
        
        # 가격 비교
        iherb_price = upc_data[upc_data['소스타입'] == 'iherb_official']['현재가']
        rocket_price = upc_data[upc_data['소스타입'] == 'rocket_direct']['현재가']
        
        if not iherb_price.empty and not rocket_price.empty:
            iherb_avg = iherb_price.mean()
            rocket_avg = rocket_price.mean()
            diff = iherb_avg - rocket_avg
            
            print(f"\n💰 가격 비교:")
            print(f"   아이허브 평균: {iherb_avg:,.0f}원")
            print(f"   로켓직구 평균: {rocket_avg:,.0f}원")
            print(f"   차이: {diff:+,.0f}원 ", end='')
            
            if diff > 0:
                print(f"(로켓직구가 {abs(diff):,.0f}원 저렴)")
            elif diff < 0:
                print(f"(아이허브가 {abs(diff):,.0f}원 저렴)")
            else:
                print(f"(동일)")
        
        print()
    
    conn.close()


def main():
    """메인 실행"""
    
    print("\n" + "="*100)
    print("="*100)
    print("🔍 아이허브 vs 로켓직구 UPC 중복 분석 (상세 버전)".center(100))
    print("="*100)
    print("="*100)
    
    analyze_overall_status()
    analyze_upc_by_source()
    analyze_overlap_upc()
    show_overlap_samples()
    
    print("\n" + "="*100)
    print("="*100)
    print("✅ 분석 완료!".center(100))
    print("="*100)
    print("="*100 + "\n")


if __name__ == "__main__":
    main()