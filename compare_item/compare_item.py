import pandas as pd
import sqlite3
import re
from urllib.parse import urlparse, parse_qs

def extract_item_id(url):
    """쿠팡 URL에서 itemId 추출"""
    if pd.isna(url):
        return None
    
    # URL 파싱
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    
    # itemId 추출
    if 'itemId' in params:
        return params['itemId'][0]
    
    # itemId가 쿼리에 없으면 정규식으로 찾기
    match = re.search(r'itemId=(\d+)', url)
    if match:
        return match.group(1)
    
    return None

def main():
    # 1. 엑셀 파일에서 itemId 추출
    print("=" * 60)
    print("1. 엑셀 파일에서 itemId 추출")
    print("=" * 60)
    
    df = pd.read_excel('/Users/brich/Desktop/iherb_price/251208/쿠팡_아이허브 베스트 가격 조사_1208.xlsx', header=1)
    
    # itemId 추출
    df['extracted_item_id'] = df['쿠팡 상품 URL'].apply(extract_item_id)
    
    # 유효한 itemId만 필터링
    valid_items = df[df['extracted_item_id'].notna()].copy()
    excel_item_ids = set(valid_items['extracted_item_id'].astype(str))
    
    print(f"엑셀 총 행 수: {len(df)}")
    print(f"URL이 있는 행 수: {df['쿠팡 상품 URL'].notna().sum()}")
    print(f"추출된 itemId 수: {len(excel_item_ids)}")
    print(f"\nitemId 샘플 (처음 10개):")
    for item_id in list(excel_item_ids)[:10]:
        print(f"  - {item_id}")
    
    # 2. DB에서 모든 스냅샷 조회 및 분석
    print("\n" + "=" * 60)
    print("2. DB에서 모든 스냅샷 조회 및 분석")
    print("=" * 60)
    
    db_path = '/Users/brich/Desktop/iherb_price/coupang/data/rocket_iherb.db'
    conn = sqlite3.connect(db_path)
    
    # 모든 스냅샷 조회
    all_snapshots_query = """
    SELECT id, snapshot_date 
    FROM snapshots 
    ORDER BY snapshot_date DESC
    """
    snapshots_df = pd.read_sql_query(all_snapshots_query, conn)
    
    print(f"총 스냅샷 수: {len(snapshots_df)}개")
    print(f"\n스냅샷 목록:")
    for _, row in snapshots_df.iterrows():
        print(f"  ID {row['id']:>2}: {row['snapshot_date']}")
    
    # 각 스냅샷별 분석 결과 저장
    all_results = []
    snapshot_details = {}
    
    for idx, snapshot_row in snapshots_df.iterrows():
        snapshot_id = snapshot_row['id']
        snapshot_date = snapshot_row['snapshot_date']
        
        print(f"\n{'─' * 60}")
        print(f"📊 스냅샷 ID {snapshot_id} ({snapshot_date}) 분석 중...")
        print(f"{'─' * 60}")
        
        # 해당 스냅샷의 item_id 조회
        item_query = f"""
        SELECT DISTINCT p.item_id
        FROM products p
        INNER JOIN product_price pp ON p.vendor_item_id = pp.vendor_item_id
        WHERE pp.snapshot_id = {snapshot_id}
        AND p.item_id IS NOT NULL
        """
        db_items = pd.read_sql_query(item_query, conn)
        db_item_ids = set(db_items['item_id'].astype(str))
        
        # 교집합 계산
        matched_items = excel_item_ids & db_item_ids
        excel_only = excel_item_ids - db_item_ids
        db_only = db_item_ids - excel_item_ids
        
        # 일치율 계산
        excel_match_rate = (len(matched_items) / len(excel_item_ids) * 100) if excel_item_ids else 0
        db_match_rate = (len(matched_items) / len(db_item_ids) * 100) if db_item_ids else 0
        
        print(f"  DB item_id: {len(db_item_ids):>6,}개")
        print(f"  일치:       {len(matched_items):>6,}개 (엑셀 기준 {excel_match_rate:.2f}%)")
        print(f"  엑셀전용:   {len(excel_only):>6,}개")
        print(f"  DB전용:     {len(db_only):>6,}개")
        
        # 결과 저장
        all_results.append({
            'snapshot_id': snapshot_id,
            'snapshot_date': snapshot_date,
            'db_item_count': len(db_item_ids),
            'matched_count': len(matched_items),
            'excel_only_count': len(excel_only),
            'db_only_count': len(db_only),
            'excel_match_rate': excel_match_rate,
            'db_match_rate': db_match_rate
        })
        
        # 상세 정보 저장
        snapshot_details[snapshot_id] = {
            'matched_items': matched_items,
            'excel_only': excel_only,
            'db_only': db_only
        }
    
    conn.close()
    
    # 3. 전체 요약 출력
    print("\n" + "=" * 60)
    print("3. 전체 요약")
    print("=" * 60)
    
    results_df = pd.DataFrame(all_results)
    print(f"\n엑셀 item_id 수: {len(excel_item_ids):,}개\n")
    print(results_df.to_string(index=False))
    
    # 4. 상세 결과 저장
    print("\n" + "=" * 60)
    print("4. 상세 결과 저장")
    print("=" * 60)
    
    output_path = '/Users/brich/Desktop/iherb_price/251208/item_comparison_result.xlsx'
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # 전체 요약 시트
        results_df.to_excel(writer, sheet_name='전체요약', index=False)
        
        # 각 스냅샷별 상세 시트
        for snapshot_id, details in snapshot_details.items():
            snapshot_date = results_df[results_df['snapshot_id'] == snapshot_id]['snapshot_date'].iloc[0]
            sheet_name = f"S{snapshot_id}_{snapshot_date}"[:31]  # 엑셀 시트명 길이 제한
            
            # 일치하는 항목
            matched_df = valid_items[valid_items['extracted_item_id'].isin(details['matched_items'])][
                ['Part Number', 'Product Id', 'Product Description', '국문상품명', '쿠팡 상품 URL', 'extracted_item_id']
            ].copy()
            matched_df.columns = ['Part Number', 'Product Id', 'Product Description', '국문상품명', 'URL', 'Item ID']
            matched_df.insert(0, '구분', '일치')
            
            # 엑셀에만 있는 항목
            excel_only_df = valid_items[valid_items['extracted_item_id'].isin(details['excel_only'])][
                ['Part Number', 'Product Id', 'Product Description', '국문상품명', '쿠팡 상품 URL', 'extracted_item_id']
            ].copy()
            excel_only_df.columns = ['Part Number', 'Product Id', 'Product Description', '국문상품명', 'URL', 'Item ID']
            excel_only_df.insert(0, '구분', '엑셀전용')
            
            # 합치기
            combined_df = pd.concat([matched_df, excel_only_df], ignore_index=True)
            combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"✅ 결과 저장 완료: {output_path}")
    print(f"   - 전체요약 시트")
    for snapshot_id in snapshot_details.keys():
        snapshot_date = results_df[results_df['snapshot_id'] == snapshot_id]['snapshot_date'].iloc[0]
        sheet_name = f"S{snapshot_id}_{snapshot_date}"[:31]
        matched_count = len(snapshot_details[snapshot_id]['matched_items'])
        excel_only_count = len(snapshot_details[snapshot_id]['excel_only'])
        print(f"   - {sheet_name} (일치: {matched_count}, 엑셀전용: {excel_only_count})")
    
    # 5. 원본 엑셀 파일을 복사하고 DB 데이터로 빈 값 채우기
    print("\n" + "=" * 60)
    print("5. 최신 스냅샷 데이터로 엑셀 파일 생성")
    print("=" * 60)
    
    # 최신 스냅샷 정보
    latest_snapshot_id = results_df.iloc[0]['snapshot_id']
    latest_snapshot_date = results_df.iloc[0]['snapshot_date']
    latest_matched_items = snapshot_details[latest_snapshot_id]['matched_items']
    
    print(f"최신 스냅샷: ID {latest_snapshot_id} ({latest_snapshot_date})")
    print(f"일치 항목 수: {len(latest_matched_items)}개")
    
    if len(latest_matched_items) > 0:
        # 원본 엑셀 파일 그대로 로드 (헤더 포함)
        df_original = pd.read_excel('/Users/brich/Desktop/iherb_price/251208/쿠팡_아이허브 베스트 가격 조사_1208.xlsx', header=None)
        
        # DB에서 최신 스냅샷의 정보 가져오기
        conn = sqlite3.connect(db_path)
        
        db_detail_query = f"""
        SELECT 
            p.item_id,
            p.part_number,
            p.product_id,
            p.name,
            pp.rocket_price,
            pf.rocket_category
        FROM products p
        LEFT JOIN product_price pp ON p.vendor_item_id = pp.vendor_item_id AND pp.snapshot_id = {latest_snapshot_id}
        LEFT JOIN product_features pf ON p.vendor_item_id = pf.vendor_item_id AND pf.snapshot_id = {latest_snapshot_id}
        WHERE pp.snapshot_id = {latest_snapshot_id}
        """
        
        db_details_df = pd.read_sql_query(db_detail_query, conn)
        conn.close()
        
        # item_id를 문자열로 변환
        db_details_df['item_id'] = db_details_df['item_id'].astype(str)
        
        # DB 데이터를 딕셔너리로 변환 (빠른 조회용)
        db_dict = db_details_df.set_index('item_id').to_dict('index')
        
        # 헤더 행 찾기 (row 1이 헤더)
        header_row_idx = 1
        
        # 데이터 행들에 대해 처리 (row 2부터)
        for idx in range(2, len(df_original)):
            # 쿠팡 상품 URL에서 item_id 추출 (컬럼 4)
            url = df_original.iloc[idx, 4]
            if pd.notna(url):
                item_id = extract_item_id(url)
                
                # 일치하는 항목인지 확인
                if item_id in latest_matched_items and item_id in db_dict:
                    db_data = db_dict[item_id]
                    
                    # Part Number 채우기 (컬럼 0)
                    if pd.isna(df_original.iloc[idx, 0]) or df_original.iloc[idx, 0] == '':
                        df_original.iloc[idx, 0] = db_data['part_number']
                    
                    # Product Id 채우기 (컬럼 1)
                    if pd.isna(df_original.iloc[idx, 1]) or df_original.iloc[idx, 1] == '':
                        df_original.iloc[idx, 1] = db_data['product_id']
                    
                    # Product Description 채우기 (컬럼 2)
                    if pd.isna(df_original.iloc[idx, 2]) or df_original.iloc[idx, 2] == '':
                        df_original.iloc[idx, 2] = db_data['name']
                    
                    # Seller 채우기 (컬럼 5)
                    if pd.isna(df_original.iloc[idx, 5]) or df_original.iloc[idx, 5] == '':
                        df_original.iloc[idx, 5] = db_data['rocket_category']
                    
                    # 가격 정보 채우기 - 첫 번째 날짜 컬럼 (컬럼 7: (비회원) 가격 정보)
                    if pd.isna(df_original.iloc[idx, 7]) or df_original.iloc[idx, 7] == '':
                        if pd.notna(db_data['rocket_price']):
                            df_original.iloc[idx, 7] = db_data['rocket_price']
        
        # 일치하는 행만 필터링
        matched_rows = [0, 1]  # 헤더 행 유지
        for idx in range(2, len(df_original)):
            url = df_original.iloc[idx, 4]
            if pd.notna(url):
                item_id = extract_item_id(url)
                if item_id in latest_matched_items:
                    matched_rows.append(idx)
        
        df_result = df_original.iloc[matched_rows].reset_index(drop=True)
        
        # 파일명 생성
        matched_output_path = f'/Users/brich/Desktop/iherb_price/251208/matched_latest_{latest_snapshot_date}.xlsx'
        
        # 저장
        df_result.to_excel(matched_output_path, index=False, header=False)
        
        print(f"✅ 일치 상품 {len(matched_rows)-2}개 저장 완료")
        print(f"   - 원본 엑셀 구조 완전 유지 (헤더 포함)")
        print(f"   - DB 데이터로 빈 값 채움:")
        print(f"     • Part Number, Product Id, Product Description")
        print(f"     • Seller (카테고리)")
        print(f"     • 첫 번째 날짜의 가격 정보")
        print(f"   → {matched_output_path}")
    else:
        print("⚠️  일치하는 항목이 없습니다.")
    
    print("\n" + "=" * 60)
    print("모든 작업 완료!")
    print("=" * 60)

if __name__ == "__main__":
    main()