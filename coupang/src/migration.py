#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
일일 판매량 마이그레이션
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
7일 집계 판매량 → 일일 판매량으로 변환
migration_excel 폴더의 SELLER_INSIGHTS 파일들을 순차 처리
"""

import pandas as pd
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database import IntegratedDatabase
from config.settings import Config


def load_seller_insights(file_path: Path) -> Dict[str, Dict]:
    print(f"  📄 읽는 중: {file_path.name}")
    
    df = pd.read_excel(file_path, sheet_name='vendor item metrics')

    # 컬럼 단위로 숫자 변환 + NaN → 0
    if '판매량' in df.columns:
        df['판매량'] = pd.to_numeric(df['판매량'], errors='coerce').fillna(0).astype(int)
    else:
        df['판매량'] = 0

    if '매출(원)' in df.columns:
        df['매출(원)'] = pd.to_numeric(df['매출(원)'], errors='coerce').fillna(0).astype(int)
    else:
        df['매출(원)'] = 0

    if '아이템위너 비율(%)' in df.columns:
        df['아이템위너 비율(%)'] = pd.to_numeric(df['아이템위너 비율(%)'], errors='coerce').fillna(0).astype(float)
    else:
        df['아이템위너 비율(%)'] = 0.0

    data = {}
    for _, row in df.iterrows():
        vendor_id = str(row.get('옵션 ID'))
        if vendor_id == 'nan' or vendor_id == '<NA>' or vendor_id is None:
            continue
        
        sales = int(row['판매량'])
        revenue = int(row['매출(원)'])
        winner = float(row['아이템위너 비율(%)'])

        category = row.get('카테고리')
        if pd.isna(category):
            category = None
        
        data[vendor_id] = {
            'sales': sales,
            'revenue': revenue,
            'winner_ratio': winner,
            'category': category
        }
    
    print(f"     ✓ {len(data):,}개 상품")
    return data


def calculate_daily_sales(current_data: Dict[str, Dict], 
                         previous_data: Dict[str, Dict]) -> Dict[str, int]:
    """일일 판매량 계산 (7일 rolling window 차분)
    
    Args:
        current_data: 현재 스냅샷 데이터 (7일 집계)
        previous_data: 이전 스냅샷 데이터 (7일 집계)
    
    Returns:
        {vendor_item_id: daily_sales}
    """
    daily_sales = {}
    
    for vendor_id, curr in current_data.items():
        curr_sales = curr['sales']
        
        if vendor_id in previous_data:
            prev_sales = previous_data[vendor_id]['sales']
            # 차분 = 최신일 판매량 - 7일 전 판매량
            daily = curr_sales - prev_sales
        else:
            # 신규 상품: 7일 평균으로 추정
            daily = curr_sales // 7
        
        daily_sales[vendor_id] = max(0, daily)  # 음수 방지
    
    return daily_sales


def update_snapshot_features(db_path: str, snapshot_id: int, 
                            seller_data: Dict[str, Dict],
                            daily_sales: Dict[str, int],
                            is_first: bool = False):
    """스냅샷의 product_features 업데이트
    
    Args:
        db_path: DB 경로
        snapshot_id: 스냅샷 ID
        seller_data: SELLER_INSIGHTS 원본 데이터
        daily_sales: 계산된 일일 판매량
        is_first: 첫 스냅샷 여부 (매출 계산 방식 다름)
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    
    updated_count = 0
    
    for vendor_id, data in seller_data.items():
        daily = daily_sales.get(vendor_id, 0)
        
        # 매출 일일 변환
        if is_first:
            # 첫 스냅샷: 7일 평균
            total_revenue = data['revenue']
            daily_revenue = total_revenue // 7
        else:
            # 차분: 판매량 비율 적용
            total_sales = data['sales']
            total_revenue = data['revenue']
            
            if total_sales > 0 and daily > 0:
                daily_revenue = int((daily / total_sales) * total_revenue)
            else:
                daily_revenue = 0
        
        # 아이템위너비율: 7일 평균값 유지 (일일 변환 불가)
        winner_ratio = data['winner_ratio']
        
        conn.execute("""
            UPDATE product_features
            SET iherb_sales_quantity = ?,
                iherb_revenue = ?,
                iherb_item_winner_ratio = ?,
                iherb_category = ?
            WHERE snapshot_id = ? AND vendor_item_id = ?
        """, (daily, daily_revenue, winner_ratio, data['category'], 
              snapshot_id, vendor_id))
        
        if conn.total_changes > 0:
            updated_count += 1
    
    conn.commit()
    conn.close()
    
    return updated_count


def main():
    """메인 마이그레이션 프로세스"""
    
    print("\n" + "="*80)
    print("🔄 일일 판매량 마이그레이션")
    print("="*80 + "\n")
    
    # 경로 설정
    migration_dir = Config.DATA_DIR / "migration_excel"
    db_path = Config.INTEGRATED_DB_PATH
    
    # migration_excel 파일 목록
    excel_files = sorted(migration_dir.glob("SELLER_INSIGHTS_*.xlsx"))
    
    if not excel_files:
        print("❌ migration_excel 폴더에 파일이 없습니다")
        return
    
    print(f"📂 발견된 파일: {len(excel_files)}개")
    for i, f in enumerate(excel_files, 1):
        print(f"  {i}. {f.name}")
    print()
    
    # 스냅샷 확인
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("""
        SELECT id, snapshot_date 
        FROM snapshots 
        ORDER BY id 
        LIMIT 5
    """)
    snapshots = cursor.fetchall()
    conn.close()
    
    if len(snapshots) != 5:
        print(f"❌ 스냅샷이 5개가 아닙니다 (현재 {len(snapshots)}개)")
        return
    
    print(f"📊 스냅샷 목록:")
    for snap_id, snap_date in snapshots:
        print(f"  ID {snap_id}: {snap_date}")
    print()
    
    # 파일과 스냅샷 매핑 확인
    if len(excel_files) != len(snapshots):
        print(f"⚠️ 파일 개수({len(excel_files)})와 스냅샷 개수({len(snapshots)})가 다릅니다")
        return
    
    mapping = []
    for (snap_id, snap_date), excel_file in zip(snapshots, excel_files):
        mapping.append({
            'snapshot_id': snap_id,
            'snapshot_date': snap_date,
            'excel_file': excel_file
        })
    
    print("📋 매핑 확인:")
    for m in mapping:
        print(f"  Snapshot {m['snapshot_id']} ({m['snapshot_date']}) ← {m['excel_file'].name}")
    print()
    
    # 확인
    confirm = input("위 매핑대로 마이그레이션을 진행하시겠습니까? (yes 입력): ").strip()
    if confirm != 'yes':
        print("❌ 취소됨")
        return
    
    print("\n" + "="*80)
    print("🚀 마이그레이션 시작")
    print("="*80 + "\n")
    
    # 백업 권장
    print("💡 백업 권장:")
    print(f"   cp {db_path} {db_path}.backup")
    proceed = input("\n백업을 완료했거나 건너뛰려면 'proceed' 입력: ").strip()
    if proceed != 'proceed':
        print("❌ 취소됨")
        return
    
    print()
    
    # 각 스냅샷 처리
    previous_data = None
    
    for i, m in enumerate(mapping, 1):
        snapshot_id = m['snapshot_id']
        excel_file = m['excel_file']
        is_first = (i == 1)
        
        print(f"[{i}/{len(mapping)}] Snapshot {snapshot_id} 처리 중...")
        
        # SELLER_INSIGHTS 로드
        current_data = load_seller_insights(excel_file)
        
        # 일일 판매량 계산
        if previous_data is None:
            # 첫 번째 스냅샷: 7일 평균으로 추정
            print(f"  ⚠️  첫 스냅샷 - 7일 평균으로 추정")
            daily_sales = {
                vid: max(1, data['sales'] // 7) 
                for vid, data in current_data.items()
            }
        else:
            # 차분 계산
            print(f"  🔢 일일 판매량 계산 (차분)")
            daily_sales = calculate_daily_sales(current_data, previous_data)
        
        # DB 업데이트
        print(f"  💾 DB 업데이트 중...")
        updated = update_snapshot_features(
            db_path, 
            snapshot_id, 
            current_data,
            daily_sales,
            is_first=is_first
        )
        
        print(f"     ✓ {updated:,}개 상품 업데이트\n")
        
        # 다음 반복을 위해 저장
        previous_data = current_data
    
    print("="*80)
    print("✅ 마이그레이션 완료")
    print("="*80 + "\n")
    
    # 검증
    print("🔍 결과 검증:")
    conn = sqlite3.connect(db_path)
    
    for snap_id, snap_date in snapshots:
        cursor = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN iherb_sales_quantity > 0 THEN 1 ELSE 0 END) as with_sales,
                AVG(iherb_sales_quantity) as avg_sales
            FROM product_features
            WHERE snapshot_id = ?
        """, (snap_id,))
        
        total, with_sales, avg_sales = cursor.fetchone()
        print(f"  Snapshot {snap_id}: 판매량 있음 {with_sales}/{total}개, 평균 {avg_sales:.1f}개/일")
    
    conn.close()
    print()


if __name__ == "__main__":
    main()