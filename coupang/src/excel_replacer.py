#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
스냅샷 엑셀 데이터 교체
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
크롤링 데이터는 유지하고 엑셀 데이터만 안전하게 교체
"""

import sys
import sqlite3
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Config
from src.database import IntegratedDatabase
from src.excel_loader import ExcelLoader


def show_snapshot_info(db_path: str, snapshot_id: int):
    """스냅샷 정보 출력"""
    conn = sqlite3.connect(db_path)
    
    # 스냅샷 기본 정보
    cursor = conn.execute("""
        SELECT snapshot_date, price_file_name, insights_file_name, reco_file_name
        FROM snapshots
        WHERE id = ?
    """, (snapshot_id,))
    
    row = cursor.fetchone()
    if not row:
        print(f"❌ Snapshot ID {snapshot_id}를 찾을 수 없습니다.")
        conn.close()
        return False
    
    print(f"\n📌 Snapshot ID {snapshot_id} 정보:")
    print("-" * 80)
    print(f"  날짜: {row[0]}")
    print(f"  엑셀 파일:")
    print(f"    • price_inventory: {row[1] or '(없음)'}")
    print(f"    • seller_insights:  {row[2] or '(없음)'}")
    print(f"    • coupang_price:    {row[3] or '(없음)'}")
    
    # 데이터 통계
    cursor = conn.execute("""
        SELECT 
            COUNT(DISTINCT CASE WHEN rocket_price IS NOT NULL THEN vendor_item_id END) as rocket_count,
            COUNT(DISTINCT CASE WHEN iherb_price IS NOT NULL THEN vendor_item_id END) as iherb_count
        FROM product_price
        WHERE snapshot_id = ?
    """, (snapshot_id,))
    
    stats = cursor.fetchone()
    print(f"\n  현재 데이터:")
    print(f"    • 로켓 상품: {stats[0]:,}개")
    print(f"    • 아이허브 상품: {stats[1]:,}개")
    
    conn.close()
    return True


def delete_iherb_data(db_path: str, snapshot_id: int):
    """아이허브 엑셀 데이터만 삭제 (크롤링 데이터 보호)"""
    
    print(f"\n🗑️  아이허브 엑셀 데이터 삭제 중...")
    
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    
    # 1. product_price에서 아이허브 데이터만 NULL로
    conn.execute("""
        UPDATE product_price
        SET iherb_price = NULL,
            iherb_original_price = NULL,
            iherb_recommended_price = NULL
        WHERE snapshot_id = ?
    """, (snapshot_id,))
    
    deleted_prices = conn.total_changes
    
    # 2. product_features에서 아이허브 데이터만 NULL로
    conn.execute("""
        UPDATE product_features
        SET iherb_stock = NULL,
            iherb_stock_status = NULL,
            iherb_revenue = NULL,
            iherb_sales_quantity = NULL,
            iherb_item_winner_ratio = NULL,
            iherb_category = NULL
        WHERE snapshot_id = ?
    """, (snapshot_id,))
    
    deleted_features = conn.total_changes - deleted_prices
    
    # 3. products 테이블에서 아이허브 전용 상품만 삭제
    # (로켓에도 있는 상품은 유지, part_number/upc만 NULL로)
    
    # 3-1. 로켓에도 있는 상품의 아이허브 정보만 NULL
    conn.execute("""
        UPDATE products
        SET part_number = NULL,
            upc = NULL
        WHERE vendor_item_id IN (
            SELECT vendor_item_id 
            FROM product_price 
            WHERE snapshot_id = ? AND rocket_price IS NOT NULL
        )
    """, (snapshot_id,))
    
    # 3-2. 아이허브 전용 상품 삭제 (로켓 가격 없음)
    cursor = conn.execute("""
        SELECT vendor_item_id
        FROM product_price
        WHERE snapshot_id = ?
          AND rocket_price IS NULL
          AND (iherb_price IS NOT NULL OR iherb_original_price IS NOT NULL)
    """, (snapshot_id,))
    
    iherb_only_ids = [row[0] for row in cursor.fetchall()]
    
    if iherb_only_ids:
        placeholders = ','.join('?' * len(iherb_only_ids))
        conn.execute(f"""
            DELETE FROM products
            WHERE vendor_item_id IN ({placeholders})
        """, iherb_only_ids)
        
        conn.execute(f"""
            DELETE FROM product_price
            WHERE vendor_item_id IN ({placeholders})
        """, iherb_only_ids)
        
        conn.execute(f"""
            DELETE FROM product_features
            WHERE vendor_item_id IN ({placeholders})
        """, iherb_only_ids)
    
    conn.commit()
    conn.close()
    
    print(f"  ✓ 가격 데이터: {deleted_prices:,}개 레코드")
    print(f"  ✓ 성과 데이터: {deleted_features:,}개 레코드")
    print(f"  ✓ 아이허브 전용 상품: {len(iherb_only_ids):,}개 삭제")


def update_snapshot_filenames(db_path: str, snapshot_id: int):
    """스냅샷에 새 파일명 업데이트"""
    
    excel_files = Config.get_all_excel_files()
    
    file_names = {}
    if excel_files.get('price_inventory'):
        file_names['price'] = excel_files['price_inventory'].name
    if excel_files.get('seller_insights'):
        file_names['insights'] = excel_files['seller_insights'].name
    if excel_files.get('coupang_price'):
        file_names['reco'] = excel_files['coupang_price'].name
    
    if not file_names:
        return
    
    conn = sqlite3.connect(db_path)
    
    update_parts = []
    params = []
    
    if 'price' in file_names:
        update_parts.append("price_file_name = ?")
        params.append(file_names['price'])
    if 'insights' in file_names:
        update_parts.append("insights_file_name = ?")
        params.append(file_names['insights'])
    if 'reco' in file_names:
        update_parts.append("reco_file_name = ?")
        params.append(file_names['reco'])
    
    params.append(snapshot_id)
    
    conn.execute(
        f"UPDATE snapshots SET {', '.join(update_parts)} WHERE id = ?",
        params
    )
    
    conn.commit()
    conn.close()


def main():
    print("\n" + "="*80)
    print("🔄 스냅샷 엑셀 데이터 교체")
    print("="*80)
    
    db = IntegratedDatabase(Config.INTEGRATED_DB_PATH)
    
    # 1. 스냅샷 목록
    print("\n📋 최근 스냅샷:")
    print("-" * 80)
    
    conn = sqlite3.connect(db.db_path)
    cursor = conn.execute("""
        SELECT id, snapshot_date, 
               price_file_name,
               (SELECT COUNT(DISTINCT vendor_item_id) 
                FROM product_price 
                WHERE snapshot_id = snapshots.id AND rocket_price IS NOT NULL) as rocket_count
        FROM snapshots
        ORDER BY id DESC
        LIMIT 10
    """)
    
    snapshots = []
    for row in cursor.fetchall():
        snapshots.append({
            'id': row[0],
            'date': row[1],
            'excel': row[2],
            'rocket_count': row[3]
        })
        print(f"  ID {row[0]:2d} | {row[1]} | 로켓: {row[3]:,}개 | {row[2] or '(엑셀없음)'}")
    
    conn.close()
    
    # 2. 스냅샷 선택
    print("\n" + "="*80)
    try:
        snapshot_id = int(input("교체할 Snapshot ID를 입력하세요: ").strip())
    except ValueError:
        print("❌ 잘못된 입력")
        return
    
    # 3. 선택한 스냅샷 정보
    if not show_snapshot_info(db.db_path, snapshot_id):
        return
    
    # 4. 새 엑셀 파일 확인
    print(f"\n📂 새로 업로드할 엑셀 파일:")
    print("-" * 80)
    
    excel_files = Config.get_all_excel_files()
    for key, path in excel_files.items():
        if path:
            print(f"  ✓ {key:20s}: {path.name}")
        else:
            print(f"  ✗ {key:20s}: 없음")
    
    # 5. 확인
    print("\n" + "="*80)
    print("⚠️  주의사항:")
    print("  • 아이허브 엑셀 데이터가 삭제됩니다")
    print("  • 로켓 크롤링 데이터는 유지됩니다")
    print("  • 새 엑셀 파일이 업로드됩니다")
    print("="*80)
    
    confirm = input(f"\nSnapshot ID {snapshot_id}의 엑셀 데이터를 교체하시겠습니까? (yes 입력): ").strip()
    
    if confirm != 'yes':
        print("❌ 취소됨")
        return
    
    # 6. 백업 권장
    print(f"\n💡 백업 권장: DB 파일을 먼저 복사해두세요")
    print(f"   cp {Config.INTEGRATED_DB_PATH} {Config.INTEGRATED_DB_PATH}.backup")
    
    proceed = input("백업을 완료했거나 건너뛰려면 'proceed' 입력: ").strip()
    
    if proceed != 'proceed':
        print("❌ 취소됨")
        return
    
    # 7. 삭제
    delete_iherb_data(db.db_path, snapshot_id)
    
    # 8. 파일명 업데이트
    update_snapshot_filenames(db.db_path, snapshot_id)
    
    # 9. 재업로드
    print(f"\n📥 새 엑셀 파일 업로드 중...")
    loader = ExcelLoader(db)
    result = loader.load_all_excel_files(
        snapshot_id=snapshot_id,
        excel_dir=Config.IHERB_EXCEL_DIR
    )
    
    # 10. 결과
    print("\n" + "="*80)
    print("✅ 교체 완료")
    print("="*80)
    
    show_snapshot_info(db.db_path, snapshot_id)
    
    print(f"\n💡 다음 단계:")
    print(f"  price_comparison.py 실행 시 최신 스냅샷을 자동으로 사용합니다")
    print(f"  또는 명시적으로 지정: df = manager.get_integrated_df(snapshot_id={snapshot_id})")
    print()


if __name__ == "__main__":
    main()