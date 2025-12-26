#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
UPC 일회성 적재 스크립트
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  이 스크립트는 딱 한 번만 실행하세요!
이후 monitoring.py는 UPC 없이 실행됩니다.

사용법:
    python load_upc_once.py
"""

import sys
from pathlib import Path
import pandas as pd
import sqlite3

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Config


def load_upc_from_excel(file_path: Path) -> dict:
    """UPC 엑셀 파싱
    
    Returns:
        {item_id: upc} 딕셔너리
    """
    print(f"\n📂 UPC 파일 읽는 중: {file_path.name}")
    
    df = pd.read_excel(file_path)
    
    print(f"   • 전체 행: {len(df):,}개")
    print(f"   • 컬럼: {list(df.columns)[:5]}...")
    
    # 컬럼명 확인
    item_col = '쿠팡 상품번호'
    upc_col = 'UPC'
    
    if item_col not in df.columns:
        raise ValueError(f"❌ '{item_col}' 컬럼이 없습니다.")
    
    if upc_col not in df.columns:
        raise ValueError(f"❌ '{upc_col}' 컬럼이 없습니다.")
    
    # UPC 맵 생성 (item_id → upc)
    upc_map = {}
    
    for _, row in df.iterrows():
        item_id = row[item_col]
        upc = row[upc_col]
        
        # 유효성 체크
        if pd.notna(item_id) and pd.notna(upc):
            # item_id를 문자열로 변환 (소수점 제거)
            item_id_str = str(int(item_id)) if isinstance(item_id, float) else str(item_id)
            
            # upc를 문자열로 변환
            upc_str = str(int(upc)) if isinstance(upc, float) else str(upc)
            
            if item_id_str and upc_str:
                upc_map[item_id_str] = upc_str
    
    print(f"   ✅ 유효 UPC: {len(upc_map):,}개")
    
    return upc_map


def update_products_upc(db_path: str, upc_map: dict):
    """products 테이블에 UPC 업데이트
    
    Args:
        db_path: DB 경로
        upc_map: {item_id: upc}
    """
    print(f"\n💾 products 테이블 업데이트 중...")
    
    conn = sqlite3.connect(db_path)
    
    # 1. 현재 상태
    cursor = conn.execute("SELECT COUNT(*) FROM products WHERE upc IS NOT NULL AND upc != ''")
    before_count = cursor.fetchone()[0]
    print(f"   • 업데이트 전 UPC 있는 상품: {before_count:,}개")
    
    # 2. item_id로 매칭하여 UPC 업데이트
    updated = 0
    not_found = 0
    already_has_upc = 0
    
    for item_id, upc in upc_map.items():
        # item_id로 상품 찾기
        cursor = conn.execute(
            "SELECT vendor_item_id, upc FROM products WHERE item_id = ?",
            (item_id,)
        )
        result = cursor.fetchone()
        
        if result:
            vendor_item_id, current_upc = result
            
            # 이미 UPC가 있으면 스킵 (덮어쓰지 않음)
            if current_upc:
                already_has_upc += 1
                continue
            
            # UPC 업데이트
            conn.execute(
                "UPDATE products SET upc = ? WHERE vendor_item_id = ?",
                (upc, vendor_item_id)
            )
            updated += 1
        else:
            not_found += 1
    
    conn.commit()
    
    # 3. 결과 확인
    cursor = conn.execute("SELECT COUNT(*) FROM products WHERE upc IS NOT NULL AND upc != ''")
    after_count = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"\n📊 업데이트 결과:")
    print(f"   ✅ 새로 추가된 UPC: {updated:,}개")
    print(f"   ℹ️  이미 UPC 있음 (스킵): {already_has_upc:,}개")
    print(f"   ⚠️  매칭 실패 (DB에 item_id 없음): {not_found:,}개")
    print(f"   ✅ 최종 UPC 있는 상품: {after_count:,}개 (이전: {before_count:,}개)")


def main():
    """메인"""
    
    print(f"\n{'='*80}")
    print(f"📦 UPC 일회성 적재")
    print(f"{'='*80}")
    print(f"⚠️  주의사항:")
    print(f"   • 이 스크립트는 딱 한 번만 실행하면 됩니다")
    print(f"   • 이후 monitoring.py는 UPC 없이 실행됩니다")
    print(f"   • 새 상품 추가 시에만 다시 실행하세요")
    print(f"{'='*80}\n")
    
    # 1. UPC 파일 찾기
    upc_file = Config.IHERB_EXCEL_DIR / "20251024_1444.xlsx"
    
    if not upc_file.exists():
        # 패턴으로 찾기
        files = list(Config.IHERB_EXCEL_DIR.glob("20251024_*.xlsx"))
        if not files:
            print(f"❌ UPC 파일을 찾을 수 없습니다:")
            print(f"   경로: {Config.IHERB_EXCEL_DIR}")
            print(f"   패턴: 20251024_*.xlsx")
            return
        
        upc_file = files[0]
    
    print(f"✅ UPC 파일 발견: {upc_file.name}")
    
    # 2. 실행 확인
    print(f"\n다음 작업을 수행합니다:")
    print(f"   1. {upc_file.name} 파일 읽기")
    print(f"   2. products 테이블에 UPC 업데이트")
    print(f"   3. 이미 UPC가 있는 상품은 스킵")
    
    confirm = input(f"\n계속하시겠습니까? (yes 입력): ").strip()
    if confirm != 'yes':
        print("❌ 취소됨")
        return
    
    # 3. UPC 로드
    try:
        upc_map = load_upc_from_excel(upc_file)
    except Exception as e:
        print(f"\n❌ UPC 파일 읽기 실패: {e}")
        import traceback
        traceback.print_exc()
        return
    
    if not upc_map:
        print(f"\n❌ 유효한 UPC 데이터가 없습니다.")
        return
    
    # 4. DB 업데이트
    try:
        update_products_upc(str(Config.INTEGRATED_DB_PATH), upc_map)
    except Exception as e:
        print(f"\n❌ DB 업데이트 실패: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 5. 완료
    print(f"\n{'='*80}")
    print(f"✅ UPC 적재 완료!")
    print(f"{'='*80}")
    print(f"\n💡 다음 단계:")
    print(f"   1. price_comparison_2.py 실행하여 UPC 확인")
    print(f"      → python coupang/analysis/price_comparison_2.py")
    print(f"")
    print(f"   2. 이후 monitoring.py는 UPC 없이 실행")
    print(f"      → python coupang/src/monitoring.py")
    print(f"")
    print(f"⚠️  이 스크립트는 다시 실행할 필요 없습니다!")
    print(f"   (새 상품 추가되었을 때만 재실행)")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()