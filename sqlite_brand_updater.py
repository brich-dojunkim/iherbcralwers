"""
브랜드별 통합 파이프라인 실행기 (SQLite 버전)
================================================

이 스크립트는 브랜드 이름과 쿠팡 검색 URL, 마지막 업데이트 정보 등을 SQLite 데이터베이스에 저장하고,
해당 브랜드에 대해 새로 크롤링(초기값 모드) 또는 업데이트 모드를 선택적으로 실행할 수 있도록 돕습니다.

기본 동작:
  * ``--list`` 플래그를 사용하면 데이터베이스에 저장된 모든 브랜드와 마지막 업데이트 날짜를 보여줍니다.
  * ``--brand <브랜드명>`` 인자를 지정하면 해당 브랜드를 즉시 실행합니다.
  * 인자가 없으면 브랜드 목록을 보여주고 콘솔에서 선택하게 합니다.

브랜드 정보를 추가하거나 수정하려면 스크립트 실행 전에 SQLite 데이터베이스에 직접 INSERT/UPDATE
명령을 실행하거나 별도의 관리 스크립트를 작성해야 합니다.

데이터베이스 스키마:

```sql
CREATE TABLE IF NOT EXISTS brand_info (
    brand_name   TEXT PRIMARY KEY,
    coupang_url  TEXT NOT NULL,
    last_updated TEXT,
    result_csv   TEXT
);
```

``last_updated``와 ``result_csv``는 파이프라인 실행 후 자동으로 갱신됩니다. ``result_csv``는 
업데이트 모드에서 사용할 베이스 CSV의 경로로 활용됩니다.
"""

import argparse
import datetime
import os
import sqlite3
import sys
from config import DatabaseConfig, PathConfig
from improved_updater import run_pipeline

os.makedirs(PathConfig.DATA_ROOT, exist_ok=True)
DB_PATH = os.path.join(PathConfig.DATA_ROOT, DatabaseConfig.DATABASE_NAME)

def init_db() -> sqlite3.Connection:
    """SQLite 데이터베이스를 초기화하고 테이블을 생성합니다."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS brand_info (
                brand_name   TEXT PRIMARY KEY,
                coupang_url  TEXT NOT NULL,
                last_updated TEXT,
                result_csv   TEXT
            )
            """
        )
    return conn


def list_brands(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """저장된 모든 브랜드를 가져와 반환합니다."""
    cur = conn.cursor()
    cur.execute("SELECT brand_name, last_updated FROM brand_info ORDER BY brand_name")
    return cur.fetchall()


def get_brand_info(conn: sqlite3.Connection, brand_name: str) -> sqlite3.Row | None:
    """특정 브랜드의 정보를 반환합니다."""
    cur = conn.cursor()
    cur.execute(
        "SELECT brand_name, coupang_url, last_updated, result_csv FROM brand_info WHERE brand_name = ?",
        (brand_name,),
    )
    return cur.fetchone()


def update_brand_info(conn: sqlite3.Connection, brand_name: str, last_updated: str, result_csv: str) -> None:
    """브랜드의 마지막 업데이트 정보와 결과 CSV 경로를 갱신합니다."""
    with conn:
        conn.execute(
            "UPDATE brand_info SET last_updated = ?, result_csv = ? WHERE brand_name = ?",
            (last_updated, result_csv, brand_name),
        )


def interactive_select_brand(brands: list[sqlite3.Row]) -> str:
    """사용자에게 브랜드 목록을 보여주고 선택하도록 합니다."""
    print("사용 가능한 브랜드 목록:")
    for idx, row in enumerate(brands, start=1):
        last = row['last_updated'] if row['last_updated'] else '-'
        print(f"  [{idx}] {row['brand_name']} (마지막 업데이트: {last})")
    while True:
        try:
            choice = input("실행할 브랜드 번호를 선택하세요: ").strip()
            index = int(choice)
            if 1 <= index <= len(brands):
                return brands[index - 1]['brand_name']
        except (ValueError, IndexError):
            pass
        print("잘못된 입력입니다. 다시 시도해주세요.")


def main() -> int:
    parser = argparse.ArgumentParser(description="브랜드별 쿠팡-iHerb 파이프라인 실행기 (SQLite 버전)")
    parser.add_argument('--list', action='store_true', help='저장된 브랜드 목록을 출력하고 종료')
    parser.add_argument('--brand', type=str, help='실행할 브랜드 이름')
    args = parser.parse_args()

    conn = init_db()

    # 브랜드 목록 출력 옵션
    if args.list:
        brands = list_brands(conn)
        if not brands:
            print("데이터베이스에 저장된 브랜드가 없습니다.")
        else:
            print("저장된 브랜드 목록:")
            for row in brands:
                last = row['last_updated'] if row['last_updated'] else '-'
                print(f"- {row['brand_name']} (마지막 업데이트: {last})")
        return 0

    # 실행할 브랜드 선택
    if args.brand:
        brand_name = args.brand
    else:
        brands = list_brands(conn)
        if not brands:
            print("실행 가능한 브랜드가 없습니다. 먼저 DB에 브랜드 정보를 추가하세요.")
            return 1
        brand_name = interactive_select_brand(brands)

    row = get_brand_info(conn, brand_name)
    if not row:
        print(f"브랜드 '{brand_name}' 정보를 찾을 수 없습니다. DB에 등록되어 있는지 확인하세요.")
        return 1

    coupang_url = row['coupang_url']
    base_csv = None
    # update 모드 여부: result_csv가 존재하고 파일이 실제로 존재하면 업데이트 모드로 처리
    if row['result_csv'] and os.path.exists(row['result_csv']):
        base_csv = row['result_csv']

    print(f"\n[실행] 브랜드: {brand_name}")
    print(f"검색 URL: {coupang_url}")
    if base_csv:
        print(f"베이스 CSV: {base_csv} (업데이트 모드)")
    else:
        print("베이스 CSV: 없음 (초기값 모드)")

    # 파이프라인 실행
    try:
        result_csv = run_pipeline(coupang_url, base_csv)
        if result_csv:
            # 마지막 업데이트 일자 갱신
            today = datetime.date.today().isoformat()
            update_brand_info(conn, brand_name, today, result_csv)
            print(f"\n브랜드 '{brand_name}' 정보가 업데이트되었습니다.")
            return 0
        else:
            print("실행 중 문제가 발생해 결과 CSV가 생성되지 않았습니다.")
            return 1
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다.")
        return 1
    except Exception as e:
        print(f"\n실행 중 오류가 발생했습니다: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())