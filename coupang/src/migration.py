#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from pathlib import Path

import pandas as pd


DB_PATH = Path("/Users/brich/Desktop/iherb_price/coupang/data/rocket_iherb.db")  # 필요시 수정
DATA_DIR = Path("/Users/brich/Desktop/iherb_price/coupang/data/integrated")                # Coupang_Price 엑셀이 있는 폴더


# ---------------------------------------------------
# 1. 컬럼 존재 여부 체크 후 필요하면 추가
# ---------------------------------------------------
def ensure_columns():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(product_features);")
        cols = {row[1] for row in cur.fetchall()}

        to_add = []
        if "coupang_sales_last_7d" not in cols:
            to_add.append(
                "ALTER TABLE product_features "
                "ADD COLUMN coupang_sales_last_7d INTEGER"
            )
        if "coupang_share_last_7d" not in cols:
            to_add.append(
                "ALTER TABLE product_features "
                "ADD COLUMN coupang_share_last_7d REAL"
            )

        for sql in to_add:
            print(f"[SCHEMA] {sql}")
            cur.execute(sql)

        if to_add:
            conn.commit()
            print("✅ product_features 컬럼 추가 완료")
        else:
            print("ℹ️  이미 두 컬럼이 존재합니다. 스키마 변경 생략.")
    finally:
        conn.close()


# ---------------------------------------------------
# 2. snapshots 테이블에서 엑셀 매핑 정보 가져오기
# ---------------------------------------------------
def get_snapshots_with_reco_file():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, snapshot_date, reco_file_name
            FROM snapshots
            WHERE reco_file_name IS NOT NULL
            ORDER BY snapshot_date, id
        """)
        rows = cur.fetchall()
        snapshots = []
        for sid, sdate, fname in rows:
            # 실제 파일 경로
            excel_path = DATA_DIR / fname
            snapshots.append(
                {
                    "snapshot_id": sid,
                    "snapshot_date": sdate,
                    "reco_file_name": fname,
                    "excel_path": excel_path,
                }
            )
        return snapshots
    finally:
        conn.close()


# ---------------------------------------------------
# 3. 엑셀 한 개 처리: 옵션ID → 판매/점유율 → DB 업데이트
# ---------------------------------------------------
def parse_percentage(val):
    """'78%' -> 0.78, 공백/NaN/0%/'-' 등은 None 또는 0으로 처리"""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        # 이미 숫자면 0~100으로 들어왔다고 가정하고 0~1 변환
        return float(val) / 100.0
    s = str(val).strip()
    if not s or s == "-":
        return None
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s) / 100.0
    except ValueError:
        return None


def load_coupang_price_excel(path: Path):
    """Coupang_Price 엑셀에서 필요한 컬럼만 추출해서 DataFrame으로 반환"""
    if not path.exists():
        print(f"⚠️  파일 없음: {path}")
        return None

    xl = pd.ExcelFile(path)
    # 시트명 결정
    if "Excel Download" in xl.sheet_names:
        sheet_name = "Excel Download"
    else:
        sheet_name = xl.sheet_names[0]

    # 1) header=None으로 전체 읽기
    df_raw = xl.parse(sheet_name, header=None)
    if df_raw.empty:
        print(f"⚠️  빈 엑셀 시트: {path}")
        return None

    # 2) 헤더 행 탐색
    target_cols = ["옵션ID", "나의 지난주 판매개수", "내상품 판매 점유율 (지난 7일간)"]
    header_row_idx = None

    for idx, row in df_raw.iterrows():
        values = [str(v).strip() for v in row if not pd.isna(v)]
        if all(any(tc == v for v in values) for tc in target_cols):
            header_row_idx = idx
            break

    if header_row_idx is None:
        print(f"⚠️  필요한 컬럼이 없음 {target_cols} in {path.name}")
        return None

    # 3) 찾은 헤더 행을 컬럼명으로 사용
    header = df_raw.iloc[header_row_idx].tolist()
    df = df_raw.iloc[header_row_idx + 1 :].copy()
    df.columns = header

    # 4) 필요한 컬럼만 추출
    col_option = "옵션ID"
    col_sales = "나의 지난주 판매개수"
    col_share = "내상품 판매 점유율 (지난 7일간)"

    sub = df[[col_option, col_sales, col_share]].copy()

    # 5) 타입 정리
    sub[col_option] = sub[col_option].astype(str).str.strip()

    def to_int_or_none(v):
        if pd.isna(v) or v == "-" or str(v).strip() == "":
            return None
        try:
            return int(str(v).replace(",", "").strip())
        except ValueError:
            return None

    def parse_percentage(val):
        if pd.isna(val):
            return None
        if isinstance(val, (int, float)):
            # 0~100이라고 보고 0~1로 변환
            return float(val) / 100.0
        s = str(val).strip()
        if not s or s == "-":
            return None
        if s.endswith("%"):
            s = s[:-1]
        try:
            return float(s) / 100.0
        except ValueError:
            return None

    sub[col_sales] = sub[col_sales].apply(to_int_or_none)
    sub[col_share] = sub[col_share].apply(parse_percentage)

    return sub.rename(
        columns={
            col_option: "vendor_item_id",
            col_sales: "coupang_sales_last_7d",
            col_share: "coupang_share_last_7d",
        }
    )


# ---------------------------------------------------
# 4. 실제 DB 업데이트
# ---------------------------------------------------
def update_features_from_excel(snapshot_id: int, df: pd.DataFrame):
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        updated = 0
        skipped = 0

        for _, row in df.iterrows():
            vendor_item_id = row["vendor_item_id"]
            sales = row["coupang_sales_last_7d"]
            share = row["coupang_share_last_7d"]

            if sales is None and share is None:
                skipped += 1
                continue

            # snapshot_id + vendor_item_id 기준으로 업데이트
            cur.execute(
                """
                UPDATE product_features
                   SET coupang_sales_last_7d = ?,
                       coupang_share_last_7d = ?
                 WHERE snapshot_id = ?
                   AND vendor_item_id = ?
                """,
                (sales, share, snapshot_id, vendor_item_id),
            )
            if cur.rowcount > 0:
                updated += 1
            else:
                # product_features에 row 자체가 없을 수도 있음
                skipped += 1

        conn.commit()
        print(
            f"  → snapshot_id={snapshot_id}: 업데이트 {updated}건, 스킵 {skipped}건"
        )
    finally:
        conn.close()


# ---------------------------------------------------
# 5. 전체 파이프라인 실행
# ---------------------------------------------------
def main():
    print("=== STEP 1. 스키마 점검 및 컬럼 추가 ===")
    ensure_columns()

    print("\n=== STEP 2. snapshots ↔ 엑셀 파일 매핑 ===")
    snapshots = get_snapshots_with_reco_file()
    if not snapshots:
        print("⚠️  reco_file_name이 있는 스냅샷이 없습니다.")
        return

    for s in snapshots:
        sid = s["snapshot_id"]
        sdate = s["snapshot_date"]
        fname = s["reco_file_name"]
        path = s["excel_path"]

        print(f"\n[SNAPSHOT] id={sid}, date={sdate}, file={fname}")
        if not path.exists():
            print(f"  ⚠️ 엑셀 파일이 존재하지 않음: {path}")
            continue

        df = load_coupang_price_excel(path)
        if df is None:
            print("  ⚠️ 엑셀 파싱 실패, 스킵")
            continue

        print(f"  엑셀 로우 수: {len(df)}")
        update_features_from_excel(sid, df)

    print("\n✅ 전체 완료")


if __name__ == "__main__":
    main()
