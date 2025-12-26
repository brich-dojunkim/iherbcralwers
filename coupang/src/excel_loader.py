#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Loader - 아이허브 엑셀 파일을 통합 DB에 적재
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
price_inventory, SELLER_INSIGHTS, Coupang_Price 3종 처리

🔥 수정사항:
  - UPC 로직 완전 제거 (load_upc_once.py로 분리)
  - upc_file 파라미터 무시
"""

import pandas as pd
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime


def safe_read_excel_header_guess(path, max_try=20):
    """상단 안내문 줄이 있는 엑셀에서 실제 헤더를 자동 탐색"""
    KEY_CANDIDATES = {"업체상품코드", "옵션 ID", "업체상품 ID", "바코드"}
    for header_row in range(max_try):
        df_try = pd.read_excel(path, header=header_row)
        cols = set(map(str, df_try.columns))
        if KEY_CANDIDATES & cols:
            return df_try
    return pd.read_excel(path)


def _pick_col(df: pd.DataFrame, candidates):
    """여러 후보 중 존재하는 첫 번째 컬럼명을 고름"""
    for c in candidates:
        if c in df.columns:
            return c
    return None


class ExcelLoader:
    """엑셀 파일을 통합 DB에 적재"""

    def __init__(self, db):
        self.db = db

    def _find_file(self, directory: Path, pattern: str) -> Optional[Path]:
        files = list(directory.glob(pattern))
        if not files:
            return None
        return max(files, key=lambda f: f.stat().st_mtime)

    def load_all_excel_files(
        self,
        snapshot_id: int,
        excel_dir: Path,
        price_file: Optional[Path] = None,
        insights_file: Optional[Path] = None,
        reco_file: Optional[Path] = None,
        upc_file: Optional[Path] = None,  # 🔥 무시됨 (호환성 유지)
    ) -> Dict[str, int]:
        """모든 엑셀 파일 적재
        
        🔥 변경사항:
          - upc_file 파라미터는 무시됨
          - UPC는 load_upc_once.py로 별도 관리
        """

        print(f"\n{'='*80}")
        print(f"📥 엑셀 파일 적재 시작 (Snapshot ID: {snapshot_id})")
        print(f"{'='*80}\n")

        # 파일 자동 탐색
        if price_file is None:
            price_file = self._find_file(excel_dir, "*price_inventory*.xlsx")
        if insights_file is None:
            insights_file = self._find_file(excel_dir, "*SELLER_INSIGHTS*.xlsx")
        if reco_file is None:
            reco_file = self._find_file(excel_dir, "Coupang_Price_*.xlsx")
        
        # 🔥 UPC는 더 이상 찾지 않음
        # if upc_file is None:
        #     upc_file = self._find_file(excel_dir, "20251024_*.xlsx")

        # 찾은 파일명을 snapshot에 업데이트
        file_names = {}
        if price_file and price_file.exists():
            file_names["price"] = price_file.name
        if insights_file and insights_file.exists():
            file_names["insights"] = insights_file.name
        if reco_file and reco_file.exists():
            file_names["reco"] = reco_file.name

        if file_names:
            conn = sqlite3.connect(self.db.db_path)
            update_parts = []
            params = []
            if "price" in file_names:
                update_parts.append("price_file_name = ?")
                params.append(file_names["price"])
            if "insights" in file_names:
                update_parts.append("insights_file_name = ?")
                params.append(file_names["insights"])
            if "reco" in file_names:
                update_parts.append("reco_file_name = ?")
                params.append(file_names["reco"])
            params.append(snapshot_id)

            conn.execute(
                f"UPDATE snapshots SET {', '.join(update_parts)} WHERE id = ?",
                params,
            )
            conn.commit()
            conn.close()

        # 1. price_inventory 처리
        products_data: List[Dict] = []
        prices_data: List[Dict] = []
        features_map: Dict[str, Dict] = {}

        if price_file and price_file.exists():
            print(f"📄 1. Price Inventory: {price_file.name}")
            prod, price, feats = self._load_price_inventory(price_file)
            products_data.extend(prod)
            prices_data.extend(price)
            for feat in feats:
                vid = feat.get("vendor_item_id")
                if vid:
                    features_map[vid] = feat
            print(f"   ✓ 상품: {len(prod):,}개, 가격: {len(price):,}개\n")
        else:
            print(f"⚠️  1. Price Inventory: 파일 없음\n")

        # 🔥 2. UPC 처리 - 완전 제거
        print(f"ℹ️  2. UPC: load_upc_once.py로 별도 관리\n")

        # 3. Coupang_Price 처리 (추천가 + 지난 7일 판매/점유율)
        if reco_file and reco_file.exists():
            print(f"📄 3. Coupang Price: {reco_file.name}")
            reco_data = self._load_coupang_recommended_price(reco_file)

            # (1) 추천가 → prices_data 병합
            self._merge_reco_to_prices(prices_data, reco_data)

            # (2) 지난 7일 판매/점유율 → features_map 병합
            for r in reco_data:
                vid = r.get("vendor_item_id")
                if not vid:
                    continue

                sales_qty_7d = r.get("iherb_sales_quantity_last_7d")
                share_7d = r.get("iherb_coupang_share_last_7d")

                if sales_qty_7d is None and share_7d is None:
                    continue

                if vid not in features_map:
                    features_map[vid] = {
                        "vendor_item_id": vid,
                        "rocket_rank": None,
                        "rocket_rating": None,
                        "rocket_reviews": None,
                        "rocket_category": None,
                        "iherb_stock": None,
                        "iherb_stock_status": None,
                        "iherb_revenue": None,
                        "iherb_sales_quantity": None,
                        "iherb_item_winner_ratio": None,
                        "iherb_category": None,
                    }

                if sales_qty_7d is not None:
                    features_map[vid]["iherb_sales_quantity_last_7d"] = sales_qty_7d
                if share_7d is not None:
                    features_map[vid]["iherb_coupang_share_last_7d"] = share_7d

            print(f"   ✓ 추천가/지난 7일 판매지표: {len(reco_data):,}개\n")
        else:
            print(f"⚠️  3. Coupang Price: 파일 없음\n")

        # 4. SELLER_INSIGHTS 처리
        if insights_file and insights_file.exists():
            print(f"📄 4. Seller Insights: {insights_file.name}")
            seller_features = self._load_seller_insights(insights_file)
            for feat in seller_features:
                vid = feat.get("vendor_item_id")
                if not vid:
                    continue
                if vid in features_map:
                    base = features_map[vid]
                    for key, val in feat.items():
                        if key in (
                            "rocket_rank",
                            "rocket_rating",
                            "rocket_reviews",
                            "rocket_category",
                            "iherb_revenue",
                            "iherb_sales_quantity",
                            "iherb_item_winner_ratio",
                            "iherb_category",
                        ):
                            if val is not None:
                                base[key] = val
                else:
                    features_map[vid] = feat
            print(f"   ✓ 성과: {len(seller_features):,}개\n")
        else:
            print(f"⚠️  4. Seller Insights: 파일 없음\n")

        # 5. DB 저장
        print(f"💾 DB 저장 중...")

        if products_data:
            self.db.batch_upsert_products(products_data)
            print(f"   ✓ Products: {len(products_data):,}개")

        if prices_data:
            for p in prices_data:
                p["snapshot_id"] = snapshot_id
            self.db.batch_save_product_prices(snapshot_id, prices_data)
            print(f"   ✓ Prices: {len(prices_data):,}개")

        features_data = list(features_map.values())
        filtered_features: List[Dict] = []

        if features_data:
            for f in features_data:
                f["snapshot_id"] = snapshot_id

            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.execute("SELECT vendor_item_id FROM products")
            existing_ids = {row[0] for row in cursor.fetchall()}
            conn.close()

            filtered_features = [
                f for f in features_data if f["vendor_item_id"] in existing_ids
            ]

            if filtered_features:
                self.db.batch_save_product_features(snapshot_id, filtered_features)
                print(
                    f"   ✓ Features: {len(filtered_features):,}개 (전체 {len(features_data):,}개 중)"
                )
            else:
                print("   ⚠️ Features: 매칭되는 상품 없음")

        print(f"\n{'='*80}")
        print("✅ 엑셀 적재 완료")
        print(f"{'='*80}\n")

        return {
            "products": len(products_data),
            "prices": len(prices_data),
            "features": len(filtered_features),
        }

    def _load_price_inventory(self, file_path: Path) -> tuple:
        """price_inventory 엑셀 읽기 (상품, 가격, 재고/상태)"""
        try:
            df = pd.read_excel(file_path, sheet_name="data", skiprows=2)
        except Exception:
            df = safe_read_excel_header_guess(file_path, max_try=30)

        col_vid = _pick_col(df, ["옵션 ID"])
        col_pid = _pick_col(df, ["Product ID", "productId", "PRODUCT_ID"])
        col_iid = _pick_col(df, ["업체상품 ID", "itemId", "ITEM_ID"])
        col_pname = _pick_col(df, ["쿠팡 노출 상품명", "상품명"])
        col_pn = _pick_col(df, ["업체상품코드"])
        col_price = _pick_col(df, ["판매가격", "판매가격.1"])
        col_original = _pick_col(df, ["할인율기준가"])
        col_stock = _pick_col(df, ["잔여수량(재고)", "잔여수량"])
        col_state = _pick_col(df, ["판매상태", "판매상태.1"])

        if col_vid is None:
            df["옵션 ID"] = None
            col_vid = "옵션 ID"
        if col_pid is None:
            df["Product ID"] = None
            col_pid = "Product ID"
        if col_iid is None:
            df["업체상품 ID"] = None
            col_iid = "업체상품 ID"
        if col_pname is None:
            df["쿠팡 노출 상품명"] = None
            col_pname = "쿠팡 노출 상품명"
        if col_pn is None:
            df["업체상품코드"] = None
            col_pn = "업체상품코드"
        if col_price is None:
            df["판매가격"] = 0
            col_price = "판매가격"
        if col_original is None:
            df["할인율기준가"] = 0
            col_original = "할인율기준가"
        if col_stock is None:
            df["잔여수량(재고)"] = 0
            col_stock = "잔여수량(재고)"
        if col_state is None:
            df["판매상태"] = None
            col_state = "판매상태"

        products_data: List[Dict] = []
        prices_data: List[Dict] = []
        features_data: List[Dict] = []

        for _, row in df.iterrows():
            vendor_id = str(row[col_vid]).split(".")[0]
            if vendor_id in ("<NA>", "nan"):
                continue

            product_id = str(row[col_pid]).replace(".0", "")
            item_id = str(row[col_iid]).replace(".0", "")
            name = row[col_pname]
            part_number = str(row[col_pn]).strip()

            products_data.append(
                {
                    "vendor_item_id": vendor_id,
                    "product_id": product_id if product_id != "nan" else None,
                    "item_id": item_id if item_id != "nan" else None,
                    "part_number": part_number if part_number != "nan" else None,
                    "upc": None,  # 🔥 UPC는 더 이상 여기서 설정하지 않음
                    "name": name if pd.notna(name) else None,
                }
            )

            iherb_price = pd.to_numeric(row[col_price], errors="coerce")
            iherb_original = pd.to_numeric(row[col_original], errors="coerce")

            prices_data.append(
                {
                    "vendor_item_id": vendor_id,
                    "rocket_price": None,
                    "rocket_original_price": None,
                    "iherb_price": int(iherb_price)
                    if pd.notna(iherb_price)
                    else None,
                    "iherb_original_price": int(iherb_original)
                    if pd.notna(iherb_original)
                    else None,
                    "iherb_recommended_price": None,
                }
            )

            stock_value = row[col_stock]
            stock_status_value = row[col_state]
            features_data.append(
                {
                    "vendor_item_id": vendor_id,
                    "rocket_rank": None,
                    "rocket_rating": None,
                    "rocket_reviews": None,
                    "rocket_category": None,
                    "iherb_stock": int(stock_value) if pd.notna(stock_value) else None,
                    "iherb_stock_status": stock_status_value
                    if pd.notna(stock_status_value)
                    else None,
                    "iherb_revenue": None,
                    "iherb_sales_quantity": None,
                    "iherb_item_winner_ratio": None,
                    "iherb_category": None,
                }
            )

        return products_data, prices_data, features_data

    # 🔥 _load_upc, _merge_upc_to_products 메서드 완전 삭제

    def _load_coupang_recommended_price(self, file_path: Path) -> List[Dict]:
        """Coupang_Price 엑셀 읽기 (추천가 + 지난 7일 판매/점유율)"""
        df = pd.read_excel(file_path, header=1)

        col_vid = "옵션ID"
        col_reco = "쿠팡추천가 (원)"
        col_sales_amount = "나의 지난주 매출"
        col_sales_qty = "나의 지난주 판매개수"
        col_share = "내상품 판매 점유율 (지난 7일간)"

        cols = set(map(str, df.columns))

        def exists_or_none(name: str):
            return name if name in cols else None

        col_vid = exists_or_none(col_vid)
        col_reco = exists_or_none(col_reco)
        col_sales_amount = exists_or_none(col_sales_amount)
        col_sales_qty = exists_or_none(col_sales_qty)
        col_share = exists_or_none(col_share)

        if col_vid is None:
            print(f"⚠️  Coupang_Price에 '옵션ID' 컬럼이 없습니다: {file_path.name}")
            return []

        def to_int_or_none(v):
            if pd.isna(v) or str(v).strip() in ("", "-"):
                return None
            try:
                return int(str(v).replace(",", "").strip())
            except ValueError:
                return None

        def parse_percentage(v):
            if pd.isna(v):
                return None
            s = str(v).strip()
            if not s or s == "-":
                return None
            if s.endswith("%"):
                s = s[:-1]
            try:
                return float(s) / 100.0
            except ValueError:
                return None

        reco_data: List[Dict] = []

        for _, row in df.iterrows():
            vendor_id = str(row.get(col_vid, "")).replace(".0", "").strip()
            if vendor_id in ("", "nan", "<NA>"):
                continue

            reco_price = None
            if col_reco:
                reco_val = pd.to_numeric(row.get(col_reco), errors="coerce")
                if pd.notna(reco_val):
                    reco_price = int(reco_val)

            sales_amount_7d = (
                to_int_or_none(row.get(col_sales_amount)) if col_sales_amount else None
            )
            sales_qty_7d = (
                to_int_or_none(row.get(col_sales_qty)) if col_sales_qty else None
            )
            share_7d = parse_percentage(row.get(col_share)) if col_share else None

            reco_data.append(
                {
                    "vendor_item_id": vendor_id,
                    "iherb_recommended_price": reco_price,
                    "iherb_sales_quantity_last_7d": sales_qty_7d,
                    "iherb_coupang_share_last_7d": share_7d,
                }
            )

        return reco_data

    def _merge_reco_to_prices(self, prices_data: List[Dict], reco_data: List[Dict]):
        """추천가를 prices에 병합"""
        reco_map = {
            r["vendor_item_id"]: r["iherb_recommended_price"] for r in reco_data
        }
        for p in prices_data:
            vid = p.get("vendor_item_id")
            if vid and vid in reco_map:
                p["iherb_recommended_price"] = reco_map[vid]

    def _load_seller_insights(self, file_path: Path) -> List[Dict]:
        """SELLER_INSIGHTS 엑셀 읽기"""
        df = pd.read_excel(file_path, sheet_name="vendor item metrics")

        def to_int(s):
            if isinstance(s, (int, float)):
                return int(s) if not pd.isna(s) else 0
            return int(pd.to_numeric(s, errors="coerce").fillna(0))

        def to_float(s):
            if isinstance(s, (int, float)):
                return round(float(s), 1) if not pd.isna(s) else 0.0
            return round(float(pd.to_numeric(s, errors="coerce").fillna(0)), 1)

        features_data: List[Dict] = []
        for _, row in df.iterrows():
            vendor_id = str(row["옵션 ID"])
            if vendor_id in ("nan", "<NA>"):
                continue
            features_data.append(
                {
                    "vendor_item_id": vendor_id,
                    "rocket_rank": None,
                    "rocket_rating": None,
                    "rocket_reviews": None,
                    "rocket_category": None,
                    "iherb_stock": None,
                    "iherb_stock_status": None,
                    "iherb_revenue": to_int(row.get("매출(원)", 0)),
                    "iherb_sales_quantity": to_int(row.get("판매량", 0)),
                    "iherb_item_winner_ratio": to_float(
                        row.get("아이템위너 비율(%)", 0)
                    ),
                    "iherb_category": row.get("카테고리"),
                }
            )
        return features_data