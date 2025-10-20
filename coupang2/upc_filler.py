#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
파트넘버로 아이허브 크롤링하여 UPC 채우기 (보정 우선 + 재개 + 주기저장 + 재시작)
- 기존 UPC 전량 보정(12자리, 앞쪽 0 패딩) → 이후 크롤링
- 10개마다 브라우저 리부트, SAVE_EVERY마다 중간 저장
- 중단 후 이어하기(_with_upc.csv + 체크포인트)
"""

import sys
import os
import re
import json
import time
import sqlite3
import pandas as pd
from datetime import datetime

# ===== 설정값 =====
RESTART_EVERY = 10          # 몇 건마다 브라우저 재시작
SAVE_EVERY = 1              # 몇 건마다 임시 저장
SLEEP_BETWEEN = (2, 4)      # 요청 간 랜덤 대기 (초)

# 프로젝트 루트 경로
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# iherbscraper 모듈 경로
iherbscraper_path = os.path.join(project_root, 'iherbscraper')
sys.path.insert(0, iherbscraper_path)

# import (상대 경로 이슈 회피)
from iherb_manager import BrowserManager
from iherb_client import IHerbClient
from iherb_config import IHerbConfig


class UPCFillerFromPartNumber:
    """파트넘버로 UPC 수집 및 CSV/DB 업데이트"""

    def __init__(self, csv_path="coupang_iherb_products_updated.csv",
                 db_path="improved_monitoring.db",
                 headless=True):
        self.csv_path = csv_path
        self.db_path = db_path
        self.headless = headless

        # 산출 파일/체크포인트
        self.output_path = self.csv_path.replace('.csv', '_with_upc.csv')
        self.checkpoint_path = self.csv_path.replace('.csv', '_upc_resume.json')

        # 통계
        self.total_count = 0
        self.success_count = 0
        self.failed_count = 0
        self._since_restart = 0

        print("\n" + "="*80)
        print("🔧 브라우저 초기화")
        print("="*80)
        self._init_browser()

    # ──────────────────────────────────────────────────────────────
    # 브라우저
    # ──────────────────────────────────────────────────────────────
    def _init_browser(self):
        self.browser = BrowserManager(headless=self.headless)
        self.iherb_client = IHerbClient(self.browser)
        try:
            self.iherb_client.set_language_to_english()
        except Exception as e:
            print(f"⚠️ 언어 설정 경고: {e}")

    def _restart_browser_if_needed(self, force=False):
        if force or self._since_restart >= RESTART_EVERY:
            print("\n" + "-"*80)
            print(f"♻️ 브라우저 재시작 (처리 {self._since_restart}건)")
            print("-"*80)
            try:
                self.browser.close()
            except Exception:
                pass
            time.sleep(1)
            self._init_browser()
            self._since_restart = 0

    # ──────────────────────────────────────────────────────────────
    # UPC 보정 유틸: 항상 12자리 만들기(가능한 경우)
    # ──────────────────────────────────────────────────────────────
    @staticmethod
    def _digits_only(s: str) -> str:
        return re.sub(r"\D", "", s or "")

    @staticmethod
    def _upc_check_digit(d11: str) -> str:
        """UPC-A 체크디짓 계산(11자리 입력)"""
        # 홀수 자리 합*3 + 짝수 자리 합 → mod 10 → 보정
        odds = sum(int(d11[i]) for i in range(0, 11, 2))
        evens = sum(int(d11[i]) for i in range(1, 11, 2))
        total = odds * 3 + evens
        return str((10 - (total % 10)) % 10)

    @staticmethod
    def _is_valid_upc12(d12: str) -> bool:
        if len(d12) != 12 or not d12.isdigit():
            return False
        chk = UPCFillerFromPartNumber._upc_check_digit(d12[:11])
        return d12[-1] == chk

    def _normalize_upc_12(self, raw: str) -> str:
        """
        보정 규칙(빈칸 금지, 가능한 한 12자리로 반환):
        - 입력 비어있으면 그대로 빈문자("") 반환 (크롤링 대상으로 남김)
        - 숫자만 남긴 뒤 길이별 처리:
          12: 그대로
          11: 앞쪽 0 패딩 → 12
          13: 맨 앞이 '0'이면 제거 → 12, 아니면 후보1=뒤 12, 후보2=앞 11 + 체크디짓
          14: 뒤 12
          <11: 앞쪽 0 패딩하여 12
          >14: 뒤 12
        - 12자 후보가 여러 개면, UPC-A 체크디짓가 유효한 후보를 우선 채택
        - 어떠한 경우에도 12자 반환을 시도; 불가 시 "" (극히 드묾)
        """
        if raw is None:
            return ""
        s = str(raw).strip()
        if s.lower() == "nan" or s == "":
            return ""

        d = self._digits_only(s)
        if not d:
            return ""

        # 빠른 경로
        if len(d) == 12:
            return d
        if len(d) == 11:
            return d.zfill(12)
        if len(d) == 13:
            cand = []
            if d.startswith("0"):
                cand.append(d[1:])              # 0 + UPC-A → UPC-A
            cand.append(d[-12:])                # EAN13의 뒤 12자
            # 앞 11자 + 계산된 체크디짓(이건 거의 EAN 전개와 안 맞을 수 있으나 보정 시도)
            cand.append(d[:11] + self._upc_check_digit(d[:11]))
            # 유효성 우선
            for c in cand:
                if len(c) == 12 and c.isdigit() and self._is_valid_upc12(c):
                    return c
            # 아무거나 12자면 반환
            for c in cand:
                if len(c) == 12 and c.isdigit():
                    return c
            return d[-12:]  # 최후 수단
        if len(d) == 14:
            c = d[-12:]
            # 유효하면 사용, 아니어도 사용(데이터 일관 위해)
            return c
        if len(d) < 11:
            return d.zfill(12)
        if len(d) > 14:
            return d[-12:]

        # 여기로 오긴 어려움. 방어적으로 처리
        if len(d) >= 12:
            return d[:12]
        return d.zfill(12)

    # ──────────────────────────────────────────────────────────────
    # 데이터 로딩 + 기존 UPC 전량 보정
    # ──────────────────────────────────────────────────────────────
    def load_data(self):
        print("\n" + "="*80)
        print("📂 데이터 로딩 + 기존 UPC 보정")
        print("="*80)

        source_path = self.output_path if os.path.exists(self.output_path) else self.csv_path
        if source_path == self.output_path:
            print(f"⏩ 이어하기 모드: {self.output_path} 로드")
        else:
            print(f"🆕 신규 실행: {self.csv_path} 로드")

        df = pd.read_csv(source_path, dtype=str, encoding='utf-8-sig')
        print(f"총 레코드: {len(df)}개")

        # product_id 없으면 생성
        if 'product_id' not in df.columns:
            df['product_id'] = df['쿠팡_상품URL'].str.extract(r'itemId=(\d+)')

        # 기존 UPC 일괄 보정 (빈칸 만들지 않음)
        if "아이허브_UPC" in df.columns:
            before = df['아이허브_UPC'].copy()
            df['아이허브_UPC'] = df['아이허브_UPC'].apply(self._normalize_upc_12)
            # 통계
            def lens(x):
                return len(re.sub(r"\D", "", x)) if x else 0
            before_valid_12 = before.fillna("").apply(lambda x: lens(str(x)) == 12).sum()
            after_valid_12 = df['아이허브_UPC'].apply(lambda x: lens(str(x)) == 12).sum()
            print(f"  ▶ 기존 UPC 보정 완료 (유효 12자리: {before_valid_12} → {after_valid_12})")
        else:
            # 컬럼이 없다면 생성
            df['아이허브_UPC'] = ""

        # 크롤링 대상 필터: 아직 빈칸("")인 것 + 파트넘버 있는 것
        no_upc = (df['아이허브_UPC'].astype(str).str.strip() == "")
        has_part = df['아이허브_파트넘버'].notna() & df['아이허브_파트넘버'].astype(str).str.strip().ne("") & (df['아이허브_파트넘버'].astype(str).str.lower() != "nan")
        target_df = df[no_upc & has_part].copy()

        print(f"\n필터링 결과:")
        print(f"  UPC 비어있음: {no_upc.sum()}개")
        print(f"  파트넘버 있음: {has_part.sum()}개")
        print(f"  → 크롤링 대상: {len(target_df)}개")

        if len(target_df) == 0:
            print("\n✓ 크롤링할 상품이 없습니다.")
            return df, None
        return df, target_df

    # ──────────────────────────────────────────────────────────────
    # UPC 추출(페이지 → 보정)
    # ──────────────────────────────────────────────────────────────
    def extract_upc_from_partnumber(self, part_number):
        if not part_number or pd.isna(part_number) or str(part_number).strip() == "":
            return ""

        try:
            url = f"https://www.iherb.com/pr/{part_number}"
            print(f"    🔗 URL: {url}")

            if not self.browser.safe_get(url):
                print(f"    ❌ 페이지 로딩 실패")
                return ""

            time.sleep(2)
            html = self.browser.driver.page_source

            # 여러 패턴 시도(11~14자리까지 수용)
            patterns = [
                r'<li>\s*UPC:\s*<span>(\d{11,14})</span>\s*</li>',
                r'UPC:\s*<span[^>]*>(\d{11,14})</span>',
                r'UPC[:\s]+(\d{11,14})',
                r'"gtin13"\s*:\s*"(\d{11,14})"',
                r'<meta[^>]*property=["\']product:upc["\'][^>]*content=["\'](\d{11,14})["\']',
            ]
            for pat in patterns:
                m = re.search(pat, html, re.IGNORECASE)
                if m:
                    raw = m.group(1)
                    upc12 = self._normalize_upc_12(raw)
                    if upc12:
                        print(f"    ✅ UPC 발견(보정 후 12자리): {upc12}")
                        return upc12

            print(f"    ❌ UPC 없음")
            return ""
        except Exception as e:
            print(f"    ❌ 오류: {e}")
            return ""

    # ──────────────────────────────────────────────────────────────
    # 체크포인트
    # ──────────────────────────────────────────────────────────────
    def _load_checkpoint(self):
        if not os.path.exists(self.checkpoint_path):
            return set()
        try:
            with open(self.checkpoint_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return set(data.get('processed_indices', []))
        except Exception:
            return set()

    def _append_checkpoint(self, idx):
        try:
            data = {'processed_indices': []}
            if os.path.exists(self.checkpoint_path):
                with open(self.checkpoint_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            data.setdefault('processed_indices', [])
            data['processed_indices'].append(str(idx))
            # 중복 제거
            data['processed_indices'] = list(dict.fromkeys(data['processed_indices']))
            with open(self.checkpoint_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ 체크포인트 저장 경고: {e}")

    # ──────────────────────────────────────────────────────────────
    # 실행
    # ──────────────────────────────────────────────────────────────
    def crawl_and_update(self):
        full_df, target_df = self.load_data()
        if target_df is None:
            self._update_database(full_df)
            return

        self.total_count = len(target_df)
        targets = list(target_df.index)
        processed = self._load_checkpoint()

        import random
        for i, idx in enumerate(targets, start=1):
            if processed and str(idx) in processed:
                continue

            row = full_df.loc[idx]
            part = str(row['아이허브_파트넘버']).strip()
            name = str(row.get('쿠팡_제품명', ''))[:50]

            print(f"\n[{i}/{self.total_count}] {name}")
            print(f"  📦 파트넘버: {part}")

            self._restart_browser_if_needed()

            upc12 = self.extract_upc_from_partnumber(part)
            if upc12:
                full_df.at[idx, '아이허브_UPC'] = upc12
                self.success_count += 1
            else:
                # 보정 불가/미발견이어도 빈칸 유지(다음 회차 재시도 가능)
                self.failed_count += 1

            self._since_restart += 1
            self._append_checkpoint(idx)

            if i % SAVE_EVERY == 0:
                self._save_progress(full_df)

            if i % RESTART_EVERY == 0:
                self._restart_browser_if_needed(force=True)

            time.sleep(random.uniform(*SLEEP_BETWEEN))

        self._print_summary()
        self._save_progress(full_df, final=True)
        self._update_database(full_df)

    # ──────────────────────────────────────────────────────────────
    # 저장/출력/DB
    # ──────────────────────────────────────────────────────────────
    def _save_progress(self, df, final=False):
        print("\n" + "="*80)
        print("💾 CSV 저장" + (" (최종)" if final else " (중간)"))
        print("="*80)

        if final and os.path.exists(self.csv_path):
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = self.csv_path.replace('.csv', f'_backup_{ts}.csv')
            import shutil
            shutil.copy2(self.csv_path, backup_path)
            print(f"✅ 원본 백업 완료: {backup_path}")

        df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        print(f"✅ 저장 완료: {self.output_path}")

    def _update_database(self, df):
        print("\n" + "="*80)
        print("🗄️ DB 업데이트")
        print("="*80)
        if not os.path.exists(self.db_path):
            print(f"⚠️ DB 파일 없음: {self.db_path}")
            return
        try:
            conn = sqlite3.connect(self.db_path)
            # 백업
            conn.execute("DROP TABLE IF EXISTS matching_reference_backup")
            conn.execute("""
                CREATE TABLE matching_reference_backup AS 
                SELECT * FROM matching_reference
            """)
            print("✅ DB 백업 완료")

            updated = 0
            for _, row in df.iterrows():
                pid = str(row.get('product_id') or "").strip()
                upc = str(row.get('아이허브_UPC') or "").strip()
                if pid and upc:
                    conn.execute("""
                        UPDATE matching_reference
                        SET iherb_upc = ?
                        WHERE coupang_product_id = ?
                    """, (upc, pid))
                    updated += 1
            conn.commit()
            print(f"✅ DB 업데이트 완료: {updated}건")
        except Exception as e:
            print(f"❌ DB 업데이트 실패: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _print_summary(self):
        print("\n" + "="*80)
        print("📊 크롤링 완료 요약")
        print("="*80)
        total = self.success_count + self.failed_count
        print(f"총 시도: {total}, 성공: {self.success_count}, 실패: {self.failed_count}")
        if total > 0:
            print(f"성공률: {self.success_count/total*100:.1f}%")

    def close(self):
        try:
            self.browser.close()
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*80)
    print("🚀 파트넘버로 UPC 크롤링 시작 (보정 → 크롤링 → 저장 → DB)")
    print("="*80)

    csv_path = "coupang_iherb_products_updated.csv"
    if not os.path.exists(csv_path) and not os.path.exists(csv_path.replace('.csv', '_with_upc.csv')):
        print(f"\n❌ CSV 파일이 없습니다: {csv_path}")
        return

    filler = UPCFillerFromPartNumber(csv_path, "improved_monitoring.db", headless=False)
    try:
        filler.crawl_and_update()
    except KeyboardInterrupt:
        print("\n⚠️ 사용자 중단 → 진행 상황 저장 중...")
        try:
            full_df, _ = filler.load_data()
            filler._save_progress(full_df)
        except Exception as e:
            print(f"⚠️ 중단 저장 경고: {e}")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        try:
            full_df, _ = filler.load_data()
            filler._save_progress(full_df)
        except Exception as e2:
            print(f"⚠️ 예외 후 저장 경고: {e2}")
    finally:
        filler.close()
        print("\n✅ 작업 종료")


if __name__ == "__main__":
    main()
