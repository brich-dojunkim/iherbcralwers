#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
쿠팡 CSV UPC 매칭 자동화
UPC가 없는 상품을 아이허브와 자동 매칭하여 CSV 업데이트

개선사항:
1. 20개마다 브라우저 자동 재시작
2. 매칭 실패 시 'NOT_FOUND' 마커 저장
3. 재실행 시 NOT_FOUND는 재시도하지 않음 (빈 값만 재시도)
4. Gemini 쿼터 초과(429) 발생 시 즉시 중단하고 현재 진행 상황 저장
5. 재실행 시 쿼터 초과로 패스된 상품(코드 비어있고 비고에 쿼터 메모)은 자동 재시도
6. 매칭 성공 시 아이허브 상세 페이지에서 UPC 추출하여 'UPC (제품 바코드번호)' 컬럼에 저장
"""

import sys, io
import os
import time
import re
import pandas as pd
from selenium.webdriver.common.by import By
from contextlib import contextmanager

project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# === 외부 모듈 import ===
import google.generativeai as genai
from config import APIConfig

# iherbscraper의 개별 모듈들을 import
sys.path.insert(0, os.path.join(project_root, 'iherbscraper'))
from iherb_manager import BrowserManager
from iherb_client import IHerbClient
from product_matcher import ProductMatcher

@contextmanager
def _suppress_prints():
    """외부 라이브러리/매처가 과도하게 print하는 동안만 콘솔을 잠시 조용히."""
    _orig_stdout = sys.stdout
    try:
        sys.stdout = io.StringIO()
        yield
    finally:
        sys.stdout = _orig_stdout

# ==========================
# 예외 클래스
# ==========================
class QuotaExceeded(Exception):
    """Gemini API 쿼터 초과 시 런을 즉시 중단하기 위한 예외"""
    pass


class CoupangUPCMatcher:
    """쿠팡 CSV의 UPC 없는 상품을 아이허브와 매칭"""

    def __init__(self, csv_path: str, headless: bool = False):
        """
        Args:
            csv_path: 쿠팡 CSV 파일 경로
            headless: 브라우저 헤드리스 모드
        """
        self.csv_path = csv_path
        self.headless = headless

        # CSV 데이터
        self.df = None
        self.no_upc_rows = []

        # 브라우저 및 매칭 모듈
        self.browser = None
        self.iherb_client = None
        self.product_matcher = None

        # 초기화
        self._init_browser()

        # Gemini 번역 설정
        genai.configure(api_key=APIConfig.GEMINI_API_KEY)
        self.translation_model = genai.GenerativeModel(APIConfig.GEMINI_TEXT_MODEL)

        # 통계
        self.stats = {
            'total_no_upc': 0,
            'translated': 0,
            'matched': 0,
            'not_found': 0,
            'error': 0
        }

        # 브라우저 재시작 카운터
        self.match_count_since_restart = 0
        self.browser_restart_interval = 10  # 🔄 20개마다 재시작

    # --------------------------
    # 브라우저 초기화
    # --------------------------
    def _init_browser(self):
        """브라우저 초기화"""
        self.browser = BrowserManager(self.headless)
        self.iherb_client = IHerbClient(self.browser)
        self.product_matcher = ProductMatcher(self.iherb_client)
        # 아이허브 UI 영어로 고정
        try:
            self.iherb_client.set_language_to_english()
        except Exception:
            # 언어 설정 실패시에도 계속
            pass

    # --------------------------
    # CSV 로드
    # --------------------------
    def load_csv(self):
        """CSV 파일 로드 및 UPC 없는 행 필터링"""
        print(f"\n{'='*80}")
        print(f"📂 CSV 파일 로드")
        print(f"{'='*80}\n")

        try:
            # CSV 읽기 (utf-8-sig로 BOM 처리)
            self.df = pd.read_csv(self.csv_path, encoding='utf-8-sig')

            print(f"총 행 수: {len(self.df)}")
            print(f"컬럼 수: {len(self.df.columns)}")
            print(f"\n컬럼명:")
            for col in self.df.columns:
                print(f"  - {col}")

            # 필수 컬럼 이름
            upc_col = 'UPC (제품 바코드번호)'
            iherb_col = '아이허브파트넘버'

            # ✅ 필수 컬럼 존재 보장
            if iherb_col not in self.df.columns:
                self.df[iherb_col] = ''
            if '비고' not in self.df.columns:
                self.df['비고'] = ''

            # ✅ UPC 컬럼을 항상 문자열(object)로 보장 (FutureWarning 방지 & 선행 0 보존)
            if upc_col in self.df.columns:
                # 전부 문자열화
                self.df[upc_col] = self.df[upc_col].astype(str)
                # 'nan' 문자열은 빈값으로 교체
                self.df[upc_col] = self.df[upc_col].where(self.df[upc_col].str.strip().ne('nan'), '')
                # dtype을 object로 고정
                self.df[upc_col] = self.df[upc_col].astype(object)
            else:
                # 없으면 빈 문자열(object dtype) 컬럼 생성
                self.df[upc_col] = pd.Series([''] * len(self.df), dtype=object)

            # UPC가 비어있거나 NaN인 행 인덱스 수집
            self.no_upc_rows = self.df[
                self.df[upc_col].isna() |
                (self.df[upc_col].astype(str).str.strip() == '')
            ].index.tolist()

            self.stats['total_no_upc'] = len(self.no_upc_rows)

            # 이미 매칭 완료된 행 확인 (아이허브파트넘버가 값 있고 NOT_FOUND 아님)
            already_matched = []
            if iherb_col in self.df.columns:
                already_matched = [
                    idx for idx in self.no_upc_rows
                    if pd.notna(self.df.loc[idx, iherb_col]) and
                    str(self.df.loc[idx, iherb_col]).strip() != '' and
                    str(self.df.loc[idx, iherb_col]).strip().upper() != 'NOT_FOUND'
                ]

            print(f"\n✓ UPC 없는 상품: {len(self.no_upc_rows)}개")

            if already_matched:
                print(f"✓ 이미 매칭 완료: {len(already_matched)}개")
                self.stats['matched'] = len(already_matched)
                # 매칭 완료된 행은 제외
                self.no_upc_rows = [idx for idx in self.no_upc_rows if idx not in already_matched]
                print(f"✓ 남은 작업: {len(self.no_upc_rows)}개")

            if len(self.no_upc_rows) == 0:
                print("  모든 상품이 이미 매칭되었습니다!")
                return False

            # 샘플 출력
            print(f"\n샘플 (처음 3개):")
            sample_name_col = '제품명' if '제품명' in self.df.columns else None
            for idx in self.no_upc_rows[:3]:
                product_name = self.df.loc[idx, sample_name_col] if sample_name_col else '(제품명 없음)'
                print(f"  [{idx}] {product_name}")

            return True

        except Exception as e:
            print(f"❌ CSV 로드 실패: {e}")
            raise

    # --------------------------
    # 번역 (배치)
    # --------------------------
    def translate_batch(self, product_names: list) -> list:
        """상품명 배치 번역 (한글 → 영어). 쿼터 초과(429) 시 즉시 중단."""
        if not product_names:
            return []

        numbered_names = "\n".join([f"{i+1}. {name}" for i, name in enumerate(product_names)])
        prompt = f"""Translate these Korean product names to English.
Keep brand names unchanged. Keep product specifications (quantity, size, etc.) unchanged.
Answer with ONLY the translations, one per line.

{numbered_names}

Translations:"""

        try:
            response = self.translation_model.generate_content(
                prompt,
                generation_config={'temperature': 0.1, 'max_output_tokens': 500}
            )
            lines = response.text.strip().split('\n')
            translations = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if line[0].isdigit():
                    parts = line.split('.', 1)
                    if len(parts) > 1:
                        line = parts[1].strip()
                    else:
                        parts = line.split(')', 1)
                        if len(parts) > 1:
                            line = parts[1].strip()
                translations.append(line)

            while len(translations) < len(product_names):
                translations.append(product_names[len(translations)])

            return translations[:len(product_names)]

        except Exception as e:
            msg = str(e)
            # 🔴 쿼터 초과 감지 → 즉시 중단
            if "429" in msg or "quota" in msg.lower() or "GEMINI_QUOTA_EXCEEDED" in msg:
                raise QuotaExceeded(f"GEMINI_QUOTA_EXCEEDED: {msg}")
            # 그 외 실패는 배치만 스킵(원문 유지)
            print(f"  ❌ 번역 실패: {msg[:100]}")
            return product_names

    # --------------------------
    # 번역 (전체)
    # --------------------------
    def translate_all(self, batch_size: int = 10):
        """UPC 없는 모든 상품 번역 (이미 번역된 것은 스킵). 쿼터 초과 시 즉시 중단."""
        print(f"\n{'='*80}")
        print(f"📝 상품명 번역 시작")
        print(f"{'='*80}\n")

        if '제품명_영문' not in self.df.columns:
            self.df['제품명_영문'] = ''

        already_translated, needs_translation = [], []
        for idx in self.no_upc_rows:
            english_name = self.df.loc[idx, '제품명_영문']
            if pd.notna(english_name) and str(english_name).strip() != '':
                already_translated.append(idx)
            else:
                needs_translation.append(idx)

        if already_translated:
            print(f"✓ 이미 번역 완료: {len(already_translated)}개")
            self.stats['translated'] += len(already_translated)

        if not needs_translation:
            print("✓ 모든 상품이 이미 번역되었습니다!\n")
            return

        print(f"✓ 번역 필요: {len(needs_translation)}개\n")
        total = len(needs_translation)

        for i in range(0, total, batch_size):
            batch_indices = needs_translation[i:i + batch_size]
            # '제품명' 컬럼이 없을 경우 안전 처리
            batch_names = [
                self.df.loc[idx, '제품명'] if '제품명' in self.df.columns else ''
                for idx in batch_indices
            ]
            print(f"[배치 {i//batch_size + 1}] {i+1}~{min(i+batch_size, total)}/{total}")

            try:
                translations = self.translate_batch(batch_names)
            except QuotaExceeded as qe:
                # ⛔ 해당 배치 전부를 '쿼터 초과'로 메모하고 저장 후 즉시 중단
                for idx in batch_indices:
                    self.df.loc[idx, '비고'] = '오류: GEMINI_QUOTA_EXCEEDED'
                self.df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
                print(f"  ⛔ 번역 중단: {qe}")
                raise  # run()에서 처리

            for idx, translation in zip(batch_indices, translations):
                if translation and translation.strip():
                    self.df.loc[idx, '제품명_영문'] = translation
                    self.stats['translated'] += 1
                    print(f"  ✓ [{idx}] {translation[:50]}...")

            self.df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
            print()
            time.sleep(0.5)

        print(f"✅ 번역 완료: {self.stats['translated']}개 (누적)\n")

    # --------------------------
    # UPC 추출 (상세 페이지 DOM)
    # --------------------------
    def _extract_upc_from_page(self) -> str:
        """
        아이허브 상세 페이지에서 UPC 추출.
        전제: 현재 self.browser.driver가 해당 제품 상세 페이지를 로드한 상태.
        """
        try:
            elements = self.browser.driver.find_elements(
                By.XPATH, "//ul[@id='product-specs-list']//li[contains(text(), 'UPC')]"
            )
            for elem in elements:
                text = elem.text.strip()
                # 예: "UPC: 733739012345" 또는 "UPC(EAN): 733739012345"
                m = re.search(r'UPC[^:]*:\s*([0-9]{12,13})', text, re.IGNORECASE)
                if m:
                    upc = m.group(1)
                    if len(upc) in (12, 13):
                        print(f"  🔢 UPC 추출됨: {upc}")  # 👈 추가된 콘솔 로그
                        return upc
            return ""
        except Exception:
            return ""

    # --------------------------
    # 매칭(단일)
    # --------------------------
    def match_single_product(self, idx: int, english_name: str):
        """단일 상품 아이허브 매칭 (가격 기반 + UPC 수집). 쿼터 초과 시 즉시 중단."""
        print(f"[{idx}] 매칭 중: {english_name[:50]}...")

        try:
            product_url, similarity_score, match_details = \
                self.product_matcher.search_product_enhanced(
                    english_name,
                    coupang_product_id=None  # 이미지 비교 안 함
                )

            if product_url:
                # 아이허브 상세 정보 추출 (이 단계에서 상세 페이지 진입)
                product_code, iherb_name, price_info = \
                    self.iherb_client.extract_product_info_with_price(product_url)

                if product_code:
                    # ✅ UPC 추출 (상세 페이지 DOM에서)
                    upc = self._extract_upc_from_page()
                    if upc:
                        self.df.loc[idx, 'UPC (제품 바코드번호)'] = upc

                    # CSV 업데이트
                    self.df.loc[idx, '아이허브파트넘버'] = product_code
                    self.df.loc[idx, '비고'] = f"자동매칭: {iherb_name}"
                    self.df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')

                    self.stats['matched'] += 1
                    print(f"  ✅ 매칭 성공: {product_code} (저장됨)")
                    if isinstance(price_info, dict) and price_info.get('discount_price'):
                        try:
                            print(f"     아이허브: {int(price_info['discount_price']):,}원")
                        except Exception:
                            pass
                    return True

                else:
                    # URL은 있었으나 코드 추출 실패
                    print(f"  ⚠️ 상품코드 추출 실패")
                    self.df.loc[idx, '아이허브파트넘버'] = 'NOT_FOUND'
                    self.df.loc[idx, '비고'] = '매칭 실패: 상품코드 추출 실패'
                    self.df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
                    self.stats['not_found'] += 1
                    return False

            else:
                # 매칭 실패
                reason = match_details.get('reason', 'unknown') if isinstance(match_details, dict) else 'unknown'
                # 내부에서 reason으로 쿼터 신호가 넘어올 수도 있으므로 체크
                if 'GEMINI_QUOTA_EXCEEDED' in reason or '429' in reason or 'quota' in reason.lower():
                    self.df.loc[idx, '비고'] = '오류: GEMINI_QUOTA_EXCEEDED'
                    self.df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
                    raise QuotaExceeded(f"GEMINI_QUOTA_EXCEEDED (reason): {reason}")

                print(f"  ❌ 매칭 실패: {reason}")
                self.df.loc[idx, '아이허브파트넘버'] = 'NOT_FOUND'
                self.df.loc[idx, '비고'] = f'매칭 실패: {reason}'
                self.df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
                self.stats['not_found'] += 1
                return False

        except Exception as e:
            msg = str(e)
            # 🔴 예외 메시지 기반 쿼터 초과 감지 → 즉시 중단
            if "429" in msg or "quota" in msg.lower() or "GEMINI_QUOTA_EXCEEDED" in msg:
                self.df.loc[idx, '비고'] = '오류: GEMINI_QUOTA_EXCEEDED'
                self.df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
                print(f"  ⛔ 쿼터 초과로 중단: {msg[:100]}")
                raise QuotaExceeded(f"GEMINI_QUOTA_EXCEEDED: {msg}")

            print(f"  💥 오류: {msg[:100]}...")
            self.df.loc[idx, '비고'] = f'오류: {msg[:100]}'
            self.df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
            self.stats['error'] += 1
            return False

    # --------------------------
    # 매칭(전체)
    # --------------------------
    def match_all(self):
        """UPC 없는 모든 상품 매칭 (NOT_FOUND는 재시도하지 않음, 빈 값만 대상)"""
        print(f"\n{'='*80}")
        print(f"🔍 아이허브 매칭 시작")
        print(f"{'='*80}\n")

        # 매칭 대상: 아이허브파트넘버가 '빈 값'인 행만 (NOT_FOUND는 제외)
        needs_matching = []
        for idx in self.no_upc_rows:
            iherb_code = self.df.loc[idx, '아이허브파트넘버'] if '아이허브파트넘버' in self.df.columns else None
            code_str = '' if pd.isna(iherb_code) else str(iherb_code).strip().upper()
            if code_str == 'NOT_FOUND':
                continue  # 재시도 X
            if code_str == '':
                needs_matching.append(idx)

        if not needs_matching:
            print("✓ 매칭할 대상이 없습니다. (NOT_FOUND 제외, 빈 값 없음)\n")
            return

        # (선택) 쿼터 초과로 표기된 행을 먼저 처리
        quota_first, others = [], []
        for idx in needs_matching:
            note = '' if pd.isna(self.df.loc[idx, '비고']) else str(self.df.loc[idx, '비고'])
            if 'GEMINI_QUOTA_EXCEEDED' in note or '429' in note or ('quota' in note.lower()):
                quota_first.append(idx)
            else:
                others.append(idx)
        needs_matching = quota_first + others

        print(f"✓ 매칭 필요: {len(needs_matching)}개\n")
        total = len(needs_matching)

        for i, idx in enumerate(needs_matching, 1):
            english_name = self.df.loc[idx, '제품명_영문'] if '제품명_영문' in self.df.columns else ''
            if pd.isna(english_name) or str(english_name).strip() == '':
                print(f"\n[{i}/{total}] [{idx}] ⚠️ 번역되지 않음 - 스킵")
                continue

            print(f"\n[{i}/{total}] ", end='')
            self.match_single_product(idx, english_name)

            # 🔄 브라우저 재시작 체크
            self.match_count_since_restart += 1
            if self.match_count_since_restart >= self.browser_restart_interval:
                self.restart_browser()
                self.match_count_since_restart = 0

            # 진행률 출력 (10개마다 혹은 마지막)
            if i % 10 == 0 or i == total:
                print(f"\n{'─'*60}")
                print(f"📈 진행률: {i}/{total} ({i/total*100:.1f}%)")
                print(f"  ✅ 매칭: {self.stats['matched']}개")
                print(f"  ❌ 실패(NOT_FOUND): {self.stats['not_found']}개")
                print(f"  💥 오류: {self.stats['error']}개")
                print(f"{'─'*60}\n")

    # --------------------------
    # 브라우저 재시작
    # --------------------------
    def restart_browser(self):
        """브라우저 재시작"""
        print(f"\n{'─'*60}")
        print(f"🔄 브라우저 재시작 중...")
        print(f"{'─'*60}\n")

        try:
            # 기존 브라우저 종료
            self.browser.close()
            time.sleep(2)
            # 새 브라우저 시작
            self._init_browser()
            print(f"✅ 브라우저 재시작 완료\n")
        except Exception as e:
            print(f"❌ 브라우저 재시작 실패: {e}")
            print(f"⚠️ 기존 브라우저로 계속 진행합니다.\n")

    # --------------------------
    # 저장
    # --------------------------
    def save_csv(self, final: bool = False):
        """CSV 저장 (오버라이드)

        Args:
            final: True면 제품명_영문 컬럼 제거, False면 유지
        """
        print(f"\n{'='*80}")
        print(f"💾 CSV 저장")
        print(f"{'='*80}\n")

        try:
            # 최종 저장 시에만 임시 컬럼 제거
            if final and '제품명_영문' in self.df.columns:
                print("  임시 컬럼(제품명_영문) 제거 중...")
                self.df = self.df.drop(columns=['제품명_영문'])

            # 원본 파일에 덮어쓰기
            self.df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')

            print(f"✅ 저장 완료: {self.csv_path}")
            print(f"  총 행 수: {len(self.df)}")
            print(f"  업데이트된 행: {self.stats['matched']}개")

            if not final:
                print(f"  📝 중간 저장 (재실행 시 이어서 진행 가능)")

        except Exception as e:
            print(f"❌ 저장 실패: {e}")
            raise

    # --------------------------
    # 요약 출력
    # --------------------------
    def print_summary(self):
        """최종 결과 요약"""
        print(f"\n{'='*80}")
        print(f"📊 최종 결과 요약")
        print(f"{'='*80}")
        print(f"UPC 없는 상품: {self.stats['total_no_upc']}개")
        print(f"번역 완료: {self.stats['translated']}개")
        print(f"✅ 매칭 성공: {self.stats['matched']}개", end='')
        if self.stats['total_no_upc'] > 0:
            print(f" ({self.stats['matched']/self.stats['total_no_upc']*100:.1f}%)")
        else:
            print()
        print(f"❌ 매칭 실패: {self.stats['not_found']}개 (NOT_FOUND로 표시)")
        print(f"💥 오류 발생: {self.stats['error']}개 (재시도 가능)")
        print(f"\n💾 결과 저장 위치:")
        print(f"   {self.csv_path}")
        print(f"\n📝 중간 저장된 데이터:")
        print(f"   - 번역: '제품명_영문' 컬럼에 저장됨")
        print(f"   - 매칭 성공: '아이허브파트넘버' 컬럼에 코드 저장")
        print(f"   - 매칭 실패: '아이허브파트넘버'에 'NOT_FOUND' 표시")
        print(f"   - 오류: 빈 칸으로 남음 (재시도 대상)")
        print(f"\n💡 재실행 시:")
        print(f"   - 성공한 항목: 건드리지 않음")
        print(f"   - NOT_FOUND: 재시도하지 않음")
        print(f"   - 오류/쿼터 초과 항목: 자동 재시도 대상")

    # --------------------------
    # 실행 전체
    # --------------------------
    def run(self):
        """전체 프로세스 실행"""
        try:
            # 1) CSV 로드
            if not self.load_csv():
                return

            # 2) 번역
            self.translate_all(batch_size=10)

            # 3) 매칭
            self.match_all()

            # 4) 최종 저장 (임시 컬럼 제거)
            self.save_csv(final=True)

            # 5) 결과 요약
            self.print_summary()

        except QuotaExceeded as qe:
            print("\n\n⛔ GEMINI 쿼터 초과로 실행을 중단합니다.")
            print(f"사유: {qe}")
            # 쿼터 초과는 '오류'로 남겨두고 재실행 시 자동 재시도 대상(코드가 비어있음)
            self.save_csv(final=False)
            self.print_summary()
            return

        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자 중단")
            print("현재까지 진행된 결과가 이미 저장되었습니다.")
            print("재실행하면 중단된 지점부터 이어서 진행됩니다.")
            self.print_summary()

        except Exception as e:
            print(f"\n\n💥 실행 오류: {e}")
            import traceback
            traceback.print_exc()

        finally:
            self.close()

    # --------------------------
    # 종료
    # --------------------------
    def close(self):
        """브라우저 종료"""
        if self.browser:
            try:
                self.browser.close()
                print("  브라우저 종료 ✓")
            except Exception:
                pass


# ==========================
# 메인
# ==========================
def main():
    """메인 실행"""
    print("="*80)
    print("🚀 쿠팡 CSV UPC 자동 매칭 시작")
    print("="*80)

    # CSV 파일 경로 (스크립트와 같은 디렉토리)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_filename = "2025 쿠팡 로켓 가격 대응.csv"
    csv_path = os.path.join(script_dir, csv_filename)

    # 파일 존재 확인
    if not os.path.exists(csv_path):
        print(f"❌ 파일을 찾을 수 없습니다: {csv_filename}")
        print(f"   찾은 경로: {csv_path}")
        print(f"\n💡 CSV 파일을 스크립트와 같은 폴더에 넣어주세요:")
        print(f"   {script_dir}/")
        return

    print(f"✓ CSV 파일 발견: {csv_filename}\n")

    # 매칭 실행
    matcher = CoupangUPCMatcher(
        csv_path=csv_path,
        headless=False  # 브라우저 보이기 (디버깅용)
    )

    matcher.run()


if __name__ == "__main__":
    main()
