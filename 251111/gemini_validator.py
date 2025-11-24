"""
썸네일 비교를 통한 제품 매칭 검증 시스템
아이허브와 쿠팡의 제품 썸네일을 수집하고 Gemini로 비교 검증
"""

import pandas as pd
import google.generativeai as genai
import time
import os
import sys
import requests
from typing import Optional, Tuple
from dataclasses import dataclass
from io import BytesIO

# 프로젝트 루트 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # iherb_price 디렉토리

# coupang 모듈 import를 위한 경로 추가
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# BrowserManager import
try:
    from coupang.coupang_manager import BrowserManager
    print("✓ BrowserManager 로드 성공")
except ImportError as e:
    print(f"✗ BrowserManager 로드 실패: {e}")
    print(f"프로젝트 루트: {project_root}")
    print("coupang/coupang_manager.py 경로를 확인하세요")
    sys.exit(1)

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

@dataclass
class ThumbnailValidationResult:
    """썸네일 검증 결과"""
    part_number: str
    iherb_thumbnail_url: str
    coupang_thumbnail_url: str
    is_same_product: bool
    confidence: str  # high/medium/low
    reason: str
    gemini_response: str


class ThumbnailCollector:
    """아이허브/쿠팡 썸네일 수집"""
    
    def __init__(self, browser_manager=None):
        """
        Args:
            browser_manager: 기존 BrowserManager 인스턴스 (필수!)
        """
        if not browser_manager:
            raise ValueError("browser_manager가 필요합니다. ThumbnailValidationSystem에 browser_manager를 전달하세요.")
        
        self.browser = browser_manager
        self.driver = self.browser.driver
    
    def get_iherb_thumbnail(self, part_number: str) -> Optional[str]:
        """
        아이허브 제품 페이지에서 썸네일 URL 추출
        
        Args:
            part_number: Part Number (예: LEX24076)
        
        Returns:
            썸네일 URL 또는 None
        """
        try:
            # 아이허브 URL 생성
            url = f"https://kr.iherb.com/pr/{part_number}"
            print(f"  아이허브 접속: {url}")
            
            self.driver.get(url)
            time.sleep(3)  # 페이지 로드 대기 증가
            
            # 여러 선택자 시도
            selectors = [
                ("ID", "iherb-product-image"),
                ("CSS_SELECTOR", "img.product-summary-image"),
                ("CSS_SELECTOR", "div.product-summary-image img"),
                ("XPATH", "//div[@class='product-summary-image']//img"),
            ]
            
            thumbnail_url = None
            
            for selector_type, selector_value in selectors:
                try:
                    if selector_type == "ID":
                        img_element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.ID, selector_value))
                        )
                    elif selector_type == "CSS_SELECTOR":
                        img_element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector_value))
                        )
                    elif selector_type == "XPATH":
                        img_element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, selector_value))
                        )
                    
                    # src 또는 data-src 속성 확인
                    thumbnail_url = img_element.get_attribute("src")
                    if not thumbnail_url or thumbnail_url.startswith("data:"):
                        thumbnail_url = img_element.get_attribute("data-src")
                    
                    if thumbnail_url and not thumbnail_url.startswith("data:"):
                        break
                    
                except Exception:
                    continue
            
            if thumbnail_url and not thumbnail_url.startswith("data:"):
                print(f"  ✓ 아이허브 썸네일: {thumbnail_url[:60]}...")
                return thumbnail_url
            else:
                print(f"  ✗ 아이허브 썸네일 없음 (유효한 이미지를 찾지 못함)")
                return None
                
        except Exception as e:
            print(f"  ✗ 아이허브 썸네일 수집 실패: {e}")
            return None
    
    def get_coupang_thumbnail(self, coupang_url: str) -> Optional[str]:
        """
        쿠팡 제품 페이지에서 썸네일 URL 추출
        
        Args:
            coupang_url: 쿠팡 제품 URL
        
        Returns:
            썸네일 URL 또는 None
        """
        try:
            print(f"  쿠팡 접속: {coupang_url[:60]}...")
            
            # BrowserManager 사용 (Coupang Referer 포함)
            self.browser.get_with_coupang_referrer(coupang_url)
            time.sleep(3)  # 페이지 로드 대기 증가
            
            # 여러 선택자 시도
            selectors = [
                "img[alt='Product image']",
                "img.twc-w-full",
                "div.prod-image__detail img",
                "div.prod-image img",
            ]
            
            thumbnail_url = None
            
            for selector in selectors:
                try:
                    img_element = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    
                    thumbnail_url = img_element.get_attribute("src")
                    
                    # // 로 시작하면 https: 추가
                    if thumbnail_url and thumbnail_url.startswith("//"):
                        thumbnail_url = "https:" + thumbnail_url
                    
                    if thumbnail_url:
                        break
                    
                except Exception:
                    continue
            
            if thumbnail_url:
                print(f"  ✓ 쿠팡 썸네일: {thumbnail_url[:60]}...")
                return thumbnail_url
            else:
                print(f"  ✗ 쿠팡 썸네일 없음 (유효한 이미지를 찾지 못함)")
                return None
                
        except Exception as e:
            print(f"  ✗ 쿠팡 썸네일 수집 실패: {e}")
            return None
    
    def close(self):
        """드라이버 종료 (BrowserManager가 관리하므로 여기서는 아무것도 안 함)"""
        pass


class ThumbnailValidator:
    """Gemini를 사용한 썸네일 비교 검증"""
    
    def __init__(self, api_key: str, model_name: str = "gemini-2.5-flash"):
        """
        Args:
            api_key: Gemini API 키
            model_name: 사용할 모델 (기본: gemini-2.0-flash)
        """
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)
        print(f"✓ Gemini 모델 초기화: {model_name}")
    
    def download_image(self, url: str) -> Optional[bytes]:
        """이미지 다운로드"""
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.content
            else:
                print(f"  ✗ 이미지 다운로드 실패 (HTTP {response.status_code})")
                return None
        except Exception as e:
            print(f"  ✗ 이미지 다운로드 오류: {e}")
            return None
    
    def compare_thumbnails(
        self,
        part_number: str,
        iherb_url: str,
        coupang_url: str
    ) -> ThumbnailValidationResult:
        """
        두 썸네일 이미지를 Gemini로 비교
        
        Args:
            part_number: Part Number
            iherb_url: 아이허브 썸네일 URL
            coupang_url: 쿠팡 썸네일 URL
        
        Returns:
            ThumbnailValidationResult
        """
        
        # 이미지 다운로드
        print(f"  이미지 다운로드 중...")
        iherb_image = self.download_image(iherb_url)
        coupang_image = self.download_image(coupang_url)
        
        if not iherb_image or not coupang_image:
            return ThumbnailValidationResult(
                part_number=part_number,
                iherb_thumbnail_url=iherb_url,
                coupang_thumbnail_url=coupang_url,
                is_same_product=False,
                confidence="low",
                reason="이미지 다운로드 실패",
                gemini_response=""
            )
        
        # Gemini로 비교
        prompt = """
다음 두 제품 이미지를 비교하여 동일한 제품인지 판단하세요.

**판단 기준:**
1. 제품 패키지 디자인이 동일한가?
2. 브랜드 로고가 동일한가?
3. 제품명/라벨이 유사한가?
4. 용기 형태가 동일한가?

**응답 형식 (JSON만 출력):**
{
    "is_same_product": true,
    "confidence": "high",
    "reason": "패키지 디자인, 브랜드 로고, 제품명이 모두 일치"
}

JSON만 출력하세요:
"""
        
        try:
            # Gemini API 호출 (이미지 2개 전송)
            from PIL import Image
            
            iherb_img = Image.open(BytesIO(iherb_image))
            coupang_img = Image.open(BytesIO(coupang_image))
            
            response = self.model.generate_content([
                "첫 번째 이미지 (아이허브):",
                iherb_img,
                "두 번째 이미지 (쿠팡):",
                coupang_img,
                prompt
            ])
            
            response_text = response.text.strip()
            
            # JSON 파싱
            if response_text.startswith("```json"):
                response_text = response_text.replace("```json", "").replace("```", "").strip()
            elif response_text.startswith("```"):
                response_text = response_text.replace("```", "").strip()
            
            import json
            result = json.loads(response_text)
            
            return ThumbnailValidationResult(
                part_number=part_number,
                iherb_thumbnail_url=iherb_url,
                coupang_thumbnail_url=coupang_url,
                is_same_product=result.get("is_same_product", False),
                confidence=result.get("confidence", "low"),
                reason=result.get("reason", ""),
                gemini_response=response_text
            )
            
        except Exception as e:
            print(f"  ✗ Gemini 비교 실패: {e}")
            return ThumbnailValidationResult(
                part_number=part_number,
                iherb_thumbnail_url=iherb_url,
                coupang_thumbnail_url=coupang_url,
                is_same_product=False,
                confidence="low",
                reason=f"Gemini 오류: {str(e)}",
                gemini_response=""
            )


class ThumbnailValidationSystem:
    """전체 썸네일 검증 시스템"""
    
    def __init__(
        self,
        gemini_api_key: str,
        browser_manager=None
    ):
        """
        Args:
            gemini_api_key: Gemini API 키
            browser_manager: 기존 BrowserManager (옵션)
        """
        self.collector = ThumbnailCollector(browser_manager)
        self.validator = ThumbnailValidator(gemini_api_key)
    
    def prepare_columns(self, input_csv: str, output_csv: str):
        """
        1단계: 컬럼 분리 및 초기 CSV 생성
        사유 컬럼을 파싱하여 정수일치/개수일치/브랜드일치 컬럼 추가
        """
        print(f"\n{'='*60}")
        print(f"1단계: 컬럼 분리 작업")
        print(f"{'='*60}")
        
        # 입력 CSV 로드
        try:
            df = pd.read_csv(input_csv, encoding='utf-8-sig')
            print(f"✓ 입력 데이터 로드: {len(df)}개 행")
        except Exception as e:
            print(f"✗ 입력 파일 로드 실패: {e}")
            return None
        
        # 새 컬럼 추가
        if '정수일치' not in df.columns:
            df['정수일치'] = None
        if '개수일치' not in df.columns:
            df['개수일치'] = None
        if '브랜드일치' not in df.columns:
            df['브랜드일치'] = None
        if '썸네일_일치여부' not in df.columns:
            df['썸네일_일치여부'] = None
        if '썸네일_신뢰도' not in df.columns:
            df['썸네일_신뢰도'] = None
        if '썸네일_판단근거' not in df.columns:
            df['썸네일_판단근거'] = None
        if '아이허브_썸네일URL' not in df.columns:
            df['아이허브_썸네일URL'] = None
        if '쿠팡_썸네일URL' not in df.columns:
            df['쿠팡_썸네일URL'] = None
        
        # 사유 컬럼 파싱 (있는 경우만)
        if '사유' in df.columns:
            print("\n사유 컬럼 파싱 중...")
            import re
            
            for idx, row in df.iterrows():
                # 이미 파싱된 경우 스킵
                if pd.notna(row.get('정수개수일치')) and row.get('정수개수일치') != '':
                    continue
                    
                reason = str(row.get('사유', ''))
                
                # 정수·개수 일치 파싱
                count_pattern = r'정수[^(]*\(([^)]+)\)'
                count_match = re.search(count_pattern, reason)
                if count_match:
                    df.at[idx, '정수개수일치'] = True
                    df.at[idx, '정수개수일치_상세'] = count_match.group(1)
                else:
                    df.at[idx, '정수개수일치'] = False
                    df.at[idx, '정수개수일치_상세'] = ''
                
                # 브랜드 일치 파싱
                brand_pattern = r'브랜드[^(]*\(([^)]+)\)'
                brand_match = re.search(brand_pattern, reason)
                if brand_match:
                    df.at[idx, '브랜드일치'] = True
                    df.at[idx, '브랜드일치_상세'] = brand_match.group(1)
                else:
                    df.at[idx, '브랜드일치'] = False
                    df.at[idx, '브랜드일치_상세'] = ''
            
            print("  ✓ 사유 컬럼 파싱 완료")
        
        # 컬럼 순서 정렬
        from utils import ColumnManager
        df = ColumnManager.reorder_columns(df)
        
        # 저장
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"✓ 컬럼 분리 완료: {output_csv}")
        print(f"{'='*60}\n")
        
        return df
    
    def validate_csv(
        self,
        input_csv: str,
        output_csv: str,
        delay_seconds: float = 3.0,
        skip_existing: bool = True
    ):
        """
        CSV 파일의 매칭 결과에 썸네일 검증 추가
        
        Args:
            input_csv: 입력 CSV (matching_results.csv)
            output_csv: 출력 CSV
            delay_seconds: 처리 간 대기 시간
            skip_existing: 기존 결과 건너뛰기
        """
        print(f"\n{'='*60}")
        print(f"썸네일 검증 시스템")
        print(f"{'='*60}")
        print(f"입력: {input_csv}")
        print(f"출력: {output_csv}")
        print(f"{'='*60}\n")
        
        # 출력 파일이 없으면 컬럼 분리 먼저 수행
        if not os.path.exists(output_csv):
            print("출력 파일이 없습니다. 컬럼 분리 작업을 먼저 수행합니다.\n")
            df = self.prepare_columns(input_csv, output_csv)
            if df is None:
                return
        else:
            # 기존 파일 로드
            try:
                df = pd.read_csv(output_csv, encoding='utf-8-sig')
                print(f"✓ 기존 결과 파일 로드: {len(df)}개 행")
            except Exception as e:
                print(f"✗ 파일 로드 실패: {e}")
                return
        
        # 이미 썸네일 검증이 완료된 Part Number 추출
        processed_parts = set()
        if '썸네일_일치여부' in df.columns:
            processed_parts = set(
                df[df['썸네일_일치여부'].notna()]['Part Number'].astype(str)
            )
            print(f"✓ 이미 처리 완료: {len(processed_parts)}개")
        
        # 매칭된 제품 중 아직 검증 안 된 제품만 필터링
        mask = (
            df['매칭제품명'].notna() & 
            (df['매칭제품명'] != '') &
            df['매칭URL'].notna() &
            (~df['Part Number'].astype(str).isin(processed_parts))
        )
        to_validate = df[mask]
        
        if len(to_validate) == 0:
            print("✓ 검증할 제품이 없습니다 (모두 처리 완료)")
            return
        
        print(f"✓ 검증 대상: {len(to_validate)}개\n")
        print(f"{'='*60}")
        print(f"2단계: 썸네일 검증 시작")
        print(f"{'='*60}\n")
        
        # 썸네일 검증
        success_count = 0
        fail_count = 0
        
        for i, (idx, row) in enumerate(to_validate.iterrows(), 1):
            part_number = str(row['Part Number'])
            coupang_url = row['매칭URL']
            
            print(f"[{i}/{len(to_validate)}] {part_number}")
            
            try:
                # 1. 아이허브 썸네일 수집
                iherb_thumbnail = self.collector.get_iherb_thumbnail(part_number)
                df.at[idx, '아이허브_썸네일URL'] = iherb_thumbnail
                
                if not iherb_thumbnail:
                    df.at[idx, '썸네일_일치여부'] = False
                    df.at[idx, '썸네일_신뢰도'] = 'low'
                    df.at[idx, '썸네일_판단근거'] = '아이허브 썸네일 수집 실패'
                    fail_count += 1
                    # 실시간 저장
                    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
                    print(f"  💾 저장 완료\n")
                    continue
                
                # 2. 쿠팡 썸네일 수집
                coupang_thumbnail = self.collector.get_coupang_thumbnail(coupang_url)
                df.at[idx, '쿠팡_썸네일URL'] = coupang_thumbnail
                
                if not coupang_thumbnail:
                    df.at[idx, '썸네일_일치여부'] = False
                    df.at[idx, '썸네일_신뢰도'] = 'low'
                    df.at[idx, '썸네일_판단근거'] = '쿠팡 썸네일 수집 실패'
                    fail_count += 1
                    # 실시간 저장
                    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
                    print(f"  💾 저장 완료\n")
                    continue
                
                # 3. Gemini로 비교
                print(f"  Gemini 비교 중...")
                validation = self.validator.compare_thumbnails(
                    part_number,
                    iherb_thumbnail,
                    coupang_thumbnail
                )
                
                # 결과 저장
                match_icon = "✓" if validation.is_same_product else "✗"
                print(f"  {match_icon} 썸네일 일치: {validation.is_same_product} ({validation.confidence})")
                print(f"     이유: {validation.reason}")
                
                df.at[idx, '썸네일_일치여부'] = validation.is_same_product
                df.at[idx, '썸네일_신뢰도'] = validation.confidence
                df.at[idx, '썸네일_판단근거'] = validation.reason
                
                if validation.is_same_product:
                    success_count += 1
                else:
                    fail_count += 1
                
            except Exception as e:
                print(f"  ✗ 오류 발생: {e}")
                df.at[idx, '썸네일_일치여부'] = False
                df.at[idx, '썸네일_신뢰도'] = 'low'
                df.at[idx, '썸네일_판단근거'] = f'오류: {str(e)}'
                fail_count += 1
            
            # 실시간 저장
            df.to_csv(output_csv, index=False, encoding='utf-8-sig')
            print(f"  💾 저장 완료 (진행: {i}/{len(to_validate)})\n")
            
            # 대기
            time.sleep(delay_seconds)
        
        # 최종 통계
        print(f"{'='*60}")
        print(f"썸네일 검증 완료!")
        print(f"{'='*60}")
        print(f"전체: {len(to_validate)}개")
        print(f"일치: {success_count}개 ({success_count/len(to_validate)*100:.1f}%)")
        print(f"불일치: {fail_count}개 ({fail_count/len(to_validate)*100:.1f}%)")
        print(f"{'='*60}\n")
        
        # 최종 컬럼 순서 정렬
        from utils import ColumnManager
        df = ColumnManager.reorder_columns(df)
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"✓ 최종 결과 (컬럼 순서 정렬): {output_csv}")
    
    def close(self):
        """리소스 정리"""
        self.collector.close()


# 실행 예시
if __name__ == "__main__":
    # Gemini API 키
    GEMINI_API_KEY = "AIzaSyAss0EgyPjm6yLAICEsgYRm_wNEbl34uWA"
    
    # 파일 경로
    INPUT_CSV = "/Users/brich/Desktop/iherb_price/coupang2/251111/outputs/matching_results.csv"
    OUTPUT_CSV = "/Users/brich/Desktop/iherb_price/coupang2/251111/outputs/matching_results_final.csv"
    
    # 브라우저 시작
    print("\n브라우저 시작 중...")
    browser = BrowserManager(headless=False)
    
    if not browser.start_driver():
        print("✗ 브라우저 시작 실패")
        sys.exit(1)
    
    # 검증 실행
    system = ThumbnailValidationSystem(GEMINI_API_KEY, browser)
    
    try:
        system.validate_csv(
            input_csv=INPUT_CSV,
            output_csv=OUTPUT_CSV,
            delay_seconds=3.0,
            skip_existing=True
        )
    finally:
        print("\n브라우저 종료 중...")
        browser.close()