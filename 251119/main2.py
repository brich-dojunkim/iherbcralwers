#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
식품안전나라와 아이허브 제품 이미지 비교 시스템 (undetected-chromedriver 버전)
- 상품 1개당 브라우저 1개 (위해식품 + 아이허브 공용)
- 아이허브: 클릭 없이 DOM 구조(data-large-img, product-summary-image)만으로 이미지 URL 추출
- CSV 단일 파일로 상태 관리
- Alert 자동 처리 및 재시도 로직 추가
"""

import os
import sys
import time
import io
from typing import List, Dict, Optional, Set

import pandas as pd
import requests
from PIL import Image
import google.generativeai as genai

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, UnexpectedAlertPresentException


class ProductImageComparator:
    def __init__(self, gemini_api_key: str, headless: bool = False):
        """
        초기화
        Args:
            gemini_api_key: Google Gemini API 키
            headless: False면 브라우저 표시
        """
        self.gemini_api_key = gemini_api_key
        genai.configure(api_key=gemini_api_key)
        self.model = genai.GenerativeModel("gemini-2.0-flash")

        # driver는 상품마다 새로 만들기 위해 옵션만 저장
        self.headless = headless

        # 기본 URL
        self.foodsafety_base_url = "https://www.foodsafetykorea.go.kr"
        self.iherb_base_url = "https://kr.iherb.com"

        # 결과 CSV 파일 (현재 디렉토리)
        self.csv_output = "comparison_results.csv"

    # ──────────────────────────────────────────────
    # undetected-chromedriver 드라이버 생성
    # ──────────────────────────────────────────────
    def _create_driver(self) -> uc.Chrome:
        """상품 1개 처리용 undetected-chromedriver 생성"""
        options = uc.ChromeOptions()
        
        if self.headless:
            options.add_argument("--headless=new")
        
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        # undetected-chromedriver는 자동으로 탐지 우회 설정
        driver = uc.Chrome(
            options=options,
            version_main=None,  # 자동으로 Chrome 버전 탐지
            use_subprocess=True
        )
        
        return driver

    def _handle_alert(self, driver: uc.Chrome) -> bool:
        """Alert 처리 (있으면 Accept하고 True 반환)"""
        try:
            alert = driver.switch_to.alert
            alert_text = alert.text
            print(f"    ⚠ Alert 감지: {alert_text}")
            alert.accept()
            time.sleep(1)
            return True
        except Exception:
            return False

    # ──────────────────────────────────────────────
    # 진행 상태 관련 (CSV 기반)
    # ──────────────────────────────────────────────
    def load_completed_partnos(self) -> Set[str]:
        """CSV에서 이미 처리 완료된 제품 코드 목록 로드"""
        if not os.path.exists(self.csv_output):
            return set()
        
        try:
            df = pd.read_csv(self.csv_output, encoding="utf-8-sig")
            # status가 completed, error, foodsafety_not_found, iherb_not_found인 것들만
            completed = df[df["status"].isin([
                "completed", "error", "foodsafety_not_found", "iherb_not_found"
            ])]["product_partno"].tolist()
            return set(completed)
        except Exception as e:
            print(f"⚠ CSV 로드 오류: {e}")
            return set()

    def clear_progress(self):
        """결과 CSV 파일 삭제"""
        if os.path.exists(self.csv_output):
            os.remove(self.csv_output)
            print(f"✓ 결과 파일 삭제: {self.csv_output}")

    def load_csv_data(self, csv_path: str) -> pd.DataFrame:
        """입력 CSV 파일 로드"""
        df = pd.read_csv(csv_path, encoding="utf-8")
        print(f"✓ CSV 로드 완료: {len(df)} 건")
        return df

    # ──────────────────────────────────────────────
    # 식품안전나라 (위해식품)
    # ──────────────────────────────────────────────
    def search_foodsafety_product(
        self, driver: uc.Chrome, product_name: str, max_retries: int = 3
    ) -> Optional[Dict]:
        """
        식품안전나라에서 제품 검색 (재시도 로직 포함)
        """
        for attempt in range(max_retries):
            try:
                search_url = (
                    "https://www.foodsafetykorea.go.kr/portal/fooddanger/"
                    "foodDirectImportBlock.do?menu_grp=MENU_NEW02&menu_no=3594"
                )
                driver.get(search_url)
                
                # Alert 처리
                time.sleep(1)
                self._handle_alert(driver)

                wait = WebDriverWait(driver, 10)
                search_input = wait.until(
                    EC.presence_of_element_located((By.ID, "search_keyword"))
                )
                search_input.clear()
                search_input.send_keys(product_name)

                # 검색 실행
                search_btn = driver.find_element(
                    By.CSS_SELECTOR,
                    "a.btn.btn-default[href='javascript:setDefault();']",
                )
                search_btn.click()

                time.sleep(2)
                self._handle_alert(driver)

                # 첫 번째 결과 클릭
                try:
                    first_result = wait.until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#listFrame tr a"))
                    )
                    first_result.click()
                    time.sleep(2)
                    self._handle_alert(driver)

                    # 페이지 끝까지 스크롤(이미지 로딩 보장용)
                    last_height = driver.execute_script(
                        "return document.body.scrollHeight"
                    )
                    while True:
                        driver.execute_script(
                            "window.scrollTo(0, document.body.scrollHeight);"
                        )
                        time.sleep(1)
                        new_height = driver.execute_script(
                            "return document.body.scrollHeight"
                        )
                        if new_height == last_height:
                            break
                        last_height = new_height

                    # 다시 위로
                    driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(1)

                    # 이미지 URL 추출
                    image_elements = driver.find_elements(
                        By.CSS_SELECTOR, "img[alt*='위해식품']"
                    )
                    image_urls: List[str] = []
                    for img in image_elements:
                        img_src = img.get_attribute("src")
                        if img_src and "commonfileView.do" in img_src:
                            if img_src not in image_urls:
                                image_urls.append(img_src)

                    detail_url = driver.current_url
                    print(f"  ✓ 식품안전나라: {len(image_urls)}개 이미지 발견")

                    return {
                        "product_name": product_name,
                        "detail_url": detail_url,
                        "image_urls": image_urls,
                    }

                except Exception:
                    print(f"  ✗ 식품안전나라에서 '{product_name}' 검색 결과 없음")
                    return None

            except UnexpectedAlertPresentException:
                print(f"  ⚠ Alert 발생 (시도 {attempt + 1}/{max_retries})")
                self._handle_alert(driver)
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    print(f"  ✗ 최대 재시도 초과")
                    return None
                    
            except Exception as e:
                print(f"  ✗ 식품안전나라 검색 오류 (시도 {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    return None

        return None

    # ──────────────────────────────────────────────
    # 아이허브 (iHerb)
    # ──────────────────────────────────────────────
    def _extract_iherb_images_from_dom(
        self, driver: uc.Chrome
    ) -> List[str]:
        """
        현재 로드된 아이허브 상세 페이지 DOM에서
        클릭 없이 이미지 URL들을 추출.

        규칙:
        - 메인 이미지: .product-summary-image a[href]
        - 썸네일: .thumbnail-container img[data-large-img]
        - 배너: /cms/banners/ 포함 시 제외
        """
        image_urls: Set[str] = set()

        # 메인 큰 이미지 (a[href])
        try:
            main_anchor = driver.find_element(
                By.CSS_SELECTOR, ".product-summary-image a[href]"
            )
            href = main_anchor.get_attribute("href")
            if href:
                image_urls.add(href)
        except Exception:
            pass

        # 썸네일 이미지 (data-large-img)
        try:
            thumbs = driver.find_elements(
                By.CSS_SELECTOR, ".thumbnail-container img[data-large-img]"
            )
            for t in thumbs:
                url = t.get_attribute("data-large-img")
                if not url:
                    continue
                if "/cms/banners/" in url:
                    # 브랜드 배너 등은 제외
                    continue
                image_urls.add(url)
        except Exception:
            pass

        return list(image_urls)

    def get_iherb_product(
        self, driver: uc.Chrome, product_partno: str
    ) -> Optional[Dict]:
        """
        아이허브에서 제품 정보 가져오기
        - 위해식품과 같은 driver로 호출
        """
        try:
            product_url = f"{self.iherb_base_url}/pr/{product_partno}"
            driver.get(product_url)

            # body 로딩 대기
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except Exception:
                pass

            time.sleep(1.5)

            image_urls = self._extract_iherb_images_from_dom(driver)
            print(f"  ✓ 아이허브: {len(image_urls)}개 이미지 발견")

            return {
                "product_partno": product_partno,
                "product_url": product_url,
                "image_urls": image_urls,
            }

        except Exception as e:
            print(f"  ✗ 아이허브 접근 오류: {str(e)}")
            return None

    # ──────────────────────────────────────────────
    # 이미지 다운로드 & Gemini 비교
    # ──────────────────────────────────────────────
    def download_image(self, url: str) -> Optional[bytes]:
        """이미지 다운로드"""
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            print(f"    이미지 다운로드 실패 ({url}): {str(e)}")
            return None

    def compare_images_with_gemini(
        self, foodsafety_images: List[str], iherb_images: List[str]
    ) -> Dict:
        """Gemini API를 사용하여 이미지 비교"""
        try:
            fs_image_data: List[Image.Image] = []
            for url in foodsafety_images[:3]:
                img_bytes = self.download_image(url)
                if img_bytes:
                    fs_image_data.append(Image.open(io.BytesIO(img_bytes)))

            ih_image_data: List[Image.Image] = []
            for url in iherb_images[:3]:
                img_bytes = self.download_image(url)
                if img_bytes:
                    ih_image_data.append(Image.open(io.BytesIO(img_bytes)))

            if not fs_image_data or not ih_image_data:
                return {
                    "match": "unknown",
                    "confidence": 0,
                    "reason": "이미지 다운로드 실패",
                }

            prompt = """
다음 두 그룹의 이미지를 비교하여 동일한 제품인지 판단해주세요.

첫 번째 그룹: 식품안전나라에서 가져온 위해식품 이미지
두 번째 그룹: 아이허브 온라인 쇼핑몰의 제품 이미지

다음 기준으로 판단해주세요:
1. 제품명, 브랜드명이 일치하는가?
2. 제품 패키징, 라벨 디자인이 유사한가?
3. 제품 용량, 정보가 일치하는가?

응답은 반드시 다음 JSON 형식으로만 작성해주세요:
{
    "match": "yes" 또는 "no" 또는 "uncertain",
    "confidence": 0-100 사이의 숫자,
    "reason": "판단 근거를 한국어로 간단히 설명"
}
"""

            content = [prompt]
            content.extend(fs_image_data)
            content.extend(ih_image_data)

            response = self.model.generate_content(content)
            result_text = response.text.strip()

            import re
            import json

            json_match = re.search(r"\{[^}]+\}", result_text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
            else:
                result = {
                    "match": "uncertain",
                    "confidence": 50,
                    "reason": result_text[:200],
                }

            print(
                f"  ✓ Gemini 분석 완료: {result['match']} "
                f"(신뢰도: {result['confidence']}%)"
            )
            print(f"     근거: {result['reason']}")

            return result

        except Exception as e:
            print(f"  ✗ Gemini 비교 오류: {str(e)}")
            return {
                "match": "error",
                "confidence": 0,
                "reason": str(e),
            }

    # ──────────────────────────────────────────────
    # 한 행 처리
    # ──────────────────────────────────────────────
    def process_row(self, row: pd.Series) -> Dict:
        """CSV의 한 행(상품 1개)을 처리"""
        result: Dict = {
            "product_partno": row["product_partno"],
            "product_id": row["product_id"],
            "위해식품_제품명": row["위해식품_제품명"],
            "아이허브_제품명": row["아이허브_제품명"],
            "foodsafety_data": None,
            "iherb_data": None,
            "comparison_result": None,
            "status": "pending",
        }

        print(f"\n[{row['product_id']}] {row['위해식품_제품명']}")

        # 🔥 상품 1개당 드라이버 1개 생성 (위해식품 + 아이허브 공유)
        driver = self._create_driver()
        try:
            # 1. 식품안전나라 검색
            foodsafety_data = self.search_foodsafety_product(
                driver, row["위해식품_제품명"]
            )
            result["foodsafety_data"] = foodsafety_data

            if not foodsafety_data or not foodsafety_data.get("image_urls"):
                result["status"] = "foodsafety_not_found"
                print("  → 식품안전나라 이미지 없음")
                return result

            time.sleep(1)

            # 2. 아이허브 상세
            iherb_data = self.get_iherb_product(driver, row["product_partno"])
            result["iherb_data"] = iherb_data

            if not iherb_data or not iherb_data.get("image_urls"):
                result["status"] = "iherb_not_found"
                print("  → 아이허브 이미지 없음")
                return result

        finally:
            # 상품 1개 처리 후 브라우저 종료
            driver.quit()
            print("  ✓ 현재 상품 브라우저 종료")

        # 3. 이미지 비교 (브라우저는 이미 닫힌 상태, URL만 사용)
        comparison = self.compare_images_with_gemini(
            foodsafety_data["image_urls"], iherb_data["image_urls"]
        )
        result["comparison_result"] = comparison
        result["status"] = "completed"

        time.sleep(2)
        return result

    # ──────────────────────────────────────────────
    # CSV 전체 처리
    # ──────────────────────────────────────────────
    def process_csv(
        self,
        csv_path: str,
        limit: Optional[int] = None,
        product_partnos: Optional[List[str]] = None,
        resume: bool = False,
    ):
        """CSV 파일 전체 처리"""
        df = self.load_csv_data(csv_path)

        # 특정 제품만 필터링
        if product_partnos:
            df = df[df["product_partno"].isin(product_partnos)]
            print(f"특정 제품 필터링: {len(df)}건")

        # 이어하기 모드
        if resume:
            completed_partnos = self.load_completed_partnos()
            if completed_partnos:
                print(f"✓ 이어하기: {len(completed_partnos)}건 이미 처리됨")
                df = df[~df["product_partno"].isin(completed_partnos)]
                print(f"남은 처리: {len(df)}건")

        if limit:
            df = df.head(limit)
            print(f"처리 제한: {limit}건만 처리")

        total = len(df)
        for idx, (_, row) in enumerate(df.iterrows(), 1):
            try:
                result = self.process_row(row)
                self.append_to_csv(result)
                
                print(f"진행률: {idx}/{total}")

            except Exception as e:
                print(f"  ✗ 처리 오류: {str(e)}")
                error_result = {
                    "product_partno": row["product_partno"],
                    "product_id": row["product_id"],
                    "위해식품_제품명": row.get("위해식품_제품명", ""),
                    "아이허브_제품명": row.get("아이허브_제품명", ""),
                    "status": "error",
                    "error": str(e),
                }
                self.append_to_csv(error_result)

        # 최종 요약
        self.print_summary()

    # ──────────────────────────────────────────────
    # 결과 저장
    # ──────────────────────────────────────────────
    def append_to_csv(self, result: Dict):
        """결과를 CSV에 추가"""
        # 리스트 → 문자열로 변환
        fs_images = []
        if isinstance(result.get("foodsafety_data"), dict):
            fs_images = result["foodsafety_data"].get("image_urls", []) or []

        ih_images = []
        if isinstance(result.get("iherb_data"), dict):
            ih_images = result["iherb_data"].get("image_urls", []) or []

        fs_images_str = "|".join(fs_images)
        ih_images_str = "|".join(ih_images)

        row_data = {
            "product_partno": result.get("product_partno", ""),
            "product_id": result.get("product_id", ""),
            "위해식품_제품명": result.get("위해식품_제품명", ""),
            "아이허브_제품명": result.get("아이허브_제품명", ""),
            "status": result.get("status", ""),
            "match": (
                result.get("comparison_result", {}).get("match", "")
                if isinstance(result.get("comparison_result"), dict)
                else ""
            ),
            "confidence": (
                result.get("comparison_result", {}).get("confidence", 0)
                if isinstance(result.get("comparison_result"), dict)
                else 0
            ),
            "reason": (
                result.get("comparison_result", {}).get("reason", "")
                if isinstance(result.get("comparison_result"), dict)
                else ""
            ),
            "foodsafety_url": (
                result.get("foodsafety_data", {}).get("detail_url", "")
                if isinstance(result.get("foodsafety_data"), dict)
                else ""
            ),
            "iherb_url": (
                result.get("iherb_data", {}).get("product_url", "")
                if isinstance(result.get("iherb_data"), dict)
                else ""
            ),
            "foodsafety_image_count": len(fs_images),
            "iherb_image_count": len(ih_images),
            "foodsafety_image_urls": fs_images_str,
            "iherb_image_urls": ih_images_str,
        }

        df_row = pd.DataFrame([row_data])

        if os.path.exists(self.csv_output):
            df_row.to_csv(
                self.csv_output,
                mode="a",
                header=False,
                index=False,
                encoding="utf-8-sig",
            )
        else:
            df_row.to_csv(
                self.csv_output,
                mode="w",
                header=True,
                index=False,
                encoding="utf-8-sig",
            )

    def print_summary(self):
        """CSV 기반 요약 출력"""
        if not os.path.exists(self.csv_output):
            print("\n⚠ 결과 파일이 없습니다.")
            return

        df = pd.read_csv(self.csv_output, encoding="utf-8-sig")
        
        total = len(df)
        completed = len(df[df["status"] == "completed"])
        matched = len(df[df["match"] == "yes"])

        print(f"\n✓ 결과 저장: {self.csv_output}")
        print("\n=== 요약 ===")
        print(f"전체: {total}건")
        print(f"완료: {completed}건")
        print(f"매칭: {matched}건")


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────
def main():
    """메인 실행 함수"""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        api_key = input("Gemini API 키를 입력하세요: ").strip()

    if not api_key:
        print("API 키가 필요합니다.")
        return

    comparator = ProductImageComparator(api_key, headless=False)
    csv_path = "./위해식품목록_아이허브_비플로우기준.csv"

    # 명령행 인자 처리
    if len(sys.argv) > 1:
        mode = sys.argv[1]

        if mode == "clear":
            comparator.clear_progress()
            return

        elif mode == "resume":
            print("\n=== 이어하기 실행 ===")
            comparator.process_csv(csv_path, resume=True)
            return

        elif mode == "product":
            if len(sys.argv) < 3:
                print("사용법: python3 main.py product [NOW-01278,CGN-02049,...]")
                return
            product_partnos = [x.strip() for x in sys.argv[2].split(",")]
            print(f"\n=== 특정 제품 실행: {product_partnos} ===")
            comparator.process_csv(csv_path, product_partnos=product_partnos)
            return

    # 인터랙티브 모드
    print("\n=== 제품 이미지 비교 시스템 ===")
    print("1. 테스트 (3건)")
    print("2. 특정 제품 실행")
    print("3. 전체 실행")
    print("4. 이어하기")
    print("5. 결과 초기화")

    choice = input("\n선택: ").strip()

    if choice == "1":
        print("\n=== 테스트 실행 (3건) ===")
        comparator.process_csv(csv_path, limit=3)

    elif choice == "2":
        partnos_input = input(
            "제품 코드를 쉼표로 구분하여 입력 (예: NOW-01278,CGN-02049): "
        ).strip()
        product_partnos = [x.strip() for x in partnos_input.split(",")]
        print(f"\n=== 특정 제품 실행: {product_partnos} ===")
        comparator.process_csv(csv_path, product_partnos=product_partnos)

    elif choice == "3":
        print("\n=== 전체 실행 ===")
        comparator.process_csv(csv_path)

    elif choice == "4":
        print("\n=== 이어하기 실행 ===")
        comparator.process_csv(csv_path, resume=True)

    elif choice == "5":
        comparator.clear_progress()


if __name__ == "__main__":
    main()