# 식약처 위해식품 × iHerb 매칭 시스템 (분리 버전)

## 워크플로우 구조

```
┌─────────────────────────────────────────────────────────────┐
│                     Phase 1: URL 수집                        │
│  식약처 API → 이미지 다운로드 → Google 역검색 → iHerb URL   │
│                          ↓                                   │
│              hazard_iherb_matched_final.csv                  │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   Phase 2: 이미지 검증                       │
│  CSV 읽기 → iHerb 크롤링 → 이미지 추출 → Gemini 비교       │
│                          ↓                                   │
│              검증 결과 업데이트 (CSV)                        │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  Phase 3: 증분 업데이트                      │
│  최신 SEQ 확인 → 신규 데이터만 API 호출                     │
│  → Phase 1 (신규) → Phase 2 (미검증)                        │
└─────────────────────────────────────────────────────────────┘
```

## 파일 구조

```
hazard_iherb/
├── config.py                          # 공통 설정
├── iherb_scraper.py                   # HTML 파싱 모듈
├── phase1_collect_urls.py             # Phase 1: URL 수집
├── phase2_verify_images.py            # Phase 2: 이미지 검증
├── phase3_incremental.py              # Phase 3: 증분 업데이트
├── hazard_iherb_matched_final.csv     # 최종 결과
└── .env                               # GEMINI_API_KEY
```

## CSV 컬럼 구조

| 컬럼명 | 설명 |
|--------|------|
| SELF_IMPORT_SEQ | 식약처 고유번호 |
| PRDT_NM | 제품명 |
| MUFC_NM | 제조사 |
| MUFC_CNTRY_NM | 제조국 |
| INGR_NM_LST | 검출 성분 |
| CRET_DTM | 등록일 (YYYYMMDD) |
| IMAGE_URL_MFDS | 식약처 이미지 URL |
| IHERB_URL | iHerb 상품 URL |
| STATUS | 상태 (아래 참조) |
| IHERB_PRODUCT_IMAGES | iHerb 이미지 리스트 (JSON) |
| GEMINI_VERIFIED | Gemini 검증 결과 (True/False) |
| GEMINI_REASON | Gemini 검증 사유 |
| VERIFIED_DTM | 검증 일시 (YYYYMMDDHHmmss) |

### STATUS 값

- `FOUND`: iHerb URL 발견 (검증 대기)
- `VERIFIED_MATCH`: Gemini 검증 통과
- `VERIFIED_MISMATCH`: Gemini 검증 실패
- `NOT_FOUND`: iHerb URL 없음
- `NO_IMAGE`: 식약처 이미지 없음
- `DOWNLOAD_FAILED`: 이미지 다운로드 실패
- `VERIFICATION_FAILED`: 검증 중 오류

## 설치

```bash
pip install pandas undetected-chromedriver selenium python-dotenv google-generativeai
```

## 환경 설정

```bash
export GEMINI_API_KEY='your-gemini-api-key'
```

또는 `.env` 파일 생성:
```
GEMINI_API_KEY=your-gemini-api-key
```

## 사용법

### 1. Phase 1: URL 수집 (처음 실행 또는 전체 재수집)

```bash
python phase1_collect_urls.py
```

- 식약처 API에서 전체 데이터 수집
- Google 역검색으로 iHerb URL 추출
- `STATUS=FOUND`로 저장

### 2. Phase 2: 이미지 검증

#### 미검증 항목만 검증 (기본)
```bash
python phase2_verify_images.py
```

#### 전체 재검증
```bash
python phase2_verify_images.py --revalidate-all
```

#### 특정 상품만 검증
```bash
python phase2_verify_images.py --seq 123456
```

#### 헤드리스 모드
```bash
python phase2_verify_images.py --headless
```

### 3. Phase 3: 증분 업데이트 (정기 실행용)

#### 최근 7일 신규 데이터 처리
```bash
python phase3_incremental.py
```

#### 최근 30일 데이터 처리
```bash
python phase3_incremental.py --days 30
```

#### Phase 1만 실행 (검증 스킵)
```bash
python phase3_incremental.py --skip-phase2
```

#### Phase 2만 실행 (URL 수집 스킵)
```bash
python phase3_incremental.py --skip-phase1
```

## 실행 시나리오

### 시나리오 1: 최초 실행
```bash
# Step 1: URL 수집
python phase1_collect_urls.py

# Step 2: 이미지 검증
python phase2_verify_images.py
```

### 시나리오 2: 일일 업데이트 (cron 작업)
```bash
# 매일 실행 - 최근 7일 신규 데이터만 처리
python phase3_incremental.py --headless
```

### 시나리오 3: 특정 상품 재검증
```bash
# 특정 SEQ 재검증
python phase2_verify_images.py --seq 123456
```

### 시나리오 4: 전체 재검증
```bash
# 기존 FOUND 상품 모두 재검증
python phase2_verify_images.py --revalidate-all --headless
```

## 데이터 분석 예제

```python
import pandas as pd

df = pd.read_csv('hazard_iherb_matched_final.csv')

# 상태별 통계
print(df['STATUS'].value_counts())

# 검증 성공률
verified = df['GEMINI_VERIFIED'].notna().sum()
matched = (df['GEMINI_VERIFIED'] == True).sum()
print(f"매칭 성공률: {matched/verified*100:.1f}%")

# iHerb 매칭 상품만 추출
matched_df = df[df['STATUS'] == 'VERIFIED_MATCH']
print(f"매칭된 상품: {len(matched_df)}건")
```

## 주의사항

1. **Rate Limiting**: Google 검색과 iHerb 크롤링에는 자동 대기 시간 포함
2. **Gemini API 요금**: Phase 2 실행 시 API 비용 발생
3. **브라우저**: undetected-chromedriver가 Chrome 자동 다운로드
4. **이미지 저장**: Phase 1은 임시 이미지를 자동 삭제
5. **CSV 백업**: 중요 데이터는 주기적으로 백업 권장

## 트러블슈팅

### Gemini API 오류
```bash
# API 키 확인
echo $GEMINI_API_KEY

# 또는 Python에서
python -c "import os; print(os.getenv('GEMINI_API_KEY'))"
```

### Chrome 드라이버 오류
```bash
# Chrome 업데이트
# undetected-chromedriver 재설치
pip install --upgrade undetected-chromedriver
```

### CSV 손상
```bash
# 백업에서 복구
cp hazard_iherb_matched_final.csv.backup hazard_iherb_matched_final.csv
```

## 성능 최적화

- **헤드리스 모드**: `--headless` 플래그로 10-15% 속도 향상
- **병렬 처리**: 현재 버전은 순차 처리 (안정성 우선)
- **캐싱**: API 응답은 자동 캐시됨

## 라이선스

MIT License
