# text_only_same_product.py
# - 임계값 없이 텍스트 규칙만으로 동일/상이 판정
# - AVIF 지원(pillow-avif-plugin)
# - NOW Foods 예시 포함(다른 브랜드/계열도 손쉽게 확장 가능)
# - ★함량 비교 시 '뒤꼬리 1' OCR 글리치(예: 25 → 251) 보정 규칙 추가★

from PIL import Image
import pillow_avif  # AVIF 지원
import numpy as np
import cv2
import pytesseract
import re
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

# ===== 비교할 이미지 경로 =====
IMG_A = "nowfoods_c.jpg"
IMG_B = "nowfoods_d.avif"

# ===== 전처리 + OCR =====
def load_rgb(path: str) -> Image.Image:
    img = Image.open(path)
    img.load()
    return img.convert("RGB")

def ocr_text(img: Image.Image) -> str:
    g = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2GRAY)
    # 소형 라벨 보강
    if max(g.shape) < 1200:
        g = cv2.resize(g, None, fx=1.5, fy=1.5, interpolation=cv2.INTER_CUBIC)
    g = cv2.medianBlur(g, 3)
    bw = cv2.adaptiveThreshold(g,255,cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                               cv2.THRESH_BINARY,31,11)
    cfg = "--oem 3 --psm 6 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-+.%/() "
    t1 = pytesseract.image_to_string(bw,  lang="eng", config=cfg)
    t2 = pytesseract.image_to_string(g,   lang="eng", config=cfg)
    return (t1 + "\n" + t2)

# ===== 정규화 & 필드 추출 =====
UNIT_ALIAS = {
    "µg":"mcg","μg":"mcg","ug":"mcg",
    "i.u.":"iu","iu.":"iu","milligram":"mg","grams":"g","mgs":"mg"
}
FORM_WORDS = ["softgel","softgels","capsule","capsules","tablet","tablets","vcap","vcaps","veggie","veg", "gels"]
COUNT_WORDS = r"(softgels?|capsules?|tablets?|vcaps?)"

# 제품 계열(핵심명) → 규칙 기반 정규화 맵 (필요시 확장)
CORE_MAP: Dict[str, str] = {
    # Milk Thistle 계열
    r"\bmilk\s*thistle\b": "milk thistle",
    r"\bsilymarin\b": "milk thistle",
    # Probiotic 계열
    r"\bprobiotic[-\s]*10\b": "probiotic",
    r"\bprobiotic\b": "probiotic",
    r"\bprobiotics\b": "probiotic",
    # 비타민/미네랄 예시(확장 가능)
    r"\bvitamin\s*d3\b": "vitamin d3",
    r"\bcoq10\b": "coq10",
    r"\balpha\s+lipoic\s+acid\b": "alpha lipoic acid",
    r"\bmagnesium\b": "magnesium",
    r"\bzinc\b": "zinc",
    r"\bmelatonin\b": "melatonin",
    r"\bomega[-\s]*3\b": "omega 3",
    r"\bfish\s+oil\b": "omega 3",
}

BRAND_MAP: Dict[str, str] = {
    r"\bnow\s+foods\b": "now foods",
    r"\bnow\b": "now foods",
    # 필요시 타 브랜드 추가
}

STRENGTH_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(mg|mcg|g|iu)\b", re.I)
CFU_RE      = re.compile(r"\b(\d+(?:\.\d+)?)\s*(billion|million)\b", re.I)
COUNT_RE    = re.compile(rf"\b(\d{{2,4}})\s*{COUNT_WORDS}\b", re.I)

@dataclass
class Fields:
    brand: str = ""
    core: str = ""
    strength: str = ""       # 원본 표현 (예: "300 mg", "25 billion")
    form: str = ""
    count: str = ""
    # 비교용 파싱 값
    strength_val: Optional[float] = None  # 숫자
    strength_unit: Optional[str] = None   # "mg"/"mcg"/"g"/"iu"/"billion"/"million"

def normalize_text(s: str) -> str:
    s = s.lower()
    for k,v in UNIT_ALIAS.items():
        s = s.replace(k, v)
    s = re.sub(r"[®™©|]", " ", s)
    s = re.sub(r"[^a-z0-9+\-./()%\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def canonical_from_map(n: str, rules: Dict[str,str]) -> str:
    for pat, canon in rules.items():
        if re.search(pat, n, flags=re.I):
            return canon
    return ""

def parse_strength(n: str) -> Tuple[str, Optional[float], Optional[str]]:
    """문자열에서 함량/단위를 찾아 (표기, 값, 단위) 반환"""
    m = STRENGTH_RE.search(n)
    if m:
        val = float(m.group(1)); unit = m.group(2).lower()
        return f"{m.group(1)} {unit}", val, unit
    cf = CFU_RE.search(n)
    if cf:
        val = float(cf.group(1)); unit = cf.group(2).lower()
        return f"{cf.group(1)} {unit}", val, unit
    return "", None, None

def extract_fields(raw_text: str) -> Fields:
    n = normalize_text(raw_text)

    # 브랜드/핵심명(제품 계열)
    brand = canonical_from_map(n, BRAND_MAP)
    core  = canonical_from_map(n, CORE_MAP)

    # 함량(두 계열 모두 지원)
    strength, sval, sunit = parse_strength(n)

    # 제형
    form = ""
    for f in FORM_WORDS:
        if re.search(rf"\b{re.escape(f)}\b", n):
            if f in ("softgel", "capsule", "tablet", "veg"):
                form = f + "s" if f != "veg" else "veg capsules"
            else:
                form = f
            break

    # 수량
    cnt = ""
    cm = COUNT_RE.search(n)
    if cm:
        cnt = f"{cm.group(1)} {cm.group(2).lower()}"

    return Fields(
        brand=brand, core=core, strength=strength,
        form=form, count=cnt,
        strength_val=sval, strength_unit=sunit
    )

# ===== 함량 비교: 글리치(뒤꼬리 '1') 보정 규칙 =====
def strengths_equal_with_glitch_fix(f1: Fields, f2: Fields) -> bool:
    """단위가 같고 숫자 한 쪽이 다른 쪽*10 + 1 형태면 동일로 인정 (예: 25 ↔ 251)"""
    if not (f1.strength_val is not None and f2.strength_val is not None):
        return True  # 둘 중 하나라도 없으면 충돌 없음으로 간주(텍스트만 규칙)
    if f1.strength_unit != f2.strength_unit:
        return False

    v1, v2 = f1.strength_val, f2.strength_val
    u = f1.strength_unit  # 동일 단위

    # 정확히 같으면 동일
    if v1 == v2:
        return True

    # 정수 비교가 아니면 보정 규칙 적용하지 않음
    if v1.is_integer() and v2.is_integer():
        a, b = int(v1), int(v2)
        big, small = (a, b) if a >= b else (b, a)
        # 뒤꼬리 '1' 글리치 보정: 251 vs 25, 301 vs 30 등
        if big % 10 == 1 and (big // 10) == small:
            # CFU(억/십억 계열)에서 자주 발생 → billion/million에 우선 적용
            if u in ("billion", "million"):
                return True
            # mg/iu에도 드물게 발생하면 허용하려면 아래 주석 해제
            # else:
            #     return True

    return False

# ===== 동일/상이 판정 (순수 규칙: 임계값 없음) =====
def same_product_by_text(f1: Fields, f2: Fields) -> (bool, str):
    # 1) 브랜드: 둘 다 추출되었고 서로 다르면 즉시 다른 제품
    if f1.brand and f2.brand and f1.brand != f2.brand:
        return False, "브랜드가 다름"

    # 2) 제품 계열(핵심명): 둘 다 추출되었고 서로 다르면 즉시 다른 제품
    if f1.core and f2.core and f1.core != f2.core:
        return False, f"제품계열 다름 ({f1.core} vs {f2.core})"

    # 3) 함량/단위: 둘 다 있으면 '정확 일치' 또는 글리치 보정 후 같아야 동일
    if f1.strength and f2.strength:
        if f1.strength_unit != f2.strength_unit:
            return False, f"함량 단위 다름 ({f1.strength} vs {f2.strength})"
        if not strengths_equal_with_glitch_fix(f1, f2):
            return False, f"함량/수치 다름 ({f1.strength} vs {f2.strength})"

    # 4) 제형: 둘 다 있으면 정확히 같아야 동일
    if f1.form and f2.form and f1.form != f2.form:
        return False, f"제형 다름 ({f1.form} vs {f2.form})"

    # 5) 수량: 둘 다 있으면 정확히 같아야 동일
    if f1.count and f2.count and f1.count != f2.count:
        return False, f"수량 다름 ({f1.count} vs {f2.count})"

    # 충돌이 없다면 동일로 간주 (일부 필드 미검출 허용)
    return True, "텍스트 충돌 없음(브랜드/계열/함량/제형/수량 기준)"

# ===== 실행 =====
if __name__ == "__main__":
    tA = ocr_text(load_rgb(IMG_A))
    tB = ocr_text(load_rgb(IMG_B))
    fA = extract_fields(tA)
    fB = extract_fields(tB)

    same, reason = same_product_by_text(fA, fB)

    print("A fields:", fA)
    print("B fields:", fB)
    print("RESULT:", "SAME ✅" if same else "DIFFERENT ❌", "| reason:", reason)
