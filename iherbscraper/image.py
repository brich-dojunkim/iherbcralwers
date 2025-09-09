"""
단순화된 이미지 비교 모듈
- pillow_avif 의존성 제거
- JPG/PNG 이미지만 지원
- 기본 OCR 및 텍스트 매칭 기능
"""

from PIL import Image
import numpy as np
import cv2
import pytesseract
import re
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

# ===== 전처리 + OCR =====
def load_rgb(path: str) -> Image.Image:
    """이미지 로드 및 RGB 변환"""
    try:
        img = Image.open(path)
        img.load()
        return img.convert("RGB")
    except Exception as e:
        print(f"이미지 로드 실패 ({path}): {e}")
        raise

def ocr_text(img: Image.Image) -> str:
    """OCR 텍스트 추출"""
    try:
        g = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2GRAY)
        
        # 소형 라벨 보강
        if max(g.shape) < 1200:
            g = cv2.resize(g, None, fx=1.5, fy=1.5, interpolation=cv2.INTER_CUBIC)
        
        g = cv2.medianBlur(g, 3)
        bw = cv2.adaptiveThreshold(g, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                   cv2.THRESH_BINARY, 31, 11)
        
        cfg = "--oem 3 --psm 6 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-+.%/() "
        t1 = pytesseract.image_to_string(bw, lang="eng", config=cfg)
        t2 = pytesseract.image_to_string(g, lang="eng", config=cfg)
        
        return (t1 + "\n" + t2)
    except Exception as e:
        print(f"OCR 처리 실패: {e}")
        return ""

# ===== 정규화 & 필드 추출 =====
UNIT_ALIAS = {
    "µg": "mcg", "μg": "mcg", "ug": "mcg",
    "i.u.": "iu", "iu.": "iu", "milligram": "mg", "grams": "g", "mgs": "mg"
}

FORM_WORDS = ["softgel", "softgels", "capsule", "capsules", "tablet", "tablets", 
              "vcap", "vcaps", "veggie", "veg", "gels"]

COUNT_WORDS = r"(softgels?|capsules?|tablets?|vcaps?)"

# 제품 계열 정규화 맵
CORE_MAP: Dict[str, str] = {
    r"\bmilk\s*thistle\b": "milk thistle",
    r"\bsilymarin\b": "milk thistle",
    r"\bprobiotic[-\s]*10\b": "probiotic",
    r"\bprobiotic\b": "probiotic",
    r"\bprobiotics\b": "probiotic",
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
}

STRENGTH_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(mg|mcg|g|iu)\b", re.I)
CFU_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(billion|million)\b", re.I)
COUNT_RE = re.compile(rf"\b(\d{{2,4}})\s*{COUNT_WORDS}\b", re.I)

@dataclass
class Fields:
    brand: str = ""
    core: str = ""
    strength: str = ""
    form: str = ""
    count: str = ""
    strength_val: Optional[float] = None
    strength_unit: Optional[str] = None

def normalize_text(s: str) -> str:
    """텍스트 정규화"""
    s = s.lower()
    for k, v in UNIT_ALIAS.items():
        s = s.replace(k, v)
    s = re.sub(r"[®™©|]", " ", s)
    s = re.sub(r"[^a-z0-9+\-./()%\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def canonical_from_map(n: str, rules: Dict[str, str]) -> str:
    """규칙 기반 정규화"""
    for pat, canon in rules.items():
        if re.search(pat, n, flags=re.I):
            return canon
    return ""

def parse_strength(n: str) -> Tuple[str, Optional[float], Optional[str]]:
    """함량/단위 파싱"""
    m = STRENGTH_RE.search(n)
    if m:
        val = float(m.group(1))
        unit = m.group(2).lower()
        return f"{m.group(1)} {unit}", val, unit
    
    cf = CFU_RE.search(n)
    if cf:
        val = float(cf.group(1))
        unit = cf.group(2).lower()
        return f"{cf.group(1)} {unit}", val, unit
    
    return "", None, None

def extract_fields(raw_text: str) -> Fields:
    """텍스트에서 필드 추출"""
    n = normalize_text(raw_text)

    # 브랜드/핵심명
    brand = canonical_from_map(n, BRAND_MAP)
    core = canonical_from_map(n, CORE_MAP)

    # 함량
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

# ===== 함량 비교 =====
def strengths_equal_with_glitch_fix(f1: Fields, f2: Fields) -> bool:
    """함량 비교 (글리치 보정 포함)"""
    if not (f1.strength_val is not None and f2.strength_val is not None):
        return True
    
    if f1.strength_unit != f2.strength_unit:
        return False

    v1, v2 = f1.strength_val, f2.strength_val
    u = f1.strength_unit

    # 정확히 같으면 동일
    if v1 == v2:
        return True

    # 뒤꼬리 '1' 글리치 보정 (예: 25 vs 251)
    if v1.is_integer() and v2.is_integer():
        a, b = int(v1), int(v2)
        big, small = (a, b) if a >= b else (b, a)
        
        if big % 10 == 1 and (big // 10) == small:
            if u in ("billion", "million"):
                return True

    return False

# ===== 동일/상이 판정 =====
def same_product_by_text(f1: Fields, f2: Fields) -> Tuple[bool, str]:
    """텍스트 기반 제품 동일성 판정"""
    # 1) 브랜드 확인
    if f1.brand and f2.brand and f1.brand != f2.brand:
        return False, "브랜드가 다름"

    # 2) 제품 계열 확인
    if f1.core and f2.core and f1.core != f2.core:
        return False, f"제품계열 다름 ({f1.core} vs {f2.core})"

    # 3) 함량/단위 확인
    if f1.strength and f2.strength:
        if f1.strength_unit != f2.strength_unit:
            return False, f"함량 단위 다름 ({f1.strength} vs {f2.strength})"
        if not strengths_equal_with_glitch_fix(f1, f2):
            return False, f"함량/수치 다름 ({f1.strength} vs {f2.strength})"

    # 4) 제형 확인
    if f1.form and f2.form and f1.form != f2.form:
        return False, f"제형 다름 ({f1.form} vs {f2.form})"

    # 5) 수량 확인
    if f1.count and f2.count and f1.count != f2.count:
        return False, f"수량 다름 ({f1.count} vs {f2.count})"

    return True, "텍스트 충돌 없음(브랜드/계열/함량/제형/수량 기준)"


# ===== 테스트 함수 =====
def test_image_comparison(img_path_a: str, img_path_b: str) -> Tuple[bool, str]:
    """두 이미지 비교 테스트"""
    try:
        # 이미지 로드 및 OCR
        img_a = load_rgb(img_path_a)
        img_b = load_rgb(img_path_b)
        
        text_a = ocr_text(img_a)
        text_b = ocr_text(img_b)
        
        # 필드 추출 및 비교
        fields_a = extract_fields(text_a)
        fields_b = extract_fields(text_b)
        
        same, reason = same_product_by_text(fields_a, fields_b)
        
        return same, reason
        
    except Exception as e:
        return False, f"이미지 비교 오류: {str(e)}"


if __name__ == "__main__":
    print("단순화된 이미지 비교 모듈 테스트")
    print("pillow_avif 의존성 제거됨")
    print("JPG/PNG 이미지만 지원")