"""
OCR 처리 모듈
이미지에서 텍스트 추출 및 파싱
"""

import os
import re
import cv2
import pytesseract
import numpy as np
from PIL import Image
from typing import Dict, Optional

# OCR 설정
# pytesseract.pytesseract.tesseract_cmd = '/opt/homebrew/bin/tesseract'


class OCRProcessor:
    """이미지 OCR 처리 클래스"""
    
    def __init__(self):
        # 브랜드 목록
        self.brands = [
            'now foods', 'now', 'solgar', 'life extension', 
            'jarrow formulas', 'nature\'s way', 'nordic naturals',
            'country life', 'source naturals', 'doctor\'s best'
        ]
        
        # 정규식 패턴
        self.patterns = {
            'count': r'(\d+)\s*(?:count|ct|tablets?|capsules?|softgels?|pieces?|정|개)',
            'dosage_mg': r'(\d+(?:,\d+)*)\s*mg',
            'dosage_mcg': r'(\d+(?:,\d+)*)\s*mcg',
            'dosage_iu': r'(\d+(?:,\d+)*)\s*iu',
            'brand_now': r'now\s*foods?',
            'product_code': r'[A-Z]{2,4}-\d{4,6}'
        }
    
    def preprocess_image(self, image_path: str) -> np.ndarray:
        """이미지 전처리로 OCR 정확도 향상"""
        try:
            img = cv2.imread(image_path)
            if img is None:
                return None
            
            # 그레이스케일 변환
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # 노이즈 제거
            denoised = cv2.fastNlMeansDenoising(gray)
            
            # 대비 향상
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
            enhanced = clahe.apply(denoised)
            
            # 이진화
            binary = cv2.adaptiveThreshold(
                enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                cv2.THRESH_BINARY, 11, 2
            )
            
            return binary
            
        except Exception as e:
            print(f"❌ 이미지 전처리 실패 {image_path}: {e}")
            return None
    
    def extract_text_from_image(self, image_path: str) -> Dict:
        """이미지에서 OCR로 텍스트 추출"""
        try:
            # 전처리된 이미지와 원본 이미지 모두 시도
            texts = []
            
            # 전처리된 이미지
            processed_img = self.preprocess_image(image_path)
            if processed_img is not None:
                config = r'--oem 3 --psm 6 -l kor+eng'
                texts.append(pytesseract.image_to_string(processed_img, config=config))
            
            # 원본 이미지
            pil_img = Image.open(image_path)
            config = r'--oem 3 --psm 6 -l kor+eng'
            texts.append(pytesseract.image_to_string(pil_img, config=config))
            
            # 결과 합치기
            combined_text = ' '.join(texts).lower()
            
            # 텍스트 파싱
            parsed_info = self.parse_extracted_text(combined_text)
            parsed_info['raw_text'] = combined_text
            parsed_info['image_path'] = image_path
            
            return parsed_info
            
        except Exception as e:
            print(f"❌ OCR 추출 실패 {image_path}: {e}")
            return {'image_path': image_path, 'error': str(e)}
    
    def parse_extracted_text(self, text: str) -> Dict:
        """추출된 텍스트에서 정보 파싱"""
        info = {
            'brand': '',
            'product_name': '',
            'count': '',
            'dosage_mg': '',
            'dosage_mcg': '',
            'dosage_iu': '',
            'product_code': '',
            'keywords': []
        }
        
        print(f"     파싱 시작: '{text[:100]}...'")
        
        # 브랜드 추출 - 더 넓은 패턴으로 시도
        for brand in self.brands:
            if brand in text:
                info['brand'] = brand
                print(f"     브랜드 발견: '{brand}'")
                break
        
        # NOW 브랜드 특별 처리 - 더 넓은 패턴
        now_patterns = ['now foods', 'now', 'nowfoods']
        for pattern in now_patterns:
            if pattern in text and not info['brand']:
                info['brand'] = 'now foods'
                print(f"     NOW 브랜드 발견: '{pattern}'")
                break
        
        # 패턴 매칭 - 더 관대한 패턴 사용
        relaxed_patterns = {
            'count': r'(\d+)\s*(?:count|ct|tablets?|capsules?|softgels?|pieces?|정|개|tab)',
            'dosage_mg': r'(\d+(?:,\d+)*)\s*mg',
            'dosage_mcg': r'(\d+(?:,\d+)*)\s*mcg',
            'dosage_iu': r'(\d+(?:,\d+)*)\s*iu'
        }
        
        for key, pattern in relaxed_patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                # 첫 번째 매치 사용
                value = matches[0]
                info[key] = value
                print(f"     {key} 패턴 매칭: '{value}' (패턴: {pattern})")
        
        # 키워드 추출 (영양소명) - 더 넓은 범위
        keyword_pattern = r'\b(?:zinc|vitamin|calcium|magnesium|omega|coq10|biotin|b12|d3|iron|probiotics?|protein|pea)\b'
        keywords = re.findall(keyword_pattern, text, re.IGNORECASE)
        info['keywords'] = list(set(keywords))
        if keywords:
            print(f"     키워드 발견: {keywords}")
        
        # 전체 텍스트 정리
        clean_text = re.sub(r'[^\w\s]', ' ', text)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        info['product_name'] = clean_text
        
        print(f"     파싱 완료: 브랜드='{info['brand']}', 개수='{info['count']}', 용량='{info['dosage_mg']}mg'")
        
        return info
    
    def calculate_similarity(self, iherb_info: Dict, coupang_info: Dict) -> tuple:
        """OCR 정보 기반 유사도 계산"""
        from difflib import SequenceMatcher
        
        score = 0.0
        details = {
            'brand_match': False,
            'count_match': False,
            'dosage_match': False,
            'keyword_match': False,
            'text_similarity': 0.0
        }
        
        # 1. 브랜드 매칭 (40점)
        if iherb_info.get('brand') and coupang_info.get('brand'):
            if iherb_info['brand'] == coupang_info['brand']:
                score += 0.4
                details['brand_match'] = True
        
        # 2. 개수 매칭 (20점)
        if iherb_info.get('count') and coupang_info.get('count'):
            if iherb_info['count'] == coupang_info['count']:
                score += 0.2
                details['count_match'] = True
            else:
                score -= 0.1  # 불일치 페널티
        
        # 3. 용량 매칭 (20점)
        for dosage_type in ['dosage_mg', 'dosage_mcg', 'dosage_iu']:
            if iherb_info.get(dosage_type) and coupang_info.get(dosage_type):
                if iherb_info[dosage_type] == coupang_info[dosage_type]:
                    score += 0.2
                    details['dosage_match'] = True
                    details[f'{dosage_type}_value'] = iherb_info[dosage_type]
                else:
                    score -= 0.1  # 불일치 페널티
                break
        
        # 4. 키워드 매칭 (10점)
        iherb_keywords = set(iherb_info.get('keywords', []))
        coupang_keywords = set(coupang_info.get('keywords', []))
        if iherb_keywords and coupang_keywords:
            overlap = len(iherb_keywords & coupang_keywords)
            total = len(iherb_keywords | coupang_keywords)
            if total > 0:
                keyword_score = (overlap / total) * 0.1
                score += keyword_score
                details['keyword_match'] = overlap > 0
                details['keyword_overlap'] = overlap
                details['keyword_total'] = total
        
        # 5. 전체 텍스트 유사도 (10점)
        iherb_text = iherb_info.get('product_name', '')
        coupang_text = coupang_info.get('product_name', '')
        if iherb_text and coupang_text:
            text_sim = SequenceMatcher(None, iherb_text, coupang_text).ratio()
            score += text_sim * 0.1
            details['text_similarity'] = text_sim
        
        final_score = max(0, min(1, score))
        return final_score, details
    
    def extract_product_code_from_filename(self, filename: str) -> Optional[str]:
        """이미지 파일명에서 상품코드 추출"""
        # iherb_01_NOW-02926.jpg → NOW-02926
        match = re.search(r'([A-Z]{2,4}-\d{4,6})', filename)
        return match.group(1) if match else None