"""
OCR 처리 및 제품 정보 추출 모듈
EasyOCR을 사용하여 이미지에서 텍스트 추출
키워드 사전 없이 완전히 패턴 없는 방식
"""

import os
from typing import Dict, List, Optional
try:
    import easyocr
except ImportError:
    print("EasyOCR가 설치되지 않았습니다. 다음 명령어로 설치하세요:")
    print("pip install easyocr")
    exit(1)


class OCRProcessor:
    """EasyOCR을 사용한 이미지 텍스트 추출"""
    
    def __init__(self):
        """EasyOCR 초기화"""
        try:
            # 영어와 한국어 지원
            self.reader = easyocr.Reader(['en', 'ko'])
            print("✅ EasyOCR 초기화 완료")
        except Exception as e:
            print(f"❌ EasyOCR 초기화 실패: {e}")
            self.reader = None
    
    def extract_text(self, image_path: str) -> Dict:
        """이미지에서 텍스트 추출"""
        if not self.reader:
            return {'success': False, 'error': 'OCR 초기화 실패'}
        
        if not os.path.exists(image_path):
            return {'success': False, 'error': f'이미지 파일 없음: {image_path}'}
        
        try:
            # EasyOCR 실행
            results = self.reader.readtext(image_path)
            
            if not results:
                return {'success': False, 'error': '텍스트 추출 실패'}
            
            # 텍스트와 신뢰도 추출
            texts = []
            confidences = []
            boxes = []
            
            for result in results:
                bbox, text, confidence = result
                texts.append(text.strip())
                confidences.append(confidence)
                boxes.append(bbox)
            
            # 전체 텍스트 결합
            full_text = ' '.join(texts)
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0
            
            return {
                'success': True,
                'texts': texts,
                'full_text': full_text,
                'confidences': confidences,
                'avg_confidence': avg_confidence,
                'boxes': boxes
            }
            
        except Exception as e:
            return {'success': False, 'error': f'OCR 처리 오류: {str(e)}'}


class ProductInfoParser:
    """제품 정보 추출 파서 - 완전히 패턴 없는 방식"""
    
    def __init__(self):
        """파서 초기화"""
        pass
    
    def extract_brand(self, text: str) -> Optional[str]:
        """브랜드명 추출 - 첫 번째 단어 또는 첫 두 단어"""
        if not text:
            return None
        
        words = text.split()
        if len(words) >= 2:
            # 첫 두 단어 시도
            two_words = ' '.join(words[:2])
            if len(two_words) > 3:  # 너무 짧지 않으면
                return two_words.title()
        
        if words:
            # 첫 단어
            first_word = words[0]
            if len(first_word) > 2:  # 너무 짧지 않으면
                return first_word.title()
        
        return None
    
    def extract_product_name(self, text: str) -> Optional[str]:
        """제품명 추출 - 가장 긴 단어"""
        if not text:
            return None
        
        words = text.split()
        if words:
            # 가장 긴 단어 찾기
            longest_word = max(words, key=len)
            if len(longest_word) > 3:  # 너무 짧지 않으면
                return longest_word.title()
        
        return None
    
    def extract_numbers_with_context(self, text: str) -> List[Dict]:
        """숫자와 주변 컨텍스트 추출"""
        if not text:
            return []
        
        words = text.lower().split()
        numbers_found = []
        
        for i, word in enumerate(words):
            # 숫자 포함된 단어 찾기
            if any(char.isdigit() for char in word):
                # 앞뒤 컨텍스트 수집
                context_before = words[max(0, i-2):i]
                context_after = words[i+1:min(len(words), i+3)]
                
                numbers_found.append({
                    'number_word': word,
                    'context_before': context_before,
                    'context_after': context_after,
                    'position': i
                })
        
        return numbers_found
    
    def extract_dosage(self, text: str) -> Optional[str]:
        """용량 추출 - mg, mcg, iu 근처 숫자"""
        numbers = self.extract_numbers_with_context(text)
        
        for num_info in numbers:
            # 전체 컨텍스트에서 단위 찾기
            all_context = num_info['context_before'] + num_info['context_after']
            context_text = ' '.join(all_context)
            
            if any(unit in context_text for unit in ['mg', 'mcg', 'iu']):
                return num_info['number_word']
        
        return None
    
    def extract_count(self, text: str) -> Optional[str]:
        """개수 추출 - tablet, capsule, count 근처 숫자"""
        numbers = self.extract_numbers_with_context(text)
        
        for num_info in numbers:
            # 전체 컨텍스트에서 개수 단위 찾기
            all_context = num_info['context_before'] + num_info['context_after']
            context_text = ' '.join(all_context)
            
            if any(unit in context_text for unit in ['tablet', 'capsule', 'count', 'piece']):
                return num_info['number_word']
        
        return None
    
    def parse_product_info(self, ocr_result: Dict) -> Dict:
        """OCR 결과에서 4가지 제품 정보 추출"""
        if not ocr_result.get('success', False):
            return {
                'brand': None,
                'product_name': None,
                'dosage': None,
                'count': None,
                'confidence': 0,
                'error': ocr_result.get('error', '알 수 없는 오류')
            }
        
        full_text = ocr_result.get('full_text', '')
        confidence = ocr_result.get('avg_confidence', 0)
        
        # 디버그: 추출된 텍스트 출력
        print(f"      추출된 텍스트: '{full_text[:100]}...'")
        print(f"      텍스트 길이: {len(full_text)}")
        
        # 4가지 정보 추출
        brand = self.extract_brand(full_text)
        product_name = self.extract_product_name(full_text)
        dosage = self.extract_dosage(full_text)
        count = self.extract_count(full_text)
        
        # 디버그: 추출 결과 출력
        print(f"      브랜드: {brand}, 제품명: {product_name}, 용량: {dosage}, 개수: {count}")
        
        return {
            'brand': brand,
            'product_name': product_name,
            'dosage': dosage,
            'count': count,
            'confidence': confidence,
            'raw_text': full_text
        }


def process_image(image_path: str) -> Dict:
    """이미지 하나 처리하는 편의 함수"""
    ocr_processor = OCRProcessor()
    parser = ProductInfoParser()
    
    # OCR 실행
    ocr_result = ocr_processor.extract_text(image_path)
    
    # 제품 정보 추출
    product_info = parser.parse_product_info(ocr_result)
    
    return product_info


# 테스트용 실행 코드
if __name__ == "__main__":
    test_image = "test_image.jpg"
    
    if os.path.exists(test_image):
        result = process_image(test_image)
        print("테스트 결과:")
        print(f"브랜드: {result['brand']}")
        print(f"제품명: {result['product_name']}")
        print(f"용량: {result['dosage']}")
        print(f"개수: {result['count']}")
        print(f"신뢰도: {result['confidence']:.2f}")
    else:
        print("EasyOCR 기반 패턴 프리 방식:")
        print("- 브랜드: 첫 단어 또는 첫 두 단어")
        print("- 제품명: 가장 긴 단어")
        print("- 용량: mg/mcg/iu 근처 숫자")
        print("- 개수: tablet/capsule/count 근처 숫자")
        print("- 영어 + 한국어 동시 지원")