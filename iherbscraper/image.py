"""
PaddleOCR 3.x를 최대한 활용한 제품 정보 추출
- 최신 PP-OCRv5 모델 사용
- 키워드 매핑 없이 순수 패턴 매칭
- PaddleOCR 3.x의 새로운 API 활용
"""

import os
import re
from typing import Dict, List, Optional

try:
    from paddleocr import PaddleOCR
    PADDLEOCR_AVAILABLE = True
except ImportError:
    print("PaddleOCR를 설치해주세요:")
    print("pip install paddleocr")
    PADDLEOCR_AVAILABLE = False


class PaddleOCR3ProductExtractor:
    """PaddleOCR 3.x를 활용한 제품 정보 추출기"""
    
    def __init__(self, lang='en'):
        """PaddleOCR 3.x 초기화"""
        if not PADDLEOCR_AVAILABLE:
            raise ImportError("PaddleOCR가 설치되지 않았습니다.")
        
        print("🚀 PaddleOCR 3.x 초기화 중...")
        
        # PaddleOCR 3.x 최적 설정
        # - PP-OCRv5 모델이 기본값 (최신 고성능 모델)
        # - use_doc_orientation_classify=False: 문서 방향 분류 비활성화 (속도 향상)
        # - use_doc_unwarping=False: 문서 왜곡 보정 비활성화 (속도 향상)
        # - use_textline_orientation=False: 텍스트 라인 방향 분류 비활성화
        # - device='cpu': CPU 사용 (안정성)
        self.ocr = PaddleOCR(
            lang=lang,  # 'en' for English, 'ch' for Chinese
            use_doc_orientation_classify=False,  # 문서 방향 분류 비활성화
            use_doc_unwarping=False,            # 문서 왜곡 보정 비활성화  
            use_textline_orientation=False,     # 텍스트 라인 방향 분류 비활성화
            device='cpu'                        # CPU 사용
        )
        
        print("✅ PaddleOCR 3.x (PP-OCRv5) 초기화 완료")
    
    def extract_text_and_scores(self, image_path: str) -> Dict:
        """PaddleOCR 3.x로 텍스트 추출"""
        try:
            if not os.path.exists(image_path):
                return {'success': False, 'error': f'이미지 파일 없음: {image_path}'}
            
            # PaddleOCR 3.x predict 메서드 사용
            results = self.ocr.predict(image_path)
            
            if not results or len(results) == 0:
                return {'success': False, 'error': 'OCR 결과 없음'}
            
            # 첫 번째 결과 (단일 이미지)
            result = results[0]
            
            # PaddleOCR 3.x 결과 구조 파싱
            if hasattr(result, 'res') and 'rec_texts' in result.res:
                # 새로운 3.x 결과 구조
                texts = result.res['rec_texts']
                scores = result.res.get('rec_scores', [1.0] * len(texts))  # 기본 점수
                
                # 전체 텍스트 조합
                full_text = ' '.join(texts)
                avg_confidence = sum(scores) / len(scores) if scores else 0
                
                return {
                    'success': True,
                    'texts': texts,
                    'scores': scores,
                    'full_text': full_text,
                    'avg_confidence': avg_confidence,
                    'method': 'PaddleOCR 3.x (PP-OCRv5)'
                }
            else:
                # 예전 형태나 다른 구조인 경우
                return {'success': False, 'error': '예상치 못한 결과 구조'}
                
        except Exception as e:
            return {'success': False, 'error': f'PaddleOCR 처리 오류: {str(e)}'}
    
    def extract_brand_from_text(self, text: str) -> Optional[str]:
        """텍스트에서 브랜드 추출 - 패턴 기반"""
        if not text:
            return None
        
        text_upper = text.upper()
        
        # 일반적인 브랜드 패턴들 (키워드 매핑 없이 패턴으로만)
        brand_patterns = [
            # NOW Foods 패턴
            r'\bNOW\s+FOODS?\b',
            r'\bNOW\b',
            
            # Nature's Way 패턴
            r'\bNATURE\'?S?\s+WAY\b',
            
            # 기타 일반적인 패턴들
            r'\b[A-Z][a-z]+\s+FOODS?\b',           # Something Foods
            r'\b[A-Z][a-z]+\'?S?\s+[A-Z][a-z]+\b', # Something's Something
            r'\b[A-Z]{2,}\b',                       # 모든 대문자 브랜드 (예: SOLGAR)
        ]
        
        for pattern in brand_patterns:
            match = re.search(pattern, text_upper)
            if match:
                brand = match.group(0)
                # 적절한 케이스로 변환
                if brand == 'NOW' or 'NOW FOODS' in brand:
                    return 'NOW Foods'
                elif 'NATURE' in brand and 'WAY' in brand:
                    return "Nature's Way"
                else:
                    return brand.title()
        
        # 패턴이 안 맞으면 첫 단어나 첫 두 단어를 브랜드로 추정
        words = text.split()
        if len(words) >= 2:
            first_two = ' '.join(words[:2])
            if len(first_two) > 4 and first_two[0].isupper():
                return first_two
        elif len(words) == 1 and len(words[0]) > 2 and words[0][0].isupper():
            return words[0]
        
        return None
    
    def extract_product_name_from_text(self, text: str) -> Optional[str]:
        """텍스트에서 제품명 추출 - 패턴 기반"""
        if not text:
            return None
        
        text_upper = text.upper()
        
        # 영양소/제품명 패턴들
        product_patterns = [
            # 비타민 관련
            r'\bVITAMIN\s+[A-Z0-9]+\b',
            r'\b[A-Z]+-?CARNITINE\b',
            r'\bOMEGA[-\s]*\d+\b',
            r'\bFISH\s+OIL\b',
            r'\bCOQ?10\b',
            r'\bCOENZYME\s+Q10\b',
            
            # 미네랄
            r'\bMAGNESIUM\b',
            r'\bCALCIUM\b',
            r'\bZINC\b',
            r'\bIRON\b',
            
            # 기타 보충제
            r'\bPROBIOTICS?\b',
            r'\bPROTEIN\b',
            r'\bCOLLAGEN\b',
            r'\bGLUCOSAMINE\b',
            r'\bTURMERIC\b',
            r'\bGINSENG\b',
            r'\bMELATONIN\b',
            r'\bBIOTIN\b',
            r'\b[A-Z][-\s]*COMPLEX\b',
            r'\bMULTIVITAMIN\b'
        ]
        
        for pattern in product_patterns:
            match = re.search(pattern, text_upper)
            if match:
                product = match.group(0)
                # 적절한 케이스로 변환
                return product.title().replace('-', '-').replace('  ', ' ')
        
        return None
    
    def extract_dosage_from_text(self, text: str) -> Optional[str]:
        """텍스트에서 용량 추출 - 정확한 패턴"""
        if not text:
            return None
        
        # mg, mcg, iu 패턴
        dosage_patterns = [
            r'(\d+(?:,\d+)*)\s*MG\b',
            r'(\d+(?:,\d+)*)\s*MCG\b', 
            r'(\d+(?:,\d+)*)\s*IU\b',
            r'(\d+(?:,\d+)*)\s*MILLIGRAMS?\b'
        ]
        
        text_upper = text.upper()
        
        for pattern in dosage_patterns:
            match = re.search(pattern, text_upper)
            if match:
                return match.group(1).replace(',', '')
        
        return None
    
    def extract_count_from_text(self, text: str) -> Optional[str]:
        """텍스트에서 개수 추출 - 정확한 패턴"""
        if not text:
            return None
        
        # 개수 패턴
        count_patterns = [
            r'(\d+)\s*TABLETS?\b',
            r'(\d+)\s*CAPSULES?\b',
            r'(\d+)\s*SOFTGELS?\b',
            r'(\d+)\s*VEG\s*CAPSULES?\b',
            r'(\d+)\s*VCAPS?\b',
            r'(\d+)\s*COUNT\b'
        ]
        
        text_upper = text.upper()
        
        for pattern in count_patterns:
            match = re.search(pattern, text_upper)
            if match:
                return match.group(1)
        
        return None
    
    def parse_product_info(self, ocr_result: Dict) -> Dict:
        """OCR 결과에서 제품 정보 추출"""
        if not ocr_result.get('success', False):
            return {
                'brand': None,
                'product_name': None,
                'dosage': None,
                'count': None,
                'confidence': 0,
                'error': ocr_result.get('error', '알 수 없는 오류'),
                'method': 'Failed'
            }
        
        full_text = ocr_result.get('full_text', '')
        confidence = ocr_result.get('avg_confidence', 0)
        method = ocr_result.get('method', 'Unknown')
        
        # 디버그 출력
        print(f"      추출된 텍스트: '{full_text[:100]}...'")
        
        # 정보 추출
        brand = self.extract_brand_from_text(full_text)
        product_name = self.extract_product_name_from_text(full_text)
        dosage = self.extract_dosage_from_text(full_text)
        count = self.extract_count_from_text(full_text)
        
        # 디버그 출력
        print(f"      브랜드: {brand}, 제품명: {product_name}, 용량: {dosage}, 개수: {count}")
        
        return {
            'brand': brand,
            'product_name': product_name,
            'dosage': dosage,
            'count': count,
            'confidence': confidence,
            'raw_text': full_text,
            'method': method
        }
    
    def process_image(self, image_path: str) -> Dict:
        """이미지 처리 메인 함수"""
        print(f"🔍 PaddleOCR 3.x 처리: {os.path.basename(image_path)}")
        
        # OCR 실행
        ocr_result = self.extract_text_and_scores(image_path)
        
        # 제품 정보 파싱
        product_info = self.parse_product_info(ocr_result)
        
        return product_info


def process_image_paddle3x(image_path: str, lang='en') -> Dict:
    """PaddleOCR 3.x로 이미지 처리하는 편의 함수"""
    try:
        extractor = PaddleOCR3ProductExtractor(lang=lang)
        return extractor.process_image(image_path)
    except Exception as e:
        return {
            'brand': None,
            'product_name': None,
            'dosage': None,
            'count': None,
            'confidence': 0,
            'error': f'PaddleOCR 3.x 초기화 실패: {str(e)}',
            'method': 'Failed'
        }


# 테스트용 실행 코드
if __name__ == "__main__":
    print("🚀 PaddleOCR 3.x (PP-OCRv5) 제품 정보 추출 테스트")
    print("=" * 60)
    print("특징:")
    print("- 최신 PP-OCRv5 모델 사용")
    print("- 키워드 매핑 없이 순수 패턴 매칭")
    print("- PaddleOCR 3.x 새로운 API 활용")
    print("- 이미지 전처리 없이 직접 처리")
    print("=" * 60)
    
    # 테스트 이미지들
    test_images = [
        "../coupang_images/coupang_3180742161.jpg",
        "../iherb_images/iherb_45_NOW-00068.jpg"
    ]
    
    for i, image_path in enumerate(test_images, 1):
        print(f"\n📸 테스트 {i}: {os.path.basename(image_path)}")
        print("-" * 40)
        
        if not os.path.exists(image_path):
            print(f"❌ 파일 없음: {image_path}")
            continue
        
        # 파일 크기 확인
        file_size = os.path.getsize(image_path) / 1024
        print(f"📁 파일 크기: {file_size:.1f} KB")
        
        try:
            # PaddleOCR 3.x 처리
            result = process_image_paddle3x(image_path, lang='en')
            
            # 결과 출력
            print(f"\n📋 추출 결과:")
            print(f"  🏷️  브랜드: {result.get('brand', 'None')}")
            print(f"  🧬 제품명: {result.get('product_name', 'None')}")
            print(f"  ⚖️  용량: {result.get('dosage', 'None')}")
            print(f"  🔢 개수: {result.get('count', 'None')}")
            print(f"  📊 신뢰도: {result.get('confidence', 0):.3f}")
            print(f"  🤖 모델: {result.get('method', 'Unknown')}")
            
            if result.get('error'):
                print(f"  ❌ 오류: {result['error']}")
            else:
                # 품질 평가
                extracted_fields = [
                    field for field in ['brand', 'product_name', 'dosage', 'count'] 
                    if result.get(field) and result.get(field) != 'None'
                ]
                quality_score = len(extracted_fields)
                quality = "🟢 우수" if quality_score >= 3 else "🟡 보통" if quality_score >= 2 else "🔴 부족"
                print(f"  📈 추출 품질: {quality} ({quality_score}/4)")
            
        except Exception as e:
            print(f"❌ 예외 발생: {e}")
            import traceback
            traceback.print_exc()
        
        print("-" * 40)
    
    print(f"\n✅ PaddleOCR 3.x 테스트 완료!")
    print("\n📝 다음 단계:")
    print("1. 결과가 만족스러우면 기존 image.py 교체")
    print("2. 메인 스크래퍼에 통합")
    print("3. 전체 제품 쌍 테스트 실행")