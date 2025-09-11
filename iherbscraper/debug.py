"""
PaddleOCR 3.x 간단한 디버그 스크립트
- 설치 확인
- 기본 사용법 테스트
- 성능 평가
"""

import os
import sys

def check_installation():
    """PaddleOCR 3.x 설치 확인"""
    print("🔍 PaddleOCR 3.x 설치 확인")
    print("=" * 40)
    
    try:
        from paddleocr import PaddleOCR
        print("✅ PaddleOCR 설치됨")
        
        # 버전 확인 시도
        try:
            import paddleocr
            if hasattr(paddleocr, '__version__'):
                print(f"   버전: {paddleocr.__version__}")
            else:
                print("   버전: 확인 불가")
        except:
            print("   버전: 확인 불가")
            
        return True
        
    except ImportError:
        print("❌ PaddleOCR 설치되지 않음")
        print("\n📦 설치 방법:")
        print("pip install paddleocr")
        return False

def test_basic_usage():
    """기본 사용법 테스트"""
    print("\n🧪 기본 사용법 테스트")
    print("=" * 40)
    
    try:
        from paddleocr import PaddleOCR
        
        print("PaddleOCR 초기화 중...")
        # 최소한의 설정으로 초기화
        ocr = PaddleOCR(
            lang='en',
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
            device='cpu'
        )
        print("✅ 초기화 성공")
        
        return ocr
        
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
        return None

def test_image_processing(ocr, image_path):
    """이미지 처리 테스트"""
    print(f"\n📸 이미지 처리 테스트: {os.path.basename(image_path)}")
    print("-" * 40)
    
    if not os.path.exists(image_path):
        print(f"❌ 파일 없음: {image_path}")
        return None
    
    # 파일 크기
    file_size = os.path.getsize(image_path) / 1024
    print(f"📁 파일 크기: {file_size:.1f} KB")
    
    try:
        # PaddleOCR 3.x predict 메서드 사용
        print("🔍 OCR 처리 중...")
        results = ocr.predict(image_path)
        
        if not results or len(results) == 0:
            print("❌ OCR 결과 없음")
            return None
        
        # 첫 번째 결과
        result = results[0]
        print("✅ OCR 처리 완료")
        
        # 결과 구조 분석
        print(f"\n📊 결과 구조 분석:")
        print(f"   타입: {type(result)}")
        
        if hasattr(result, 'res'):
            print(f"   result.res 키들: {list(result.res.keys())}")
            
            if 'rec_texts' in result.res:
                texts = result.res['rec_texts']
                print(f"   추출된 텍스트 개수: {len(texts)}")
                print(f"   첫 3개 텍스트: {texts[:3]}")
                
                # 전체 텍스트 조합
                full_text = ' '.join(texts)
                print(f"\n📝 전체 텍스트:")
                print(f"   '{full_text[:200]}{'...' if len(full_text) > 200 else ''}'")
                
                return full_text
            else:
                print("   ❌ rec_texts 키 없음")
        else:
            print("   ❌ result.res 속성 없음")
            
        return None
        
    except Exception as e:
        print(f"❌ OCR 처리 실패: {e}")
        import traceback
        traceback.print_exc()
        return None

def analyze_text_for_products(text):
    """텍스트에서 제품 정보 분석"""
    if not text:
        return
    
    print(f"\n🔍 제품 정보 분석")
    print("-" * 40)
    
    text_upper = text.upper()
    
    # 브랜드 찾기
    brand_patterns = [
        r'\bNOW\s+FOODS?\b',
        r'\bNOW\b',
        r'\bNATURE\'?S?\s+WAY\b',
        r'\bSOLGAR\b'
    ]
    
    found_brands = []
    for pattern in brand_patterns:
        import re
        matches = re.findall(pattern, text_upper)
        found_brands.extend(matches)
    
    if found_brands:
        print(f"🏷️  브랜드 후보: {found_brands}")
    else:
        # 첫 단어들 확인
        words = text.split()[:3]
        print(f"🏷️  브랜드 추정: {words}")
    
    # 제품명 찾기
    product_patterns = [
        r'\bVITAMIN\s+[A-Z0-9]+\b',
        r'\b[A-Z]+-?CARNITINE\b',
        r'\bZINC\b',
        r'\bMAGNESIUM\b',
        r'\bCALCIUM\b'
    ]
    
    found_products = []
    for pattern in product_patterns:
        matches = re.findall(pattern, text_upper)
        found_products.extend(matches)
    
    if found_products:
        print(f"🧬 제품명 후보: {found_products}")
    else:
        print("🧬 제품명: 패턴 매칭 실패")
    
    # 용량 찾기
    dosage_patterns = [
        r'(\d+(?:,\d+)*)\s*MG\b',
        r'(\d+(?:,\d+)*)\s*MCG\b',
        r'(\d+(?:,\d+)*)\s*IU\b'
    ]
    
    found_dosages = []
    for pattern in dosage_patterns:
        matches = re.findall(pattern, text_upper)
        found_dosages.extend(matches)
    
    if found_dosages:
        print(f"⚖️  용량 후보: {found_dosages}")
    else:
        print("⚖️  용량: 패턴 매칭 실패")
    
    # 개수 찾기
    count_patterns = [
        r'(\d+)\s*TABLETS?\b',
        r'(\d+)\s*CAPSULES?\b',
        r'(\d+)\s*SOFTGELS?\b'
    ]
    
    found_counts = []
    for pattern in count_patterns:
        matches = re.findall(pattern, text_upper)
        found_counts.extend(matches)
    
    if found_counts:
        print(f"🔢 개수 후보: {found_counts}")
    else:
        print("🔢 개수: 패턴 매칭 실패")

def main():
    """메인 함수"""
    print("🚀 PaddleOCR 3.x 간단 디버그")
    print("=" * 50)
    
    # 1. 설치 확인
    if not check_installation():
        return
    
    # 2. 기본 사용법 테스트
    ocr = test_basic_usage()
    if not ocr:
        return
    
    # 3. 테스트 이미지들
    test_images = [
        "../coupang_images/coupang_3180742161.jpg",
        "../iherb_images/iherb_45_NOW-00068.jpg"
    ]
    
    for image_path in test_images:
        # 4. 이미지 처리 테스트
        extracted_text = test_image_processing(ocr, image_path)
        
        # 5. 제품 정보 분석
        if extracted_text:
            analyze_text_for_products(extracted_text)
        
        print("\n" + "="*50)
    
    print("✅ 디버그 완료!")
    print("\n📝 결론:")
    print("- PaddleOCR 3.x가 정상 작동하면 메인 코드에 통합")
    print("- 텍스트 추출이 잘 되면 패턴 매칭 개선")
    print("- 품질이 좋으면 전체 시스템에 적용")

if __name__ == "__main__":
    main()