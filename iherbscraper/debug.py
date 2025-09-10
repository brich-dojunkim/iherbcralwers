"""
단일 이미지 OCR 디버그 테스트
"""

import os
from image import process_image

def test_single_image():
    """단일 이미지로 OCR 테스트"""
    
    # 테스트할 이미지 경로들
    test_images = [
        "../coupang_images/coupang_3180742161.jpg",
        "../iherb_images/iherb_45_NOW-00068.jpg"
    ]
    
    for i, image_path in enumerate(test_images, 1):
        print(f"\n=== 테스트 {i}: {os.path.basename(image_path)} ===")
        
        # 파일 존재 확인
        if not os.path.exists(image_path):
            print(f"❌ 파일 없음: {image_path}")
            continue
        
        # 파일 크기 확인
        file_size = os.path.getsize(image_path) / 1024  # KB
        print(f"📁 파일 크기: {file_size:.1f} KB")
        
        try:
            # OCR 실행
            result = process_image(image_path)
            
            # 결과 출력
            print(f"브랜드: {result.get('brand', 'None')}")
            print(f"제품명: {result.get('product_name', 'None')}")
            print(f"용량: {result.get('dosage', 'None')}")
            print(f"개수: {result.get('count', 'None')}")
            print(f"신뢰도: {result.get('confidence', 0):.3f}")
            print(f"원본 텍스트: '{result.get('raw_text', '')[:200]}...'")
            
            if result.get('error'):
                print(f"❌ 오류: {result['error']}")
            
        except Exception as e:
            print(f"❌ 예외 발생: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_single_image()