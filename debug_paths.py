# iherb_inspector.py - iHerb 모듈 구조 확인
import sys
import os

# 프로젝트 경로 추가
BASE_DIR = "/Users/brich/Desktop/iherb_price"
IHERB_MODULE_PATH = os.path.join(BASE_DIR, 'iherbscraper')
sys.path.insert(0, IHERB_MODULE_PATH)

print("=== iHerb 모듈 구조 확인 ===")

try:
    import config as iherb_config
    print("✓ config 모듈 임포트 성공")
    
    # 모듈의 모든 속성 확인
    print("\nconfig 모듈의 모든 속성:")
    for attr_name in dir(iherb_config):
        if not attr_name.startswith('_'):
            attr_value = getattr(iherb_config, attr_name)
            attr_type = type(attr_value).__name__
            print(f"  - {attr_name}: {attr_type}")
            
            # 클래스인 경우 더 자세히 확인
            if attr_type == 'type':
                print(f"    └─ 클래스: {attr_name}")
                # 클래스의 주요 속성 확인
                for class_attr in dir(attr_value):
                    if not class_attr.startswith('_') and class_attr.isupper():
                        try:
                            class_attr_value = getattr(attr_value, class_attr)
                            if isinstance(class_attr_value, (str, int, list, dict)):
                                print(f"      └─ {class_attr}: {type(class_attr_value).__name__}")
                        except:
                            pass
    
    # Config 클래스 확인
    print("\n=== Config 클래스 확인 ===")
    if hasattr(iherb_config, 'Config'):
        Config = iherb_config.Config
        print("✓ Config 클래스 발견")
        
        # OUTPUT_COLUMNS 확인
        if hasattr(Config, 'OUTPUT_COLUMNS'):
            output_columns = Config.OUTPUT_COLUMNS
            print(f"✓ OUTPUT_COLUMNS 발견: {len(output_columns)}개 컬럼")
            print("  처음 5개 컬럼:")
            for i, col in enumerate(output_columns[:5]):
                print(f"    {i+1}. {col}")
        else:
            print("✗ OUTPUT_COLUMNS 없음")
            
        # 기타 주요 속성 확인
        important_attrs = ['GEMINI_API_KEY', 'BASE_URL', 'SELECTORS', 'PATTERNS']
        for attr in important_attrs:
            if hasattr(Config, attr):
                print(f"✓ {attr} 있음")
            else:
                print(f"✗ {attr} 없음")
    else:
        print("✗ Config 클래스 없음")
    
    # FailureType 클래스 확인
    print("\n=== FailureType 클래스 확인 ===")
    if hasattr(iherb_config, 'FailureType'):
        FailureType = iherb_config.FailureType
        print("✓ FailureType 클래스 발견")
        
        # 클래스 메서드 확인
        methods = [m for m in dir(FailureType) if not m.startswith('_')]
        print(f"  메서드/속성: {len(methods)}개")
        for method in methods[:10]:  # 처음 10개만
            print(f"    - {method}")
    else:
        print("✗ FailureType 클래스 없음")
        
except ImportError as e:
    print(f"✗ config 모듈 임포트 실패: {e}")

# 다른 주요 모듈들도 확인
other_modules = ['main', 'browser_manager', 'product_matcher']
print(f"\n=== 다른 모듈들 확인 ===")
for module_name in other_modules:
    try:
        module = __import__(module_name)
        print(f"✓ {module_name} 임포트 성공")
        
        # 주요 클래스 확인
        if module_name == 'main' and hasattr(module, 'EnglishIHerbScraper'):
            print("  └─ EnglishIHerbScraper 클래스 있음")
        elif module_name == 'browser_manager' and hasattr(module, 'BrowserManager'):
            print("  └─ BrowserManager 클래스 있음")
            
    except ImportError as e:
        print(f"✗ {module_name} 임포트 실패: {e}")

print("\n=== 최종 권장사항 ===")
print("1. 현재 translator만 작동하므로 번역 기능은 사용 가능")
print("2. 완전한 기능을 위해서는 모듈 경로 수정이 필요")
print("3. 당장은 '파일 상태 확인' 기능만 사용 가능")