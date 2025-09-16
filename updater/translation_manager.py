"""
번역 관리자
"""

import sys
from settings import COUPANG_PATH, UPDATER_CONFIG

# 쿠팡 번역기 모듈 경로 추가
sys.path.insert(0, str(COUPANG_PATH))

try:
    from translator import GeminiCSVTranslator
    TRANSLATOR_AVAILABLE = True
    print("✅ 번역기 모듈 로드 성공")
except ImportError as e:
    print(f"❌ 번역기 모듈 로드 실패: {e}")
    TRANSLATOR_AVAILABLE = False


class TranslationManager:
    """번역 전담 관리자"""
    
    def __init__(self):
        if not TRANSLATOR_AVAILABLE:
            raise ImportError("번역기 모듈을 사용할 수 없습니다.")
        
        self.api_key = UPDATER_CONFIG['GEMINI_API_KEY']
        self.batch_size = UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']
        self.translator = None
    
    def batch_translate_products(self, products):
        """배치 번역 실행"""
        if not self.translator:
            self.translator = GeminiCSVTranslator(self.api_key)
        
        # 상품명 추출
        product_names = [p['product_name'] for p in products]
        
        print(f"🔤 배치 번역 시작: {len(product_names)}개 → {self.batch_size}개씩")
        
        # 배치 번역 실행
        translated_names = self.translator.translate_batch(
            product_names, 
            batch_size=self.batch_size
        )
        
        print(f"✅ 배치 번역 완료: {len(translated_names)}개")
        
        # 원본 상품과 번역명 페어링
        translated_products = []
        for product, english_name in zip(products, translated_names):
            translated_products.append((product, english_name))
        
        return translated_products
    
    def translate_untranslated_products(self, df, output_file):
        """미번역 상품만 번역"""
        from datetime import datetime
        
        today = datetime.now().strftime("_%Y%m%d")
        new_products = df[df['update_status'] == f'NEW_PRODUCT__{today}']
        
        untranslated = new_products[
            new_products['coupang_product_name_english'].isna() | 
            (new_products['coupang_product_name_english'] == '')
        ]
        
        if len(untranslated) == 0:
            print(f"ℹ️ 번역할 상품이 없습니다")
            return df
        
        print(f"🔤 미번역 상품 {len(untranslated)}개 배치 번역 시작...")
        
        # 번역기 초기화
        if not self.translator:
            self.translator = GeminiCSVTranslator(self.api_key)
        
        # 배치 번역
        product_names = untranslated['coupang_product_name'].tolist()
        translated_names = self.translator.translate_batch(product_names, batch_size=self.batch_size)
        
        # DataFrame 업데이트
        for (idx, row), translated_name in zip(untranslated.iterrows(), translated_names):
            df.at[idx, 'coupang_product_name_english'] = translated_name
        
        # 중간 저장
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"✅ 번역 완료: {len(untranslated)}개")
        
        return df