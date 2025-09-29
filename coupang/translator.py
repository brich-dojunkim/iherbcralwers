"""
Translator - DB 버전
CSV 대신 DB에서 읽고 저장
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import google.generativeai as genai
from db import Database
from config import PathConfig
from coupang_config import CoupangConfig


class TranslatorDB:
    """DB 연동 번역기"""
    
    def __init__(self, db: Database, api_key: str = None):
        """
        Args:
            db: Database 인스턴스
            api_key: Gemini API Key
        """
        self.db = db
        
        # Gemini 초기화
        api_key = api_key or CoupangConfig.GEMINI_API_KEY
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(CoupangConfig.GEMINI_TEXT_MODEL)
        
        self.translated_count = 0
        self.skipped_count = 0
        self.failed_count = 0
    
    def translate_brand(self, brand_name: str, batch_size: int = 10) -> dict:
        """
        브랜드의 crawled 상품들을 번역
        
        Args:
            brand_name: 브랜드명
            batch_size: 배치 크기
            
        Returns:
            통계 딕셔너리
        """
        print(f"\n{'='*80}")
        print(f"📝 번역 시작: {brand_name}")
        print(f"{'='*80}\n")
        
        # crawled 상품 조회
        products = self.db.get_products_by_stage(brand_name, 'crawled')
        
        if not products:
            print("✓ 번역할 상품이 없습니다 (모두 번역 완료)")
            return self._get_stats()
        
        print(f"번역 대상: {len(products)}개\n")
        
        # 배치 단위로 번역
        total = len(products)
        for i in range(0, total, batch_size):
            batch = products[i:i + batch_size]
            self._translate_batch(batch, i, total, batch_size)
        
        # 최종 통계
        self._print_summary(brand_name)
        
        return self._get_stats()
    
    def _translate_batch(self, batch: list, start_idx: int, 
                        total: int, batch_size: int):
        """배치 번역"""
        batch_num = start_idx // batch_size + 1
        print(f"--- 배치 {batch_num} ({start_idx + 1}~{min(start_idx + batch_size, total)}/{total}) ---")
        
        # 상품명 추출
        product_names = [p['coupang_product_name'] for p in batch]
        
        # 번역 요청
        try:
            translated_names = self._call_gemini_batch(product_names)
            
            # DB 저장
            for product, english_name in zip(batch, translated_names):
                try:
                    self.db.update_translation(product['id'], english_name)
                    self.translated_count += 1
                    print(f"  ✓ {product['coupang_product_name'][:40]}... → {english_name[:40]}...")
                except Exception as e:
                    print(f"  ✗ DB 저장 실패: {e}")
                    self.failed_count += 1
            
            print(f"✓ 배치 {batch_num} 완료\n")
            
            # API 제한 고려
            time.sleep(0.5)
            
        except Exception as e:
            print(f"✗ 배치 {batch_num} 실패: {e}")
            self.failed_count += len(batch)
            print()
    
    def _call_gemini_batch(self, product_names: list) -> list:
        """Gemini API 배치 호출"""
        # 프롬프트 구성
        numbered_names = "\n".join([
            f"{i+1}. {name}" 
            for i, name in enumerate(product_names)
        ])
        
        prompt = f"""Translate these Korean product names to English.
Keep brand names unchanged. Answer with ONLY the translations, one per line.

{numbered_names}

Translations:"""
        
        # API 호출
        response = self.model.generate_content(
            prompt,
            generation_config=CoupangConfig.TRANSLATION_GENERATION_CONFIG
        )
        
        # 응답 파싱
        lines = response.text.strip().split('\n')
        translations = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # "1. " 또는 "1) " 제거
            if line[0].isdigit():
                parts = line.split('.', 1)
                if len(parts) > 1:
                    line = parts[1].strip()
                else:
                    parts = line.split(')', 1)
                    if len(parts) > 1:
                        line = parts[1].strip()
            
            translations.append(line)
        
        # 개수 맞추기
        while len(translations) < len(product_names):
            translations.append(product_names[len(translations)])
        
        return translations[:len(product_names)]
    
    def _print_summary(self, brand_name: str):
        """번역 결과 요약"""
        print(f"{'='*80}")
        print(f"📊 번역 완료 요약")
        print(f"{'='*80}")
        print(f"성공: {self.translated_count}개")
        print(f"실패: {self.failed_count}개")
        
        # 브랜드 통계
        stats = self.db.get_brand_stats(brand_name)
        by_stage = stats.get('by_stage', {})
        
        print(f"\n파이프라인 단계:")
        print(f"  🆕 crawled: {by_stage.get('crawled', 0)}개")
        print(f"  📝 translated: {by_stage.get('translated', 0)}개")
        print(f"  ✅ matched: {by_stage.get('matched', 0)}개")
    
    def _get_stats(self) -> dict:
        """통계 반환"""
        return {
            'translated': self.translated_count,
            'skipped': self.skipped_count,
            'failed': self.failed_count
        }


def main():
    """테스트 실행"""
    print("🧪 DB 연동 번역기 테스트\n")
    
    # DB 연결
    db_path = os.path.join(PathConfig.DATA_ROOT, "products.db")
    db = Database(db_path)
    
    # 브랜드 확인
    brand_name = "thorne"
    brand = db.get_brand(brand_name)
    
    if not brand:
        print(f"❌ 브랜드 '{brand_name}'가 없습니다")
        print("먼저 crawler_db.py를 실행하세요")
        return
    
    # 번역 대상 확인
    products = db.get_products_by_stage(brand_name, 'crawled')
    print(f"번역 대상: {len(products)}개\n")
    
    if not products:
        print("✓ 번역할 상품이 없습니다")
        return
    
    # 번역 실행
    translator = TranslatorDB(db)
    
    try:
        stats = translator.translate_brand(
            brand_name=brand_name,
            batch_size=10
        )
        
        print(f"\n{'='*80}")
        print(f"🎉 테스트 완료!")
        print(f"{'='*80}")
        print(f"번역: {stats['translated']}개")
        print(f"실패: {stats['failed']}개")
        
    except KeyboardInterrupt:
        print("\n⚠️ 테스트 중단")
    except Exception as e:
        print(f"\n❌ 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()