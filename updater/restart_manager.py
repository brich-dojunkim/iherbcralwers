"""
재시작 관리자
"""

import json
import os
import pandas as pd
from datetime import datetime
from settings import UPDATER_CONFIG


class RestartManager:
    """재시작 전담 관리자"""
    
    def __init__(self):
        self.metadata_file = UPDATER_CONFIG['RESTART_METADATA_FILE']
    
    def save_metadata(self, input_file, brand_name, output_file, fill_iherb):
        """재시작 메타데이터 저장"""
        metadata = {
            'input_file': input_file,
            'brand_name': brand_name,
            'output_file': output_file,
            'fill_iherb': fill_iherb,
            'started_at': datetime.now().isoformat(),
            'batch_size': UPDATER_CONFIG['TRANSLATION_BATCH_SIZE'],
            'checkpoint_interval': UPDATER_CONFIG['CHECKPOINT_INTERVAL']
        }
        
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    def cleanup_metadata(self):
        """재시작 메타데이터 정리"""
        if os.path.exists(self.metadata_file):
            os.remove(self.metadata_file)
            print(f"🧹 재시작 메타데이터 정리 완료")
    
    def check_incomplete_work(self, df):
        """미완료 작업 정밀 감지"""
        today = datetime.now().strftime("_%Y%m%d")
        
        # 1. 쿠팡 업데이트 완료 여부 확인
        updated_products = df[df['update_status'] == 'UPDATED']
        existing_products = df[~df['update_status'].str.startswith('NEW_PRODUCT', na=False)]
        coupang_complete = len(updated_products) == len(existing_products)
        
        # 2. 신규 상품 처리 상태 확인
        new_products = df[df['update_status'] == f'NEW_PRODUCT_{today}']
        new_count = len(new_products)
        
        if new_count == 0:
            return {
                'has_incomplete': not coupang_complete,
                'coupang_complete': coupang_complete,
                'translation_complete': True,
                'iherb_complete': True,
                'new_products_count': 0,
                'translated_count': 0,
                'iherb_processed_count': 0
            }
        
        # 3. 번역 완료 여부 확인
        translated = new_products[
            new_products['coupang_product_name_english'].notna() & 
            (new_products['coupang_product_name_english'] != '')
        ]
        translation_complete = len(translated) == new_count
        
        # 4. 아이허브 매칭 완료 여부 확인
        iherb_processed = new_products[
            new_products['status'].notna() & 
            (new_products['status'] != '')
        ]
        iherb_complete = len(iherb_processed) == new_count
        
        return {
            'has_incomplete': not (coupang_complete and translation_complete and iherb_complete),
            'coupang_complete': coupang_complete,
            'translation_complete': translation_complete,
            'iherb_complete': iherb_complete,
            'new_products_count': new_count,
            'translated_count': len(translated),
            'iherb_processed_count': len(iherb_processed)
        }
    
    def print_progress_status(self, df):
        """진행 상황 출력"""
        total = len(df)
        updated = len(df[df['update_status'] == 'UPDATED']) if 'update_status' in df.columns else 0
        new_products = len(df[df['update_status'].str.startswith('NEW_PRODUCT', na=False)]) if 'update_status' in df.columns else 0
        completed = len(df[df['status'] == 'success']) if 'status' in df.columns else 0
        
        print(f"📊 현재 상태:")
        print(f"   - 총 상품: {total}개")
        print(f"   - 업데이트 완료: {updated}개")
        print(f"   - 신규 상품: {new_products}개")
        print(f"   - 아이허브 매칭 완료: {completed}개")
    
    def print_final_stats(self, df):
        """최종 통계 출력"""
        print(f"\n" + "="*60)
        print(f"📈 최종 통계 (완전한 재시작 기능)")
        print(f"="*60)
        
        total = len(df)
        print(f"📦 총 상품: {total}개")
        
        # 상태별 통계
        if 'update_status' in df.columns:
            print(f"\n📊 업데이트 상태:")
            status_counts = df['update_status'].value_counts()
            for status, count in status_counts.items():
                print(f"   - {status}: {count}개")
        
        # 매칭 성공률
        if 'status' in df.columns:
            success_count = len(df[df['status'] == 'success'])
            print(f"\n🌿 아이허브 매칭:")
            print(f"   - 성공: {success_count}개")
            print(f"   - 성공률: {success_count/total*100:.1f}%")
        
        # 가격 정보 통계
        today = datetime.now().strftime("_%Y%m%d")
        coupang_price_col = f'쿠팡현재가격{today}'
        
        if coupang_price_col in df.columns:
            price_count = len(df[df[coupang_price_col].notna()])
            print(f"\n💰 가격 정보:")
            print(f"   - 쿠팡 가격: {price_count}개")
        
        if 'iherb_discount_price_krw' in df.columns:
            iherb_price_count = len(df[
                (df['iherb_discount_price_krw'].notna()) | 
                (df['iherb_list_price_krw'].notna() if 'iherb_list_price_krw' in df.columns else False)
            ])
            print(f"   - 아이허브 가격: {iherb_price_count}개")
        
        # 효율성 통계
        print(f"\n⚡ 효율성 개선:")
        print(f"   - 배치 번역: {UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']}개씩 처리")
        print(f"   - 기존 모듈 재사용: 쿠팡 translator + 아이허브 scraper")
        print(f"   - 완전한 재시작: {UPDATER_CONFIG['CHECKPOINT_INTERVAL']}개마다 체크포인트")
        print(f"   - 정밀 재개: 번역/매칭 단계별 독립 재시작")
        
        print(f"="*60)