"""
재시작 관리자 - 상태 감지 로직 수정 (아이허브 매칭 상태 정확한 판별)
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
        """미완료 작업 정밀 감지 - 아이허브 상태 판별 로직 수정"""
        today = datetime.now().strftime("_%Y%m%d")
        
        # 1. 쿠팡 업데이트 완료 여부 확인 (NOT_FOUND도 완료로 간주)
        if 'update_status' in df.columns:
            # 기존 상품 (NEW_PRODUCT가 아닌 상품들)
            existing_products = df[~df['update_status'].str.startswith('NEW_PRODUCT', na=False)]
            
            # UPDATED 또는 NOT_FOUND 상태인 상품 (둘 다 처리 완료)
            completed_updates = df[
                (df['update_status'] == 'UPDATED') | 
                (df['update_status'] == 'NOT_FOUND')
            ]
            
            # 기존 상품의 수와 처리 완료된 상품의 수 비교
            coupang_complete = len(completed_updates) >= len(existing_products)
            
            print(f"  🔍 쿠팡 업데이트 상태 분석:")
            print(f"    - 기존 상품: {len(existing_products)}개")
            print(f"    - 처리 완료: {len(completed_updates)}개 (UPDATED: {len(df[df['update_status'] == 'UPDATED'])}, NOT_FOUND: {len(df[df['update_status'] == 'NOT_FOUND'])})")
            print(f"    - 완료 여부: {'✅' if coupang_complete else '❌'}")
        else:
            coupang_complete = False
            print(f"  ⚠️ update_status 컬럼이 없음 - 쿠팡 업데이트 미완료로 간주")
        
        # 2. 신규 상품 처리 상태 확인 - 패턴 수정
        new_products = df[df['update_status'] == f'NEW_PRODUCT__{today}'] if 'update_status' in df.columns else pd.DataFrame()
        new_count = len(new_products)
        
        if new_count == 0:
            print(f"  ℹ️ 신규 상품 없음")
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
        if 'coupang_product_name_english' in df.columns:
            translated = new_products[
                new_products['coupang_product_name_english'].notna() & 
                (new_products['coupang_product_name_english'] != '')
            ]
            translation_complete = len(translated) == new_count
            translated_count = len(translated)
        else:
            translation_complete = False
            translated_count = 0
        
        # 4. 아이허브 매칭 완료 여부 확인 - 오늘 날짜 기준 신규 매칭 확인
        # ⚠️ 핵심 수정: 오늘 날짜 기준 아이허브 매칭 결과 확인
        iherb_processed_count = 0
        iherb_complete = False
        
        if new_count > 0:
            print(f"  🔍 신규 상품 아이허브 매칭 상태 상세 분석 (오늘 날짜 기준):")
            
            date_suffix = today[1:]  # _20250916 → 20250916
            
            # 오늘 날짜 기준 아이허브 매칭 상태 컬럼들 확인
            today_iherb_status_columns = [
                f'아이허브매칭상태_{date_suffix}',
                f'아이허브상품명_{date_suffix}',
                f'아이허브정가_{date_suffix}',
                f'아이허브할인가_{date_suffix}',
                f'아이허브매칭일시_{date_suffix}'
            ]
            
            # 어떤 오늘 날짜 아이허브 컬럼이든 데이터가 있는 상품들
            has_today_iherb_data = new_products[
                new_products[today_iherb_status_columns].notna().any(axis=1)
            ]
            
            print(f"    - 오늘 날짜 아이허브 매칭 데이터 있음: {len(has_today_iherb_data)}개")
            
            # 구체적인 매칭 상태별 분석
            if f'아이허브매칭상태_{date_suffix}' in df.columns:
                status_counts = new_products[f'아이허브매칭상태_{date_suffix}'].value_counts()
                for status, count in status_counts.items():
                    print(f"      * {status}: {count}개")
                
                # 처리된 상품 = 매칭 상태가 있는 상품
                processed_today = new_products[
                    new_products[f'아이허브매칭상태_{date_suffix}'].notna() &
                    (new_products[f'아이허브매칭상태_{date_suffix}'] != '')
                ]
                iherb_processed_count = len(processed_today)
            else:
                # 매칭 상태 컬럼이 없으면 다른 오늘 날짜 컬럼으로 판단
                iherb_processed_count = len(has_today_iherb_data)
                print(f"    - 매칭 상태 컬럼 없음, 기타 오늘 날짜 데이터로 판단")
            
            iherb_complete = iherb_processed_count == new_count
            print(f"    - 최종 판단: {iherb_processed_count}개 처리됨 (완료: {'✅' if iherb_complete else '❌'})")
            
            # 미처리 상품들의 상태 확인
            if not iherb_complete:
                unprocessed = new_products[
                    ~new_products[today_iherb_status_columns].notna().any(axis=1)
                ]
                print(f"    - 미처리 상품: {len(unprocessed)}개")
                if len(unprocessed) <= 5:  # 5개 이하면 상품명 표시
                    for idx, row in unprocessed.iterrows():
                        product_name = row['coupang_product_name'][:30] + "..."
                        print(f"      * {product_name}")
        else:
            print(f"    - 신규 상품이 없으므로 아이허브 매칭 불필요")
        
        print(f"  🔍 신규 상품 처리 상태:")
        print(f"    - 총 신규 상품: {new_count}개")
        print(f"    - 번역 완료: {translated_count}개 ({'✅' if translation_complete else '❌'})")
        print(f"    - 아이허브 매칭: {iherb_processed_count}개 ({'✅' if iherb_complete else '❌'})")
        
        # 전체 완료 여부 판단
        has_incomplete = not (coupang_complete and translation_complete and iherb_complete)
        
        # 🔧 추가 디버깅: 미완료 이유 명시
        if has_incomplete:
            incomplete_reasons = []
            if not coupang_complete:
                incomplete_reasons.append("쿠팡 업데이트 미완료")
            if not translation_complete:
                incomplete_reasons.append(f"번역 미완료 ({new_count - translated_count}개)")
            if not iherb_complete:
                incomplete_reasons.append(f"아이허브 매칭 미완료 ({new_count - iherb_processed_count}개)")
            
            print(f"  ⚠️ 미완료 이유: {', '.join(incomplete_reasons)}")
        
        return {
            'has_incomplete': has_incomplete,
            'coupang_complete': coupang_complete,
            'translation_complete': translation_complete,
            'iherb_complete': iherb_complete,
            'new_products_count': new_count,
            'translated_count': translated_count,
            'iherb_processed_count': iherb_processed_count
        }
    
    def print_progress_status(self, df):
        """진행 상황 출력"""
        total = len(df)
        
        # update_status 기반 통계
        if 'update_status' in df.columns:
            updated = len(df[df['update_status'] == 'UPDATED'])
            not_found = len(df[df['update_status'] == 'NOT_FOUND'])
            new_products = len(df[df['update_status'].str.startswith('NEW_PRODUCT', na=False)])
            error_products = len(df[df['update_status'].str.startswith('ERROR', na=False)])
        else:
            updated = not_found = new_products = error_products = 0
        
        # status 기반 통계
        if 'status' in df.columns:
            completed = len(df[df['status'] == 'success'])
            failed = len(df[df['status'] == 'not_found'])
            errors = len(df[df['status'] == 'error'])
        else:
            completed = failed = errors = 0
        
        print(f"📊 현재 상태:")
        print(f"   - 총 상품: {total}개")
        print(f"   - 쿠팡 업데이트 완료: {updated + not_found}개 (UPDATED: {updated}, NOT_FOUND: {not_found})")
        print(f"   - 신규 상품: {new_products}개")
        print(f"   - 아이허브 매칭 성공: {completed}개")
        if failed > 0:
            print(f"   - 아이허브 매칭 실패: {failed}개")
        if errors > 0:
            print(f"   - 처리 오류: {errors}개")
    
    def print_final_stats(self, df):
        """최종 통계 출력 - 개선된 분석"""
        print(f"\n" + "="*60)
        print(f"📈 최종 통계 (완전한 재시작 기능)")
        print(f"="*60)
        
        total = len(df)
        print(f"📦 총 상품: {total}개")
        
        # 상태별 상세 통계
        if 'update_status' in df.columns:
            print(f"\n📊 업데이트 상태:")
            status_counts = df['update_status'].value_counts()
            for status, count in status_counts.items():
                print(f"   - {status}: {count}개")
        
        # 매칭 성공률 상세 분석
        if 'status' in df.columns:
            success_count = len(df[df['status'] == 'success'])
            not_found_count = len(df[df['status'] == 'not_found'])
            error_count = len(df[df['status'] == 'error'])
            
            print(f"\n🌿 아이허브 매칭:")
            print(f"   - 성공: {success_count}개")
            print(f"   - 성공률: {success_count/total*100:.1f}%")
            
            if not_found_count > 0:
                print(f"   - 매칭 없음: {not_found_count}개 ({not_found_count/total*100:.1f}%)")
            if error_count > 0:
                print(f"   - 오류: {error_count}개 ({error_count/total*100:.1f}%)")
        
        # 가격 정보 통계
        today = datetime.now().strftime("_%Y%m%d")
        coupang_price_col = f'쿠팡현재가격{today}'
        
        if coupang_price_col in df.columns:
            price_count = len(df[df[coupang_price_col].notna() & (df[coupang_price_col] != '')])
            print(f"\n💰 가격 정보:")
            print(f"   - 쿠팡 가격: {price_count}개")
        
        if 'iherb_discount_price_krw' in df.columns or 'iherb_list_price_krw' in df.columns:
            iherb_price_count = 0
            if 'iherb_discount_price_krw' in df.columns:
                iherb_price_count += len(df[df['iherb_discount_price_krw'].notna() & (df['iherb_discount_price_krw'] != '')])
            if 'iherb_list_price_krw' in df.columns:
                iherb_price_count += len(df[
                    df['iherb_list_price_krw'].notna() & (df['iherb_list_price_krw'] != '') &
                    (df['iherb_discount_price_krw'].isna() | (df['iherb_discount_price_krw'] == ''))
                ])
            print(f"   - 아이허브 가격: {iherb_price_count}개")
        
        # 효율성 통계
        print(f"\n⚡ 효율성 개선:")
        print(f"   - 배치 번역: {UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']}개씩 처리")
        print(f"   - 기존 모듈 재사용: 쿠팡 translator + 아이허브 scraper")
        print(f"   - 완전한 재시작: {UPDATER_CONFIG['CHECKPOINT_INTERVAL']}개마다 체크포인트")
        print(f"   - 정밀 재개: 번역/매칭 단계별 독립 재시작")
        
        print(f"="*60)
    
    def validate_dataframe_structure(self, df):
        """DataFrame 구조 유효성 검사"""
        required_columns = ['update_status']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"⚠️ 필수 컬럼 누락: {missing_columns}")
            print("이는 작업 파일이 이전 버전이거나 손상되었을 가능성이 있습니다.")
            return False
        
        return True
    
    def get_restart_recommendations(self, incomplete_status):
        """재시작 권장사항 제공"""
        recommendations = []
        
        if not incomplete_status['coupang_complete']:
            recommendations.append("🔄 쿠팡 재크롤링 필요")
        
        if incomplete_status['new_products_count'] > 0:
            if not incomplete_status['translation_complete']:
                untranslated = incomplete_status['new_products_count'] - incomplete_status['translated_count']
                recommendations.append(f"🔤 번역 작업 {untranslated}개 상품 남음")
            
            if not incomplete_status['iherb_complete']:
                unmatched = incomplete_status['new_products_count'] - incomplete_status['iherb_processed_count']
                recommendations.append(f"🌿 아이허브 매칭 {unmatched}개 상품 남음")
        
        return recommendations
    
    def load_metadata(self):
        """재시작 메타데이터 로드"""
        if not os.path.exists(self.metadata_file):
            return None
        
        try:
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"메타데이터 로드 실패: {e}")
            return None
    
    def analyze_work_distribution(self, df):
        """작업 분포 분석"""
        analysis = {
            'total_products': len(df),
            'existing_products': 0,
            'new_products': 0,
            'completed_updates': 0,
            'successful_matches': 0,
            'failed_matches': 0,
            'error_products': 0
        }
        
        if 'update_status' in df.columns:
            # 기존 상품 vs 신규 상품
            analysis['existing_products'] = len(df[~df['update_status'].str.startswith('NEW_PRODUCT', na=False)])
            analysis['new_products'] = len(df[df['update_status'].str.startswith('NEW_PRODUCT', na=False)])
            
            # 완료된 업데이트
            analysis['completed_updates'] = len(df[
                (df['update_status'] == 'UPDATED') | 
                (df['update_status'] == 'NOT_FOUND')
            ])
        
        if 'status' in df.columns:
            analysis['successful_matches'] = len(df[df['status'] == 'success'])
            analysis['failed_matches'] = len(df[df['status'] == 'not_found'])
            analysis['error_products'] = len(df[df['status'] == 'error'])
        
        return analysis