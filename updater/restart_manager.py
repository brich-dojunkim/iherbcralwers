"""
재시작 관리자 - 마스터 파일 시스템 지원
"""

import json
import os
import pandas as pd
from datetime import datetime
from settings import UPDATER_CONFIG

class RestartManager:
    """재시작 전담 관리자 - 마스터 파일 시스템"""
    
    def __init__(self):
        self.metadata_file = UPDATER_CONFIG['RESTART_METADATA_FILE']
    
    def save_metadata(self, input_file, brand_name, master_file, fill_iherb):
        """재시작 메타데이터 저장"""
        metadata = {
            'input_file': input_file,
            'brand_name': brand_name,
            'master_file': master_file,
            'fill_iherb': fill_iherb,
            'started_at': datetime.now().isoformat(),
            'batch_size': UPDATER_CONFIG['TRANSLATION_BATCH_SIZE'],
            'checkpoint_interval': UPDATER_CONFIG['CHECKPOINT_INTERVAL'],
            'system_type': 'master_file'
        }
        
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    def cleanup_metadata(self):
        """재시작 메타데이터 정리"""
        if os.path.exists(self.metadata_file):
            os.remove(self.metadata_file)
            print(f"🧹 재시작 메타데이터 정리 완료")
    
    def check_incomplete_work(self, master_df):
        """마스터 파일에서 미완료 작업 정밀 감지"""
        today = datetime.now().strftime("_%Y%m%d")
        
        # 1. 쿠팡 업데이트 완료 여부 확인
        if 'update_status' in master_df.columns:
            # 오늘 업데이트가 필요한 상품들 (기존 상품)
            existing_products = master_df[~master_df['update_status'].str.startswith('NEW_PRODUCT', na=False)]
            
            # 오늘 업데이트 완료된 상품들
            today_updated = master_df[
                (master_df['update_status'] == 'UPDATED') |
                (master_df['update_status'] == 'NOT_FOUND') |
                (master_df[f'쿠팡크롤링시간{today}'].notna() if f'쿠팡크롤링시간{today}' in master_df.columns else False)
            ]
            
            coupang_complete = len(today_updated) >= len(existing_products)
            
            print(f"  🔍 쿠팡 업데이트 상태 분석 (마스터 파일):")
            print(f"    - 기존 상품: {len(existing_products)}개")
            print(f"    - 오늘 업데이트됨: {len(today_updated)}개")
            print(f"    - 완료 여부: {'✅' if coupang_complete else '❌'}")
        else:
            coupang_complete = False
            print(f"  ⚠️ update_status 컬럼이 없음 - 쿠팡 업데이트 미완료로 간주")
        
        # 2. 신규 상품 처리 상태 확인
        new_products = master_df[master_df['update_status'] == f'NEW_PRODUCT{today}'] if 'update_status' in master_df.columns else pd.DataFrame()
        new_count = len(new_products)
        
        if new_count == 0:
            print(f"  ℹ️ 오늘 신규 상품 없음")
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
        if 'coupang_product_name_english' in master_df.columns:
            translated = new_products[
                new_products['coupang_product_name_english'].notna() & 
                (new_products['coupang_product_name_english'] != '')
            ]
            translation_complete = len(translated) == new_count
            translated_count = len(translated)
        else:
            translation_complete = False
            translated_count = 0
        
        # 4. 아이허브 매칭 완료 여부 확인 - 오늘 날짜 기준
        iherb_processed_count = 0
        iherb_complete = False
        
        if new_count > 0:
            print(f"  🔍 신규 상품 아이허브 매칭 상태 (오늘 날짜 기준):")
            
            # 오늘 날짜 기준 아이허브 매칭 상태 컬럼들 확인
            today_iherb_status_columns = [
                f'아이허브매칭상태{today}',
                f'아이허브상품명{today}',
                f'아이허브정가{today}',
                f'아이허브할인가{today}',
                f'아이허브매칭일시{today}'
            ]
            
            # 오늘 날짜 아이허브 데이터가 있는 상품들
            has_today_iherb_data = new_products[
                new_products[today_iherb_status_columns].notna().any(axis=1)
            ]
            
            print(f"    - 오늘 날짜 아이허브 데이터 있음: {len(has_today_iherb_data)}개")
            
            # 매칭 상태별 분석
            if f'아이허브매칭상태{today}' in master_df.columns:
                status_counts = new_products[f'아이허브매칭상태{today}'].value_counts()
                for status, count in status_counts.items():
                    print(f"      * {status}: {count}개")
                
                processed_today = new_products[
                    new_products[f'아이허브매칭상태{today}'].notna() &
                    (new_products[f'아이허브매칭상태{today}'] != '')
                ]
                iherb_processed_count = len(processed_today)
            else:
                iherb_processed_count = len(has_today_iherb_data)
                print(f"    - 매칭 상태 컬럼 없음, 기타 오늘 날짜 데이터로 판단")
            
            iherb_complete = iherb_processed_count == new_count
            print(f"    - 최종 판단: {iherb_processed_count}개 처리됨 (완료: {'✅' if iherb_complete else '❌'})")
            
            # 미처리 상품들 확인
            if not iherb_complete:
                unprocessed = new_products[
                    ~new_products[today_iherb_status_columns].notna().any(axis=1)
                ]
                print(f"    - 미처리 상품: {len(unprocessed)}개")
                if len(unprocessed) <= 5:
                    for idx, row in unprocessed.iterrows():
                        product_name = row['coupang_product_name'][:30] + "..."
                        print(f"      * {product_name}")
        
        print(f"  🔍 신규 상품 처리 상태:")
        print(f"    - 총 신규 상품: {new_count}개")
        print(f"    - 번역 완료: {translated_count}개 ({'✅' if translation_complete else '❌'})")
        print(f"    - 아이허브 매칭: {iherb_processed_count}개 ({'✅' if iherb_complete else '❌'})")
        
        # 전체 완료 여부 판단
        has_incomplete = not (coupang_complete and translation_complete and iherb_complete)
        
        if has_incomplete:
            incomplete_reasons = []
            if not coupang_complete:
                incomplete_reasons.append("쿠팡 가격 업데이트 미완료")
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
    
    def print_progress_status(self, master_df):
        """마스터 파일 진행 상황 출력"""
        total = len(master_df)
        today = datetime.now().strftime("_%Y%m%d")
        
        # 오늘 업데이트 상태
        if 'update_status' in master_df.columns:
            updated = len(master_df[master_df['update_status'] == 'UPDATED'])
            not_found = len(master_df[master_df['update_status'] == 'NOT_FOUND'])
            new_products = len(master_df[master_df['update_status'] == f'NEW_PRODUCT{today}'])
            error_products = len(master_df[master_df['update_status'].str.startswith('ERROR', na=False)])
        else:
            updated = not_found = new_products = error_products = 0
        
        # 매칭 상태
        if 'status' in master_df.columns:
            completed = len(master_df[master_df['status'] == 'success'])
            failed = len(master_df[master_df['status'] == 'not_found'])
            errors = len(master_df[master_df['status'] == 'error'])
        else:
            completed = failed = errors = 0
        
        # 가격 히스토리 분석
        price_columns = [col for col in master_df.columns if col.startswith('쿠팡현재가격_')]
        price_history_dates = len(price_columns)
        
        iherb_price_columns = [col for col in master_df.columns if col.startswith('아이허브할인가_')]
        iherb_history_dates = len(iherb_price_columns)
        
        print(f"📊 마스터 파일 현재 상태:")
        print(f"   - 총 상품: {total}개")
        print(f"   - 쿠팡 가격 히스토리: {price_history_dates}개 날짜")
        print(f"   - 아이허브 가격 히스토리: {iherb_history_dates}개 날짜")
        print(f"   - 오늘({today[1:]}) 쿠팡 업데이트: {updated + not_found}개 (성공: {updated}, 미발견: {not_found})")
        print(f"   - 오늘 신규 상품: {new_products}개")
        print(f"   - 아이허브 매칭 성공: {completed}개")
        if failed > 0:
            print(f"   - 아이허브 매칭 실패: {failed}개")
        if errors > 0:
            print(f"   - 처리 오류: {errors}개")
    
    def print_final_stats(self, master_df):
        """마스터 파일 최종 통계 출력"""
        print(f"\n" + "="*60)
        print(f"📈 마스터 파일 시스템 최종 통계")
        print(f"="*60)
        
        total = len(master_df)
        print(f"📦 총 상품: {total}개")
        
        # 가격 히스토리 분석
        price_columns = [col for col in master_df.columns if col.startswith('쿠팡현재가격_')]
        iherb_price_columns = [col for col in master_df.columns if col.startswith('아이허브할인가_')]
        
        print(f"\n📊 가격 히스토리:")
        print(f"   - 쿠팡 가격 추적: {len(price_columns)}개 날짜")
        if price_columns:
            print(f"     범위: {price_columns[0]} ~ {price_columns[-1]}")
        print(f"   - 아이허브 가격 추적: {len(iherb_price_columns)}개 날짜")
        if iherb_price_columns:
            print(f"     범위: {iherb_price_columns[0]} ~ {iherb_price_columns[-1]}")
        
        # 오늘 업데이트 상태
        today = datetime.now().strftime("_%Y%m%d")
        if 'update_status' in master_df.columns:
            print(f"\n📊 오늘({today[1:]}) 업데이트 상태:")
            status_counts = master_df['update_status'].value_counts()
            for status, count in status_counts.items():
                if 'NEW_PRODUCT' in str(status) or 'UPDATED' in str(status) or 'NOT_FOUND' in str(status):
                    print(f"   - {status}: {count}개")
        
        # 매칭 성공률
        if 'status' in master_df.columns:
            success_count = len(master_df[master_df['status'] == 'success'])
            not_found_count = len(master_df[master_df['status'] == 'not_found'])
            error_count = len(master_df[master_df['status'] == 'error'])
            
            print(f"\n🌿 아이허브 매칭:")
            print(f"   - 성공: {success_count}개")
            print(f"   - 성공률: {success_count/total*100:.1f}%")
            
            if not_found_count > 0:
                print(f"   - 매칭 없음: {not_found_count}개 ({not_found_count/total*100:.1f}%)")
            if error_count > 0:
                print(f"   - 오류: {error_count}개 ({error_count/total*100:.1f}%)")
        
        # 가격 정보 통계
        today_coupang_price_col = f'쿠팡현재가격{today}'
        today_iherb_price_col = f'아이허브할인가{today}'
        
        if today_coupang_price_col in master_df.columns:
            coupang_price_count = len(master_df[
                master_df[today_coupang_price_col].notna() & 
                (master_df[today_coupang_price_col] != '')
            ])
            print(f"\n💰 오늘 가격 정보:")
            print(f"   - 쿠팡 가격: {coupang_price_count}개")
        
        if today_iherb_price_col in master_df.columns:
            iherb_price_count = len(master_df[
                master_df[today_iherb_price_col].notna() & 
                (master_df[today_iherb_price_col] != '')
            ])
            print(f"   - 아이허브 가격: {iherb_price_count}개")
        
        # 마스터 파일 시스템 장점
        print(f"\n⚡ 마스터 파일 시스템 장점:")
        print(f"   - 단일 파일 관리: 모든 데이터가 하나의 파일에")
        print(f"   - 가격 히스토리: 날짜별 가격 변화 추적 가능")
        print(f"   - 효율적 업데이트: 신규 상품만 추가, 기존 상품은 가격만 갱신")
        print(f"   - 완전한 재시작: 중단 지점부터 정확한 재개")
        print(f"   - 배치 처리: {UPDATER_CONFIG['TRANSLATION_BATCH_SIZE']}개씩 효율적 번역")
        print(f"   - 체크포인트: {UPDATER_CONFIG['CHECKPOINT_INTERVAL']}개마다 안전 저장")
        
        print(f"="*60)
    
    def validate_dataframe_structure(self, master_df):
        """마스터 파일 구조 유효성 검사"""
        required_columns = ['coupang_product_id', 'coupang_product_name']
        missing_columns = [col for col in required_columns if col not in master_df.columns]
        
        if missing_columns:
            print(f"⚠️ 필수 컬럼 누락: {missing_columns}")
            print("마스터 파일이 손상되었거나 잘못된 형식일 가능성이 있습니다.")
            return False
        
        # 마스터 파일 특수 컬럼 확인
        master_specific_columns = ['created_at', 'last_updated']
        for col in master_specific_columns:
            if col not in master_df.columns:
                print(f"ℹ️ 마스터 파일 전용 컬럼 '{col}' 추가 필요")
                master_df[col] = pd.NaType() if col not in master_df.columns else master_df[col]
        
        return True
    
    def get_restart_recommendations(self, incomplete_status):
        """재시작 권장사항 제공"""
        recommendations = []
        
        if not incomplete_status['coupang_complete']:
            recommendations.append("🔄 쿠팡 가격 재수집 및 업데이트 필요")
        
        if incomplete_status['new_products_count'] > 0:
            if not incomplete_status['translation_complete']:
                untranslated = incomplete_status['new_products_count'] - incomplete_status['translated_count']
                recommendations.append(f"🔤 신규 상품 번역 작업 {untranslated}개 남음")
            
            if not incomplete_status['iherb_complete']:
                unmatched = incomplete_status['new_products_count'] - incomplete_status['iherb_processed_count']
                recommendations.append(f"🌿 신규 상품 아이허브 매칭 {unmatched}개 남음")
        
        return recommendations
    
    def analyze_master_file_health(self, master_df):
        """마스터 파일 건강상태 분석"""
        health_report = {
            'total_products': len(master_df),
            'has_price_history': False,
            'has_iherb_history': False,
            'duplicate_products': 0,
            'incomplete_products': 0,
            'data_quality_score': 0
        }
        
        # 가격 히스토리 확인
        price_columns = [col for col in master_df.columns if col.startswith('쿠팡현재가격_')]
        health_report['has_price_history'] = len(price_columns) > 1
        health_report['price_history_days'] = len(price_columns)
        
        # 아이허브 히스토리 확인
        iherb_columns = [col for col in master_df.columns if col.startswith('아이허브할인가_')]
        health_report['has_iherb_history'] = len(iherb_columns) > 1
        health_report['iherb_history_days'] = len(iherb_columns)
        
        # 중복 상품 확인
        if 'coupang_product_id' in master_df.columns:
            duplicate_ids = master_df['coupang_product_id'].duplicated().sum()
            health_report['duplicate_products'] = duplicate_ids
        
        # 불완전한 상품 확인 (기본 정보가 누락된 상품)
        required_fields = ['coupang_product_name', 'coupang_product_id']
        incomplete = 0
        for field in required_fields:
            if field in master_df.columns:
                incomplete += master_df[field].isna().sum()
        health_report['incomplete_products'] = incomplete
        
        # 데이터 품질 점수 계산 (0-100)
        score = 100
        if health_report['duplicate_products'] > 0:
            score -= min(20, health_report['duplicate_products'] * 2)
        if health_report['incomplete_products'] > 0:
            score -= min(30, health_report['incomplete_products'] * 3)
        if not health_report['has_price_history']:
            score -= 10
        
        health_report['data_quality_score'] = max(0, score)
        
        print(f"🏥 마스터 파일 건강상태:")
        print(f"   - 총 상품: {health_report['total_products']}개")
        print(f"   - 가격 히스토리: {health_report['price_history_days']}일")
        print(f"   - 아이허브 히스토리: {health_report['iherb_history_days']}일")
        print(f"   - 중복 상품: {health_report['duplicate_products']}개")
        print(f"   - 불완전한 상품: {health_report['incomplete_products']}개")
        print(f"   - 데이터 품질 점수: {health_report['data_quality_score']}/100")
        
        if health_report['data_quality_score'] < 80:
            print(f"   ⚠️ 데이터 품질 개선 권장")
        
        return health_report
    
    def load_metadata(self):
        """재시작 메타데이터 로드"""
        if not os.path.exists(self.metadata_file):
            return None
        
        try:
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
                
            # 마스터 파일 시스템 확인
            if metadata.get('system_type') != 'master_file':
                print(f"ℹ️ 기존 메타데이터가 다른 시스템용입니다. 새로 시작합니다.")
                return None
                
            return metadata
        except Exception as e:
            print(f"메타데이터 로드 실패: {e}")
            return None
    
    def create_backup_before_update(self, master_file):
        """업데이트 전 마스터 파일 백업"""
        if not os.path.exists(master_file):
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"{master_file}.backup_{timestamp}"
        
        try:
            import shutil
            shutil.copy2(master_file, backup_file)
            print(f"📦 업데이트 전 백업 생성: {backup_file}")
            return backup_file
        except Exception as e:
            print(f"⚠️ 백업 생성 실패: {e}")
            return None
    
    def analyze_work_distribution(self, master_df):
        """마스터 파일 작업 분포 분석"""
        today = datetime.now().strftime("_%Y%m%d")
        
        analysis = {
            'total_products': len(master_df),
            'existing_products': 0,
            'new_products_today': 0,
            'updated_today': 0,
            'successful_matches': 0,
            'failed_matches': 0,
            'error_products': 0,
            'price_history_coverage': 0
        }
        
        if 'update_status' in master_df.columns:
            # 기존 상품 vs 오늘 신규 상품
            analysis['existing_products'] = len(master_df[~master_df['update_status'].str.startswith('NEW_PRODUCT', na=False)])
            analysis['new_products_today'] = len(master_df[master_df['update_status'] == f'NEW_PRODUCT{today}'])
            
            # 오늘 업데이트된 상품
            analysis['updated_today'] = len(master_df[
                (master_df['update_status'] == 'UPDATED') | 
                (master_df['update_status'] == 'NOT_FOUND')
            ])
        
        if 'status' in master_df.columns:
            analysis['successful_matches'] = len(master_df[master_df['status'] == 'success'])
            analysis['failed_matches'] = len(master_df[master_df['status'] == 'not_found'])
            analysis['error_products'] = len(master_df[master_df['status'] == 'error'])
        
        # 가격 히스토리 커버리지
        price_columns = [col for col in master_df.columns if col.startswith('쿠팡현재가격_')]
        if price_columns:
            total_possible_entries = len(master_df) * len(price_columns)
            actual_entries = 0
            for col in price_columns:
                actual_entries += len(master_df[master_df[col].notna() & (master_df[col] != '')])
            analysis['price_history_coverage'] = (actual_entries / total_possible_entries * 100) if total_possible_entries > 0 else 0
        
        print(f"📊 작업 분포 분석:")
        print(f"   - 총 상품: {analysis['total_products']}개")
        print(f"   - 기존 상품: {analysis['existing_products']}개")
        print(f"   - 오늘 신규: {analysis['new_products_today']}개")
        print(f"   - 오늘 업데이트: {analysis['updated_today']}개")
        print(f"   - 매칭 성공: {analysis['successful_matches']}개")
        print(f"   - 가격 히스토리 커버리지: {analysis['price_history_coverage']:.1f}%")
        
        return analysis
    
    def get_maintenance_recommendations(self, master_df):
        """마스터 파일 유지보수 권장사항"""
        recommendations = []
        
        # 중복 제품 확인
        if 'coupang_product_id' in master_df.columns:
            duplicates = master_df['coupang_product_id'].duplicated().sum()
            if duplicates > 0:
                recommendations.append(f"🔧 중복 상품 {duplicates}개 정리 필요")
        
        # 오래된 데이터 확인
        price_columns = [col for col in master_df.columns if col.startswith('쿠팡현재가격_')]
        if len(price_columns) > 30:  # 30일 이상의 히스토리
            recommendations.append(f"🗂️ 오래된 가격 히스토리 아카이빙 고려 ({len(price_columns)}일)")
        
        # 불완전한 매칭 확인
        if 'status' in master_df.columns:
            unprocessed = len(master_df[master_df['status'].isna() | (master_df['status'] == '')])
            if unprocessed > 0:
                recommendations.append(f"🔄 미처리 상품 {unprocessed}개 매칭 완료 필요")
        
        # 파일 크기 확인
        if len(master_df) > 10000:
            recommendations.append(f"📦 대용량 파일 ({len(master_df)}개 상품) - 성능 최적화 고려")
        
        if recommendations:
            print(f"🔧 유지보수 권장사항:")
            for rec in recommendations:
                print(f"   - {rec}")
        else:
            print(f"✅ 마스터 파일 상태 양호 - 유지보수 불필요")
        
        return recommendations