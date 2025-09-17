"""
재시작 관리자 - 마스터 파일 시스템 지원 (공통 패턴 적용)
"""

import json
import os
import pandas as pd
from datetime import datetime
from settings import UPDATER_CONFIG
from common import MasterFilePatterns, get_new_products_filter

class RestartManager:
    """재시작 전담 관리자 - 마스터 파일 시스템 (공통 패턴 적용)"""
    
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
        """마스터 파일에서 미완료 작업 정밀 감지 - 공통 패턴 적용"""
        
        # 1. 쿠팡 업데이트 완료 여부 확인
        if 'update_status' in master_df.columns:
            # 오늘 업데이트가 필요한 상품들 (기존 상품)
            existing_products = master_df[~get_new_products_filter(master_df)]
            
            # 오늘 업데이트 완료된 상품들
            coupang_columns = MasterFilePatterns.get_daily_coupang_columns()
            today_updated = master_df[
                (master_df['update_status'] == 'UPDATED') |
                (master_df['update_status'] == 'NOT_FOUND') |
                (master_df[coupang_columns['crawled_at']].notna() if coupang_columns['crawled_at'] in master_df.columns else False)
            ]
            
            coupang_complete = len(today_updated) >= len(existing_products)
            
            print(f"  🔍 쿠팡 업데이트 상태 분석 (마스터 파일):")
            print(f"    - 기존 상품: {len(existing_products)}개")
            print(f"    - 오늘 업데이트됨: {len(today_updated)}개")
            print(f"    - 완료 여부: {'✅' if coupang_complete else '❌'}")
        else:
            coupang_complete = False
            print(f"  ⚠️ update_status 컬럼이 없음 - 쿠팡 업데이트 미완료로 간주")
        
        # 2. 신규 상품 처리 상태 확인 - 공통 패턴 적용
        new_products = master_df[get_new_products_filter(master_df)]
        new_count = len(new_products)
        
        print(f"  🔍 신규 상품 패턴 확인:")
        print(f"    - 사용된 패턴: '{MasterFilePatterns.get_new_product_status()}'")
        print(f"    - 매칭된 상품: {new_count}개")
        
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
        
        # 4. 아이허브 매칭 완료 여부 확인 - 공통 컬럼명 적용
        iherb_processed_count = 0
        iherb_complete = False
        
        if new_count > 0:
            print(f"  🔍 신규 상품 아이허브 매칭 상태 (오늘 날짜 기준):")
            
            # ✅ 공통 컬럼명 사용 - 오늘 날짜 기준 아이허브 매칭 상태 컬럼들 정의
            iherb_columns = MasterFilePatterns.get_daily_iherb_columns()
            today_iherb_status_columns = [
                iherb_columns['matching_status'],
                iherb_columns['list_price'],
                iherb_columns['discount_price'],
                iherb_columns['matched_at']
            ]
            
            # 실제 존재하는 컬럼들만 필터링
            existing_today_iherb_columns = [col for col in today_iherb_status_columns if col in master_df.columns]
            
            print(f"    - 예상 오늘 날짜 아이허브 컬럼: {len(today_iherb_status_columns)}개")
            print(f"    - 실제 존재하는 컬럼: {len(existing_today_iherb_columns)}개")
            
            if existing_today_iherb_columns:
                # 오늘 날짜 아이허브 데이터가 있는 상품들 (안전한 방식)
                has_today_iherb_data = new_products[
                    new_products[existing_today_iherb_columns].notna().any(axis=1)
                ]
                
                print(f"    - 오늘 날짜 아이허브 데이터 있음: {len(has_today_iherb_data)}개")
                
                # 매칭 상태별 분석 (컬럼 존재 확인 후)
                status_col = iherb_columns['matching_status']
                if status_col in master_df.columns:
                    # new_products에서 해당 컬럼의 값 분포 확인
                    status_counts = new_products[status_col].value_counts()
                    for status, count in status_counts.items():
                        if pd.notna(status):  # NaN 값은 제외
                            print(f"      * {status}: {count}개")
                    
                    processed_today = new_products[
                        new_products[status_col].notna() &
                        (new_products[status_col] != '')
                    ]
                    iherb_processed_count = len(processed_today)
                else:
                    # 상태 컬럼이 없으면 다른 오늘 날짜 데이터로 판단
                    iherb_processed_count = len(has_today_iherb_data)
                    print(f"    - 매칭 상태 컬럼 없음, 기타 오늘 날짜 데이터로 판단")
            else:
                # 오늘 날짜 아이허브 컬럼이 전혀 없음
                print(f"    - 오늘 날짜 아이허브 컬럼이 전혀 없음")
                iherb_processed_count = 0
            
            iherb_complete = iherb_processed_count == new_count
            print(f"    - 최종 판단: {iherb_processed_count}개 처리됨 (완료: {'✅' if iherb_complete else '❌'})")
            
            # 미처리 상품들 확인 (안전한 방식)
            if not iherb_complete and existing_today_iherb_columns:
                unprocessed = new_products[
                    ~new_products[existing_today_iherb_columns].notna().any(axis=1)
                ]
                print(f"    - 미처리 상품: {len(unprocessed)}개")
                if len(unprocessed) <= 5:
                    for idx, row in unprocessed.iterrows():
                        product_name = row.get('coupang_product_name', 'N/A')[:30] + "..."
                        print(f"      * {product_name}")
            elif not iherb_complete:
                print(f"    - 미처리 상품: {new_count - iherb_processed_count}개 (컬럼 부재로 정확한 목록 확인 불가)")
        
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
        """마스터 파일 진행 상황 출력 - 공통 패턴 적용"""
        total = len(master_df)
        
        # ✅ 공통 패턴 사용 - 오늘 업데이트 상태 (컬럼 존재 확인)
        if 'update_status' in master_df.columns:
            updated = len(master_df[master_df['update_status'] == 'UPDATED'])
            not_found = len(master_df[master_df['update_status'] == 'NOT_FOUND'])
            new_products = len(master_df[get_new_products_filter(master_df)])
            error_products = len(master_df[master_df['update_status'].str.startswith('ERROR', na=False)])
        else:
            updated = not_found = new_products = error_products = 0
        
        # 매칭 상태 (컬럼 존재 확인)
        if 'status' in master_df.columns:
            completed = len(master_df[master_df['status'] == 'success'])
            failed = len(master_df[master_df['status'] == 'not_found'])
            errors = len(master_df[master_df['status'] == 'error'])
        else:
            completed = failed = errors = 0
        
        # ✅ 공통 패턴 사용 - 가격 히스토리 분석
        coupang_columns = MasterFilePatterns.get_daily_coupang_columns()
        iherb_columns = MasterFilePatterns.get_daily_iherb_columns()
        
        price_columns = [col for col in master_df.columns if col.startswith('쿠팡현재가격_')]
        price_history_dates = len(price_columns)
        
        iherb_price_columns = [col for col in master_df.columns if col.startswith('아이허브할인가_')]
        iherb_history_dates = len(iherb_price_columns)
        
        today_suffix = MasterFilePatterns.get_today_suffix()
        
        print(f"📊 마스터 파일 현재 상태:")
        print(f"   - 총 상품: {total}개")
        print(f"   - 쿠팡 가격 히스토리: {price_history_dates}개 날짜")
        print(f"   - 아이허브 가격 히스토리: {iherb_history_dates}개 날짜")
        print(f"   - 오늘({today_suffix}) 쿠팡 업데이트: {updated + not_found}개 (성공: {updated}, 미발견: {not_found})")
        print(f"   - 오늘 신규 상품: {new_products}개")
        print(f"   - 아이허브 매칭 성공: {completed}개")
        if failed > 0:
            print(f"   - 아이허브 매칭 실패: {failed}개")
        if errors > 0:
            print(f"   - 처리 오류: {errors}개")
    
    def print_final_stats(self, master_df):
        """마스터 파일 최종 통계 출력 - 공통 패턴 적용"""
        print(f"\n" + "="*60)
        print(f"📈 마스터 파일 시스템 최종 통계")
        print(f"="*60)
        
        total = len(master_df)
        print(f"📦 총 상품: {total}개")
        
        # ✅ 공통 패턴 사용 - 가격 히스토리 분석
        price_columns = [col for col in master_df.columns if col.startswith('쿠팡현재가격_')]
        iherb_price_columns = [col for col in master_df.columns if col.startswith('아이허브할인가_')]
        
        print(f"\n📊 가격 히스토리:")
        print(f"   - 쿠팡 가격 추적: {len(price_columns)}개 날짜")
        if price_columns:
            print(f"     범위: {price_columns[0]} ~ {price_columns[-1]}")
        print(f"   - 아이허브 가격 추적: {len(iherb_price_columns)}개 날짜")
        if iherb_price_columns:
            print(f"     범위: {iherb_price_columns[0]} ~ {iherb_price_columns[-1]}")
        
        # ✅ 공통 패턴 사용 - 오늘 업데이트 상태
        today_suffix = MasterFilePatterns.get_today_suffix()
        if 'update_status' in master_df.columns:
            print(f"\n📊 오늘({today_suffix}) 업데이트 상태:")
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
        
        # ✅ 공통 컬럼명 사용 - 가격 정보 통계 (컬럼 안전성 확인)
        coupang_columns = MasterFilePatterns.get_daily_coupang_columns()
        iherb_columns = MasterFilePatterns.get_daily_iherb_columns()
        
        today_coupang_price_col = coupang_columns['current_price']
        today_iherb_price_col = iherb_columns['discount_price']
        
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
        print(f"   - 공통 패턴: 모든 모듈에서 일관된 컬럼명/패턴 사용")
        
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