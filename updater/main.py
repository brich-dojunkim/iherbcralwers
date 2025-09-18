"""
Product Updater 메인 실행 파일 (수정된 버전)
기존 iHerb 매칭 결과 CSV를 베이스로 하여 새로운 쿠팡 데이터와 통합
"""

import pandas as pd
import sys
import os
from datetime import datetime
from typing import Optional

# 설정 및 모듈 로드
from config import CONFIG
from master_manager import MasterManager
from price_updater import PriceUpdater
from new_product_processor import NewProductProcessor


class ProductUpdater:
    """통합 상품 업데이터 - CSV 파일 기반"""
    
    def __init__(self):
        self.config = CONFIG
        self.master_manager = MasterManager()
        self.price_updater = PriceUpdater()
        self.new_product_processor = NewProductProcessor()
        
        # 업데이트 통계
        self.update_stats = {
            'start_time': datetime.now(),
            'base_products_count': 0,
            'new_coupang_count': 0,
            'total_existing_products': 0,
            'total_new_products': 0,
            'successful_price_updates': 0,
            'successful_new_matches': 0,
            'errors': []
        }
    
    def run_complete_update(self, 
                          base_iherb_csv: str,
                          new_coupang_csv: str) -> str:
        """완전한 업데이트 실행 - CSV 파일 기반"""
        
        print("="*80)
        print("🔄 Product Updater 시작 (CSV 기반)")
        print(f"⏰ 시작 시간: {self.update_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        try:
            # 1. 베이스 iHerb 매칭 결과 로드
            print("\n📂 1단계: 베이스 iHerb 매칭 결과 로드")
            base_df = self._load_base_iherb_data(base_iherb_csv)
            
            if base_df is None or len(base_df) == 0:
                print("  베이스 데이터를 로드할 수 없습니다. 업데이트를 중단합니다.")
                return ""
            
            # 2. 새로운 쿠팡 데이터 로드
            print("\n🛒 2단계: 새로운 쿠팡 데이터 로드")
            new_coupang_df = self._load_new_coupang_data(new_coupang_csv)
            
            if new_coupang_df is None or len(new_coupang_df) == 0:
                print("  새로운 쿠팡 데이터를 로드할 수 없습니다. 업데이트를 중단합니다.")
                return ""
            
            # 3. 상품 분류
            print("\n📊 3단계: 상품 분류")
            existing_products, new_products = self.master_manager.classify_products(new_coupang_df, base_df)
            
            self.update_stats['total_existing_products'] = len(existing_products)
            self.update_stats['total_new_products'] = len(new_products)
            
            # 4. 기존 상품 가격 업데이트
            print("\n💰 4단계: 기존 상품 가격 업데이트")
            updated_existing_df = pd.DataFrame(columns=self.config.MASTER_COLUMNS)
            
            if existing_products:
                updated_existing_df = self.price_updater.update_existing_products(existing_products, base_df)
                self.update_stats['successful_price_updates'] = len(updated_existing_df)
            else:
                print("  업데이트할 기존 상품이 없습니다.")
            
            # 5. 신규 상품 처리
            print("\n🆕 5단계: 신규 상품 처리")
            processed_new_df = pd.DataFrame(columns=self.config.MASTER_COLUMNS)
            
            if new_products:
                processed_new_df = self.new_product_processor.process_new_products(new_products)
                if len(processed_new_df) > 0:
                    self.update_stats['successful_new_matches'] = len(processed_new_df[processed_new_df['status'] == 'success'])
            else:
                print("  처리할 신규 상품이 없습니다.")
            
            # 6. 데이터 통합 및 저장
            print("\n💾 6단계: 데이터 통합 및 저장")
            final_master_df = self.master_manager.merge_dataframes(base_df, updated_existing_df, processed_new_df)
            
            final_master_path = self.master_manager.save_master_data(final_master_df, create_backup=True)
            
            # 7. 최종 결과 리포트
            self._print_final_report(final_master_path)
            
            return final_master_path
            
        except KeyboardInterrupt:
            print("\n⚠️ 사용자에 의해 중단되었습니다.")
            return ""
        except Exception as e:
            print(f"\n❌ 업데이트 중 오류: {e}")
            self.update_stats['errors'].append(str(e))
            return ""
    
    def _load_base_iherb_data(self, csv_path: str) -> Optional[pd.DataFrame]:
        """베이스 iHerb 매칭 결과 데이터 로드"""
        try:
            if not os.path.exists(csv_path):
                print(f"  ❌ 파일이 존재하지 않습니다: {csv_path}")
                return None
            
            print(f"  베이스 파일 로드 중: {os.path.basename(csv_path)}")
            
            base_df = pd.read_csv(csv_path, encoding='utf-8-sig')
            
            # 스키마 검증 및 보완
            base_df = self.master_manager._validate_and_fix_schema(base_df)
            
            self.update_stats['base_products_count'] = len(base_df)
            
            print(f"  로드 완료: {len(base_df)}개 상품")
            if len(base_df) > 0:
                success_count = len(base_df[base_df['status'] == 'success']) if 'status' in base_df.columns else 0
                print(f"    - 성공한 매칭: {success_count}개")
                print(f"    - 기타: {len(base_df) - success_count}개")
            
            return base_df
            
        except Exception as e:
            print(f"  ❌ 베이스 데이터 로드 실패: {e}")
            return None
    
    def _load_new_coupang_data(self, csv_path: str) -> Optional[pd.DataFrame]:
        """새로운 쿠팡 크롤링 데이터 로드"""
        try:
            if not os.path.exists(csv_path):
                print(f"  ❌ 파일이 존재하지 않습니다: {csv_path}")
                return None
            
            print(f"  새 쿠팡 데이터 로드 중: {os.path.basename(csv_path)}")
            
            coupang_df = pd.read_csv(csv_path, encoding='utf-8-sig')
            
            # 필수 컬럼 확인
            required_columns = ['product_id', 'product_name']
            missing_columns = [col for col in required_columns if col not in coupang_df.columns]
            
            if missing_columns:
                print(f"  ❌ 필수 컬럼 누락: {missing_columns}")
                return None
            
            # 유효한 데이터만 필터링
            before_count = len(coupang_df)
            coupang_df = coupang_df.dropna(subset=['product_name'])
            coupang_df = coupang_df[coupang_df['product_name'].str.strip() != '']
            after_count = len(coupang_df)
            
            self.update_stats['new_coupang_count'] = after_count
            
            print(f"  로드 완료: {after_count}개 유효한 상품")
            if before_count != after_count:
                print(f"    - 제외된 상품: {before_count - after_count}개 (제품명 없음)")
            
            return coupang_df
            
        except Exception as e:
            print(f"  ❌ 쿠팡 데이터 로드 실패: {e}")
            return None
    
    def _print_final_report(self, master_path: str):
        """최종 결과 리포트"""
        end_time = datetime.now()
        duration = end_time - self.update_stats['start_time']
        
        print("\n" + "="*80)
        print("📊 업데이트 완료 리포트")
        print("="*80)
        
        print(f"⏰ 처리 시간: {duration}")
        print(f"📁 결과 파일: {os.path.basename(master_path) if master_path else '저장 실패'}")
        
        print(f"\n📈 입력 데이터:")
        print(f"  베이스 iHerb 결과: {self.update_stats['base_products_count']}개")
        print(f"  새 쿠팡 데이터: {self.update_stats['new_coupang_count']}개")
        
        print(f"\n📈 처리 통계:")
        print(f"  기존 상품: {self.update_stats['total_existing_products']}개")
        print(f"  신규 상품: {self.update_stats['total_new_products']}개")
        print(f"  가격 업데이트 성공: {self.update_stats['successful_price_updates']}개")
        print(f"  신규 매칭 성공: {self.update_stats['successful_new_matches']}개")
        
        # 성공률 계산
        total_processed = self.update_stats['total_existing_products'] + self.update_stats['total_new_products']
        total_success = self.update_stats['successful_price_updates'] + self.update_stats['successful_new_matches']
        
        if total_processed > 0:
            success_rate = (total_success / total_processed) * 100
            print(f"  전체 성공률: {success_rate:.1f}%")
        
        # 최종 데이터 크기
        if master_path and os.path.exists(master_path):
            try:
                final_df = pd.read_csv(master_path, encoding='utf-8-sig')
                final_success = len(final_df[final_df['status'] == 'success']) if 'status' in final_df.columns else 0
                print(f"\n📊 최종 통합 결과:")
                print(f"  총 상품: {len(final_df)}개")
                print(f"  성공한 매칭: {final_success}개")
            except:
                pass
        
        # 오류 정보
        if self.update_stats['errors']:
            print(f"\n⚠️ 발생한 오류 ({len(self.update_stats['errors'])}개):")
            for i, error in enumerate(self.update_stats['errors'][:5], 1):
                print(f"  {i}. {error[:100]}...")
        
        print("\n🎉 업데이트 완료!")
        print("="*80)
    
    def run_price_only_update(self, base_csv_path: str) -> str:
        """가격 정보만 업데이트 (기존 매칭 결과 기반)"""
        try:
            print("💰 가격 정보만 업데이트 모드")
            
            # 베이스 데이터 로드
            base_df = self._load_base_iherb_data(base_csv_path)
            
            if base_df is None or len(base_df) == 0:
                print("  업데이트할 베이스 데이터가 없습니다.")
                return ""
            
            # 성공한 매칭 상품들만 추출
            successful_products = base_df[base_df['status'] == 'success'] if 'status' in base_df.columns else base_df
            
            if len(successful_products) == 0:
                print("  가격 업데이트할 상품이 없습니다.")
                return ""
            
            print(f"  가격 업데이트 대상: {len(successful_products)}개")
            
            # 가격 업데이트 (아이허브만)
            product_ids = successful_products['coupang_product_id'].astype(str).tolist() if 'coupang_product_id' in successful_products.columns else []
            
            if not product_ids:
                print("  유효한 상품 ID가 없습니다.")
                return ""
            
            updated_df = self.price_updater.update_specific_products(product_ids, base_df)
            
            if len(updated_df) > 0:
                # 베이스 데이터에 반영
                final_master_df = self.master_manager.merge_dataframes(base_df, updated_df, pd.DataFrame())
                master_path = self.master_manager.save_master_data(final_master_df)
                
                print(f"\n💾 가격 업데이트 완료: {os.path.basename(master_path)}")
                return master_path
            else:
                print("\n❌ 가격 업데이트 실패")
                return ""
                
        except Exception as e:
            print(f"\n❌ 가격 업데이트 중 오류: {e}")
            return ""
    
    def run_interactive_mode(self):
        """대화형 모드"""
        print("\n🔧 Product Updater 대화형 모드")
        print("="*50)
        
        while True:
            print("\n📋 옵션을 선택하세요:")
            print("  1. 완전 업데이트 (베이스 CSV + 새 쿠팡 CSV)")
            print("  2. 가격 정보만 업데이트 (기존 매칭 결과 기반)")
            print("  3. 파일 상태 확인")
            print("  4. 종료")
            
            choice = input("\n선택 (1-4): ").strip()
            
            try:
                if choice == '1':
                    base_csv = input("베이스 iHerb 매칭 결과 CSV 경로: ").strip()
                    new_coupang_csv = input("새 쿠팡 크롤링 데이터 CSV 경로: ").strip()
                    
                    if base_csv and new_coupang_csv:
                        result = self.run_complete_update(base_csv, new_coupang_csv)
                        if result:
                            print(f"✅ 완료: {result}")
                    else:
                        print("❌ 파일 경로를 모두 입력해주세요.")
                    
                elif choice == '2':
                    base_csv = input("베이스 CSV 파일 경로: ").strip()
                    if base_csv:
                        result = self.run_price_only_update(base_csv)
                        if result:
                            print(f"✅ 완료: {result}")
                    else:
                        print("❌ 파일 경로를 입력해주세요.")
                
                elif choice == '3':
                    self._show_file_status()
                
                elif choice == '4':
                    print("👋 종료합니다.")
                    break
                
                else:
                    print("❌ 잘못된 선택입니다.")
                    
            except KeyboardInterrupt:
                print("\n⚠️ 작업이 중단되었습니다.")
            except Exception as e:
                print(f"❌ 오류: {e}")
    
    def _show_file_status(self):
        """파일 상태 확인"""
        print("\n📁 파일 상태 확인")
        
        # 현재 디렉토리의 CSV 파일들 확인
        current_dir = os.getcwd()
        csv_files = [f for f in os.listdir(current_dir) if f.endswith('.csv')]
        
        if csv_files:
            print(f"\n현재 디렉토리의 CSV 파일들:")
            for i, file in enumerate(csv_files, 1):
                file_path = os.path.join(current_dir, file)
                try:
                    df = pd.read_csv(file_path, encoding='utf-8-sig')
                    print(f"  {i}. {file} ({len(df)}개 행)")
                    
                    # 파일 타입 추정
                    if 'iherb_product_name' in df.columns and 'coupang_product_name' in df.columns:
                        success_count = len(df[df['status'] == 'success']) if 'status' in df.columns else 0
                        print(f"     → iHerb 매칭 결과 (성공: {success_count}개)")
                    elif 'product_name' in df.columns and 'product_id' in df.columns:
                        print(f"     → 쿠팡 크롤링 데이터")
                    else:
                        print(f"     → 기타 CSV 파일")
                        
                except Exception as e:
                    print(f"  {i}. {file} (읽기 실패: {str(e)[:50]}...)")
        else:
            print("  현재 디렉토리에 CSV 파일이 없습니다.")
        
        # 출력 디렉토리 확인
        if os.path.exists(self.config.OUTPUT_DIR):
            output_files = [f for f in os.listdir(self.config.OUTPUT_DIR) if f.endswith('.csv')]
            if output_files:
                print(f"\n출력 디렉토리의 결과 파일들:")
                for file in sorted(output_files)[-5:]:  # 최근 5개만
                    print(f"  - {file}")
            else:
                print(f"\n출력 디렉토리에 파일이 없습니다: {self.config.OUTPUT_DIR}")


def main():
    """메인 실행 함수"""
    try:
        updater = ProductUpdater()
        
        # 명령행 인수 확인
        if len(sys.argv) > 1:
            if sys.argv[1] == '--interactive':
                updater.run_interactive_mode()
            elif sys.argv[1] == '--price-only':
                if len(sys.argv) > 2:
                    base_csv = sys.argv[2]
                    result = updater.run_price_only_update(base_csv)
                    if result:
                        print(f"✅ 가격 업데이트 완료: {result}")
                else:
                    print("❌ 베이스 CSV 파일 경로를 제공해주세요.")
                    print("사용법: python main.py --price-only <base_csv_path>")
            elif len(sys.argv) >= 3:
                # 두 개의 CSV 파일이 제공된 경우
                base_csv = sys.argv[1]
                new_coupang_csv = sys.argv[2]
                
                result = updater.run_complete_update(base_csv, new_coupang_csv)
                if result:
                    print(f"✅ 완전 업데이트 완료: {result}")
            else:
                print("❌ 인수가 부족합니다.")
                print("사용법:")
                print("  python main.py <base_iherb_csv> <new_coupang_csv>")
                print("  python main.py --price-only <base_csv>")
                print("  python main.py --interactive")
        else:
            # 기본: 대화형 모드
            updater.run_interactive_mode()
            
    except KeyboardInterrupt:
        print("\n👋 사용자에 의해 종료되었습니다.")
    except Exception as e:
        print(f"❌ 실행 중 오류: {e}")


if __name__ == "__main__":
    main()