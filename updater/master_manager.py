"""
마스터 CSV 관리 모듈
- 마스터 데이터 로드/저장
- 기존/신규 상품 분류
- 백업 관리
"""

import os
import pandas as pd
import shutil
from datetime import datetime
from typing import Tuple, List, Optional
from config import CONFIG


class MasterManager:
    """마스터 CSV 파일 관리 클래스"""
    
    def __init__(self):
        self.config = CONFIG
        
    def find_latest_master(self) -> Optional[str]:
        """가장 최신 마스터 파일 찾기"""
        try:
            # output 디렉토리에서 마스터 파일 검색
            master_files = []
            
            if os.path.exists(self.config.OUTPUT_DIR):
                for filename in os.listdir(self.config.OUTPUT_DIR):
                    if filename.startswith('master_products_') and filename.endswith('.csv'):
                        filepath = os.path.join(self.config.OUTPUT_DIR, filename)
                        if os.path.isfile(filepath):
                            master_files.append(filepath)
            
            if not master_files:
                print("  기존 마스터 파일이 없습니다.")
                return None
            
            # 파일명으로 정렬하여 최신 파일 선택
            latest_file = sorted(master_files)[-1]
            print(f"  최신 마스터 파일: {os.path.basename(latest_file)}")
            
            return latest_file
            
        except Exception as e:
            print(f"  마스터 파일 검색 실패: {e}")
            return None
    
    def load_master_data(self, master_file_path: Optional[str] = None) -> pd.DataFrame:
        """마스터 데이터 로드"""
        try:
            if not master_file_path:
                master_file_path = self.find_latest_master()
            
            if not master_file_path or not os.path.exists(master_file_path):
                print("  마스터 파일이 없습니다. 빈 DataFrame 생성")
                return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
            
            print(f"  마스터 데이터 로드 중: {os.path.basename(master_file_path)}")
            
            master_df = pd.read_csv(master_file_path, encoding='utf-8-sig')
            
            # 스키마 검증 및 보완
            master_df = self._validate_and_fix_schema(master_df)
            
            print(f"  로드 완료: {len(master_df)}개 상품")
            print(f"    - 성공한 매칭: {len(master_df[master_df['status'] == 'success'])}개")
            print(f"    - 실패한 매칭: {len(master_df[master_df['status'] != 'success'])}개")
            
            return master_df
            
        except Exception as e:
            print(f"  마스터 데이터 로드 실패: {e}")
            return pd.DataFrame(columns=self.config.MASTER_COLUMNS)
    
    def _validate_and_fix_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """스키마 검증 및 누락된 컬럼 보완"""
        try:
            # 누락된 컬럼 추가
            for col in self.config.MASTER_COLUMNS:
                if col not in df.columns:
                    if col == 'last_updated':
                        df[col] = datetime.now().isoformat()
                    elif col == 'data_source':
                        df[col] = 'initial'
                    elif col == 'update_count':
                        df[col] = 0
                    else:
                        df[col] = ''
            
            # 컬럼 순서 정렬
            df = df.reindex(columns=self.config.MASTER_COLUMNS, fill_value='')
            
            return df
            
        except Exception as e:
            print(f"  스키마 검증 실패: {e}")
            return df
    
    def classify_products(self, new_coupang_df: pd.DataFrame, master_df: pd.DataFrame) -> Tuple[List[dict], List[dict]]:
        """신규/기존 상품 분류"""
        try:
            print("\n상품 분류 중...")
            
            # 기존 쿠팡 상품 ID 집합
            if len(master_df) > 0 and 'coupang_product_id' in master_df.columns:
                existing_coupang_ids = set(master_df['coupang_product_id'].dropna().astype(str))
            else:
                existing_coupang_ids = set()
            
            existing_products = []
            new_products = []
            
            for _, product in new_coupang_df.iterrows():
                product_id = str(product.get('product_id', ''))
                
                if product_id in existing_coupang_ids:
                    existing_products.append(product.to_dict())
                else:
                    new_products.append(product.to_dict())
            
            print(f"  기존 상품: {len(existing_products)}개")
            print(f"  신규 상품: {len(new_products)}개")
            
            return existing_products, new_products
            
        except Exception as e:
            print(f"  상품 분류 실패: {e}")
            # 실패 시 모든 상품을 신규로 처리
            new_products = [product.to_dict() for _, product in new_coupang_df.iterrows()]
            return [], new_products
    
    def create_backup(self, master_file_path: str) -> str:
        """마스터 파일 백업"""
        try:
            if not os.path.exists(master_file_path):
                return ""
            
            backup_filename = self.config.generate_backup_filename()
            backup_path = os.path.join(self.config.BACKUP_DIR, backup_filename)
            
            shutil.copy2(master_file_path, backup_path)
            print(f"  백업 생성: {backup_filename}")
            
            return backup_path
            
        except Exception as e:
            print(f"  백업 생성 실패: {e}")
            return ""
    
    def save_master_data(self, master_df: pd.DataFrame, create_backup: bool = True) -> str:
        """마스터 데이터 저장"""
        try:
            print("\n마스터 데이터 저장 중...")
            
            # 기존 마스터 파일 백업
            if create_backup:
                existing_master = self.find_latest_master()
                if existing_master:
                    self.create_backup(existing_master)
            
            # 새로운 마스터 파일 저장
            master_filename = self.config.generate_master_filename()
            master_path = os.path.join(self.config.OUTPUT_DIR, master_filename)
            
            # 메타 정보 업데이트
            master_df['last_updated'] = datetime.now().isoformat()
            
            # 컬럼 순서 정렬
            master_df = master_df.reindex(columns=self.config.MASTER_COLUMNS, fill_value='')
            
            # CSV 저장
            master_df.to_csv(master_path, index=False, encoding='utf-8-sig')
            
            print(f"  저장 완료: {master_filename}")
            print(f"  총 상품 수: {len(master_df)}개")
            
            # 통계 출력
            self._print_save_statistics(master_df)
            
            return master_path
            
        except Exception as e:
            print(f"  마스터 데이터 저장 실패: {e}")
            return ""
    
    def _print_save_statistics(self, master_df: pd.DataFrame):
        """저장 통계 출력"""
        try:
            if len(master_df) == 0:
                return
            
            # 기본 통계
            total_products = len(master_df)
            successful_matches = len(master_df[master_df['status'] == 'success'])
            
            print(f"  마스터 데이터 통계:")
            print(f"    - 총 상품: {total_products}개")
            print(f"    - 성공한 매칭: {successful_matches}개 ({successful_matches/total_products*100:.1f}%)")
            
            # 데이터 소스별 통계
            if 'data_source' in master_df.columns:
                source_counts = master_df['data_source'].value_counts()
                for source, count in source_counts.items():
                    print(f"    - {source}: {count}개")
            
            # 가격 정보 통계
            price_available = len(master_df[
                (master_df['coupang_current_price_krw'] != '') & 
                (master_df['iherb_discount_price_krw'] != '')
            ])
            if price_available > 0:
                print(f"    - 가격 비교 가능: {price_available}개")
            
        except Exception as e:
            print(f"  통계 출력 오류: {e}")
    
    def merge_dataframes(self, master_df: pd.DataFrame, updated_existing: pd.DataFrame, new_processed: pd.DataFrame) -> pd.DataFrame:
        """여러 DataFrame 병합"""
        try:
            print("\n데이터 병합 중...")
            
            # 기존 상품 업데이트: master_df에서 업데이트된 상품들 교체
            if len(updated_existing) > 0:
                updated_ids = set(updated_existing['coupang_product_id'].astype(str))
                master_df = master_df[~master_df['coupang_product_id'].astype(str).isin(updated_ids)]
                master_df = pd.concat([master_df, updated_existing], ignore_index=True)
                print(f"  기존 상품 업데이트: {len(updated_existing)}개")
            
            # 신규 상품 추가
            if len(new_processed) > 0:
                master_df = pd.concat([master_df, new_processed], ignore_index=True)
                print(f"  신규 상품 추가: {len(new_processed)}개")
            
            # 중복 제거 (혹시 모를 중복 방지)
            if 'coupang_product_id' in master_df.columns:
                before_dedup = len(master_df)
                master_df = master_df.drop_duplicates(subset=['coupang_product_id'], keep='last')
                after_dedup = len(master_df)
                if before_dedup != after_dedup:
                    print(f"  중복 제거: {before_dedup - after_dedup}개")
            
            print(f"  병합 완료: 총 {len(master_df)}개 상품")
            
            return master_df
            
        except Exception as e:
            print(f"  데이터 병합 실패: {e}")
            return master_df
    
    def get_existing_product_mapping(self, master_df: pd.DataFrame) -> dict:
        """기존 상품의 쿠팡 ID -> 마스터 레코드 매핑 생성"""
        try:
            mapping = {}
            
            for _, row in master_df.iterrows():
                coupang_id = str(row.get('coupang_product_id', ''))
                if coupang_id and coupang_id != 'nan':
                    mapping[coupang_id] = row.to_dict()
            
            return mapping
            
        except Exception as e:
            print(f"  매핑 생성 실패: {e}")
            return {}
    
    def cleanup_old_backups(self, keep_count: int = 10):
        """오래된 백업 파일 정리"""
        try:
            if not os.path.exists(self.config.BACKUP_DIR):
                return
            
            backup_files = []
            for filename in os.listdir(self.config.BACKUP_DIR):
                if filename.startswith('backup_master_') and filename.endswith('.csv'):
                    filepath = os.path.join(self.config.BACKUP_DIR, filename)
                    backup_files.append(filepath)
            
            if len(backup_files) <= keep_count:
                return
            
            # 오래된 파일부터 정렬
            backup_files.sort()
            files_to_delete = backup_files[:-keep_count]
            
            for file_path in files_to_delete:
                os.remove(file_path)
                print(f"  오래된 백업 삭제: {os.path.basename(file_path)}")
            
        except Exception as e:
            print(f"  백업 정리 실패: {e}")