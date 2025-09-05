"""
상품명 파싱 전처리 스크립트 (실제 크롤링 결과 구조 기반)
- 크롤링 결과: 개당_수량(60캡슐, 120정 등), 수량(1개, 2개 등), 용량(464g, 30ml 등), 맛 등
- 새 인풋 상품명에서 이 형태에 맞게 옵션값 추출
"""

import pandas as pd
import re
import sys
import argparse
from datetime import datetime

class ProductNameParser:
    def __init__(self):
        # 크롤링 결과 분석 기반 패턴 정의
        self.quantity_patterns = {
            # 개당_수량 패턴 (캡슐, 정, 소프트젤 등)
            'capsules': r'(\d+)(?:캡슐|캡|capsules?)',
            'tablets': r'(\d+)(?:정|타블렛|tablets?)',
            'softgels': r'(\d+)(?:소프트젤|소프트겔|softgels?)',
            'vegicaps': r'(\d+)(?:베지캡슐|베지캡|vegicaps?)',
        }
        
        # 용량 패턴 (g, ml, kg 등)
        self.capacity_patterns = {
            'grams': r'(\d+(?:\.\d+)?)g(?!\w)',
            'kilograms': r'(\d+(?:\.\d+)?)kg(?!\w)',
            'milliliters': r'(\d+(?:\.\d+)?)ml(?!\w)',
            'fluid_ounces': r'(\d+(?:\.\d+)?)fl\s*oz(?!\w)',
        }
        
        # 맛 패턴 (크롤링 결과에서 확인된 맛들)
        self.flavor_patterns = [
            '오리지널', '다크 초코', '무맛', '무향', '오렌지', '바닐라', 
            '초콜릿', '베리', '딸기', 'original', 'chocolate', 'vanilla'
        ]
        
    def extract_quantity_unit(self, product_name):
        """개당_수량 형태의 값 추출 (60캡슐, 120정 등)"""
        name_lower = product_name.lower()
        
        for unit_type, pattern in self.quantity_patterns.items():
            matches = re.findall(pattern, name_lower, re.IGNORECASE)
            if matches:
                quantity = matches[0]
                try:
                    num_val = int(quantity)
                    if 10 <= num_val <= 1000:  # 합리적인 범위
                        # 크롤링 결과 형태로 정규화
                        if unit_type == 'capsules':
                            return f"{quantity}캡슐"
                        elif unit_type == 'tablets':
                            return f"{quantity}정"
                        elif unit_type == 'softgels':
                            return f"{quantity}소프트젤"
                        elif unit_type == 'vegicaps':
                            return f"{quantity}베지캡슐"
                except ValueError:
                    continue
        
        return None
    
    def extract_capacity(self, product_name):
        """용량 형태의 값 추출 (464g, 30ml 등)"""
        name_lower = product_name.lower()
        
        for unit_type, pattern in self.capacity_patterns.items():
            matches = re.findall(pattern, name_lower, re.IGNORECASE)
            if matches:
                quantity = matches[0]
                try:
                    num_val = float(quantity)
                    if 0.1 <= num_val <= 10000:  # 합리적인 범위
                        # 크롤링 결과 형태로 정규화
                        if unit_type == 'grams':
                            return f"{quantity}g"
                        elif unit_type == 'kilograms':
                            return f"{quantity}kg"
                        elif unit_type == 'milliliters':
                            return f"{quantity}ml"
                        elif unit_type == 'fluid_ounces':
                            return f"{quantity}fl oz"
                except ValueError:
                    continue
        
        return None
    
    def extract_flavor(self, product_name):
        """맛 정보 추출"""
        name_lower = product_name.lower()
        
        for flavor in self.flavor_patterns:
            if flavor.lower() in name_lower:
                return flavor
        
        return None
    
    def extract_base_model_name(self, model_name):
        """모델명에서 기본 모델명 추출 (CGN00965-2 → CGN00965)"""
        # 숫자나 하이픈으로 끝나는 suffix 제거
        base_model = re.sub(r'-\d+$', '', model_name)
        return base_model
    
    def parse_product_name(self, product_name, option_count, model_name):
        """상품명을 파싱하여 크롤링 결과와 매칭 가능한 형태로 변환"""
        result = {
            '기본_모델명': self.extract_base_model_name(model_name),
            '파싱된_개당수량': self.extract_quantity_unit(product_name),
            '파싱된_용량': self.extract_capacity(product_name),
            '파싱된_맛': self.extract_flavor(product_name),
            '파싱된_수량': option_count,  # 새 인풋의 옵션을 그대로 사용
            '파싱_성공': False,
            '매칭_키': None
        }
        
        # 파싱 성공 여부 판단
        if result['파싱된_개당수량'] or result['파싱된_용량']:
            result['파싱_성공'] = True
        
        # 매칭용 키 생성
        key_parts = []
        if result['파싱된_개당수량']:
            key_parts.append(f"개당수량:{result['파싱된_개당수량']}")
        if result['파싱된_용량']:
            key_parts.append(f"용량:{result['파싱된_용량']}")
        if result['파싱된_맛']:
            key_parts.append(f"맛:{result['파싱된_맛']}")
        key_parts.append(f"수량:{result['파싱된_수량']}")
        
        result['매칭_키'] = "|".join(key_parts)
        
        return result

class CSVPreprocessor:
    def __init__(self):
        self.parser = ProductNameParser()
    
    def process_csv(self, input_path, output_path=None):
        """CSV 파일을 처리하여 크롤링 결과와 매칭 가능한 형태로 전처리"""
        try:
            # CSV 읽기
            df = pd.read_csv(input_path, encoding='utf-8-sig')
            print(f"입력 파일 읽기 완료: {len(df)}개 행")
            
            # 필수 컬럼 확인
            required_columns = ['상품링크', '모델명', '상품명', '옵션']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"필수 컬럼이 없습니다: {missing_columns}")
                return False
            
            # 파싱 결과를 저장할 새로운 컬럼들
            parsing_columns = [
                '기본_모델명', '파싱된_개당수량', '파싱된_용량', '파싱된_맛', 
                '파싱된_수량', '파싱_성공', '매칭_키'
            ]
            
            for col in parsing_columns:
                df[col] = None
            
            # 각 행에 대해 파싱 수행
            successful_parses = 0
            
            for idx, row in df.iterrows():
                product_name = row['상품명']
                option_count = row['옵션']
                model_name = row['모델명']
                
                print(f"[{idx+1}/{len(df)}] 파싱 중: {model_name} - {product_name[:40]}...")
                
                # 상품명 파싱
                parsed_info = self.parser.parse_product_name(product_name, option_count, model_name)
                
                # 결과를 DataFrame에 저장
                for key, value in parsed_info.items():
                    df.at[idx, key] = value
                
                if parsed_info['파싱_성공']:
                    successful_parses += 1
                    print(f"  → 성공: {parsed_info['파싱된_개당수량'] or parsed_info['파싱된_용량']} + {parsed_info['파싱된_수량']}")
                else:
                    print(f"  → 실패: 파싱 불가")
            
            # 결과 저장
            if output_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = f'preprocessed_for_crawl_matching_{timestamp}.csv'
            
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            print(f"\n=== 전처리 완료 ===")
            print(f"총 처리 항목: {len(df)}개")
            print(f"파싱 성공: {successful_parses}개 ({successful_parses/len(df)*100:.1f}%)")
            print(f"파싱 실패: {len(df)-successful_parses}개")
            print(f"결과 파일: {output_path}")
            
            # 파싱 결과 통계
            self.print_parsing_statistics(df)
            
            return True
            
        except Exception as e:
            print(f"처리 중 오류 발생: {e}")
            return False
    
    def print_parsing_statistics(self, df):
        """파싱 결과 통계 출력"""
        print(f"\n=== 파싱 통계 ===")
        
        # 성공률
        success_rate = df['파싱_성공'].sum() / len(df) * 100
        print(f"전체 파싱 성공률: {success_rate:.1f}%")
        
        # 개당수량별 분포
        quantity_counts = df[df['파싱된_개당수량'].notnull()]['파싱된_개당수량'].value_counts()
        if len(quantity_counts) > 0:
            print(f"\n파싱된 개당수량 분포:")
            for quantity, count in quantity_counts.head(10).items():
                print(f"  {quantity}: {count}개")
        
        # 용량별 분포
        capacity_counts = df[df['파싱된_용량'].notnull()]['파싱된_용량'].value_counts()
        if len(capacity_counts) > 0:
            print(f"\n파싱된 용량 분포:")
            for capacity, count in capacity_counts.head(10).items():
                print(f"  {capacity}: {count}개")
        
        # 기본 모델명별 분포
        base_model_counts = df['기본_모델명'].value_counts()
        print(f"\n기본 모델명 분포 (상위 10개):")
        for model, count in base_model_counts.head(10).items():
            print(f"  {model}: {count}개")
        
        # 파싱 실패 사례 (매칭키 분석)
        failed_cases = df[df['파싱_성공'] == False]
        if len(failed_cases) > 0:
            print(f"\n파싱 실패 사례 (상위 5개):")
            for idx, (_, row) in enumerate(failed_cases.head(5).iterrows(), 1):
                print(f"  {idx}. {row['모델명']}: {row['상품명']}")
        
        # 매칭키 예시
        successful_cases = df[df['파싱_성공'] == True]
        if len(successful_cases) > 0:
            print(f"\n매칭키 예시 (상위 5개):")
            for idx, (_, row) in enumerate(successful_cases.head(5).iterrows(), 1):
                print(f"  {idx}. {row['모델명']}: {row['매칭_키']}")

def main():
    parser = argparse.ArgumentParser(description='크롤링 결과와 매칭을 위한 상품명 파싱 전처리')
    parser.add_argument('input_csv', help='입력 CSV 파일 경로 (새 인풋)')
    parser.add_argument('-o', '--output', help='출력 CSV 파일 경로')
    
    args = parser.parse_args()
    
    # 파일 존재 확인
    try:
        pd.read_csv(args.input_csv, nrows=1)
    except FileNotFoundError:
        print(f"입력 파일을 찾을 수 없습니다: {args.input_csv}")
        sys.exit(1)
    except Exception as e:
        print(f"입력 파일을 읽을 수 없습니다: {e}")
        sys.exit(1)
    
    # 전처리 실행
    preprocessor = CSVPreprocessor()
    success = preprocessor.process_csv(args.input_csv, args.output)
    
    if not success:
        sys.exit(1)

if __name__ == '__main__':
    main()