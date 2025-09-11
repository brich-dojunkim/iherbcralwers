import google.generativeai as genai
import pandas as pd
import time
import sys
import os

# 설정
API_KEY = "AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE"

class DirectMatcher:
    """파일 업로드 방식으로 직접 매칭"""
    
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('models/gemini-2.0-flash')
    
    def create_matching_prompt(self):
        """매칭용 프롬프트 생성"""
        prompt = """
첨부된 두 CSV 파일을 분석해서 동일한 제품들을 매칭해주세요.

파일 정보:
1. 쿠팡 파일: product_name_english 컬럼의 상품명 사용
2. 아이허브 파일: product_name 컬럼의 상품명 사용 (이미 NOW Foods만 필터링됨)

매칭 기준:
- 주성분이 동일해야 함 (L-Carnitine, Magnesium, Omega-3 등)
- 함량이 동일하거나 매우 유사해야 함 (500mg, 1000mg 등)
- 제형이 동일해야 함 (Capsules, Tablets, Softgels 등)
- 개수가 동일하거나 유사해야 함 (180개, 120개 등)

결과 형식 (CSV):
coupang_id,coupang_name,iherb_id,iherb_name,match_confidence,match_reason

규칙:
1. match_confidence는 0.0-1.0 점수 (0.7 이상만 포함)
2. match_reason은 매칭 근거 간단히 설명
3. 한 쿠팡 제품당 최대 1개의 아이허브 제품만 매칭
4. 확실하지 않으면 매칭하지 마세요

응답은 헤더 포함해서 CSV 형태로만 주세요.
"""
        return prompt
    
    def upload_and_match(self, coupang_csv: str, iherb_xlsx: str, output_csv: str):
        """파일 업로드하고 매칭 수행"""
        print("📤 파일 업로드 시작...")
        
        temp_iherb_csv = None
        coupang_file = None
        iherb_file = None
        
        try:
            # 아이허브 Excel을 CSV로 변환
            print(f"   Excel → CSV 변환: {iherb_xlsx}")
            iherb_df = pd.read_excel(iherb_xlsx)
            print(f"   전체 아이허브 데이터: {len(iherb_df)}개")
            
            # NOW Foods만 필터링
            iherb_now = iherb_df[iherb_df['product_brand'].str.contains('NOW', case=False, na=False)]
            print(f"   NOW Foods 필터링: {len(iherb_now)}개")
            
            # 임시 CSV 파일 생성
            temp_iherb_csv = "temp_iherb_now_foods.csv"
            iherb_now.to_csv(temp_iherb_csv, index=False, encoding='utf-8')
            print(f"   임시 CSV 생성: {temp_iherb_csv}")
            
            # 파일 업로드
            print(f"   업로드 중: {coupang_csv}")
            coupang_file = genai.upload_file(coupang_csv)
            print(f"   ✅ 완료: {coupang_file.name}")
            
            print(f"   업로드 중: {temp_iherb_csv}")
            iherb_file = genai.upload_file(temp_iherb_csv)
            print(f"   ✅ 완료: {iherb_file.name}")
            
            # 파일 처리 대기
            print("⏱️ 파일 처리 대기 중...")
            time.sleep(10)
            
            # 프롬프트 생성
            prompt = self.create_matching_prompt()
            
            print("🤖 AI 매칭 시작...")
            print("   (대용량 데이터 처리로 시간이 걸릴 수 있습니다)")
            
            # AI 매칭 수행
            response = self.model.generate_content([
                prompt,
                coupang_file,
                iherb_file
            ])
            
            print("✅ AI 매칭 완료!")
            
            # 결과 저장
            print(f"💾 결과 저장: {output_csv}")
            with open(output_csv, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            print("🎉 매칭 완료!")
            
            # 결과 미리보기
            self.preview_results(output_csv)
            
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
            raise
            
        finally:
            # 파일 정리
            print("🗑️ 파일 정리 중...")
            
            # 업로드된 파일 정리
            try:
                if coupang_file:
                    genai.delete_file(coupang_file.name)
                    print(f"   삭제: {coupang_file.name}")
            except:
                pass
                
            try:
                if iherb_file:
                    genai.delete_file(iherb_file.name)
                    print(f"   삭제: {iherb_file.name}")
            except:
                pass
            
            # 임시 파일 정리
            if temp_iherb_csv and os.path.exists(temp_iherb_csv):
                os.remove(temp_iherb_csv)
                print(f"   삭제: {temp_iherb_csv}")
    
    def preview_results(self, output_csv: str):
        """결과 미리보기"""
        try:
            df = pd.read_csv(output_csv)
            
            print(f"\n📊 매칭 결과 요약:")
            print(f"총 매칭: {len(df)}개")
            
            if len(df) > 0:
                high_conf = len(df[df['match_confidence'] >= 0.9])
                medium_conf = len(df[(df['match_confidence'] >= 0.7) & (df['match_confidence'] < 0.9)])
                
                print(f"고신뢰도 (0.9+): {high_conf}개")
                print(f"중신뢰도 (0.7+): {medium_conf}개")
                print(f"평균 신뢰도: {df['match_confidence'].mean():.2f}")
                
                print(f"\n🔍 상위 5개 매칭:")
                top_matches = df.nlargest(5, 'match_confidence')
                for i, row in top_matches.iterrows():
                    print(f"{i+1}. 신뢰도 {row['match_confidence']:.2f}")
                    print(f"   쿠팡: {row['coupang_name'][:60]}...")
                    print(f"   아이허브: {row['iherb_name'][:60]}...")
                    print(f"   이유: {row['match_reason']}")
                    print()
            else:
                print("⚠️ 매칭된 결과가 없습니다.")
            
        except Exception as e:
            print(f"미리보기 오류: {e}")
            print("원본 응답을 확인해보세요.")
    
    def batch_matching(self, coupang_csv: str, iherb_xlsx: str, output_csv: str, batch_size: int = 100):
        """배치 방식으로 매칭 (대용량 데이터용)"""
        print("📊 배치 매칭 시작...")
        
        # 쿠팡 데이터 로드
        coupang_df = pd.read_csv(coupang_csv)
        print(f"쿠팡 데이터: {len(coupang_df)}개")
        
        # 아이허브 데이터 로드 및 필터링
        iherb_df = pd.read_excel(iherb_xlsx)
        iherb_now = iherb_df[iherb_df['product_brand'].str.contains('NOW', case=False, na=False)]
        print(f"아이허브 NOW Foods: {len(iherb_now)}개")
        
        # 아이허브 임시 CSV 생성
        temp_iherb = "temp_iherb_now_batch.csv"
        iherb_now.to_csv(temp_iherb, index=False, encoding='utf-8')
        
        all_matches = []
        total_batches = (len(coupang_df) + batch_size - 1) // batch_size
        
        for i in range(0, len(coupang_df), batch_size):
            batch_num = i // batch_size + 1
            print(f"\n📦 배치 {batch_num}/{total_batches} 처리 중...")
            print(f"   범위: {i+1}~{min(i+batch_size, len(coupang_df))}번째 상품")
            
            # 배치 데이터 준비
            coupang_batch = coupang_df.iloc[i:i+batch_size]
            
            # 임시 파일 생성
            temp_coupang = f"temp_coupang_batch_{batch_num}.csv"
            coupang_batch.to_csv(temp_coupang, index=False, encoding='utf-8')
            
            try:
                # 배치 매칭
                batch_output = f"temp_matches_batch_{batch_num}.csv"
                
                print(f"   🤖 AI 매칭 중...")
                
                # 파일 업로드
                coupang_file = genai.upload_file(temp_coupang)
                iherb_file = genai.upload_file(temp_iherb)
                
                # 대기
                time.sleep(5)
                
                # AI 매칭
                prompt = self.create_matching_prompt()
                response = self.model.generate_content([prompt, coupang_file, iherb_file])
                
                # 결과 저장
                with open(batch_output, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                # 결과 수집
                batch_df = pd.read_csv(batch_output)
                all_matches.append(batch_df)
                
                print(f"   ✅ 완료! {len(batch_df)}개 매칭")
                
                # 파일 정리
                genai.delete_file(coupang_file.name)
                genai.delete_file(iherb_file.name)
                
                for temp_file in [temp_coupang, batch_output]:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                
            except Exception as e:
                print(f"   ❌ 배치 {batch_num} 실패: {e}")
                
                # 실패시 임시 파일 정리
                for temp_file in [temp_coupang, f"temp_matches_batch_{batch_num}.csv"]:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                continue
        
        # 임시 아이허브 파일 정리
        if os.path.exists(temp_iherb):
            os.remove(temp_iherb)
        
        # 최종 결과 통합
        if all_matches:
            final_df = pd.concat(all_matches, ignore_index=True)
            final_df.to_csv(output_csv, index=False, encoding='utf-8')
            print(f"\n🎉 배치 매칭 완료!")
            print(f"총 {len(final_df)}개 매칭 결과를 {output_csv}에 저장했습니다.")
            
            # 요약 통계
            if len(final_df) > 0:
                high_conf = len(final_df[final_df['match_confidence'] >= 0.9])
                medium_conf = len(final_df[(final_df['match_confidence'] >= 0.7) & (final_df['match_confidence'] < 0.9)])
                
                print(f"📊 최종 통계:")
                print(f"- 고신뢰도 (0.9+): {high_conf}개")
                print(f"- 중신뢰도 (0.7+): {medium_conf}개")
                print(f"- 평균 신뢰도: {final_df['match_confidence'].mean():.2f}")
        else:
            print("❌ 매칭 결과가 없습니다.")

def main():
    """메인 실행 함수"""
    if len(sys.argv) < 2:
        print("사용법:")
        print("1. 직접 매칭: python direct_matcher.py direct")
        print("2. 배치 매칭: python direct_matcher.py batch")
        print("\n설명:")
        print("- direct: 전체 데이터를 한번에 업로드해서 매칭 (빠름, 위험)")
        print("- batch: 데이터를 나누어서 안전하게 매칭 (안전함)")
        return
    
    mode = sys.argv[1]
    matcher = DirectMatcher(API_KEY)
    
    coupang_file = "coupang_products_translated.csv"
    iherb_file = "US ITEM FEED TITLE BRAND EN.xlsx"
    output_file = "direct_matches.csv"
    
    # 파일 존재 확인
    if not os.path.exists(coupang_file):
        print(f"❌ 파일이 없습니다: {coupang_file}")
        return
    
    if not os.path.exists(iherb_file):
        print(f"❌ 파일이 없습니다: {iherb_file}")
        return
    
    print(f"=== 직접 매칭 시작 ({mode} 모드) ===")
    
    try:
        if mode == "direct":
            matcher.upload_and_match(coupang_file, iherb_file, output_file)
        elif mode == "batch":
            matcher.batch_matching(coupang_file, iherb_file, output_file, batch_size=50)
        else:
            print("❌ 잘못된 모드입니다. 'direct' 또는 'batch'를 사용하세요.")
            return
            
        print(f"\n✅ 완료! 결과: {output_file}")
        
    except Exception as e:
        print(f"❌ 실패: {e}")

if __name__ == "__main__":
    main()