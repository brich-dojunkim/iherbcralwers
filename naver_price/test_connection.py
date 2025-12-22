"""
Google Sheets API 연결 테스트 스크립트
설정이 올바르게 되었는지 확인
"""

import gspread
from google.oauth2.service_account import Credentials
import sys


def test_google_sheets_connection():
    """구글 스프레드시트 연결 테스트"""
    
    print("="*60)
    print("Google Sheets API 연결 테스트")
    print("="*60 + "\n")
    
    # 설정
    SPREADSHEET_ID = '1o2xj4R02Wr4QDhfR3VdQSYbtoaueZzEZend15DqObR8'
    CREDENTIALS_FILE = 'credentials.json'
    
    try:
        # 1. credentials.json 파일 확인
        print("1️⃣ credentials.json 파일 확인...")
        try:
            with open(CREDENTIALS_FILE, 'r') as f:
                import json
                creds_data = json.load(f)
                client_email = creds_data.get('client_email', 'N/A')
                print(f"   ✅ 파일 존재")
                print(f"   📧 서비스 계정: {client_email}")
        except FileNotFoundError:
            print(f"   ❌ {CREDENTIALS_FILE} 파일을 찾을 수 없습니다!")
            print(f"   → Google Cloud Console에서 JSON 키 파일을 다운로드하세요.")
            return False
        except Exception as e:
            print(f"   ❌ 파일 읽기 오류: {e}")
            return False
        
        # 2. Google API 인증
        print("\n2️⃣ Google API 인증...")
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_file(
                CREDENTIALS_FILE, 
                scopes=scope
            )
            client = gspread.authorize(creds)
            print("   ✅ 인증 성공")
        except Exception as e:
            print(f"   ❌ 인증 실패: {e}")
            return False
        
        # 3. 스프레드시트 열기
        print("\n3️⃣ 스프레드시트 연결...")
        try:
            spreadsheet = client.open_by_key(SPREADSHEET_ID)
            print(f"   ✅ 연결 성공: {spreadsheet.title}")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"   ❌ 스프레드시트를 찾을 수 없습니다!")
            print(f"   → 스프레드시트 ID 확인: {SPREADSHEET_ID}")
            print(f"   → 서비스 계정({client_email})과 공유했는지 확인하세요!")
            return False
        except Exception as e:
            print(f"   ❌ 연결 실패: {e}")
            return False
        
        # 4. 워크시트 접근
        print("\n4️⃣ 워크시트 접근...")
        try:
            sheet = spreadsheet.worksheet("0")
            print(f"   ✅ 시트 이름: {sheet.title}")
            print(f"   📊 행 개수: {sheet.row_count}")
            print(f"   📊 열 개수: {sheet.col_count}")
        except gspread.exceptions.WorksheetNotFound:
            print(f"   ❌ '0' 시트를 찾을 수 없습니다!")
            print(f"   → 사용 가능한 시트:")
            for ws in spreadsheet.worksheets():
                print(f"      - {ws.title}")
            return False
        except Exception as e:
            print(f"   ❌ 시트 접근 실패: {e}")
            return False
        
        # 5. 데이터 읽기 테스트
        print("\n5️⃣ 데이터 읽기 테스트...")
        try:
            all_data = sheet.get_all_values()
            if all_data:
                print(f"   ✅ 데이터 읽기 성공")
                print(f"   📝 총 데이터 행: {len(all_data)}")
                if len(all_data) > 0:
                    print(f"   📋 헤더: {all_data[0]}")
            else:
                print(f"   ⚠️ 시트에 데이터가 없습니다")
        except Exception as e:
            print(f"   ❌ 데이터 읽기 실패: {e}")
            return False
        
        # 6. 쓰기 권한 테스트
        print("\n6️⃣ 쓰기 권한 테스트...")
        try:
            # 테스트용 셀에 쓰기 (마지막 행의 마지막 열)
            test_value = "연결테스트"
            sheet.update_cell(sheet.row_count, sheet.col_count, test_value)
            
            # 읽어서 확인
            read_value = sheet.cell(sheet.row_count, sheet.col_count).value
            if read_value == test_value:
                print(f"   ✅ 쓰기 권한 확인")
                # 테스트 값 삭제
                sheet.update_cell(sheet.row_count, sheet.col_count, "")
            else:
                print(f"   ⚠️ 쓰기는 되었으나 값이 다름")
        except Exception as e:
            print(f"   ❌ 쓰기 권한 없음: {e}")
            print(f"   → 서비스 계정을 '편집자' 권한으로 공유했는지 확인하세요!")
            return False
        
        # 성공!
        print("\n" + "="*60)
        print("✅ 모든 테스트 통과!")
        print("="*60)
        print("\n프로그램을 실행할 준비가 되었습니다.")
        print("실행 명령: python naver_price_crawler.py")
        print("="*60)
        return True
        
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_google_sheets_connection()
    sys.exit(0 if success else 1)