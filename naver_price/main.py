"""
환경변수를 사용한 안전한 실행 스크립트
.env 파일 또는 시스템 환경변수 사용
"""

import os
from dotenv import load_dotenv
from naver_price_crawler import NaverPriceCrawler

# .env 파일 로드
load_dotenv()

def main():
    """환경변수를 사용한 메인 함수"""
    
    # 환경변수에서 설정 가져오기
    SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '1o2xj4R02Wr4QDhfR3VdQSYbtoaueZzEZend15DqObR8')
    CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE', 'credentials.json')
    NAVER_ID = os.getenv('NAVER_ID')
    NAVER_PW = os.getenv('NAVER_PW')
    
    # 환경변수 확인
    if not NAVER_ID or not NAVER_PW:
        print("❌ 환경변수 설정이 필요합니다.")
        print("   .env 파일을 생성하거나 환경변수를 설정하세요.")
        print("\n예시:")
        print("   NAVER_ID=your_id")
        print("   NAVER_PW=your_password")
        return
    
    crawler = None
    
    try:
        print("="*60)
        print("네이버 쇼핑 가격 크롤러 시작")
        print("="*60)
        print(f"스프레드시트 ID: {SPREADSHEET_ID}")
        print(f"네이버 ID: {NAVER_ID}")
        print("="*60 + "\n")
        
        # 크롤러 초기화
        crawler = NaverPriceCrawler(SPREADSHEET_ID, CREDENTIALS_FILE)
        
        # 드라이버 설정
        crawler.setup_driver()
        
        # 네이버 로그인
        if not crawler.naver_login(NAVER_ID, NAVER_PW):
            print("❌ 로그인 실패. 프로그램을 종료합니다.")
            return
        
        # 사용자 확인
        print("\n" + "="*60)
        print("모든 URL을 자동으로 처리합니다.")
        print("="*60)
        
        confirm = input("\n시작하시겠습니까? (y/n): ").strip().lower()
        if confirm != 'y':
            print("취소되었습니다.")
            return
        
        # URL 처리 (자동으로 모든 URL 처리)
        crawler.process_urls()
        
        print("\n" + "="*60)
        print("✅ 모든 작업이 완료되었습니다!")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자가 중단했습니다.")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if crawler:
            crawler.close()


if __name__ == "__main__":
    main()