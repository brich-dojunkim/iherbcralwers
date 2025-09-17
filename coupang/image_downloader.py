import os
import requests
from PIL import Image
from datetime import datetime

class ImageDownloader:
    def __init__(self, image_dir="/coupang_images"):
        self.image_dir = image_dir
        self.downloaded_images = []
        self.image_download_stats = {
            'total_attempts': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'skipped_existing': 0
        }
        
        # 이미지 디렉토리 생성
        os.makedirs(self.image_dir, exist_ok=True)
        print(f"이미지 저장 디렉토리: {self.image_dir}")
    
    def extract_image_url_from_element(self, product_item):
        """상품 항목에서 이미지 URL 추출"""
        try:
            # 여러 이미지 선택자 시도 (품질 우선순위대로)
            image_selectors = [
                "figure.ProductUnit_productImage__Mqcg1 img[src*='320x320']",  # 320x320 고해상도
                "figure.ProductUnit_productImage__Mqcg1 img",                 # 기본 상품 이미지
                "img[src*='coupangcdn.com']",                                 # 쿠팡 CDN 모든 이미지
                "img"                                                         # 백업용
            ]
            
            for selector in image_selectors:
                try:
                    img_element = product_item.select_one(selector)
                    if img_element:
                        img_url = img_element.get('src')
                        if img_url and 'coupangcdn.com' in img_url:
                            # 고해상도 이미지 URL로 변환 시도
                            if '/thumbnails/remote/' in img_url:
                                high_res_url = img_url.replace('/320x320ex/', '/600x600ex/').replace('/230x230ex/', '/600x600ex/')
                                return high_res_url
                            return img_url
                except:
                    continue
            
            return None
            
        except Exception as e:
            print(f"이미지 URL 추출 오류: {e}")
            return None
    
    def download_image(self, image_url, product_id):
        """
        이미지 다운로드 (Gemini 이미지 매칭용)
        
        Args:
            image_url: 이미지 URL
            product_id: 쿠팡 상품 ID
            
        Returns:
            dict: 다운로드 결과 정보
        """
        if not image_url or not product_id:
            return {'success': False, 'reason': 'download_disabled_or_invalid_params'}
        
        try:
            self.image_download_stats['total_attempts'] += 1
            
            # 파일명 생성 (Gemini 매칭용 규칙)
            filename = f"coupang_{product_id}.jpg"
            filepath = os.path.join(self.image_dir, filename)
            
            # 기존 파일 존재 확인 (중복 다운로드 방지)
            if os.path.exists(filepath):
                # 파일 크기 확인 (유효한 이미지인지)
                if os.path.getsize(filepath) > 1024:  # 1KB 이상
                    self.image_download_stats['skipped_existing'] += 1
                    return {
                        'success': True, 
                        'reason': 'already_exists',
                        'filepath': filepath,
                        'filename': filename
                    }
            
            # URL 정리 및 검증
            if not image_url.startswith('http'):
                image_url = 'https:' + image_url if image_url.startswith('//') else 'https://' + image_url
            
            # 이미지 다운로드
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.coupang.com/',
                'Accept': 'image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'image',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'cross-site'
            }
            
            response = requests.get(image_url, headers=headers, timeout=10, stream=True)
            response.raise_for_status()
            
            # Content-Type 확인
            content_type = response.headers.get('content-type', '').lower()
            if not any(img_type in content_type for img_type in ['image/jpeg', 'image/jpg', 'image/png', 'image/webp']):
                self.image_download_stats['failed_downloads'] += 1
                return {'success': False, 'reason': f'invalid_content_type: {content_type}'}
            
            # 이미지 저장
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # 이미지 유효성 검증
            try:
                with Image.open(filepath) as img:
                    # 이미지 정보 확인
                    width, height = img.size
                    if width < 50 or height < 50:  # 너무 작은 이미지 제외
                        os.remove(filepath)
                        self.image_download_stats['failed_downloads'] += 1
                        return {'success': False, 'reason': 'image_too_small'}
                    
                    # 이미지 검증 (손상 여부)
                    img.verify()
                    
                    # Gemini 매칭을 위한 이미지 정보
                    file_size = os.path.getsize(filepath)
                    
                    self.image_download_stats['successful_downloads'] += 1
                    self.downloaded_images.append({
                        'product_id': product_id,
                        'filename': filename,
                        'filepath': filepath,
                        'image_url': image_url,
                        'width': width,
                        'height': height,
                        'file_size': file_size,
                        'downloaded_at': datetime.now().isoformat()
                    })
                    
                    return {
                        'success': True,
                        'reason': 'download_success',
                        'filepath': filepath,
                        'filename': filename,
                        'width': width,
                        'height': height,
                        'file_size': file_size
                    }
                    
            except Exception as img_error:
                # 손상된 이미지 파일 삭제
                if os.path.exists(filepath):
                    os.remove(filepath)
                self.image_download_stats['failed_downloads'] += 1
                return {'success': False, 'reason': f'image_verification_failed: {img_error}'}
        
        except requests.exceptions.RequestException as e:
            self.image_download_stats['failed_downloads'] += 1
            return {'success': False, 'reason': f'download_error: {e}'}
        
        except Exception as e:
            self.image_download_stats['failed_downloads'] += 1
            return {'success': False, 'reason': f'unexpected_error: {e}'}
    
    def print_image_download_summary(self):
        """이미지 다운로드 통계 출력"""
        stats = self.image_download_stats
        total = stats['total_attempts']
        
        if total == 0:
            print("이미지 다운로드 시도 없음")
            return
        
        print(f"\n=== 이미지 다운로드 최종 통계 ===")
        print(f"총 시도: {total}개")
        print(f"성공: {stats['successful_downloads']}개 ({stats['successful_downloads']/total*100:.1f}%)")
        print(f"실패: {stats['failed_downloads']}개 ({stats['failed_downloads']/total*100:.1f}%)")
        print(f"기존파일 사용: {stats['skipped_existing']}개 ({stats['skipped_existing']/total*100:.1f}%)")
        print(f"저장 위치: {self.image_dir}")
        
        # 성공한 이미지들의 정보
        if self.downloaded_images:
            print(f"\n다운로드된 이미지 샘플 (상위 5개):")
            for i, img_info in enumerate(self.downloaded_images[:5], 1):
                file_size_kb = img_info['file_size'] / 1024
                print(f"  {i}. {img_info['filename']} ({img_info['width']}x{img_info['height']}, {file_size_kb:.1f}KB)")
        
        print(f"\n🔍 Gemini 이미지 매칭 준비 완료:")
        print(f"  - 고품질 상품 이미지 {stats['successful_downloads'] + stats['skipped_existing']}개 확보")
        print(f"  - 파일명 규칙: coupang_{{product_id}}.jpg")
        print(f"  - 아이허브 이미지와 Gemini Pro Vision 비교 가능")