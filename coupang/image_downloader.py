import os
import requests
from PIL import Image
from datetime import datetime
from coupang_config import CoupangConfig

class ImageDownloader:
    def __init__(self, image_dir=None):
        if image_dir is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.image_dir = os.path.join(current_dir, CoupangConfig.DEFAULT_IMAGE_DIR_NAME)
        else:
            self.image_dir = image_dir
        
        # 디렉토리 생성
        os.makedirs(self.image_dir, exist_ok=True)
        
        self.downloaded_images = []
        self.image_download_stats = {
            'total_attempts': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'skipped_existing': 0
        }
        
        print(f"🖼️ 쿠팡 이미지 저장: {self.image_dir}")
    
    def extract_image_url_from_element(self, product_item):
        """상품 항목에서 이미지 URL 추출"""
        try:
            # 고품질 이미지 선택자들
            image_selectors = [
                "figure.ProductUnit_productImage__Mqcg1 img[src*='320x320']",  # 320x320 고해상도
                "figure.ProductUnit_productImage__Mqcg1 img",                 # 기본 상품 이미지
                "img[src*='coupangcdn.com']",                                 # 쿠팡 CDN 이미지
                "img"                                                         # 백업용
            ]
            
            for selector in image_selectors:
                try:
                    img_element = product_item.select_one(selector)
                    if img_element:
                        img_url = img_element.get('src')
                        if img_url and 'coupangcdn.com' in img_url:
                            # 고해상도 URL로 변환
                            if '/thumbnails/remote/' in img_url:
                                high_res_url = img_url.replace('/320x320ex/', '/600x600ex/').replace('/230x230ex/', '/600x600ex/')
                                return high_res_url
                            return img_url
                except:
                    continue
            
            return None
            
        except Exception as e:
            return None
    
    def download_image(self, image_url, product_id):
        """
        이미지 다운로드
        
        Args:
            image_url: 이미지 URL
            product_id: 쿠팡 상품 ID
            
        Returns:
            dict: 다운로드 결과 정보
        """
        if not image_url or not product_id:
            return {'success': False, 'reason': 'invalid_params'}
        
        try:
            self.image_download_stats['total_attempts'] += 1
            
            # 파일명 생성 (Gemini 매칭용 규칙)
            filename = f"coupang_{product_id}.jpg"
            filepath = os.path.join(self.image_dir, filename)
            
            # 기존 파일 존재 확인
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1024:
                self.image_download_stats['skipped_existing'] += 1
                return {
                    'success': True, 
                    'reason': 'already_exists',
                    'filepath': filepath,
                    'filename': filename
                }
            
            # URL 정리
            if not image_url.startswith('http'):
                image_url = 'https:' + image_url if image_url.startswith('//') else 'https://' + image_url
            
            # 이미지 다운로드
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Referer': 'https://www.coupang.com/',
                'Accept': 'image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
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
                    width, height = img.size
                    if width < 50 or height < 50:
                        os.remove(filepath)
                        self.image_download_stats['failed_downloads'] += 1
                        return {'success': False, 'reason': 'image_too_small'}
                    
                    img.verify()
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
            return
        
        print(f"\n=== 쿠팡 이미지 다운로드 통계 ===")
        print(f"저장 위치: {self.image_dir}")
        print(f"총 시도: {total}개")
        print(f"성공: {stats['successful_downloads']}개 ({stats['successful_downloads']/total*100:.1f}%)")
        print(f"기존파일: {stats['skipped_existing']}개 ({stats['skipped_existing']/total*100:.1f}%)")
        print(f"실패: {stats['failed_downloads']}개 ({stats['failed_downloads']/total*100:.1f}%)")
        
        if self.downloaded_images:
            print(f"\n다운로드된 이미지 샘플:")
            for i, img_info in enumerate(self.downloaded_images[:3], 1):
                file_size_kb = img_info['file_size'] / 1024
                print(f"  {i}. {img_info['filename']} ({img_info['width']}x{img_info['height']}, {file_size_kb:.1f}KB)")
        
        print(f"🎯 Gemini 이미지 매칭 준비 완료")
    
    def extract_image_url_from_element(self, product_item):
        """상품 항목에서 이미지 URL 추출"""
        try:
            # 고품질 이미지 선택자들
            image_selectors = [
                "figure.ProductUnit_productImage__Mqcg1 img[src*='320x320']",  # 320x320 고해상도
                "figure.ProductUnit_productImage__Mqcg1 img",                 # 기본 상품 이미지
                "img[src*='coupangcdn.com']",                                 # 쿠팡 CDN 이미지
                "img"                                                         # 백업용
            ]
            
            for selector in image_selectors:
                try:
                    img_element = product_item.select_one(selector)
                    if img_element:
                        img_url = img_element.get('src')
                        if img_url and 'coupangcdn.com' in img_url:
                            # 고해상도 URL로 변환
                            if '/thumbnails/remote/' in img_url:
                                high_res_url = img_url.replace('/320x320ex/', '/600x600ex/').replace('/230x230ex/', '/600x600ex/')
                                return high_res_url
                            return img_url
                except:
                    continue
            
            return None
            
        except Exception as e:
            return None
    
    def download_image(self, image_url, product_id):
        """
        이미지 다운로드
        
        Args:
            image_url: 이미지 URL
            product_id: 쿠팡 상품 ID
            
        Returns:
            dict: 다운로드 결과 정보
        """
        if not image_url or not product_id:
            return {'success': False, 'reason': 'invalid_params'}
        
        try:
            self.image_download_stats['total_attempts'] += 1
            
            # 파일명 생성 (Gemini 매칭용 규칙)
            filename = f"coupang_{product_id}.jpg"
            filepath = os.path.join(self.image_dir, filename)
            
            # 기존 파일 존재 확인
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1024:
                self.image_download_stats['skipped_existing'] += 1
                return {
                    'success': True, 
                    'reason': 'already_exists',
                    'filepath': filepath,
                    'filename': filename
                }
            
            # URL 정리
            if not image_url.startswith('http'):
                image_url = 'https:' + image_url if image_url.startswith('//') else 'https://' + image_url
            
            # 이미지 다운로드
            headers = CoupangConfig.DEFAULT_HEADERS
            
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
                    width, height = img.size
                    if width < 50 or height < 50:
                        os.remove(filepath)
                        self.image_download_stats['failed_downloads'] += 1
                        return {'success': False, 'reason': 'image_too_small'}
                    
                    img.verify()
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
            return
        
        print(f"\n=== 쿠팡 이미지 다운로드 통계 ===")
        print(f"저장 위치: {self.image_dir}")
        print(f"총 시도: {total}개")
        print(f"성공: {stats['successful_downloads']}개 ({stats['successful_downloads']/total*100:.1f}%)")
        print(f"기존파일: {stats['skipped_existing']}개 ({stats['skipped_existing']/total*100:.1f}%)")
        print(f"실패: {stats['failed_downloads']}개 ({stats['failed_downloads']/total*100:.1f}%)")
        
        if self.downloaded_images:
            print(f"\n다운로드된 이미지 샘플:")
            for i, img_info in enumerate(self.downloaded_images[:3], 1):
                file_size_kb = img_info['file_size'] / 1024
                print(f"  {i}. {img_info['filename']} ({img_info['width']}x{img_info['height']}, {file_size_kb:.1f}KB)")
        
        print(f"🎯 Gemini 이미지 매칭 준비 완료")