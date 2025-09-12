"""
상품 매칭 로직 모듈 - 영어명 검색 + Gemini AI + 스마트 Vision
Gemini 2.0 Flash 최적화 버전 (Gemini 판단 기반 이미지 검증)
"""

import re
import time
import urllib.parse
import os
import requests
from PIL import Image
from io import BytesIO
from config import Config, FailureType

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    print("Gemini AI 패키지를 설치해주세요:")
    print("pip install google-generativeai")
    GEMINI_AVAILABLE = False


class ProductMatcher:
    """영어명 검색 + Gemini AI + 스마트 Vision 기반 상품 매칭 시스템"""
    
    def __init__(self, iherb_client):
        self.iherb_client = iherb_client
        self.api_call_count = 0
        self.vision_api_call_count = 0
        self.last_api_call_time = 0
        self._setup_gemini()
    
    def _setup_gemini(self):
        """Gemini AI 초기화"""
        if not GEMINI_AVAILABLE:
            raise ImportError("google-generativeai 패키지가 설치되지 않았습니다.")
        
        try:
            genai.configure(api_key=Config.GEMINI_API_KEY)
            self.text_model = genai.GenerativeModel(Config.GEMINI_TEXT_MODEL)
            self.vision_model = genai.GenerativeModel(Config.GEMINI_VISION_MODEL)
            
            print("  Gemini AI 초기화 완료")
            print(f"    텍스트 모델: {Config.GEMINI_TEXT_MODEL}")
            print(f"    Vision 모델: {Config.GEMINI_VISION_MODEL}")
            print(f"    이미지 비교: {'활성화' if Config.IMAGE_COMPARISON_ENABLED else '비활성화'}")
            
        except Exception as e:
            print(f"  Gemini AI 초기화 실패: {e}")
            raise
    
    def _safe_gemini_call(self, prompt, max_retries=None, use_vision=False, image_data=None):
        """안전한 Gemini API 호출"""
        max_retries = max_retries or Config.GEMINI_MAX_RETRIES
        
        safety_settings = [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
        ]
        
        for attempt in range(max_retries):
            try:
                current_time = time.time()
                if current_time - self.last_api_call_time < Config.GEMINI_RATE_LIMIT_DELAY:
                    time.sleep(Config.GEMINI_RATE_LIMIT_DELAY)
                
                generation_config = {
                    'temperature': 0.1,
                    'max_output_tokens': 50,
                    'top_p': 0.8,
                    'top_k': 20
                }
                
                if use_vision and image_data:
                    response = self.vision_model.generate_content(
                        [prompt] + image_data,
                        generation_config=generation_config,
                        safety_settings=safety_settings
                    )
                    self.vision_api_call_count += 1
                else:
                    response = self.text_model.generate_content(
                        prompt,
                        generation_config=generation_config,
                        safety_settings=safety_settings
                    )
                
                self.api_call_count += 1
                self.last_api_call_time = time.time()
                
                return self._extract_response_text(response)
                
            except Exception as e:
                error_msg = str(e).lower()
                
                if any(keyword in error_msg for keyword in ['quota', 'limit', 'exceeded', 'resource_exhausted']):
                    raise Exception(f"GEMINI_QUOTA_EXCEEDED: {e}")
                elif 'timeout' in error_msg:
                    if attempt == max_retries - 1:
                        raise Exception(f"GEMINI_TIMEOUT: {e}")
                    time.sleep(2 ** attempt)
                else:
                    if attempt == max_retries - 1:
                        raise Exception(f"GEMINI_API_ERROR: {e}")
                    time.sleep(2 ** attempt)
        
        return None
    
    def _extract_response_text(self, response):
        """Gemini 응답에서 텍스트 추출"""
        try:
            if hasattr(response, 'text') and response.text:
                return response.text.strip()
            
            if hasattr(response, 'candidates') and response.candidates:
                for candidate in response.candidates:
                    if hasattr(candidate, 'content') and candidate.content:
                        if hasattr(candidate.content, 'parts') and candidate.content.parts:
                            for part in candidate.content.parts:
                                if hasattr(part, 'text') and part.text:
                                    return part.text.strip()
            
            return None
            
        except Exception as e:
            print(f"    응답 추출 오류: {e}")
            return None
    
    def _load_image_for_gemini(self, image_path):
        """Gemini Vision용 이미지 로드"""
        try:
            if not os.path.exists(image_path):
                return None
            
            file_size_mb = os.path.getsize(image_path) / (1024 * 1024)
            if file_size_mb > Config.MAX_IMAGE_SIZE_MB:
                return None
            
            with Image.open(image_path) as img:
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                max_dimension = 1024
                if max(img.size) > max_dimension:
                    img.thumbnail((max_dimension, max_dimension), Image.Resampling.LANCZOS)
                
                buffer = BytesIO()
                img.save(buffer, format='JPEG', quality=85)
                buffer.seek(0)
                
                return {
                    'mime_type': 'image/jpeg',
                    'data': buffer.getvalue()
                }
                
        except Exception as e:
            return None
    
    def _download_iherb_image(self, product_url, product_code):
        """아이허브 이미지 다운로드"""
        try:
            if not Config.IMAGE_COMPARISON_ENABLED:
                return None
            
            os.makedirs(Config.IHERB_IMAGES_DIR, exist_ok=True)
            image_filename = f"iherb_{product_code}.jpg"
            image_path = os.path.join(Config.IHERB_IMAGES_DIR, image_filename)
            
            if os.path.exists(image_path) and os.path.getsize(image_path) > 1024:
                return image_path
            
            image_url = self.iherb_client.extract_product_image_url(product_url)
            if not image_url:
                return None
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Referer': 'https://www.iherb.com/'
            }
            
            response = requests.get(image_url, headers=headers, timeout=Config.IMAGE_DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            
            with open(image_path, 'wb') as f:
                f.write(response.content)
            
            try:
                with Image.open(image_path) as img:
                    img.verify()
                return image_path
            except:
                if os.path.exists(image_path):
                    os.remove(image_path)
                return None
                
        except Exception as e:
            return None
    
    def _compare_images_with_gemini(self, coupang_image_path, iherb_image_path, search_name):
        """Gemini Vision으로 이미지 비교"""
        try:
            if not Config.IMAGE_COMPARISON_ENABLED:
                return {'success': False, 'reason': 'image_comparison_disabled'}
            
            coupang_image = self._load_image_for_gemini(coupang_image_path)
            iherb_image = self._load_image_for_gemini(iherb_image_path)
            
            if not coupang_image or not iherb_image:
                return {'success': False, 'reason': 'image_load_failed'}
            
            prompt = "Are these the same product? Answer YES or NO only."
            
            print(f"    이미지 비교 중... (Vision API 호출 {self.vision_api_call_count + 1}회)")
            
            response = self._safe_gemini_call(
                prompt, 
                use_vision=True, 
                image_data=[coupang_image, iherb_image]
            )
            
            if not response:
                return {'success': False, 'reason': 'gemini_no_response'}
            
            response_clean = response.strip().upper()
            
            if "YES" in response_clean:
                return {'success': True, 'match': True, 'response': response}
            else:
                return {'success': True, 'match': False, 'response': response}
            
        except Exception as e:
            error_msg = str(e)
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise
            return {'success': False, 'reason': 'vision_api_error', 'error': error_msg}
    
    def _check_strict_dosage_count_filter(self, search_name, iherb_products):
        """엄격한 용량/개수 필터링"""
        search_mg = re.search(r'(\d+(?:,\d+)*)\s*mg', search_name.lower())
        search_count = re.search(r'(\d+)\s*(?:count|ct|tablets?|capsules?|softgels?|veg\s*capsules?|vcaps?|pieces?|servings?|tab)(?!\w)', search_name.lower())
        
        filtered_products = []
        
        for product in iherb_products:
            iherb_name = product['title']
            should_include = True
            rejection_reason = None
            
            if search_mg:
                s_mg = int(search_mg.group(1).replace(',', ''))
                iherb_mg = re.search(Config.PATTERNS['dosage_mg'], iherb_name.lower())
                
                if iherb_mg:
                    i_mg = int(iherb_mg.group(1).replace(',', ''))
                    if s_mg != i_mg:
                        should_include = False
                        rejection_reason = f"용량 불일치 (검색:{s_mg}mg vs 아이허브:{i_mg}mg)"
            
            if search_count and should_include:
                s_count = int(search_count.group(1))
                iherb_count = re.search(Config.PATTERNS['english_count'], iherb_name.lower())
                
                if iherb_count:
                    i_count = int(iherb_count.group(1))
                    if s_count != i_count:
                        should_include = False
                        rejection_reason = f"개수 불일치 (검색:{s_count}개 vs 아이허브:{i_count}개)"
            
            if should_include:
                filtered_products.append(product)
            else:
                print(f"    필터링 제외: {iherb_name[:50]}... ({rejection_reason})")
        
        return filtered_products
    
    def _gemini_text_match(self, search_name, filtered_products):
        """Gemini AI 텍스트 기반 매칭 + 신뢰도 판단"""
        candidates_text = "\n".join([
            f"{i+1}. {product['title']}" 
            for i, product in enumerate(filtered_products)
        ])
        
        prompt = f"Which number matches? Also, is this match confident or uncertain?\n\nTarget: {search_name}\n\n{candidates_text}\n\nAnswer: number (1-{len(filtered_products)}) or 0, and CONFIDENT or UNCERTAIN"
        
        try:
            print(f"    텍스트 매칭 시도 1/3 (API 호출 {self.api_call_count + 1}회)")
            response = self._safe_gemini_call(prompt)
            
            if not response:
                return None, {'reason': 'gemini_no_response'}
            
            # 숫자와 신뢰도 추출
            number_match = re.search(r'(\d+)', response.strip())
            confidence_uncertain = "UNCERTAIN" in response.upper()
            
            if number_match:
                selected_number = int(number_match.group(1))
                
                if selected_number == 0:
                    return None, {
                        'reason': 'gemini_no_match',
                        'gemini_response': response,
                        'candidates_count': len(filtered_products)
                    }
                elif 1 <= selected_number <= len(filtered_products):
                    selected_index = selected_number - 1
                    selected_product = filtered_products[selected_index]
                    return selected_product, {
                        'reason': 'gemini_text_match',
                        'gemini_response': response,
                        'selected_index': selected_index,
                        'selected_product': selected_product['title'],
                        'needs_image_verification': confidence_uncertain
                    }
            
            return None, {
                'reason': 'gemini_parse_error',
                'gemini_response': response,
                'candidates_count': len(filtered_products)
            }
            
        except Exception as e:
            error_msg = str(e)
            
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise
            elif "GEMINI_TIMEOUT" in error_msg:
                return None, {'reason': 'gemini_timeout', 'error': error_msg}
            else:
                return None, {'reason': 'gemini_api_error', 'error': error_msg}
    
    def _final_match_with_smart_image_verification(self, search_name, filtered_products, coupang_product_id=None):
        """최종 매칭: Gemini가 직접 이미지 검증 필요성 판단"""
        if not filtered_products:
            return None, {'reason': 'no_products_after_filtering'}
        
        text_product, text_details = self._gemini_text_match(search_name, filtered_products)
        
        if not text_product:
            return None, text_details
        
        # Gemini가 판단한 이미지 검증 필요성 확인
        needs_image_verification = (
            Config.IMAGE_COMPARISON_ENABLED and 
            coupang_product_id and
            text_details.get('needs_image_verification', False)
        )
        
        if needs_image_verification:
            print(f"    이미지 검증 필요 (Gemini 판단: UNCERTAIN)")
            return self._verify_with_images(text_product, search_name, coupang_product_id, text_details)
        else:
            print(f"    이미지 검증 생략 (Gemini 판단: CONFIDENT)")
            text_details['image_verification'] = 'skipped_confident_match'
            return text_product, text_details
    
    def _verify_with_images(self, text_product, search_name, coupang_product_id, text_details):
        """이미지로 텍스트 매칭 결과 검증"""
        try:
            print(f"    === 이미지 검증 시작 ===")
            
            coupang_image_path = os.path.join(
                Config.COUPANG_IMAGES_DIR, 
                f"coupang_{coupang_product_id}.jpg"
            )

            if not os.path.exists(coupang_image_path):
                print(f"    쿠팡 이미지 없음")
                text_details['image_verification'] = 'coupang_image_missing'
                return text_product, text_details
            else:
                print(f"    쿠팡 이미지 발견: {os.path.basename(coupang_image_path)}")
            
            product_code = self._extract_product_code_from_url(text_product['url'])
            if not product_code:
                print(f"    상품 코드 추출 실패")
                text_details['image_verification'] = 'iherb_code_missing'
                return text_product, text_details
            
            print(f"    아이허브 이미지 다운로드 시도...")
            iherb_image_path = self._download_iherb_image(text_product['url'], product_code)
            
            if not iherb_image_path:
                print(f"    아이허브 이미지 다운로드 실패")
                text_details['image_verification'] = 'iherb_image_download_failed'
                return text_product, text_details
            
            print(f"    Vision API 비교 시작...")
            vision_result = self._compare_images_with_gemini(
                coupang_image_path, iherb_image_path, search_name
            )
            
            if not vision_result.get('success'):
                print(f"    Vision API 실패: {vision_result.get('reason')}")
                text_details['image_verification'] = vision_result.get('reason', 'vision_failed')
                return text_product, text_details
            
            image_match = vision_result.get('match', False)
            
            if image_match:
                text_details.update({
                    'image_verification': 'match',
                    'final_result': 'text_and_image_match'
                })
                print(f"    이미지 매칭 성공!")
                return text_product, text_details
            else:
                text_details.update({
                    'image_verification': 'no_match',
                    'final_result': 'image_mismatch'
                })
                print(f"    이미지 매칭 실패 - 다른 제품으로 판단")
                return None, text_details
                
        except Exception as e:
            error_msg = str(e)
            print(f"    이미지 검증 중 오류: {error_msg}")
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise
            
            text_details['image_verification'] = f'error: {str(e)[:50]}'
            return text_product, text_details
    
    def _extract_product_code_from_url(self, product_url):
        """아이허브 URL에서 상품 코드 추출 - 숫자 ID도 허용"""
        try:
            # 기존 패턴: /pr/ABC-12345 형태
            match = re.search(r'/pr/([A-Z0-9-]+)', product_url)
            if match:
                return match.group(1)
            
            # 새 패턴: /pr/.../12678 형태의 숫자 ID
            match = re.search(r'/pr/[^/]+/(\d+)', product_url)
            if match:
                return match.group(1)
                
            # 마지막 숫자만 추출
            match = re.search(r'/(\d+)/?$', product_url)
            if match:
                return match.group(1)
                
            return None
        except:
            return None
    
    def search_product_enhanced(self, search_name, coupang_product_id=None):
        """영어명으로 검색 + Gemini AI 매칭 + 스마트 이미지 검증"""
        try:
            print(f"  검색어: {search_name}")
            if coupang_product_id and Config.IMAGE_COMPARISON_ENABLED:
                print(f"  쿠팡 제품 ID: {coupang_product_id} (이미지 비교 대상)")
            
            search_url = f"{Config.BASE_URL}/search?kw={urllib.parse.quote(search_name)}"
            products = self.iherb_client.get_multiple_products(search_url)
            
            if not products:
                return None, 0, {'no_results': True}
            
            print(f"  검색 결과: {len(products)}개 제품 발견")
            
            print("  용량/개수 필터링 중...")
            filtered_products = self._check_strict_dosage_count_filter(search_name, products)
            
            if not filtered_products:
                print("  필터링 결과: 용량/개수 일치하는 제품 없음")
                return None, 0, {
                    'reason': 'no_products_after_filtering',
                    'original_count': len(products),
                    'filtered_count': 0
                }
            
            print(f"  필터링 결과: {len(filtered_products)}개 제품 남음")
            
            best_product, match_details = self._final_match_with_smart_image_verification(
                search_name, filtered_products, coupang_product_id
            )
            
            if best_product:
                verification = match_details.get('image_verification', 'not_attempted')
                if verification == 'match':
                    match_type = "텍스트+이미지"
                elif verification == 'skipped_confident_match':
                    match_type = "텍스트만"
                else:
                    match_type = "텍스트만"
                print(f"  매칭 성공 ({match_type}): {best_product['title'][:50]}...")
                return best_product['url'], 0.9, match_details
            else:
                reason = match_details.get('reason', 'unknown')
                print(f"  매칭 실패: {reason}")
                return None, 0, match_details
            
        except Exception as e:
            error_msg = str(e)
            
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise
            else:
                return None, 0, {'search_error': error_msg}
    
    def get_api_usage_stats(self):
        """API 사용량 통계 반환"""
        return {
            'total_calls': self.api_call_count,
            'vision_calls': self.vision_api_call_count,
            'text_calls': self.api_call_count - self.vision_api_call_count,
            'last_call_time': self.last_api_call_time
        }