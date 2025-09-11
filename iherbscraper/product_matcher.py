"""
상품 매칭 로직 모듈 - Gemini AI + Vision (단순 맞음/틀림 판단)
"""

import re
import time
import urllib.parse
import os
import base64
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
    """Gemini AI + Vision 기반 상품 매칭 시스템 (단순 맞음/틀림)"""
    
    def __init__(self, iherb_client):
        self.iherb_client = iherb_client
        self.api_call_count = 0
        self.vision_api_call_count = 0
        self.last_api_call_time = 0
        self._setup_gemini()
    
    def _setup_gemini(self):
        """Gemini AI 초기화 - Text + Vision 모델"""
        if not GEMINI_AVAILABLE:
            raise ImportError("google-generativeai 패키지가 설치되지 않았습니다.")
        
        try:
            genai.configure(api_key=Config.GEMINI_API_KEY)
            
            # 텍스트 모델
            self.text_model = genai.GenerativeModel(Config.GEMINI_TEXT_MODEL)
            
            # Vision 모델 (이미지 처리 가능)
            self.vision_model = genai.GenerativeModel(Config.GEMINI_VISION_MODEL)
            
            print("  Gemini AI 초기화 완료")
            print(f"    텍스트 모델: {Config.GEMINI_TEXT_MODEL}")
            print(f"    Vision 모델: {Config.GEMINI_VISION_MODEL}")
            print(f"    이미지 비교: {'활성화' if Config.IMAGE_COMPARISON_ENABLED else '비활성화'}")
            
        except Exception as e:
            print(f"  Gemini AI 초기화 실패: {e}")
            print("  API 키를 확인해주세요.")
            raise
    
    def _safe_gemini_call(self, prompt, max_retries=None, use_vision=False, image_data=None):
        """안전한 Gemini API 호출 (텍스트 + 이미지 지원)"""
        max_retries = max_retries or Config.GEMINI_MAX_RETRIES
        
        for attempt in range(max_retries):
            try:
                # API 호출 간격 제어
                current_time = time.time()
                if current_time - self.last_api_call_time < Config.GEMINI_RATE_LIMIT_DELAY:
                    time.sleep(Config.GEMINI_RATE_LIMIT_DELAY)
                
                # 모델 선택 및 API 호출
                if use_vision and image_data:
                    # Vision API 호출
                    response = self.vision_model.generate_content(
                        [prompt] + image_data,
                        generation_config={
                            'temperature': 0.1,
                            'max_output_tokens': 500,
                        }
                    )
                    self.vision_api_call_count += 1
                else:
                    # 텍스트 API 호출
                    response = self.text_model.generate_content(
                        prompt,
                        generation_config={
                            'temperature': 0.1,
                            'max_output_tokens': 500,
                        }
                    )
                
                self.api_call_count += 1
                self.last_api_call_time = time.time()
                
                return response.text.strip()
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # API 할당량 초과 - 즉시 실패
                if any(keyword in error_msg for keyword in ['quota', 'limit', 'exceeded', 'resource_exhausted']):
                    print(f"    Gemini API 할당량 초과: {e}")
                    print(f"    현재까지 API 호출: {self.api_call_count}회 (Vision: {self.vision_api_call_count}회)")
                    raise Exception(f"GEMINI_QUOTA_EXCEEDED: {e}")
                
                # 타임아웃 오류
                elif 'timeout' in error_msg:
                    print(f"    Gemini API 타임아웃 (시도 {attempt + 1}): {e}")
                    if attempt == max_retries - 1:
                        raise Exception(f"GEMINI_TIMEOUT: {e}")
                    time.sleep(2 ** attempt)
                
                # 일반 API 오류
                else:
                    print(f"    Gemini API 오류 (시도 {attempt + 1}): {e}")
                    if attempt == max_retries - 1:
                        raise Exception(f"GEMINI_API_ERROR: {e}")
                    time.sleep(2 ** attempt)
        
        return None
    
    def _load_image_for_gemini(self, image_path):
        """Gemini Vision용 이미지 로드"""
        try:
            if not os.path.exists(image_path):
                return None
            
            # 파일 크기 확인
            file_size_mb = os.path.getsize(image_path) / (1024 * 1024)
            if file_size_mb > Config.MAX_IMAGE_SIZE_MB:
                print(f"    이미지 크기 초과: {file_size_mb:.1f}MB > {Config.MAX_IMAGE_SIZE_MB}MB")
                return None
            
            # PIL로 이미지 열기 및 검증
            with Image.open(image_path) as img:
                # RGB로 변환 (RGBA, P 모드 등 처리)
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # 이미지 크기가 너무 크면 리사이즈
                max_dimension = 1024
                if max(img.size) > max_dimension:
                    img.thumbnail((max_dimension, max_dimension), Image.Resampling.LANCZOS)
                
                # BytesIO로 변환
                buffer = BytesIO()
                img.save(buffer, format='JPEG', quality=85)
                buffer.seek(0)
                
                return {
                    'mime_type': 'image/jpeg',
                    'data': buffer.getvalue()
                }
                
        except Exception as e:
            print(f"    이미지 로드 실패 ({os.path.basename(image_path)}): {e}")
            return None
    
    def _download_iherb_image(self, product_url, product_code):
        """아이허브 이미지 다운로드"""
        try:
            if not Config.IMAGE_COMPARISON_ENABLED:
                return None
            
            # 이미지 저장 경로
            os.makedirs(Config.IHERB_IMAGES_DIR, exist_ok=True)
            image_filename = f"iherb_{product_code}.jpg"
            image_path = os.path.join(Config.IHERB_IMAGES_DIR, image_filename)
            
            # 기존 파일 존재 확인
            if os.path.exists(image_path):
                if os.path.getsize(image_path) > 1024:  # 1KB 이상
                    return image_path
            
            # 아이허브 이미지 URL 추출
            image_url = self.iherb_client.extract_product_image_url(product_url)
            if not image_url:
                return None
            
            # 이미지 다운로드
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Referer': 'https://www.iherb.com/'
            }
            
            response = requests.get(image_url, headers=headers, timeout=Config.IMAGE_DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            
            # 이미지 저장
            with open(image_path, 'wb') as f:
                f.write(response.content)
            
            # 이미지 유효성 검증
            try:
                with Image.open(image_path) as img:
                    img.verify()
                return image_path
            except:
                if os.path.exists(image_path):
                    os.remove(image_path)
                return None
                
        except Exception as e:
            print(f"    아이허브 이미지 다운로드 실패: {e}")
            return None
    
    def _compare_images_with_gemini(self, coupang_image_path, iherb_image_path, korean_product_name):
        """Gemini Vision으로 이미지 비교 (단순 맞음/틀림)"""
        try:
            if not Config.IMAGE_COMPARISON_ENABLED:
                return {'success': False, 'reason': 'image_comparison_disabled'}
            
            # 이미지 로드
            coupang_image = self._load_image_for_gemini(coupang_image_path)
            iherb_image = self._load_image_for_gemini(iherb_image_path)
            
            if not coupang_image or not iherb_image:
                return {'success': False, 'reason': 'image_load_failed'}
            
            # Gemini Vision 프롬프트 (단순화)
            prompt = f"""두 제품 이미지를 비교하여 동일한 제품인지 판단해주세요.

한국 제품명: {korean_product_name}

비교 기준:
1. 브랜드가 동일한가?
2. 제품 패키지가 동일한가?
3. 제형이 동일한가?

답변은 반드시 다음 중 하나만 입력하세요:
- MATCH (동일한 제품)
- NO_MATCH (다른 제품)

답변:"""

            print(f"    이미지 비교 중... (Vision API 호출 {self.vision_api_call_count + 1}회)")
            
            # Vision API 호출
            response = self._safe_gemini_call(
                prompt, 
                use_vision=True, 
                image_data=[coupang_image, iherb_image]
            )
            
            if not response:
                return {'success': False, 'reason': 'gemini_no_response'}
            
            # 응답 파싱 (단순화)
            response_clean = response.strip().upper()
            
            if "MATCH" in response_clean and "NO_MATCH" not in response_clean:
                return {'success': True, 'match': True, 'response': response}
            elif "NO_MATCH" in response_clean:
                return {'success': True, 'match': False, 'response': response}
            else:
                # 명확하지 않은 응답은 불일치로 처리
                return {'success': True, 'match': False, 'response': response}
            
        except Exception as e:
            error_msg = str(e)
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise
            elif "GEMINI_TIMEOUT" in error_msg:
                return {'success': False, 'reason': 'vision_timeout', 'error': error_msg}
            else:
                return {'success': False, 'reason': 'vision_api_error', 'error': error_msg}
    
    def _check_strict_dosage_count_filter(self, korean_name, iherb_products):
        """엄격한 용량/개수 필터링 (불일치 시 완전 탈락)"""
        # 한글 제품명에서 용량과 개수 추출
        korean_mg = re.search(r'(\d+(?:,\d+)*)\s*mg', korean_name.lower())
        korean_count = re.search(r'(\d+)\s*(?:정|캡슐|타블렛|tablet|capsule)', korean_name.lower())
        
        filtered_products = []
        
        for product in iherb_products:
            iherb_name = product['title']
            should_include = True
            rejection_reason = None
            
            # 용량 체크 (mg)
            if korean_mg:
                k_mg = int(korean_mg.group(1).replace(',', ''))
                iherb_mg = re.search(Config.PATTERNS['dosage_mg'], iherb_name.lower())
                
                if iherb_mg:
                    i_mg = int(iherb_mg.group(1).replace(',', ''))
                    if k_mg != i_mg:
                        should_include = False
                        rejection_reason = f"용량 불일치 (한글:{k_mg}mg vs 아이허브:{i_mg}mg)"
            
            # 개수 체크
            if korean_count and should_include:
                k_count = int(korean_count.group(1))
                iherb_count = re.search(Config.PATTERNS['english_count'], iherb_name.lower())
                
                if iherb_count:
                    i_count = int(iherb_count.group(1))
                    if k_count != i_count:
                        should_include = False
                        rejection_reason = f"개수 불일치 (한글:{k_count}개 vs 아이허브:{i_count}개)"
            
            if should_include:
                filtered_products.append(product)
            else:
                print(f"    필터링 제외: {iherb_name[:50]}... ({rejection_reason})")
        
        return filtered_products
    
    def _gemini_text_match(self, korean_name, filtered_products):
        """Gemini AI 텍스트 기반 매칭 (단순 맞음/틀림)"""
        candidates_text = "\n".join([
            f"{i+1}. {product['title']}" 
            for i, product in enumerate(filtered_products)
        ])
        
        prompt = f"""다음은 한국 쿠팡 제품과 아이허브 후보 제품들입니다.

한국 제품: {korean_name}

아이허브 후보 제품들:
{candidates_text}

위 아이허브 제품들 중에서 한국 제품과 완전히 동일한 제품이 있는지 판단해주세요.

판단 기준:
1. 브랜드가 같아야 합니다
2. 주성분/영양소가 같아야 합니다  
3. 제형(정제, 캡슐 등)이 같아야 합니다
4. 용량과 개수는 이미 필터링되어 동일합니다

동일한 제품이 있다면 해당 번호만 답변하세요 (예: "2")
동일한 제품이 없다면 "없음"이라고 답변하세요.

답변:"""

        try:
            print(f"    텍스트 매칭 중... (API 호출 {self.api_call_count + 1}회)")
            response = self._safe_gemini_call(prompt)
            
            # 응답 파싱
            if not response:
                return None, {'reason': 'gemini_no_response'}
            
            response = response.strip().lower()
            
            # "없음" 응답 처리
            if "없음" in response or "none" in response or "no" in response:
                return None, {
                    'reason': 'gemini_no_match',
                    'gemini_response': response,
                    'candidates_count': len(filtered_products)
                }
            
            # 숫자 응답 처리
            import re
            number_match = re.search(r'(\d+)', response)
            if number_match:
                selected_index = int(number_match.group(1)) - 1  # 1-based to 0-based
                
                if 0 <= selected_index < len(filtered_products):
                    selected_product = filtered_products[selected_index]
                    return selected_product, {
                        'reason': 'gemini_text_match',
                        'gemini_response': response,
                        'selected_index': selected_index,
                        'selected_product': selected_product['title']
                    }
            
            # 파싱 실패
            return None, {
                'reason': 'gemini_parse_error',
                'gemini_response': response,
                'candidates_count': len(filtered_products)
            }
            
        except Exception as e:
            error_msg = str(e)
            
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise  # 할당량 초과는 상위로 전파
            elif "GEMINI_TIMEOUT" in error_msg:
                return None, {'reason': 'gemini_timeout', 'error': error_msg}
            else:
                return None, {'reason': 'gemini_api_error', 'error': error_msg}
    
    def _final_match_with_images(self, korean_name, filtered_products, coupang_product_id=None):
        """최종 매칭: 텍스트 매칭 후 이미지 검증 (맞음/틀림만)"""
        if not filtered_products:
            return None, {'reason': 'no_products_after_filtering'}
        
        # 1단계: 텍스트 기반 매칭
        text_product, text_details = self._gemini_text_match(korean_name, filtered_products)
        
        if not text_product:  # 텍스트 매칭 실패
            return None, text_details
        
        # 2단계: 이미지 비교 (활성화된 경우에만)
        if Config.IMAGE_COMPARISON_ENABLED and coupang_product_id:
            image_result = self._verify_with_images(
                text_product, korean_name, coupang_product_id, text_details
            )
            return image_result
        
        # 이미지 비교 없이 텍스트 결과만 반환
        return text_product, text_details
    
    def _verify_with_images(self, text_product, korean_name, coupang_product_id, text_details):
        """이미지로 텍스트 매칭 결과 검증"""
        try:
            # 쿠팡 이미지 경로
            coupang_image_path = os.path.join(
                Config.COUPANG_IMAGES_DIR, 
                f"coupang_{coupang_product_id}.jpg"
            )
            
            if not os.path.exists(coupang_image_path):
                print(f"    쿠팡 이미지 없음: {os.path.basename(coupang_image_path)}")
                # 이미지 없어도 텍스트 결과는 유지
                text_details['image_verification'] = 'coupang_image_missing'
                return text_product, text_details
            
            # 아이허브 이미지 다운로드
            product_code = self._extract_product_code_from_url(text_product['url'])
            if not product_code:
                text_details['image_verification'] = 'iherb_code_missing'
                return text_product, text_details
            
            iherb_image_path = self._download_iherb_image(text_product['url'], product_code)
            if not iherb_image_path:
                print(f"    아이허브 이미지 다운로드 실패")
                text_details['image_verification'] = 'iherb_image_download_failed'
                return text_product, text_details
            
            # 이미지 비교
            vision_result = self._compare_images_with_gemini(
                coupang_image_path, iherb_image_path, korean_name
            )
            
            if not vision_result.get('success'):
                text_details['image_verification'] = vision_result.get('reason', 'vision_failed')
                return text_product, text_details
            
            # 이미지 매칭 결과 판단 (단순 맞음/틀림)
            image_match = vision_result.get('match', False)
            
            if image_match:
                # 이미지도 일치 - 최종 성공
                text_details.update({
                    'image_verification': 'match',
                    'final_result': 'text_and_image_match'
                })
                print(f"    이미지 매칭 성공!")
                return text_product, text_details
            else:
                # 이미지 불일치 - 최종 실패
                text_details.update({
                    'image_verification': 'no_match',
                    'final_result': 'image_mismatch'
                })
                print(f"    이미지 매칭 실패 - 다른 제품으로 판단")
                return None, text_details  # 텍스트 매칭 성공해도 이미지 불일치시 실패
                
        except Exception as e:
            error_msg = str(e)
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise
            
            print(f"    이미지 검증 오류: {e}")
            text_details['image_verification'] = f'error: {str(e)[:50]}'
            return text_product, text_details  # 이미지 오류시 텍스트 결과 유지
    
    def _extract_product_code_from_url(self, product_url):
        """아이허브 URL에서 상품 코드 추출"""
        try:
            match = re.search(r'/pr/([A-Z0-9-]+)', product_url)
            return match.group(1) if match else None
        except:
            return None
    
    def search_product_enhanced(self, korean_name, coupang_product_id=None):
        """한글 제품명으로 검색 + Gemini AI 매칭 + 이미지 검증 (맞음/틀림)"""
        try:
            print(f"  검색어 (한글): {korean_name}")
            if coupang_product_id and Config.IMAGE_COMPARISON_ENABLED:
                print(f"  쿠팡 제품 ID: {coupang_product_id} (이미지 비교 대상)")
            
            # 1. 한글 제품명으로 아이허브 검색
            search_url = f"{Config.BASE_URL}/search?kw={urllib.parse.quote(korean_name)}"
            products = self.iherb_client.get_multiple_products(search_url)
            
            if not products:
                return None, {'no_results': True}
            
            print(f"  검색 결과: {len(products)}개 제품 발견")
            
            # 2. 용량/개수 엄격 필터링
            print("  용량/개수 필터링 중...")
            filtered_products = self._check_strict_dosage_count_filter(korean_name, products)
            
            if not filtered_products:
                print("  필터링 결과: 용량/개수 일치하는 제품 없음")
                return None, {
                    'reason': 'no_products_after_filtering',
                    'original_count': len(products),
                    'filtered_count': 0
                }
            
            print(f"  필터링 결과: {len(filtered_products)}개 제품 남음")
            
            # 3. Gemini AI 최종 매칭 + 이미지 검증
            best_product, match_details = self._final_match_with_images(
                korean_name, filtered_products, coupang_product_id
            )
            
            if best_product:
                verification = match_details.get('image_verification', 'not_attempted')
                match_type = "텍스트+이미지" if verification == 'match' else "텍스트만"
                print(f"  매칭 성공 ({match_type}): {best_product['title'][:50]}...")
                return best_product['url'], match_details
            else:
                reason = match_details.get('reason', 'unknown')
                print(f"  매칭 실패: {reason}")
                return None, match_details
            
        except Exception as e:
            error_msg = str(e)
            print(f"    검색 중 오류: {error_msg}")
            
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise  # 할당량 초과는 상위로 전파
            else:
                return None, {'search_error': error_msg}
    
    def get_api_usage_stats(self):
        """API 사용량 통계 반환"""
        return {
            'total_calls': self.api_call_count,
            'vision_calls': self.vision_api_call_count,
            'text_calls': self.api_call_count - self.vision_api_call_count,
            'last_call_time': self.last_api_call_time
        }