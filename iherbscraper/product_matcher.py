"""
상품 매칭 로직 모듈 - Gemini AI 전담 검증 시스템
모든 필터링과 매칭을 Gemini가 처리
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
    """Gemini AI 전담 상품 매칭 시스템"""
    
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
    
    def _print_candidates(self, candidates):
        """후보군 시각적 출력"""
        print("    " + "="*80)
        print(f"    📋 검색 후보군 ({len(candidates)}개):")
        for i, product in enumerate(candidates):
            print(f"      {i+1:2d}. {product['title']}")
        print("    " + "-"*80)
    
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
    
    def _parse_number_flexibly(self, response):
        """유연한 숫자 파싱 - 다양한 형식 지원"""
        patterns = [
            r'[Nn]umber[:\s]*(\d+)',  # "Number: 1"
            r'^(\d+)[:\s]',           # "1: CONFIDENT"
            r'\b(\d+)\b',             # 어떤 숫자든
        ]
        
        for pattern in patterns:
            match = re.search(pattern, response)
            if match:
                return int(match.group(1))
        
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
            start_time = time.time()
            
            response = self._safe_gemini_call(
                prompt, 
                use_vision=True, 
                image_data=[coupang_image, iherb_image]
            )
            
            api_time = time.time() - start_time
            print(f"    ⏱️ Vision API 시간: {api_time:.2f}초")
            
            if not response:
                return {'success': False, 'reason': 'gemini_no_response'}
            
            print(f"    🤖 Vision 응답: '{response}'")
            response_clean = response.strip().upper()
            
            if "YES" in response_clean:
                print(f"    ✅ 이미지 매칭 성공")
                return {'success': True, 'match': True, 'response': response}
            else:
                print(f"    ❌ 이미지 매칭 실패")
                return {'success': True, 'match': False, 'response': response}
            
        except Exception as e:
            error_msg = str(e)
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise
            return {'success': False, 'reason': 'vision_api_error', 'error': error_msg}
    
    def _gemini_comprehensive_match(self, search_name, all_products):
        """Gemini AI 종합 매칭 - 필터링과 매칭 통합"""
        
        # 후보군 표시
        self._print_candidates(all_products)
        
        candidates_text = "\n".join([
            f"{i+1}. {product['title']}" 
            for i, product in enumerate(all_products)
        ])
        
        prompt = (
            "Select the exact same product from the list.\n\n"
            f"Target: {search_name}\n\n"
            f"{candidates_text}\n\n"
            f"Respond ONLY as: <number 1-{len(all_products)}> CONFIDENT, "
            f"<number 1-{len(all_products)}> UNCERTAIN, or 0 UNCERTAIN.\n"
            "\n"
            "Decision order:\n"
            "- Exclude only explicit conflicts:\n"
            "  • Brand mismatch.\n"
            "  • Form mismatch.\n"
            "  • Variant/line conflict ONLY when BOTH Target and candidate state a line/variant and they differ.\n"
            "  • Numeric conflict ONLY when BOTH state a value and they differ (per-unit strength, total amount, concentration, IU, size/volume/weight, quantity/count).\n"
            "- Missing or unstated information is NOT a conflict. Do not infer unstated values.\n"
            "- If all stated details match, output <number> CONFIDENT.\n"
            "- If no candidate is CONFIDENT and at least one candidate has no conflicts, you MUST output a non-zero <number> UNCERTAIN.\n"
            "- When choosing UNCERTAIN, prefer greater token overlap with the Target across brand, line, form, and stated numeric fields; if tied, choose the lowest index.\n"
            "- Only if every candidate has a conflict, output 0 UNCERTAIN.\n"
            "- One line only. No explanations."
        )
        
        try:
            print(f"    🔍 Gemini 종합 매칭 (API 호출 {self.api_call_count + 1}회)")
            start_time = time.time()
            
            response = self._safe_gemini_call(prompt)
            
            api_time = time.time() - start_time
            print(f"    ⏱️ API 시간: {api_time:.2f}초")
            
            if not response:
                print(f"    ❌ Gemini 응답 없음")
                return None, {'reason': 'gemini_no_response'}
            
            print(f"    🤖 Gemini 응답: '{response}'")
            
            # 유연한 숫자 추출
            selected_number = self._parse_number_flexibly(response)
            confidence_uncertain = "UNCERTAIN" in response.upper()
            confidence_status = "UNCERTAIN" if confidence_uncertain else "CONFIDENT"
            
            if selected_number is None:
                print(f"    ❌ 번호 추출 실패")
                return None, {
                    'reason': 'gemini_parse_error',
                    'gemini_response': response,
                    'candidates_count': len(all_products)
                }
            
            if selected_number == 0:
                print(f"    ❌ 매칭 실패 - Gemini 판단: 동일 제품 없음 ({confidence_status})")
                return None, {
                    'reason': 'gemini_no_match',
                    'gemini_response': response,
                    'candidates_count': len(all_products)
                }
            elif 1 <= selected_number <= len(all_products):
                selected_index = selected_number - 1
                selected_product = all_products[selected_index]
                
                print(f"    🎯 매칭 성공: #{selected_number}")
                print(f"       제품: {selected_product['title']}")
                print(f"    🎚️ 신뢰도: {confidence_status}")
                
                if confidence_uncertain:
                    print(f"    ⚠️ 신뢰도 낮음 - 이미지 검증 필요")
                else:
                    print(f"    ✅ 신뢰도 높음 - 이미지 검증 생략")
                
                return selected_product, {
                    'reason': 'gemini_comprehensive_match',
                    'gemini_response': response,
                    'selected_index': selected_index,
                    'selected_product': selected_product['title'],
                    'needs_image_verification': confidence_uncertain
                }
            else:
                print(f"    ❌ 잘못된 번호: {selected_number}")
                return None, {
                    'reason': 'gemini_parse_error',
                    'gemini_response': response,
                    'candidates_count': len(all_products)
                }
            
        except Exception as e:
            error_msg = str(e)
            print(f"    💥 API 오류: {error_msg[:30]}...")
            
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise
            elif "GEMINI_TIMEOUT" in error_msg:
                return None, {'reason': 'gemini_timeout', 'error': error_msg}
            else:
                return None, {'reason': 'gemini_api_error', 'error': error_msg}
    
    def _final_match_with_smart_verification(self, search_name, all_products, coupang_product_id=None):
        """최종 매칭: Gemini 종합 판단 + 조건부 이미지 검증"""
        if not all_products:
            return None, {'reason': 'no_search_results'}
        
        text_product, text_details = self._gemini_comprehensive_match(search_name, all_products)
        
        if not text_product:
            return None, text_details
        
        # UNCERTAIN인 경우에만 이미지 검증
        needs_image_verification = (
            Config.IMAGE_COMPARISON_ENABLED and 
            coupang_product_id and
            text_details.get('needs_image_verification', False)
        )
        
        if needs_image_verification:
            print(f"    📸 이미지 검증 시작 (UNCERTAIN)")
            return self._verify_with_images(text_product, search_name, coupang_product_id, text_details)
        else:
            print(f"    ⏭️ 이미지 검증 생략 (CONFIDENT)")
            text_details['image_verification'] = 'skipped_confident'
            return text_product, text_details
    
    def _verify_with_images(self, text_product, search_name, coupang_product_id, text_details):
        """이미지 검증"""
        try:
            print(f"    " + "="*40)
            print(f"    📸 이미지 검증")
            print(f"    " + "-"*40)
            
            coupang_image_path = os.path.join(
                Config.COUPANG_IMAGES_DIR, 
                f"coupang_{coupang_product_id}.jpg"
            )

            if not os.path.exists(coupang_image_path):
                print(f"    ❌ 쿠팡 이미지 없음")
                text_details['image_verification'] = 'coupang_image_missing'
                print(f"    " + "="*40)
                return text_product, text_details
            
            product_code = self._extract_product_code_from_url(text_product['url'])
            if not product_code:
                print(f"    ❌ 상품 코드 추출 실패")
                text_details['image_verification'] = 'iherb_code_missing'
                print(f"    " + "="*40)
                return text_product, text_details
            
            print(f"    🔄 아이허브 이미지 다운로드...")
            iherb_image_path = self._download_iherb_image(text_product['url'], product_code)
            
            if not iherb_image_path:
                print(f"    ❌ 다운로드 실패")
                text_details['image_verification'] = 'iherb_image_download_failed'
                print(f"    " + "="*40)
                return text_product, text_details
            
            vision_result = self._compare_images_with_gemini(
                coupang_image_path, iherb_image_path, search_name
            )
            
            if not vision_result.get('success'):
                print(f"    ❌ Vision API 실패")
                text_details['image_verification'] = vision_result.get('reason', 'vision_failed')
                print(f"    " + "="*40)
                return text_product, text_details
            
            image_match = vision_result.get('match', False)
            
            if image_match:
                text_details.update({
                    'image_verification': 'match',
                    'final_result': 'text_and_image_success'
                })
                print(f"    🎯 이미지 검증 통과")
                print(f"    " + "="*40)
                return text_product, text_details
            else:
                text_details.update({
                    'image_verification': 'no_match',
                    'final_result': 'image_verification_failed'
                })
                print(f"    ❌ 이미지 검증 실패 - 매칭 거부")
                print(f"    " + "="*40)
                return None, text_details
                
        except Exception as e:
            error_msg = str(e)
            print(f"    💥 검증 오류: {error_msg[:30]}...")
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise
            
            text_details['image_verification'] = f'error: {str(e)[:50]}'
            print(f"    " + "="*40)
            return text_product, text_details
    
    def _extract_product_code_from_url(self, product_url):
        """URL에서 상품 코드 추출"""
        try:
            patterns = [
                r'/pr/([A-Z0-9-]+)',      # /pr/ABC-12345
                r'/pr/[^/]+/(\d+)',       # /pr/.../12678
                r'/(\d+)/?$'              # 마지막 숫자
            ]
            
            for pattern in patterns:
                match = re.search(pattern, product_url)
                if match:
                    return match.group(1)
            return None
        except:
            return None
    
    def search_product_enhanced(self, search_name, coupang_product_id=None):
        """Gemini 전담 상품 매칭"""
        try:
            print("\n" + "="*80)
            print(f"🔍 Gemini 전담 매칭: {search_name}")
            if coupang_product_id and Config.IMAGE_COMPARISON_ENABLED:
                print(f"🏷️ 쿠팡 ID: {coupang_product_id}")
            print("="*80)
            
            search_url = f"{Config.BASE_URL}/search?kw={urllib.parse.quote(search_name)}"
            products = self.iherb_client.get_multiple_products(search_url)
            
            if not products:
                print("❌ 검색 결과 없음")
                print("="*80)
                return None, 0, {'no_results': True}
            
            print(f"📊 검색 결과: {len(products)}개")
            
            best_product, match_details = self._final_match_with_smart_verification(
                search_name, products, coupang_product_id
            )
            
            if best_product:
                verification = match_details.get('image_verification', 'not_attempted')
                final_result = match_details.get('final_result', 'text_only')
                
                if final_result == 'text_and_image_success':
                    match_type = "텍스트+이미지"
                elif verification == 'skipped_confident':
                    match_type = "텍스트만(확신)"
                else:
                    match_type = "텍스트만"
                
                print(f"    🎯 최종 성공: {match_type}")
                print(f"    📋 제품: {best_product['title'][:60]}...")
                print("="*80)
                return best_product['url'], 0.9, match_details
            else:
                reason = match_details.get('reason', 'unknown')
                final_result = match_details.get('final_result', '')
                
                if final_result == 'image_verification_failed':
                    print(f"    ❌ 최종 실패: 텍스트 성공했으나 이미지 검증 실패")
                else:
                    print(f"    ❌ 최종 실패: {reason}")
                print("="*80)
                return None, 0, match_details
            
        except Exception as e:
            error_msg = str(e)
            print(f"💥 오류: {error_msg[:50]}...")
            print("="*80)
            
            if "GEMINI_QUOTA_EXCEEDED" in error_msg:
                raise
            else:
                return None, 0, {'search_error': error_msg}
    
    def get_api_usage_stats(self):
        """API 사용량 통계"""
        return {
            'total_calls': self.api_call_count,
            'vision_calls': self.vision_api_call_count,
            'text_calls': self.api_call_count - self.vision_api_call_count,
            'last_call_time': self.last_api_call_time
        }