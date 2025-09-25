import re
from datetime import datetime
from coupang_config import CoupangConfig

class ProductScraper:
    def clean_text(self, text):
        """텍스트 정리"""
        if not text:
            return ""
        
        # 연속된 공백을 하나로 변경
        text = re.sub(r'\s+', ' ', text)
        
        # 앞뒤 공백 제거
        return text.strip()
    
    def extract_number_from_text(self, text):
        """텍스트에서 숫자만 추출"""
        if not text:
            return ""
        
        cleaned_text = self.clean_text(text)
        
        # 숫자와 쉼표, 소수점만 추출
        numbers = re.findall(r'[\d,\.]+', cleaned_text)
        
        if numbers:
            # 첫 번째 숫자 반환 (쉼표 제거)
            return numbers[0].replace(',', '')
        
        return ""
    
    def extract_product_info(self, product_item):
        """개별 상품 정보 추출 - 핵심 개선만"""
        try:
            product = {}
            
            # 상품 ID (이미지 파일명에 사용)
            product_id = product_item.get('data-id', '')
            product['product_id'] = product_id
            
            # 상품 링크
            link_element = product_item.find('a')
            if link_element and link_element.get('href'):
                product['product_url'] = 'https://www.coupang.com' + link_element.get('href')
            else:
                product['product_url'] = ''
            
            # 상품명 - 새로운 구조 대응
            name_selectors = [
                CoupangConfig.SELECTORS['product_name_v2'],
                CoupangConfig.SELECTORS['product_name_legacy'],
                CoupangConfig.SELECTORS['product_name_general'],
            ]
            
            product_name = ''
            for selector in name_selectors:
                name_element = product_item.select_one(selector)
                if name_element:
                    product_name = self.clean_text(name_element.get_text())
                    break
            
            # 상품명이 비어있으면 대체 방법 시도
            if not product_name:
                all_text_divs = product_item.find_all('div')
                for div in all_text_divs:
                    text = self.clean_text(div.get_text())
                    if len(text) > 15 and ('나우푸드' in text or 'NOW' in text.upper()):
                        product_name = text[:100]  # 너무 긴 텍스트 방지
                        break
            
            product['product_name'] = product_name
            
            # 가격 정보 - 새로운 Tailwind CSS 구조 대응
            price_area = product_item.select_one('div.PriceArea_priceArea__NntJz')
            
            if price_area:
                # 현재 가격 - 새로운 구조
                current_price_selectors = [
                    'div.custom-oos.fw-text-\\[20px\\]\\/\\[24px\\].fw-font-bold.fw-mr-\\[4px\\].fw-text-red-700',       # 할인 가격
                    'div.custom-oos.fw-text-\\[20px\\]\\/\\[24px\\].fw-font-bold.fw-mr-\\[4px\\].fw-text-bluegray-900',  # 일반 가격
                    'div.custom-oos.fw-text-\\[20px\\]\\/\\[24px\\].fw-font-bold.fw-mr-\\[4px\\].fw-text-bluegray-400',  # 품절 가격
                    'div[class*="fw-text-[20px]"][class*="fw-font-bold"]',                                                # 포괄적 선택자
                    'strong.Price_priceValue__A4KOr',                                                                      # 기존 구조 (백업)
                ]
                
                current_price = ''
                for selector in current_price_selectors:
                    try:
                        price_elem = price_area.select_one(selector)
                        if price_elem:
                            current_price = self.clean_text(price_elem.get_text())
                            if current_price and '원' in current_price:
                                break
                    except:
                        continue
                
                product['current_price'] = current_price
                
                # 원래 가격 - 새로운 구조
                original_price_selectors = [
                    'del.custom-oos.fw-text-\\[12px\\]\\/\\[14px\\].fw-line-through.fw-text-bluegray-400',
                    'del[class*="custom-oos"]',
                    'del.PriceInfo_basePrice__8BQ32',  # 기존 구조 (백업)
                ]
                
                original_price = ''
                for selector in original_price_selectors:
                    try:
                        price_elem = price_area.select_one(selector)
                        if price_elem:
                            original_price = self.clean_text(price_elem.get_text())
                            if original_price and '원' in original_price:
                                break
                    except:
                        continue
                
                product['original_price'] = original_price
                
                # 할인율 - 새로운 구조
                discount_selectors = [
                    'span.custom-oos.fw-translate-y-\\[1px\\]',
                    'span[class*="custom-oos"][class*="fw-translate-y"]',
                    'span.PriceInfo_discountRate__EsQ8I',  # 기존 구조 (백업)
                ]
                
                discount_rate = ''
                for selector in discount_selectors:
                    try:
                        discount_elem = price_area.select_one(selector)
                        if discount_elem:
                            discount_text = self.clean_text(discount_elem.get_text())
                            if discount_text and '%' in discount_text:
                                discount_rate = discount_text
                                break
                    except:
                        continue
                
                product['discount_rate'] = discount_rate
                
                # 🆕 단위당 가격 (간단 추출)
                unit_price_elem = price_area.select_one('span[class*="fw-text-bluegray-400"]')
                unit_price = ''
                if unit_price_elem:
                    unit_text = self.clean_text(unit_price_elem.get_text())
                    if '당' in unit_text and '원' in unit_text:
                        unit_price = unit_text
                product['unit_price'] = unit_price
                
            else:
                product['current_price'] = ''
                product['original_price'] = ''
                product['discount_rate'] = ''
                product['unit_price'] = ''
            
            # 평점 및 리뷰 - 기존 구조 유지
            rating_area = product_item.select_one('div.ProductRating_productRating__jjf7W')
            
            if rating_area:
                # 평점 - width 스타일에서 추출
                rating_elem = rating_area.select_one('div.ProductRating_star__RGSlV')
                if rating_elem:
                    width_style = rating_elem.get('style', '')
                    width_match = re.search(r'width:\s*(\d+)%', width_style)
                    if width_match:
                        rating_percent = int(width_match.group(1))
                        product['rating'] = round(rating_percent / 20, 1)
                    else:
                        product['rating'] = ''
                else:
                    product['rating'] = ''
                
                # 리뷰 수
                review_count_elem = rating_area.select_one('span.ProductRating_ratingCount__R0Vhz')
                if review_count_elem:
                    review_text = self.clean_text(review_count_elem.get_text())
                    # 괄호 제거 후 숫자만 추출
                    review_number = self.extract_number_from_text(review_text.replace('(', '').replace(')', ''))
                    product['review_count'] = review_number
                else:
                    product['review_count'] = ''
            else:
                product['rating'] = ''
                product['review_count'] = ''
            
            # 배송 정보
            delivery_selectors = [
                'div.TextBadge_delivery__STgTC',
                'div.TextBadge_feePrice__n_gta',
                '[data-badge-type="delivery"]',
                '[data-badge-type="feePrice"]',
            ]
            
            delivery_badge = ''
            for selector in delivery_selectors:
                try:
                    badge_elem = product_item.select_one(selector)
                    if badge_elem:
                        badge_text = self.clean_text(badge_elem.get_text())
                        if badge_text:
                            delivery_badge = badge_text
                            break
                except:
                    continue
            
            product['delivery_badge'] = delivery_badge
            
            # 로켓직구 여부
            rocket_imgs = product_item.select('img')
            is_rocket = False
            for img in rocket_imgs:
                alt_text = img.get('alt', '')
                src_text = img.get('src', '')
                if ('로켓직구' in alt_text or 'rocket' in alt_text.lower() or 
                    'logo_jikgu' in src_text):
                    is_rocket = True
                    break
            
            product['is_rocket'] = is_rocket
            
            # 🆕 품절 상태 (간단 체크)
            stock_elem = product_item.select_one('div[class*="fw-font-bold"][class*="fw-text-bluegray-800"]')
            if stock_elem and '품절' in stock_elem.get_text():
                product['stock_status'] = 'out_of_stock'
            else:
                product['stock_status'] = 'in_stock'
            
            # 🆕 원산지 (간단 추출)
            origin_elem = product_item.select_one('span.fw-text-\\[14px\\].fw-text-bluegray-900')
            origin_country = ''
            if origin_elem:
                origin_text = self.clean_text(origin_elem.get_text())
                countries = ['한국', '미국', '독일', '일본', '중국', '프랑스']
                for country in countries:
                    if country in origin_text:
                        origin_country = origin_text
                        break
            product['origin_country'] = origin_country
            
            # 크롤링 시간
            product['crawled_at'] = datetime.now().isoformat()
            
            return product
            
        except Exception as e:
            print(f"상품 정보 추출 오류: {e}")
            return {}