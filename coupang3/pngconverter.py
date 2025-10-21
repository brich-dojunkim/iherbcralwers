import os
from PIL import Image

def convert_png_to_jpg(input_folder, output_folder=None, quality=85):
    """
    특정 폴더 내의 PNG 파일을 JPG로 변환하며 용량을 줄이는 함수

    Args:
        input_folder (str): PNG 파일이 있는 폴더 경로
        output_folder (str, optional): 결과 JPG를 저장할 폴더 (기본값: input_folder)
        quality (int, optional): JPG 품질 (0~100, 낮을수록 용량 작음 / 기본 85)
    """
    if output_folder is None:
        output_folder = "/Users/brich/Desktop/iherb_price/coupang3/screenshots_251021_jpg"

    os.makedirs(output_folder, exist_ok=True)

    for filename in os.listdir(input_folder):
        if filename.lower().endswith(".png"):
            input_path = os.path.join(input_folder, filename)
            output_name = os.path.splitext(filename)[0] + ".jpg"
            output_path = os.path.join(output_folder, output_name)

            try:
                with Image.open(input_path) as img:
                    # PNG에는 투명도가 있을 수 있으므로 흰색 배경으로 합성
                    if img.mode in ("RGBA", "LA"):
                        background = Image.new("RGB", img.size, (255, 255, 255))
                        background.paste(img, mask=img.split()[-1])
                        img = background
                    else:
                        img = img.convert("RGB")

                    img.save(output_path, "JPEG", quality=quality, optimize=True)
                    print(f"✅ 변환 완료: {output_name}")
            except Exception as e:
                print(f"❌ 변환 실패 ({filename}): {e}")

    print("\n🎉 모든 PNG → JPG 변환이 완료되었습니다.")

# 사용 예시
if __name__ == "__main__":
    folder = "/Users/brich/Desktop/iherb_price/coupang3/screenshots_251021"  # 변환할 폴더 경로
    convert_png_to_jpg(folder, quality=80)     # JPG 품질 80으로 변환
