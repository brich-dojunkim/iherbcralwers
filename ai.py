import google.generativeai as genai

genai.configure(api_key='AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE')

for model in genai.list_models():
    print(f"모델: {model.name}")
    print(f"지원 기능: {model.supported_generation_methods}")
    print("---")