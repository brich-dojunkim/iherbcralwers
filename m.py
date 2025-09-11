import google.generativeai as genai
genai.configure(api_key="AIzaSyDNB7zwp36ICInpj3SRV9GiX7ovBxyFHHE")

for model in genai.list_models():
    print(model.name)