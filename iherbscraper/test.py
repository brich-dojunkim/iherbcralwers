import google.generativeai as genai

# Test your API key
genai.configure(api_key="AIzaSyBe9EQB8cvXko5fWoU309sYpkiUkQLe2ZM")

try:
    model = genai.GenerativeModel('models/gemini-2.5-flash')
    
    # Simple test
    response = model.generate_content(
        "What is 2+2?", 
        safety_settings=[
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"}, 
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
        ]
    )
    
    print("API Key Status: ✅ Working")
    print(f"Response: {response.text}")
    
    # Test product matching prompt
    test_prompt = """Task: Select item.
Target: Vitamin C 1000mg
Items:
1. Vitamin C 500mg
2. Vitamin C 1000mg  
3. Vitamin D 1000mg
Answer:"""
    
    response2 = model.generate_content(test_prompt, safety_settings=[
        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"}, 
        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
    ])
    
    print("Product Matching Test: ✅ Working")
    print(f"Response: {response2.text}")
    
except Exception as e:
    print(f"API Error: {e}")