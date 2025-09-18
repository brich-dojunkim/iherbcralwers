import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PATHS = {
    'images': os.path.join(BASE_DIR, 'coupang_images'),
}

def ensure_directories():
    for path in PATHS.values():
        os.makedirs(path, exist_ok=True)