"""
Config 패키지 초기화
전역 공통 설정을 쉽게 import할 수 있도록 제공
"""

from .global_config import (
    APIConfig,
    PathConfig, 
    DatabaseConfig,
    is_development,
    ensure_directories,
    validate_config
)

__version__ = "1.0.0"

__all__ = [
    'APIConfig',
    'PathConfig',
    'DatabaseConfig', 
    'is_development',
    'ensure_directories',
    'validate_config'
]

# 패키지 import 시 자동으로 디렉토리 생성
ensure_directories()