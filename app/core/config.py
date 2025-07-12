from pydantic_settings import BaseSettings
from pydantic import ConfigDict, field_validator
from typing import List, Union
import json

class Settings(BaseSettings):
    # Server Configuration
    host: str = "localhost"
    port: int = 8008
    
    # Environment
    environment: str = "development"
    
    # API Configuration
    api_title: str = "EDGP Rules Engine API"
    api_version: str = "1.0.0"
    api_description: str = "Data Quality Validation API using Great Expectations rules"
    
    # CORS Configuration
    allowed_origins: Union[List[str], str] = [
        "http://localhost:3000",  # React default
        "http://localhost:3001",  # React alternative
        "http://localhost:8080",  # Vue.js default
        "http://localhost:4200",  # Angular default
        "http://localhost:5173",  # Vite default
        "http://localhost:8000",  # Django/FastAPI
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8080",
        "http://127.0.0.1:4200",
        "http://127.0.0.1:5173",
        "*"  # Allow all origins (use with caution in production)
    ]
    
    @field_validator('allowed_origins')
    @classmethod
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return [origin.strip() for origin in v.split(',')]
        return v

    model_config = ConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore"  # Ignore extra fields from .env file
    )

settings = Settings()