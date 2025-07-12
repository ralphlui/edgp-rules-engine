from pydantic_settings import BaseSettings
from pydantic import ConfigDict

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

    model_config = ConfigDict(
        env_file=".env",
        case_sensitive=False
    )

settings = Settings()