from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    port: int = 8008
    host: str = "localhost"

    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()