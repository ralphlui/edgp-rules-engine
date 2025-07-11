from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    port: int = 8000

    class Config:
        env_file = ".env"

settings = Settings()