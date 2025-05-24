from typing import List, Optional

from pydantic import computed_field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Основные переменные
    DATABASE_URL: Optional[str] = None
    KAFKA_BOOTSTRAP_SERVERS: str
    ALLOWED_ORIGINS: List[str] = ["http://localhost:3000"]

    # Опциональные
    POSTGRES_USER: Optional[str] = None
    POSTGRES_PASSWORD: Optional[str] = None
    POSTGRES_DB: Optional[str] = None

    @computed_field
    @property
    def db_url(self) -> str:
        """Конструирует DATABASE_URL, если он не задан явно"""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        if self.POSTGRES_USER and self.POSTGRES_PASSWORD and self.POSTGRES_DB:
            return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@db:5432/{self.POSTGRES_DB}"
        raise ValueError("DATABASE_URL is not set and can't be constructed")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()

# Debug
print("DATABASE_URL:", settings.db_url)
print("KAFKA_BOOTSTRAP_SERVERS:", settings.KAFKA_BOOTSTRAP_SERVERS)
