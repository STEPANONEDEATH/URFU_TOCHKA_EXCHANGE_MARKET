from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/toyexchange"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    ALLOWED_ORIGINS: list = ["http://localhost:3000"]

    class Config:
        env_file = ".env"


settings = Settings()