import logging

from config import settings
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# Настройка логирования SQLAlchemy
logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

# Используем пул соединений для production
engine = create_engine(
    settings.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=50,
    max_overflow=10,
    pool_pre_ping=True,  # Проверка соединения
    connect_args={"connect_timeout": 5}  # Таймаут подключения
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    """
    Генератор сессий для Dependency Injection в FastAPI.
    Автоматически закрывает сессию после использования.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()