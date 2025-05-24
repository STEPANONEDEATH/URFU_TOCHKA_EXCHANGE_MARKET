import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from routers import admin, balance, order, public, ws

# Создаем папку для логов, если её нет
Path("logs").mkdir(exist_ok=True)

# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Формат логов
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

# Консольный вывод
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Файловый вывод с ротацией (макс. 5 файлов по 10 МБ каждый)
file_handler = RotatingFileHandler(
    "logs/app.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
file_handler.setFormatter(formatter)

# Добавляем обработчики
logger.addHandler(console_handler)
logger.addHandler(file_handler)

tags_metadata = [
    {"name": "root", "description": "Root"},
    {"name": "public", "description": "Публичные маршруты"},
    {"name": "balance", "description": "Работа с балансами"},
    {"name": "order", "description": "Работа с ордерами"},
    {"name": "admin", "description": "Админ-панель"},
    {"name": "user", "description": "Управление пользователями"},
]

app = FastAPI(
    title="Toy Exchange API",
    description="API for toy exchange platform with real-time trading",
    version="0.1.0",
    contact={
        "name": "API Support",
        "email": "stepanonedeath@gmail.com / support@toyexchange.com",
    },
    license_info={
        "name": "MIT",
    },
    openapi_tags=tags_metadata,
)


# Middleware для логирования запросов
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()

    logger.info(f"Request: {request.method} {request.url.path}")
    logger.debug(f"Headers: {request.headers}")
    logger.debug(f"Query params: {request.query_params}")

    try:
        response = await call_next(request)
    except Exception as e:
        logger.error(f"Request failed: {str(e)}", exc_info=True)
        raise

    process_time = (time.time() - start_time) * 1000
    logger.info(f"Response: {response.status_code} ({process_time:.2f}ms)")

    return response


# Обработчик исключений
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(status_code=500, content={"message": "Internal server error"})


# Root endpoint
@app.get("/", tags=["root"])
async def root():
    logger.info("Root endpoint accessed")
    return {
        "message": "Welcome to Toy Exchange API",
        "docs": "/docs",
        "redoc": "/redoc",
    }


# Include routers
app.include_router(public.router, prefix="/api/v1/public", tags=["public"])
app.include_router(order.router, prefix="/api/v1", tags=["order"])
app.include_router(balance.router, prefix="/api/v1", tags=["balance"])
app.include_router(admin.router, prefix="/api/v1", tags=["admin"])
app.include_router(ws.router, prefix="/ws", tags=["websocket"])


@app.on_event("startup")
async def startup_event():
    """Запуск Kafka"""
    logger.info("Starting application...")

    try:
        from kafka.producer import init_producer

        await init_producer()
        logger.info("Kafka producer initialized")

        from kafka.consumer import start_consumers

        await start_consumers()
        logger.info("Kafka consumers started")
    except Exception as e:
        logger.critical(f"Failed to start Kafka: {str(e)}", exc_info=True)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Выключение Kafka"""
    logger.info("Shutting down application...")

    try:
        from kafka.producer import close_producer

        await close_producer()
        logger.info("Kafka producer closed")
    except Exception as e:
        logger.error(f"Error while shutting down Kafka: {str(e)}", exc_info=True)
