FROM python:3.9-slim as builder

# Рабочая директория
WORKDIR /app

# Копируем requirements.txt для кэширования слоя
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Финальный образ
FROM python:3.9-slim
WORKDIR /app

COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY . .

# Копируем переменные окружения
COPY .env .env.local

# Устанавливаем переменные окружения
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN which uvicorn && uvicorn --version

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port 8000"]