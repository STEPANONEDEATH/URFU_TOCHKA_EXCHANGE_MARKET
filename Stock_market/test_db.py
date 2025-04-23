import os
import psycopg2
from dotenv import load_dotenv

# Выбираем нужный файл окружения
load_dotenv(".env")


DATABASE_URL = os.getenv("DATABASE_URL")

# Подключение к базе данных
try:
    conn = psycopg2.connect(DATABASE_URL)
    print("База данных работает.")
except Exception as e:
    print("Ошибка подключения к базе данных:", e)

