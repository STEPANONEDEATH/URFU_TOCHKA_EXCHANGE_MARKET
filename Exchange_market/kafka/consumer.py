from aiokafka import AIOKafkaConsumer
import asyncio
import json
from config import settings
from typing import Dict

# consumers: это словарь с текущими потребителями
consumers: Dict[str, AIOKafkaConsumer] = {}

# Удаляем прямой импорт manager из routers.ws
# from routers.ws import manager

async def start_consumers():
    # Будем управлять потребителями динамически в точках WebSocket
    pass

async def match_orders():
    consumer = AIOKafkaConsumer(
        "stockmarket.orders.place",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
    )
    await consumer.start()
    async for msg in consumer:
        order = json.loads(msg.value)
        # Логика матчинга с другими ордерами


async def consume_order_updates(user_id: str):
    # Импортируем manager только когда он понадобится
    from routers.ws import manager  # Переносим импорт сюда

    consumer = AIOKafkaConsumer(
        f"stockmarket.orders.{user_id}.status",  # Тема для заказов
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,  # Адреса брокеров Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Десериализация сообщений
    )

    consumers[user_id] = consumer  # Сохраняем потребителя по user_id
    await consumer.start()  # Запускаем потребителя

    try:
        async for msg in consumer:
            # Отправляем сообщения WebSocket пользователю
            await manager.send_personal_message(
                json.dumps(msg.value),  # Преобразуем сообщение в строку JSON
                user_id
            )
    finally:
        # Останавливаем потребителя и удаляем его из словаря
        await consumer.stop()
        del consumers[user_id]  # Удаляем потребителя из словаря после остановки