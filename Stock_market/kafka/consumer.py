import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict

from aiokafka import AIOKafkaConsumer

from config import settings

# Глобальный словарь для хранения активных consumers
consumers: Dict[str, AIOKafkaConsumer] = {}


async def start_consumers():
    """Запускает все необходимые consumers для работы приложения"""
    # Здесь может быть логика инициализации всех consumers
    pass


async def match_orders():
    """Consumer для обработки новых ордеров"""
    consumer = AIOKafkaConsumer(
        "stockmarket.orders.place",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    try:
        await consumer.start()
        async for msg in consumer:
            order = msg.value
            # Здесь должна быть логика ордеров
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] Processing order:", order
            )
    finally:
        await consumer.stop()


@asynccontextmanager
async def get_consumer(user_id: str):
    """Контекстный менеджер для безопасной работы с consumer"""
    consumer = AIOKafkaConsumer(
        f"stockmarket.orders.{user_id}.status",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
    )

    try:
        await consumer.start()
        consumers[user_id] = consumer
        yield consumer
    finally:
        await consumer.stop()
        consumers.pop(user_id, None)


async def consume_order_updates(user_id: str):
    """Consumer для получения обновлений по ордерам конкретного пользователя"""
    from routers.ws import manager

    async with get_consumer(user_id) as consumer:
        try:
            async for msg in consumer:
                await manager.send_personal_message(
                    json.dumps(
                        {
                            **msg.value,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    ),
                    user_id,
                )
        except asyncio.CancelledError:
            print(f"Consumer for user {user_id} was cancelled")
        except Exception as e:
            print(f"Error in consumer for user {user_id}: {str(e)}")
            raise
