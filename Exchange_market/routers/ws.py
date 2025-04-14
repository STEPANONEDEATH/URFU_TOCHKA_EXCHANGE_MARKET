from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer  # Используем AIOKafkaConsumer
from typing import Dict
import json
import asyncio
from config import settings  # Убедитесь, что вы импортируете settings

router = APIRouter()


class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        self.active_connections[user_id] = websocket

    def disconnect(self, user_id: str):
        if user_id in self.active_connections:
            del self.active_connections[user_id]

    async def send_personal_message(self, message: str, user_id: str):
        if user_id in self.active_connections:
            await self.active_connections[user_id].send_text(message)


manager = ConnectionManager()


@router.websocket("/orders/{user_id}")
async def websocket_order_updates(websocket: WebSocket, user_id: str):
    await manager.connect(websocket, user_id)

    # Subscribe to Kafka topics for this user using AIOKafkaConsumer
    consumer = AIOKafkaConsumer(
        f"stockmarket.orders.{user_id}.status",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    # Start the consumer
    await consumer.start()

    try:
        while True:
            # Check for messages from Kafka
            async for message in consumer:
                await manager.send_personal_message(
                    json.dumps(message.value),
                    user_id
                )
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        manager.disconnect(user_id)
        await consumer.stop()  # Ensure to stop the consumer on disconnect


@router.websocket("/trades")
async def websocket_trade_updates(websocket: WebSocket):
    await websocket.accept()

    # Subscribe to Kafka trades topic using AIOKafkaConsumer
    consumer = AIOKafkaConsumer(
        "stockmarket.trades",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )  # Закрыта скобка здесь

    # Start the consumer
    await consumer.start()

    try:
        while True:
            # Check for messages from Kafka
            async for message in consumer:
                await websocket.send_text(json.dumps(message.value))
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        await consumer.stop()  # Ensure to stop the consumer on disconnect
