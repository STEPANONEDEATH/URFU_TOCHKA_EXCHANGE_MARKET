import json
import uuid
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaProducer

from config import settings

producer: Optional[AIOKafkaProducer] = None


async def init_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()


async def close_producer():
    if producer:
        await producer.stop()


async def produce_order_event(order, action: str):
    if not producer:
        raise RuntimeError("Kafka producer not initialized")

    message = {
        "orderId": str(order.id),
        "userId": str(order.user_id),
        "instrument": order.instrument_ticker,
        "type": "limit" if order.type == "LIMIT" else "market",
        "price": order.price,
        "quantity": order.quantity,
        "status": action.lower(),
        "timestamp": order.created_at.isoformat(),
    }

    await producer.send(f"stockmarket.orders.{order.user_id}.status", value=message)

    if action == "EXECUTED":
        await producer.send(
            "stockmarket.trades",
            value={
                "tradeId": str(uuid.uuid4()),
                "buyerId": str(order.user_id) if order.direction == "BUY" else None,
                "sellerId": str(order.user_id) if order.direction == "SELL" else None,
                "instrument": order.instrument_ticker,
                "price": order.price,
                "quantity": order.quantity,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )


async def produce_trade_event(trade_data):
    if not producer:
        raise RuntimeError("Kafka producer not initialized")

    await producer.send("stockmarket.trades", value=trade_data)
