from pydantic import BaseModel
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"

class OrderStatus(str, Enum):
    PENDING = "pending"
    EXECUTED = "executed"
    CANCELED = "canceled"
    REJECTED = "rejected"

class PlaceOrderPayload(BaseModel):
    """Схема для сообщения о размещении ордера"""
    order_id: UUID
    user_id: UUID
    instrument: str
    type: OrderType
    price: Optional[float] = None  # Для рыночных ордеров может быть None
    quantity: int
    timestamp: datetime

class CancelOrderPayload(BaseModel):
    """Схема для сообщения об отмене ордера"""
    order_id: UUID
    user_id: UUID
    timestamp: datetime

class OrderStatusPayload(BaseModel):
    """Схема для сообщения о статусе ордера"""
    order_id: UUID
    status: OrderStatus
    timestamp: datetime

class TradeUpdatePayload(BaseModel):
    """Схема для сообщения о совершенной сделке"""
    trade_id: UUID
    buyer_id: Optional[UUID] = None
    seller_id: Optional[UUID] = None
    instrument: str
    price: float
    quantity: int
    timestamp: datetime