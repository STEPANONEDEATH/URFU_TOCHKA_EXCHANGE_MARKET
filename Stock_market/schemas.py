import uuid

from database import Base
from datetime import datetime
from zoneinfo import ZoneInfo
from pydantic import BaseModel
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy import Column, Integer, String, Enum, UUID as SQLUUID, Boolean, ForeignKey


def utcnow():
    return datetime.now(ZoneInfo("UTC"))


class User(Base):
    __tablename__ = "users"

    id = Column(SQLUUID, primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    role = Column(Enum("USER", "ADMIN", name="user_role"), default="USER")
    api_key = Column(String, unique=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP(timezone=True), default=utcnow)

    orders = relationship("Order", back_populates="user", passive_deletes=True)
    balances = relationship("Balance", back_populates="user", passive_deletes=True)


class Instrument(Base):
    __tablename__ = "instruments"

    ticker = Column(String(10), primary_key=True)
    name = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP(timezone=True), default=utcnow)

    orders = relationship("Order", back_populates="instrument", passive_deletes=True)


class Order(Base):
    __tablename__ = "orders"

    id = Column(SQLUUID, primary_key=True, default=uuid.uuid4)
    user_id = Column(SQLUUID, ForeignKey("users.id", ondelete="CASCADE"))
    instrument_ticker = Column(String, ForeignKey("instruments.ticker", ondelete="CASCADE"))
    direction = Column(Enum("BUY", "SELL", name="order_direction"))
    type = Column(Enum("MARKET", "LIMIT", name="order_type"))
    price = Column(Integer, nullable=True)
    quantity = Column(Integer, nullable=False)
    filled = Column(Integer, default=0)
    status = Column(Enum("NEW", "EXECUTED", "PARTIALLY_EXECUTED", "CANCELLED", name="order_status"), default="NEW")
    created_at = Column(TIMESTAMP(timezone=True), default=utcnow)
    updated_at = Column(TIMESTAMP(timezone=True), default=utcnow, onupdate=utcnow)

    user = relationship("User", back_populates="orders", passive_deletes=True)
    instrument = relationship("Instrument", back_populates="orders", passive_deletes=True)


class Balance(Base):
    __tablename__ = "balances"

    user_id = Column(SQLUUID, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    ticker = Column(String, primary_key=True)
    amount = Column(Integer, default=0)

    user = relationship("User", back_populates="balances", passive_deletes=True)


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(SQLUUID, primary_key=True, default=uuid.uuid4)
    buyer_id = Column(SQLUUID)
    seller_id = Column(SQLUUID)
    instrument_ticker = Column(String)
    price = Column(Integer)
    quantity = Column(Integer)
    created_at = Column(TIMESTAMP(timezone=True), default=utcnow)