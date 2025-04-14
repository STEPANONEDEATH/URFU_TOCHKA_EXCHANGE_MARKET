from sqlalchemy import Column, Integer, String, Enum, DateTime, UUID as SQLUUID, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from database import Base
import uuid
from datetime import datetime


class User(Base):
    __tablename__ = "users"

    id = Column(SQLUUID, primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    role = Column(Enum("USER", "ADMIN", name="user_role"), default="USER")
    api_key = Column(String, unique=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    orders = relationship("Order", back_populates="user")
    balances = relationship("Balance", back_populates="user")

class Instrument(Base):
    __tablename__ = "instruments"

    ticker = Column(String(10), primary_key=True)
    name = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    orders = relationship("Order", back_populates="instrument")

class Order(Base):
    __tablename__ = "orders"

    id = Column(SQLUUID, primary_key=True, default=uuid.uuid4)
    user_id = Column(SQLUUID, ForeignKey("users.id"))
    instrument_ticker = Column(String, ForeignKey("instruments.ticker"))
    direction = Column(Enum("BUY", "SELL", name="order_direction"))
    type = Column(Enum("MARKET", "LIMIT", name="order_type"))
    price = Column(Integer, nullable=True)  # Null for market orders
    quantity = Column(Integer, nullable=False)
    filled = Column(Integer, default=0)
    status = Column(Enum("NEW", "EXECUTED", "PARTIALLY_EXECUTED", "CANCELLED", name="order_status"), default="NEW")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="orders")
    instrument = relationship("Instrument", back_populates="orders")

class Balance(Base):
    __tablename__ = "balances"

    user_id = Column(SQLUUID, ForeignKey("users.id"), primary_key=True)
    ticker = Column(String, primary_key=True)
    amount = Column(Integer, default=0)

    user = relationship("User", back_populates="balances")

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(SQLUUID, primary_key=True, default=uuid.uuid4)
    buyer_id = Column(SQLUUID)
    seller_id = Column(SQLUUID)
    instrument_ticker = Column(String)
    price = Column(Integer)
    quantity = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)