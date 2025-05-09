import uuid
import models
import schemas

from uuid import UUID
from typing import Union
from schemas import Instrument
from sqlalchemy.orm import Session


def get_user(db: Session, user_id: UUID):
    return db.query(schemas.User).filter(schemas.User.id == user_id).first()

def get_user_by_api_key(db: Session, api_key: str):
    return db.query(schemas.User).filter(schemas.User.api_key == api_key).first()

def create_user(db: Session, user: models.NewUser):
    db_user = schemas.User(
        name=user.name,
        api_key=str(uuid.uuid4())
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    print(f"[REGISTERED] New user: {db_user.name} with API key: {db_user.api_key}")
    return db_user

def get_instruments(db: Session, skip: int = 0, limit: int = 100):
    return db.query(schemas.Instrument).filter(schemas.Instrument.is_active == True).offset(skip).limit(limit).all()

def get_instrument(db: Session, ticker: str):
    return db.query(schemas.Instrument).filter(schemas.Instrument.ticker == ticker).first()

def create_instrument(db: Session, instrument: models.Instrument):
    db_instrument = schemas.Instrument(
        ticker=instrument.ticker,
        name=instrument.name
    )
    db.add(db_instrument)
    db.commit()
    db.refresh(db_instrument)
    return db_instrument


def delete_instrument(db: Session, ticker: str) -> bool:
    # Поиск инструмента по тикеру
    instrument = db.query(Instrument).filter(Instrument.ticker == ticker).first()
    if not instrument:
        return False  # Если инструмент не найден, возвращаем False

    db.delete(instrument)  # Удаляем инструмент
    db.commit()  # Применяем изменения в базе
    return True  # Возвращаем True, если удаление прошло успешно

def get_orderbook(db: Session, ticker: str, limit: int = 10):
    # Get active buy orders (bids)
    bids = db.query(schemas.Order).filter(
        schemas.Order.instrument_ticker == ticker,
        schemas.Order.direction == "BUY",
        schemas.Order.status.in_(["NEW", "PARTIALLY_EXECUTED"])
    ).order_by(schemas.Order.price.desc()).limit(limit).all()

    # Get active sell orders (asks)
    asks = db.query(schemas.Order).filter(
        schemas.Order.instrument_ticker == ticker,
        schemas.Order.direction == "SELL",
        schemas.Order.status.in_(["NEW", "PARTIALLY_EXECUTED"])
    ).order_by(schemas.Order.price.asc()).limit(limit).all()

    return bids, asks

def get_transactions(db: Session, ticker: str, limit: int = 10):
    return db.query(schemas.Transaction).filter(
        schemas.Transaction.instrument_ticker == ticker
    ).order_by(schemas.Transaction.created_at.desc()).limit(limit).all()

def get_orders(db: Session, user_id: UUID):
    return db.query(schemas.Order).filter(schemas.Order.user_id == user_id).all()

def get_order(db: Session, order_id: UUID):
    return db.query(schemas.Order).filter(schemas.Order.id == order_id).first()

def create_order(db: Session, order: Union[models.LimitOrderBody, models.MarketOrderBody], user_id: UUID):
    db_order = schemas.Order(
        user_id=user_id,
        instrument_ticker=order.ticker,
        direction=order.direction.value,
        type="LIMIT" if isinstance(order, models.LimitOrderBody) else "MARKET",
        price=order.price if isinstance(order, models.LimitOrderBody) else None,
        quantity=order.qty,
        status="NEW"
    )
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    return db_order

def cancel_order(db: Session, order_id: UUID):
    order = db.query(schemas.Order).filter(schemas.Order.id == order_id).first()
    if order and order.status in ["NEW", "PARTIALLY_EXECUTED"]:
        order.status = "CANCELLED"
        db.commit()
        return True
    return False

def get_balances(db: Session, user_id: UUID):
    return db.query(schemas.Balance).filter(schemas.Balance.user_id == user_id).all()

def update_balance(db: Session, user_id: UUID, ticker: str, amount: int):
    balance = db.query(schemas.Balance).filter(
        schemas.Balance.user_id == user_id,
        schemas.Balance.ticker == ticker
    ).first()

    if balance:
        balance.amount += amount
    else:
        balance = schemas.Balance(
            user_id=user_id,
            ticker=ticker,
            amount=amount
        )
        db.add(balance)

    db.commit()
    db.refresh(balance)
    return balance

def delete_user(db: Session, user_id: UUID):
    user = db.query(schemas.User).filter(schemas.User.id == user_id).first()
    if user:
        db.delete(user)
        db.commit()
        return True
    return False