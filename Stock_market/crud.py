import uuid
import models
import schemas

from uuid import UUID
from typing import Union
from schemas import Instrument
from sqlalchemy.orm import Session
from schemas import Instrument as ORMInstrument


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
    try:
        db.add(db_instrument)
        db.commit()
        db.refresh(db_instrument)
        return db_instrument
    except IntegrityError:
        db.rollback()
        return db.query(schemas.Instrument).filter(schemas.Instrument.ticker == instrument.ticker).first()

def delete_instrument(db: Session, ticker: str) -> bool:
    # Поиск инструмента по тикеру
    instrument = db.query(schemas.Instrument).filter(schemas.Instrument.ticker == ticker).first()
    if not instrument:
        return False

    db.delete(instrument)
    db.commit()
    return True

def get_orderbook(db: Session, ticker: str, limit: int = 10):
    bids = db.query(schemas.Order).filter(
        schemas.Order.instrument_ticker == ticker,
        schemas.Order.direction == "BUY",
        schemas.Order.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
        schemas.Order.type == "LIMIT",
        schemas.Order.price != None
    ).order_by(schemas.Order.price.desc()).limit(limit).all()

    asks = db.query(schemas.Order).filter(
        schemas.Order.instrument_ticker == ticker,
        schemas.Order.direction == "SELL",
        schemas.Order.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
        schemas.Order.type == "LIMIT",
        schemas.Order.price != None
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
    instrument = get_instrument(db, order.ticker)
    if not instrument:
        raise ValueError("Invalid ticker")

    user_balance = get_balance(db, user_id, order.ticker if order.direction == "SELL" else "RUB")
    required_amount = order.qty if order.direction == "SELL" else (
        order.qty * order.price if hasattr(order, 'price') and order.price is not None 
        else 0
    )

    if not user_balance or user_balance.amount < required_amount:
        raise ValueError("Insufficient balance")

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
    if not order:
        raise ValueError("Order not found")
    if order.status not in ["NEW", "PARTIALLY_EXECUTED"]:
        raise ValueError("Cannot cancel order with current status")
    
    order.status = "CANCELLED"
    db.commit()
    return True

def get_balance(db: Session, user_id: UUID, ticker: str):
    return db.query(schemas.Balance).filter(
        schemas.Balance.user_id == user_id,
        schemas.Balance.ticker == ticker
    ).first()

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

def match_order(db: Session, new_order: schemas.Order):
    opposite_side = "SELL" if new_order.direction == "BUY" else "BUY"
    
    if new_order.direction == "BUY":
        matched_orders = db.query(schemas.Order).filter(
            schemas.Order.instrument_ticker == new_order.instrument_ticker,
            schemas.Order.direction == opposite_side,
            schemas.Order.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
            schemas.Order.price <= new_order.price  # покупатель хочет купить >= этой цены
        ).order_by(schemas.Order.price.asc(), schemas.Order.created_at.asc()).all()
    else:
        matched_orders = db.query(schemas.Order).filter(
            schemas.Order.instrument_ticker == new_order.instrument_ticker,
            schemas.Order.direction == opposite_side,
            schemas.Order.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
            schemas.Order.price >= new_order.price  # продавец хочет продать <= этой цены
        ).order_by(schemas.Order.price.desc(), schemas.Order.created_at.asc()).all()

    qty_to_fill = new_order.quantity
    for counter_order in matched_orders:
        trade_qty = min(qty_to_fill, counter_order.quantity - counter_order.filled)

        if trade_qty <= 0:
            continue

        trade_price = counter_order.price  # по цене встречного ордера

        # Обновление балансов
        if new_order.direction == "BUY":
            update_balance(db, new_order.user_id, new_order.instrument_ticker, trade_qty)
            update_balance(db, new_order.user_id, "RUB", -trade_qty * trade_price)
            update_balance(db, counter_order.user_id, "RUB", trade_qty * trade_price)
            update_balance(db, counter_order.user_id, new_order.instrument_ticker, -trade_qty)
        else:
            update_balance(db, new_order.user_id, "RUB", trade_qty * trade_price)
            update_balance(db, new_order.user_id, new_order.instrument_ticker, -trade_qty)
            update_balance(db, counter_order.user_id, new_order.instrument_ticker, trade_qty)
            update_balance(db, counter_order.user_id, "RUB", -trade_qty * trade_price)

        # Создание сделки
        db_transaction = schemas.Transaction(
            instrument_ticker=new_order.instrument_ticker,
            price=trade_price,
            quantity=trade_qty,
            buyer_id=new_order.user_id if new_order.direction == "BUY" else counter_order.user_id,
            seller_id=counter_order.user_id if new_order.direction == "BUY" else new_order.user_id
        )
        db.add(db_transaction)

        # Обновление ордеров
        new_order.filled += trade_qty
        counter_order.filled += trade_qty

        if counter_order.filled == counter_order.quantity:
            counter_order.status = "EXECUTED"
        else:
            counter_order.status = "PARTIALLY_EXECUTED"

        qty_to_fill -= trade_qty
        if qty_to_fill == 0:
            break

    new_order.status = (
        "EXECUTED" if new_order.filled == new_order.quantity else
        "PARTIALLY_EXECUTED" if new_order.filled > 0 else "NEW"
    )

    db.commit()