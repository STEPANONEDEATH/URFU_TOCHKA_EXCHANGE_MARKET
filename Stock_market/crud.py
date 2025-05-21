import uuid
import models
import schemas
import logging


from uuid import UUID
from typing import Union
from datetime import datetime
from schemas import User, Balance
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from schemas import Instrument as ORMInstrument

logger = logging.getLogger(__name__)

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
    logger.info(f"[REGISTERED] New user: {db_user.name} with API key: {db_user.api_key}")
    return db_user

def get_instruments(db: Session, skip: int = 0, limit: int = 100):
    return db.query(schemas.Instrument).filter(schemas.Instrument.is_active == True).offset(skip).limit(limit).all()

def get_instrument(db: Session, ticker: str):
    return db.query(schemas.Instrument).filter(schemas.Instrument.ticker == ticker).first()

from fastapi import HTTPException, status

def create_instrument(db: Session, instrument: models.Instrument):
    existing_instrument = db.query(schemas.Instrument).filter(
        schemas.Instrument.ticker == instrument.ticker
    ).first()
    
    if existing_instrument:
        logger.warning(f"Instrument {instrument.ticker} already exists")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Instrument with ticker {instrument.ticker} already exists"
        )
    
    logger.info(f"Creating instrument: ticker={instrument.ticker}, name={instrument.name}")
    db_instrument = schemas.Instrument(
        ticker=instrument.ticker,
        name=instrument.name
    )
    
    db.add(db_instrument)
    db.commit()
    db.refresh(db_instrument)
    return db_instrument

def delete_instrument(db: Session, ticker: str) -> bool:
    instrument = db.query(schemas.Instrument).filter(schemas.Instrument.ticker == ticker).first()
    if not instrument:
        return False
    db.delete(instrument)
    db.commit()
    logger.info(f"Deleted instrument: {ticker}")
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid ticker")
    if order.qty <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Order quantity must be greater than zero")
    if isinstance(order, models.LimitOrderBody) and order.price is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Limit order must include a price")

    if order.direction == "SELL":
        user_balance = get_balance(db, user_id, order.ticker)
        required_amount = order.qty
        if not user_balance or user_balance.amount < required_amount:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Insufficient balance")

    else:  # BUY
        if isinstance(order, models.LimitOrderBody):
            required_amount = order.qty * order.price
            user_balance = get_balance(db, user_id, "RUB")
            if not user_balance or user_balance.amount < required_amount:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Insufficient balance")

        else:  # MARKET ORDER
            bids, asks = get_orderbook(db, order.ticker, limit=1)
            if not asks:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No matching sell orders available")
            best_ask = asks[0]
            market_price = best_ask.price
            required_amount = order.qty * market_price
            user_balance = get_balance(db, user_id, "RUB")
            if not user_balance or user_balance.amount < required_amount:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Insufficient balance")

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
    match_order(db, db_order)
    return db_order

def cancel_order(db: Session, order_id: UUID):
    order = db.query(schemas.Order).filter(schemas.Order.id == order_id).first()

    # Проверки
    if not order:
        raise ValueError("Order not found")
    if order.type == "MARKET":
        raise ValueError("Cannot cancel market order")
    if order.status not in ["NEW", "PARTIALLY_EXECUTED"]:
        raise ValueError("Cannot cancel order with current status")

    order.status = "CANCELLED"
    db.commit()
    logger.info(f"Order {order_id} cancelled")
    return True

def get_balance(db: Session, user_id: UUID, ticker: str):
    return db.query(schemas.Balance).filter(
        schemas.Balance.user_id == user_id,
        schemas.Balance.ticker == ticker
    ).first()

def get_balances(db: Session, user_id: UUID):
    return db.query(schemas.Balance).filter(schemas.Balance.user_id == user_id).all()

def update_balance(db: Session, user_id: UUID, ticker: str, amount: int):
    logger.info(f"Updating balance for user {user_id}, ticker={ticker}, amount={amount}")

    user = db.query(schemas.User).filter(schemas.User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    # Поиск текущего баланса
    balance = db.query(schemas.Balance).filter(
        schemas.Balance.user_id == user_id,
        schemas.Balance.ticker == ticker
    ).first()

    # Проверка на отрицательный баланс
    if balance and (balance.amount + amount < 0):
        raise ValueError("Insufficient balance to deduct")

    # Обновление или создание баланса
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
        logger.info(f"Deleted user: {user_id}")
        return True
    return False

def match_order(db: Session, new_order: schemas.Order):
    logger.info(f"Matching new order: {new_order}")
    opposite_side = "SELL" if new_order.direction == "BUY" else "BUY"

    try:
        # Блокируем ордера для конкурентного доступа
        matched_orders = (
            db.query(schemas.Order)
            .filter(
                schemas.Order.instrument_ticker == new_order.instrument_ticker,
                schemas.Order.direction == opposite_side,
                schemas.Order.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
                schemas.Order.price != None
            )
            .filter(
                schemas.Order.price <= new_order.price
                if new_order.direction == "BUY"
                else schemas.Order.price >= new_order.price
            )
            .order_by(
                schemas.Order.price.asc() if new_order.direction == "BUY" 
                else schemas.Order.price.desc(),
                schemas.Order.created_at.asc()
            )
            .with_for_update()  # Блокировка строк
            .all()
        )

        qty_to_fill = new_order.quantity - new_order.filled
        executed_trades = []

        for counter_order in matched_orders:
            if qty_to_fill == 0:
                break

            available_qty = counter_order.quantity - counter_order.filled
            trade_qty = min(qty_to_fill, available_qty)

            if trade_qty <= 0:
                continue

            trade_price = counter_order.price or 0

            # Обновляем балансы
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

            # Создаем транзакцию
            transaction = schemas.Transaction(
                instrument_ticker=new_order.instrument_ticker,
                price=trade_price,
                quantity=trade_qty,
                buyer_id=new_order.user_id if new_order.direction == "BUY" else counter_order.user_id,
                seller_id=counter_order.user_id if new_order.direction == "BUY" else new_order.user_id
            )
            db.add(transaction)
            executed_trades.append(transaction)

            # Обновляем ордера
            new_order.filled += trade_qty
            counter_order.filled += trade_qty

            new_order.status = (
                "EXECUTED" if new_order.filled == new_order.quantity
                else "PARTIALLY_EXECUTED"
            )
            counter_order.status = (
                "EXECUTED" if counter_order.filled == counter_order.quantity
                else "PARTIALLY_EXECUTED"
            )

            # Явно помечаем ордера как измененные
            db.add(new_order)
            db.add(counter_order)
            db.flush()

            logger.info(
                f"Matched trade: {trade_qty} {new_order.instrument_ticker} at {trade_price} "
                f"between {new_order.user_id} and {counter_order.user_id}. "
                f"New order status: {new_order.status}, Counter order status: {counter_order.status}"
            )

            qty_to_fill -= trade_qty

        db.commit()
        logger.info(f"Successfully committed {len(executed_trades)} trades")
        
        # Обновляем состояние ордера после коммита
        db.refresh(new_order)
        return new_order

    except Exception as e:
        db.rollback()
        logger.error(f"Failed to match orders: {str(e)}", exc_info=True)
        raise