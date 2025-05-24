import logging
import uuid
from datetime import datetime
from typing import Union
from uuid import UUID

import models
import schemas
from fastapi import HTTPException, status
from schemas import Balance
from schemas import Instrument as ORMInstrument
from schemas import User
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def get_user(db: Session, user_id: UUID):
    try:
        return db.query(schemas.User).filter(schemas.User.id == user_id).first()
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {str(e)}", exc_info=True)
        raise


def get_user_by_api_key(db: Session, api_key: str):
    try:
        return db.query(schemas.User).filter(schemas.User.api_key == api_key).first()
    except Exception as e:
        logger.error(f"Error getting user by API key: {str(e)}", exc_info=True)
        raise


def create_user(db: Session, user: models.NewUser):
    try:
        db_user = schemas.User(name=user.name, api_key=str(uuid.uuid4()))
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        logger.info(f"New user created: {db_user.name}")
        return db_user
    except IntegrityError as e:
        logger.error(f"User creation failed (duplicate name?): {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}", exc_info=True)
        raise


def get_instruments(db: Session, skip: int = 0, limit: int = 100):
    try:
        return (
            db.query(schemas.Instrument)
            .filter(schemas.Instrument.is_active == True)
            .offset(skip)
            .limit(limit)
            .all()
        )
    except Exception as e:
        logger.error(f"Error getting instruments: {str(e)}", exc_info=True)
        raise


def get_instrument(db: Session, ticker: str):
    try:
        return (
            db.query(schemas.Instrument)
            .filter(schemas.Instrument.ticker == ticker)
            .first()
        )
    except Exception as e:
        logger.error(f"Error getting instrument {ticker}: {str(e)}", exc_info=True)
        raise


def create_instrument(db: Session, instrument: models.Instrument):
    try:
        existing_instrument = (
            db.query(schemas.Instrument)
            .filter(schemas.Instrument.ticker == instrument.ticker)
            .first()
        )

        if existing_instrument:
            logger.error(f"Instrument already exists: {instrument.ticker}")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Instrument with ticker {instrument.ticker} already exists",
            )

        db_instrument = schemas.Instrument(
            ticker=instrument.ticker, name=instrument.name
        )

        db.add(db_instrument)
        db.commit()
        db.refresh(db_instrument)
        return db_instrument
    except Exception as e:
        logger.error(
            f"Error creating instrument {instrument.ticker}: {str(e)}", exc_info=True
        )
        raise


def delete_instrument(db: Session, ticker: str) -> bool:
    try:
        instrument = (
            db.query(schemas.Instrument)
            .filter(schemas.Instrument.ticker == ticker)
            .first()
        )
        if not instrument:
            logger.error(f"Instrument not found for deletion: {ticker}")
            return False
        db.delete(instrument)
        db.commit()
        return True
    except Exception as e:
        logger.error(f"Error deleting instrument {ticker}: {str(e)}", exc_info=True)
        raise


def get_orderbook(db: Session, ticker: str, limit: int = 10):
    try:
        bids = (
            db.query(schemas.Order)
            .filter(
                schemas.Order.instrument_ticker == ticker,
                schemas.Order.direction == "BUY",
                schemas.Order.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
                schemas.Order.type == "LIMIT",
                schemas.Order.price != None,
            )
            .order_by(schemas.Order.price.desc())
            .limit(limit)
            .all()
        )

        asks = (
            db.query(schemas.Order)
            .filter(
                schemas.Order.instrument_ticker == ticker,
                schemas.Order.direction == "SELL",
                schemas.Order.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
                schemas.Order.type == "LIMIT",
                schemas.Order.price != None,
            )
            .order_by(schemas.Order.price.asc())
            .limit(limit)
            .all()
        )

        return bids, asks
    except Exception as e:
        logger.error(f"Error getting orderbook for {ticker}: {str(e)}", exc_info=True)
        raise


def get_transactions(db: Session, ticker: str, limit: int = 10):
    try:
        return (
            db.query(schemas.Transaction)
            .filter(schemas.Transaction.instrument_ticker == ticker)
            .order_by(schemas.Transaction.created_at.desc())
            .limit(limit)
            .all()
        )
    except Exception as e:
        logger.error(
            f"Error getting transactions for {ticker}: {str(e)}", exc_info=True
        )
        raise


def get_orders(db: Session, user_id: UUID):
    try:
        return db.query(schemas.Order).filter(schemas.Order.user_id == user_id).all()
    except Exception as e:
        logger.error(
            f"Error getting orders for user {user_id}: {str(e)}", exc_info=True
        )
        raise


def get_order(db: Session, order_id: UUID):
    try:
        return db.query(schemas.Order).filter(schemas.Order.id == order_id).first()
    except Exception as e:
        logger.error(f"Error getting order {order_id}: {str(e)}", exc_info=True)
        raise


def create_order(
    db: Session,
    order: Union[models.LimitOrderBody, models.MarketOrderBody],
    user_id: UUID,
):
    try:
        instrument = get_instrument(db, order.ticker)

        if not instrument:
            logger.error(f"Invalid ticker for order: {order.ticker}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid ticker"
            )

        if order.qty <= 0:
            logger.error(f"Invalid order quantity: {order.qty}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Order quantity must be greater than zero",
            )

        if isinstance(order, models.LimitOrderBody) and order.price is None:
            logger.error("Limit order created without price")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Limit order must include a price",
            )

        if order.direction == "SELL":
            user_balance = get_balance(db, user_id, order.ticker)
            required_amount = order.qty
            if not user_balance or user_balance.amount < required_amount:
                logger.error(
                    f"Insufficient balance for sell order: user {user_id}, ticker {order.ticker}"
                )
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Insufficient balance",
                )

        db_order = schemas.Order(
            user_id=user_id,
            instrument_ticker=order.ticker,
            direction=order.direction.value,
            type="LIMIT" if isinstance(order, models.LimitOrderBody) else "MARKET",
            price=order.price if isinstance(order, models.LimitOrderBody) else None,
            quantity=order.qty,
            status="NEW",
        )
        db.add(db_order)
        db.flush()

        match_order(db, db_order)

        db.commit()
        db.refresh(db_order)
        return db_order
    except Exception as e:
        logger.error(f"Error creating order: {str(e)}", exc_info=True)
        raise


def cancel_order(db: Session, order_id: UUID) -> bool:
    try:
        order = db.query(schemas.Order).filter(schemas.Order.id == order_id).first()

        if not order:
            logger.error(f"Order not found for cancellation: {order_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Order not found"
            )

        if order.type == "MARKET":
            if order.status != "NEW" or order.filled > 0:
                logger.error(
                    f"Cannot cancel market order {order_id} - status: {order.status}, filled: {order.filled}"
                )
                raise HTTPException(
                    status_code=status.HTTP_402_PAYMENT_REQUIRED,
                    detail="Cannot cancel market order (already executed or processing)",
                )
        else:
            if order.status not in ["NEW", "PARTIALLY_EXECUTED"]:
                logger.error(
                    f"Cannot cancel limit order {order_id} - status: {order.status}"
                )
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Cannot cancel limit order (already executed, cancelled, or rejected)",
                )

        order.status = "CANCELLED"
        db.commit()
        return True
    except Exception as e:
        logger.error(f"Error cancelling order {order_id}: {str(e)}", exc_info=True)
        raise


def get_balance(db: Session, user_id: UUID, ticker: str):
    try:
        return (
            db.query(schemas.Balance)
            .filter(
                schemas.Balance.user_id == user_id, schemas.Balance.ticker == ticker
            )
            .first()
        )
    except Exception as e:
        logger.error(
            f"Error getting balance for user {user_id}, ticker {ticker}: {str(e)}",
            exc_info=True,
        )
        raise


def get_balances(db: Session, user_id: UUID):
    try:
        return (
            db.query(schemas.Balance).filter(schemas.Balance.user_id == user_id).all()
        )
    except Exception as e:
        logger.error(
            f"Error getting balances for user {user_id}: {str(e)}", exc_info=True
        )
        raise


def update_balance(db: Session, user_id: UUID, ticker: str, amount: int):
    try:
        user = db.query(schemas.User).filter(schemas.User.id == user_id).first()
        if user is None:
            logger.error(f"User not found for balance update: {user_id}")
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")

        balance = (
            db.query(schemas.Balance)
            .filter(
                schemas.Balance.user_id == user_id, schemas.Balance.ticker == ticker
            )
            .first()
        )

        if balance and (balance.amount + amount < 0):
            logger.error(f"Insufficient balance for user {user_id}, ticker {ticker}")
            raise ValueError("Insufficient balance to deduct")

        if balance:
            balance.amount += amount
        else:
            balance = schemas.Balance(user_id=user_id, ticker=ticker, amount=amount)
            db.add(balance)

        db.commit()
        db.refresh(balance)
        return balance
    except Exception as e:
        logger.error(
            f"Error updating balance for user {user_id}, ticker {ticker}: {str(e)}",
            exc_info=True,
        )
        raise


def delete_user(db: Session, user_id: UUID):
    try:
        user = db.query(schemas.User).filter(schemas.User.id == user_id).first()
        if user:
            db.delete(user)
            db.commit()
            return True
        logger.error(f"User not found for deletion: {user_id}")
        return False
    except Exception as e:
        logger.error(f"Error deleting user {user_id}: {str(e)}", exc_info=True)
        raise


def match_order(db: Session, new_order: schemas.Order):
    try:
        is_market_order = new_order.type == "MARKET"
        opposite_side = "BUY" if new_order.direction == "SELL" else "SELL"

        with db.begin_nested():
            base_query = db.query(schemas.Order).filter(
                schemas.Order.instrument_ticker == new_order.instrument_ticker,
                schemas.Order.direction == opposite_side,
                schemas.Order.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
                schemas.Order.price.isnot(None),
            )

            if not is_market_order:
                if new_order.direction == "BUY":
                    base_query = base_query.filter(
                        schemas.Order.price <= new_order.price
                    )
                else:
                    base_query = base_query.filter(
                        schemas.Order.price >= new_order.price
                    )

            matching_orders = (
                base_query.order_by(
                    (
                        schemas.Order.price.asc()
                        if new_order.direction == "BUY"
                        else schemas.Order.price.desc()
                    ),
                    schemas.Order.created_at.asc(),
                )
                .with_for_update()
                .all()
            )

            qty_to_fill = new_order.quantity - new_order.filled
            trades = []

            for counter_order in matching_orders:
                if qty_to_fill <= 0:
                    break

                available_qty = counter_order.quantity - counter_order.filled
                trade_qty = min(qty_to_fill, available_qty)

                if trade_qty <= 0:
                    continue

                trade_price = counter_order.price

                if new_order.direction == "BUY":
                    buyer_rub_balance = get_balance(db, new_order.user_id, "RUB")
                    seller_ticker_balance = get_balance(
                        db, counter_order.user_id, new_order.instrument_ticker
                    )

                    required_rub = trade_qty * trade_price

                    if not buyer_rub_balance or buyer_rub_balance.amount < required_rub:
                        logger.error(f"Insufficient RUB for user {new_order.user_id}")
                        continue

                    if (
                        not seller_ticker_balance
                        or seller_ticker_balance.amount < trade_qty
                    ):
                        logger.error(
                            f"Insufficient {new_order.instrument_ticker} for user {counter_order.user_id}"
                        )
                        continue
                else:
                    seller_ticker_balance = get_balance(
                        db, new_order.user_id, new_order.instrument_ticker
                    )
                    buyer_rub_balance = get_balance(db, counter_order.user_id, "RUB")

                    required_rub = trade_qty * trade_price

                    if (
                        not seller_ticker_balance
                        or seller_ticker_balance.amount < trade_qty
                    ):
                        logger.error(
                            f"Insufficient {new_order.instrument_ticker} for user {new_order.user_id}"
                        )
                        continue

                    if not buyer_rub_balance or buyer_rub_balance.amount < required_rub:
                        logger.error(
                            f"Insufficient RUB for user {counter_order.user_id}"
                        )
                        continue

                trade = schemas.Transaction(
                    instrument_ticker=new_order.instrument_ticker,
                    price=trade_price,
                    quantity=trade_qty,
                    buyer_id=(
                        new_order.user_id
                        if new_order.direction == "BUY"
                        else counter_order.user_id
                    ),
                    seller_id=(
                        counter_order.user_id
                        if new_order.direction == "BUY"
                        else new_order.user_id
                    ),
                    created_at=datetime.utcnow(),
                )
                db.add(trade)
                trades.append(trade)

                new_order.filled += trade_qty
                counter_order.filled += trade_qty

                new_order.status = (
                    "EXECUTED"
                    if new_order.filled == new_order.quantity
                    else "PARTIALLY_EXECUTED" if new_order.filled > 0 else "NEW"
                )
                counter_order.status = (
                    "EXECUTED"
                    if counter_order.filled == counter_order.quantity
                    else "PARTIALLY_EXECUTED"
                )

                db.add(new_order)
                db.add(counter_order)
                qty_to_fill -= trade_qty

        for trade in trades:
            try:
                if new_order.direction == "BUY":
                    update_balance(
                        db, trade.buyer_id, new_order.instrument_ticker, trade.quantity
                    )
                    update_balance(
                        db, trade.buyer_id, "RUB", -trade.quantity * trade.price
                    )
                    update_balance(
                        db, trade.seller_id, "RUB", trade.quantity * trade.price
                    )
                    update_balance(
                        db,
                        trade.seller_id,
                        new_order.instrument_ticker,
                        -trade.quantity,
                    )
                else:
                    update_balance(
                        db, trade.seller_id, "RUB", trade.quantity * trade.price
                    )
                    update_balance(
                        db,
                        trade.seller_id,
                        new_order.instrument_ticker,
                        -trade.quantity,
                    )
                    update_balance(
                        db, trade.buyer_id, new_order.instrument_ticker, trade.quantity
                    )
                    update_balance(
                        db, trade.buyer_id, "RUB", -trade.quantity * trade.price
                    )
            except ValueError as e:
                logger.error(f"Balance update failed for trade {trade.id}: {str(e)}")
                db.rollback()
                raise

        db.commit()
        db.refresh(new_order)
        return new_order

    except Exception as e:
        logger.error(
            f"Order matching failed for order {new_order.id}: {str(e)}", exc_info=True
        )
        db.rollback()
        raise
