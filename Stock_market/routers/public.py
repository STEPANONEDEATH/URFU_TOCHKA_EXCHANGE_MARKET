from crud import (create_user, get_instrument, get_instruments, get_orderbook,
                  get_transactions)
from database import get_db
from fastapi import APIRouter, Depends, HTTPException, Query
from models import Instrument, L2OrderBook, Level, NewUser, Transaction, User
from sqlalchemy.orm import Session

router = APIRouter(tags=["public"])


@router.post(
    "/register",
    response_model=User,
    summary="Register",
    description=(
        "Регистрация пользователя в платформе. Обязательна для совершения сделок. "
        "api_key полученный из этого метода следует передавать в другие через заголовок Authorization. "
        "Например для api_key='key-bee6de4d-7a23-4bb1-a048-523c2ef0ea0c` значение будет таким: "
        "Authorization: TOKEN key-bee6de4d-7a23-4bb1-a048-523c2ef0ea0c"
    )
)
def register(new_user: NewUser, db: Session = Depends(get_db)):
    return create_user(db, new_user)


@router.get(
    "/instrument",
    response_model=list[Instrument],
    summary="List Instruments",
    description=(
        "Список доступных инструментов"
    )
)
def list_instruments(db: Session = Depends(get_db)):
    return get_instruments(db)


@router.get(
    "/orderbook/{ticker}",
    response_model=L2OrderBook,
    summary="Get Orderbook",
    description=(
        "Текущие заявки"
    )
)
def get_orderbook_endpoint(
    ticker: str, limit: int = Query(10, le=25), db: Session = Depends(get_db)
):
    if limit > 25:
        raise HTTPException(status_code=400, detail="Limit cannot exceed 25")

    # Проверка: существует ли инструмент
    instrument = get_instrument(db, ticker)
    if not instrument:
        raise HTTPException(status_code=404, detail=f"Instrument '{ticker}' not found.")

    bids, asks = get_orderbook(db, ticker, limit)

    return L2OrderBook(
        bid_levels=[Level(price=o.price, qty=o.quantity - o.filled) for o in bids],
        ask_levels=[
            Level(price=o.price, qty=o.quantity - o.filled)
            for o in asks
            if o.price is not None
        ],
    )


@router.get(
    "/transactions/{ticker}",
    response_model=list[Transaction],
    summary="Get Transaction History",
    description=(
        "История сделок"
    )
)
def get_transaction_history(
    ticker: str, limit: int = Query(10, le=100), db: Session = Depends(get_db)
):
    if limit > 100:
        raise HTTPException(status_code=400, detail="Limit cannot exceed 100")

    db_transactions = get_transactions(db, ticker, limit)
    return [
        Transaction(
            ticker=t.instrument_ticker,
            amount=t.quantity,
            price=t.price,
            timestamp=t.created_at,
        )
        for t in db_transactions
    ]
