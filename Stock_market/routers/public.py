from fastapi import APIRouter, Depends, HTTPException
from models import NewUser, User, Instrument, L2OrderBook, Transaction
from database import get_db
from sqlalchemy.orm import Session
from crud import create_user, get_instruments, get_orderbook, get_transactions
from models import Level 


router = APIRouter(tags=["public"])

@router.get("/health")
async def health_check():
    return {"status": "ok"}


@router.post("/register", response_model=User)
def register(new_user: NewUser, db: Session = Depends(get_db)):
    return create_user(db, new_user)


@router.get("/instrument", response_model=list[Instrument])
def list_instruments(db: Session = Depends(get_db)):
    return get_instruments(db)


@router.get("/{ticker}", response_model=L2OrderBook)
def get_orderbook_endpoint(
        ticker: str,
        limit: int = 10,
        db: Session = Depends(get_db)
):
    if limit > 25:
        raise HTTPException(status_code=400, detail="Limit cannot exceed 25")

    bids, asks = get_orderbook(db, ticker, limit)

    return L2OrderBook(
        bid_levels=[Level(price=o.price, qty=o.quantity - o.filled) for o in bids],
        ask_levels=[Level(price=o.price, qty=o.quantity - o.filled) for o in asks]
    )


@router.get("/{ticker}", response_model=list[Transaction])
def get_transaction_history(
        ticker: str,
        limit: int = 10,
        db: Session = Depends(get_db)
):
    if limit > 100:
        raise HTTPException(status_code=400, detail="Limit cannot exceed 100")

    db_transactions = get_transactions(db, ticker, limit)
    return [
        Transaction(
            ticker=t.instrument_ticker,
            amount=t.quantity,
            price=t.price,
            timestamp=t.created_at
        ) for t in db_transactions
    ]