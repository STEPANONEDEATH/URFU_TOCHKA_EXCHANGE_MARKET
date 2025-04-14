from fastapi import APIRouter, Depends, HTTPException
from models import Instrument, Ok, DepositRequest, WithdrawRequest, User
from crud import (
    create_instrument, delete_instrument,
    update_balance, get_user, delete_user
)
from database import get_db
from dependencies import get_admin_user
from uuid import UUID
from sqlalchemy.orm import Session


router = APIRouter()


@router.post("/instrument", response_model=Ok)
def add_instrument(
        instrument: Instrument,
        admin=Depends(get_admin_user),
        db: Session = Depends(get_db)
):
    create_instrument(db, instrument)
    return Ok()


@router.delete("/instrument/{ticker}", response_model=Ok)
def delete_instrument_endpoint(
        ticker: str,
        admin=Depends(get_admin_user),
        db: Session = Depends(get_db)
):
    if delete_instrument(db, ticker):
        return Ok()
    else:
        raise HTTPException(status_code=404, detail="Instrument not found")


@router.post("/balance/deposit", response_model=Ok)
def deposit(
        request: DepositRequest,
        admin=Depends(get_admin_user),
        db: Session = Depends(get_db)
):
    update_balance(db, request.user_id, request.ticker, request.amount)
    return Ok()


@router.post("/balance/withdraw", response_model=Ok)
def withdraw(
        request: WithdrawRequest,
        admin=Depends(get_admin_user),
        db: Session = Depends(get_db)
):
    update_balance(db, request.user_id, request.ticker, -request.amount)
    return Ok()


@router.delete("/user/{user_id}", response_model=User)
def delete_user_endpoint(
        user_id: UUID,
        admin=Depends(get_admin_user),
        db: Session = Depends(get_db)
):
    user = get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    delete_user(db, user_id)
    return user
