from models import Instrument, Ok, DepositRequest, WithdrawRequest, User
from fastapi import APIRouter, Depends, HTTPException, status
from dependencies import get_admin_user
from sqlalchemy.orm import Session
from typing import Annotated
from database import get_db
from uuid import UUID
from crud import (
    create_instrument, delete_instrument,
    update_balance, get_user, delete_user
)


router = APIRouter(prefix="/admin", tags=["admin"])

DbSession = Annotated[Session, Depends(get_db)]
AdminUser = Annotated[User, Depends(get_admin_user)]


@router.post("/instruments", response_model=Ok, status_code=status.HTTP_201_CREATED)
def add_instrument(
        instrument: Instrument,
        admin: AdminUser,
        db: DbSession
):
    """Добавление нового инструмента (Монет / Мем-коинов / Криптовалюты)"""
    create_instrument(db, instrument)
    return Ok()


@router.delete("/instruments/{ticker}", response_model=Ok)
def remove_instrument(
        ticker: str,
        admin: AdminUser,
        db: DbSession
):
    """Удаление финансового инструмента по тикеру"""
    if not delete_instrument(db, ticker):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Instrument not found"
        )
    return Ok()


@router.post("/balances/deposits", response_model=Ok)
def deposit_funds(
        request: DepositRequest,
        admin: AdminUser,
        db: DbSession
):
    """Пополнение баланса"""
    update_balance(db, request.user_id, request.ticker, request.amount)
    return Ok()


@router.post("/balances/withdrawals", response_model=Ok)
def withdraw_funds(
        request: WithdrawRequest,
        admin: AdminUser,
        db: DbSession
):
    """Вывод средств (по сути просто списание с баланса)"""
    update_balance(db, request.user_id, request.ticker, -request.amount)
    return Ok()


@router.delete("/users/{user_id}", response_model=User)
def remove_user(
        user_id: UUID,
        admin: AdminUser,
        db: DbSession
):
    """Удаление пользователя по ID"""
    user = get_user(db, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    delete_user(db, user_id)
    return user