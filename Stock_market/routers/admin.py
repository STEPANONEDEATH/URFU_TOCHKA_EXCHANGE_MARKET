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


@router.delete("/user/{user_id}",
               response_model=User,
               tags=["user"],
               summary="Delete User",
               description = """Удаление пользователя по ID.
    
                                Этот метод позволяет администратору удалить пользователя из платформы по уникальному ID. 
                                Если пользователь не найден, возвращается ошибка 404."""
              )
def remove_user(
        user_id: UUID,
        admin: AdminUser,
        db: DbSession
):

    user = get_user(db, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    delete_user(db, user_id)
    return user


@router.post("/instrument",
             response_model=Ok,
             status_code=status.HTTP_201_CREATED,
             summary="Add Instrument",
             description = """Добавление нового инструмента (Монет / Мем-коинов / Криптовалюты).
    
                              Этот метод позволяет администратору добавить новый финансовый инструмент на платформу, например, криптовалюту или токен."""
            )
def add_instrument(
        instrument: Instrument,
        admin: AdminUser,
        db: DbSession
):

    create_instrument(db, instrument)
    return Ok()


@router.delete("/instrument/{ticker}",
               response_model=Ok,
               summary="Delete Instrument",
               description = """Удаление финансового инструмента по тикеру.
    
                                Этот метод позволяет администратору удалить финансовый инструмент (например, криптовалюту или токен) по тикеру. 
                                В случае если инструмент не найден, возвращается ошибка 404"""
              )
def remove_instrument(
        ticker: str,
        admin: AdminUser,
        db: DbSession
):

    # Логирование запроса на удаление
    print(f"Trying to delete instrument with ticker: {ticker}")

    # Пытаемся удалить инструмент
    if not delete_instrument(db, ticker):
        print(f"Instrument with ticker {ticker} not found")  # Логирование, если инструмент не найден
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Instrument not found"
        )

    # Логирование успешного удаления
    print(f"Instrument with ticker {ticker} deleted successfully")

    return Ok()

@router.post("/balance/deposit",
             response_model=Ok,
             tags=["admin", "balance"],
             summary="Deposit",
             description = """Пополнение баланса пользователя.
             
                              Этот метод позволяет администратору пополнить баланс пользователя в определённой валюте."""
            )
def deposit_funds(
        request: DepositRequest,
        admin: AdminUser,
        db: DbSession
):

    update_balance(db, request.user_id, request.ticker, request.amount)
    return Ok()


@router.post("/balance/withdraw",
             response_model=Ok,
             tags=["admin", "balance"],
             summary="Withdraw",
             description = """Вывод средств (списание с баланса)
             
                              Этот метод позволяет администратору списать средства с баланса пользователя в указанной валюте."""
            )
def withdraw_funds(
        request: WithdrawRequest,
        admin: AdminUser,
        db: DbSession
):

    update_balance(db, request.user_id, request.ticker, -request.amount)
    return Ok()
