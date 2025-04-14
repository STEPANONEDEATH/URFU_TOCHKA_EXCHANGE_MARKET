from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from models import Ok
from crud import get_balances
from database import get_db
from dependencies import get_current_user
from typing import Dict




router = APIRouter()

@router.get("/balance", response_model=Dict[str, int])
def get_balances_endpoint(
    user = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    balances = get_balances(db, user.id)
    return {b.ticker: b.amount for b in balances}