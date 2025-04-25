from typing import Dict
from database import get_db
from crud import get_balances
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from dependencies import get_current_user


router = APIRouter()


@router.get("/balance", response_model=Dict[str, int])
def get_balances_endpoint(
    user = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    balances = get_balances(db, user.id)
    return {b.ticker: b.amount for b in balances}
