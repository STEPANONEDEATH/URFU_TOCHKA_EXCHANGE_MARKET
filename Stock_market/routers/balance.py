from typing import Dict

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from crud import get_balances
from database import get_db
from dependencies import get_current_user

router = APIRouter()


@router.get(
    "/balance",
    response_model=Dict[str, int],
    operation_id="get_balances_api_v1_balance_get",
    summary="Get Balances",
    tags=["balance"],
    responses={
        200: {
            "description": "Successful Response",
            "content": {
                "application/json": {"example": {"Mem-coin": 15, "Bitcoin": 1.2342}}
            },
        }
    },
)
def get_balances_endpoint(
    user=Depends(get_current_user), db: Session = Depends(get_db)
):

    balances = get_balances(db, user.id)
    return {b.ticker: b.amount for b in balances}
