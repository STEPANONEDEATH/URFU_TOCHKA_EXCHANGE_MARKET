from uuid import UUID
from database import get_db
from typing import List, Union
from sqlalchemy.orm import Session
from dependencies import get_current_user
from kafka.producer import produce_order_event
from fastapi import APIRouter, Depends, HTTPException
from crud import create_order, get_orders, get_order, cancel_order, get_instrument
from models import LimitOrderBody, MarketOrderBody, CreateOrderResponse, LimitOrder, MarketOrder

router = APIRouter(tags=["order"])


@router.get("/order",
            response_model=List[Union[LimitOrder, MarketOrder]],
            summary="List Orders",
           )
def list_orders(
        user=Depends(get_current_user),
        db: Session = Depends(get_db)
):
    db_orders = get_orders(db, user.id)
    result = []

    for o in db_orders:
        # Чтобы избежать DetachedInstanceError, здесь работаем с объектами пока сессия жива
        if not o.instrument_ticker:
            continue

        if o.type == "LIMIT":
            result.append(
                LimitOrder(
                    id=o.id,
                    status=o.status,
                    user_id=o.user_id,
                    timestamp=o.created_at,
                    body=LimitOrderBody(
                        direction=o.direction,
                        ticker=o.instrument_ticker,
                        qty=o.quantity,
                        price=o.price
                    ),
                    filled=o.filled
                )
            )
        else:
            result.append(
                MarketOrder(
                    id=o.id,
                    status=o.status,
                    user_id=o.user_id,
                    timestamp=o.created_at,
                    body=MarketOrderBody(
                        direction=o.direction,
                        ticker=o.instrument_ticker,
                        qty=o.quantity
                    )
                )
            )

    return result


@router.post("/order",
             response_model=CreateOrderResponse,
             summary="Create Order",
            )
async def create_order_endpoint(
        order: Union[LimitOrderBody, MarketOrderBody],
        user=Depends(get_current_user),
        db: Session = Depends(get_db)
):
    instrument = get_instrument(db, order.ticker)
    if not instrument:
        raise HTTPException(status_code=422, detail=f"Invalid ticker: {order.ticker}")

    if isinstance(order, LimitOrderBody):
        if order.price <= 0:
            raise HTTPException(status_code=422, detail="Price must be greater than zero.")
        if order.price != int(order.price):
            raise HTTPException(status_code=422, detail="Price must be an integer.")

    try:
        db_order = create_order(db, order, user.id)
        # сразу "принудительно" загрузим все нужные атрибуты,
        # чтобы объект не был detached при передаче в produce_order_event
        db.refresh(db_order)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    await produce_order_event(db_order, "PLACED")
    return CreateOrderResponse(order_id=db_order.id)


@router.get("/order/{order_id}",
            response_model=Union[LimitOrder, MarketOrder],
            summary="Get Order"
           )
def get_order_endpoint(
        order_id: UUID,
        user=Depends(get_current_user),
        db: Session = Depends(get_db)
):
    db_order = get_order(db, order_id)
    if not db_order or db_order.user_id != user.id:
        raise HTTPException(status_code=404, detail="Order not found")

    if db_order.type == "LIMIT":
        return LimitOrder(
            id=db_order.id,
            status=db_order.status,
            user_id=db_order.user_id,
            timestamp=db_order.created_at,
            body=LimitOrderBody(
                direction=db_order.direction,
                ticker=db_order.instrument_ticker,
                qty=db_order.quantity,
                price=db_order.price
            ),
            filled=db_order.filled
        )
    else:
        return MarketOrder(
            id=db_order.id,
            status=db_order.status,
            user_id=db_order.user_id,
            timestamp=db_order.created_at,
            body=MarketOrderBody(
                direction=db_order.direction,
                ticker=db_order.instrument_ticker,
                qty=db_order.quantity
            )
        )


@router.delete("/order/{order_id}",
               response_model=dict,
               summary="Cancel Order"
              )
async def cancel_order_endpoint(
        order_id: UUID,
        user=Depends(get_current_user),
        db: Session = Depends(get_db)
):
    db_order = get_order(db, order_id)
    if not db_order or db_order.user_id != user.id:
        raise HTTPException(status_code=404, detail="Order not found")

    success = cancel_order(db, order_id)
    if success:
        # refresh, чтобы объект был привязан к сессии
        db.refresh(db_order)
        await produce_order_event(db_order, "CANCELLED")
        return {"success": True}
    else:
        raise HTTPException(status_code=400, detail="Order cannot be cancelled")