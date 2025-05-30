import logging
from typing import List, Union
from uuid import UUID

from crud import (cancel_order, create_order, get_instrument, get_order,
                  get_orders)
from database import get_db
from dependencies import get_current_user
from fastapi import APIRouter, Depends, HTTPException
from kafka.producer import produce_order_event
from models import (CreateOrderResponse, LimitOrder, LimitOrderBody,
                    MarketOrder, MarketOrderBody)
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

router = APIRouter(tags=["order"])


@router.get(
    "/order",
    response_model=List[Union[LimitOrder, MarketOrder]],
    summary="List Orders",
)
def list_orders(user=Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        logger.info(f"Listing orders for user {user.id}")
        db_orders = get_orders(db, user.id)
        result = []

        for o in db_orders:
            if not o.instrument_ticker:
                logger.warning(f"Order {o.id} has no instrument ticker, skipping")
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
                            price=o.price,
                        ),
                        filled=o.filled,
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
                            qty=o.quantity,
                        ),
                    )
                )

        logger.info(f"Returning {len(result)} orders for user {user.id}")
        return result

    except Exception as e:
        logger.error(
            f"Error listing orders for user {user.id}: {str(e)}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post(
    "/order",
    response_model=CreateOrderResponse,
    summary="Create Order",
)
async def create_order_endpoint(
    order: Union[LimitOrderBody, MarketOrderBody],
    user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    logger.info(
        f"Creating new order for user {user.id}, ticker: {order.ticker}, "
        f"type: {'LIMIT' if isinstance(order, LimitOrderBody) else 'MARKET'}"
    )

    instrument = get_instrument(db, order.ticker)
    if not instrument:
        logger.error(f"Invalid ticker: {order.ticker}")
        raise HTTPException(status_code=422, detail=f"Invalid ticker: {order.ticker}")

    if isinstance(order, LimitOrderBody):
        if order.price <= 0:
            logger.error(f"Invalid price {order.price} for limit order")
            raise HTTPException(
                status_code=422, detail="Price must be greater than zero."
            )
        if order.price != int(order.price):
            logger.error(f"Non-integer price {order.price} for limit order")
            raise HTTPException(status_code=422, detail="Price must be an integer.")

    try:
        db_order = create_order(db, order, user.id)
        db.refresh(db_order)
        logger.info(f"Order created successfully: {db_order.id}")
    except HTTPException as e:
        raise e
    except ValueError as e:
        logger.error(f"Order creation failed: {str(e)}")
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating order: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

    try:
        await produce_order_event(db_order, "PLACED")
        logger.debug(f"Order event produced for order {db_order.id}")
    except Exception as e:
        logger.error(f"Failed to produce order event: {str(e)}", exc_info=True)

    return CreateOrderResponse(order_id=db_order.id)


@router.get(
    "/order/{order_id}",
    response_model=Union[LimitOrder, MarketOrder],
    summary="Get Order",
)
def get_order_endpoint(
    order_id: UUID, user=Depends(get_current_user), db: Session = Depends(get_db)
):
    logger.info(f"Fetching order {order_id} for user {user.id}")

    db_order = get_order(db, order_id)
    if not db_order or (db_order.user_id != user.id and user.role != "ADMIN"):
        logger.warning(
            f"Order {order_id} not found or access denied for user {user.id}"
        )
        raise HTTPException(status_code=404, detail="Order not found")

    try:
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
                    price=db_order.price,
                ),
                filled=db_order.filled,
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
                    qty=db_order.quantity,
                ),
            )
    except Exception as e:
        logger.error(f"Error processing order {order_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/order/{order_id}", response_model=dict, summary="Cancel Order")
async def cancel_order_endpoint(
    order_id: UUID, user=Depends(get_current_user), db: Session = Depends(get_db)
):
    logger.info(f"Cancelling order {order_id} for user {user.id}")

    db_order = get_order(db, order_id)

    if not db_order or db_order.user_id != user.id:
        logger.warning(
            f"Order {order_id} not found or access denied for user {user.id}"
        )
        raise HTTPException(status_code=404, detail="Order not found")

    if db_order.status == "CANCELLED":
        logger.warning(f"Order {order_id} already cancelled")
        raise HTTPException(status_code=422, detail="Order already cancelled")

    try:
        success = cancel_order(db, order_id)
        if not success:
            logger.error(f"Order {order_id} cannot be cancelled in current state")
            raise HTTPException(
                status_code=400, detail="Order cannot be cancelled in its current state"
            )

        db.refresh(db_order)
        logger.info(f"Order {order_id} cancelled successfully")

        try:
            await produce_order_event(db_order, "CANCELLED")
            logger.debug(f"Order cancellation event produced for order {order_id}")
        except Exception as e:
            logger.error(
                f"Failed to produce cancellation event: {str(e)}", exc_info=True
            )
            # Не прерываем выполнение, так как ордер уже отменен

        return {"success": True}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling order {order_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while processing order cancellation",
        )
