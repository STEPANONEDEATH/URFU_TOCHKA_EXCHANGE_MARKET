from fastapi import FastAPI
from routers import public, orders, balance, admin, ws


app = FastAPI(
    title="Toy Exchange API",
    description="API for toy exchange platform with real-time trading",
    version="0.1.0",
    contact={
        "name": "API Support",
        "email": "stepanonedeath@gmail.com / support@toyexchange.com"
    },
    license_info={
        "name": "MIT",
    },
)

# Root endpoint
@app.get("/", tags=["root"])
async def root():
    return {
        "message": "Welcome to Toy Exchange API",
        "docs": "/docs",
        "redoc": "/redoc"
    }

# Include routers
app.include_router(public.router, prefix="/api/v1/public", tags=["public"])
app.include_router(orders.router, prefix="/api/v1", tags=["orders"])
app.include_router(balance.router, prefix="/api/v1", tags=["balance"])
app.include_router(admin.router, prefix="/api/v1/admin", tags=["admin"])
app.include_router(ws.router, prefix="/ws", tags=["websocket"])

@app.on_event("startup")
async def startup_event():
    """Запуск Kafka"""
    from kafka.producer import init_producer
    await init_producer()

    from kafka.consumer import start_consumers
    await start_consumers()

@app.on_event("shutdown")
async def shutdown_event():
    """Выключение Kafka"""
    from kafka.producer import close_producer
    await close_producer()