from database import Base, engine
from schemas import User, Instrument, Order, Balance, Transaction

def init_db():
    Base.metadata.create_all(bind=engine)
    print("Таблицы успешно созданы!")

if __name__ == "__main__":
    init_db()