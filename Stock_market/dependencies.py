from database import get_db
from sqlalchemy.orm import Session
from crud import get_user_by_api_key
from fastapi.security import APIKeyHeader
from fastapi import Depends, HTTPException, status


api_key_scheme = APIKeyHeader(name="Authorization")


def get_current_user(
        authorization: str = Depends(api_key_scheme),
        db: Session = Depends(get_db)
):
    if not authorization.startswith("TOKEN "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication scheme"
        )

    api_key = authorization[6:]
    user = get_user_by_api_key(db, api_key)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    return user

def get_admin_user(user=Depends(get_current_user)):
    if user.role != "ADMIN":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required"
        )
    return user