from datetime import datetime, timedelta
from typing import List, Optional

import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt

from src.app.database import db_settings
from src.app.core.roles import ROLE_ADMIN, ROLE_VIEWER

security = HTTPBearer()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())


def get_password_hash(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta or timedelta(
            minutes=db_settings.access_token_expire_minutes)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, db_settings.secret_key, algorithm=db_settings.algorithm)


def decode_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, db_settings.secret_key,
                             algorithms=[db_settings.algorithm])
        return payload
    except JWTError:
        return None


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    token = credentials.credentials
    payload = decode_token(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )
    return payload


def require_roles(allowed_roles: List[str]):

    async def _require_roles(
        current_user: dict = Depends(get_current_user),
    ) -> dict:
        role = current_user.get("role") or ROLE_VIEWER
        if role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions for this action",
            )
        return current_user

    return _require_roles
