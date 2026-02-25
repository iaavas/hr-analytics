"""JWT and password utilities, and FastAPI dependencies for authentication and role-based authorization."""

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
    """Return True if the plain password matches the bcrypt hash."""
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())


def get_password_hash(password: str) -> str:
    """Return a bcrypt hash of the password for storage."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Build a JWT with the given payload and optional expiration. Uses db_settings for secret and algorithm."""
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta or timedelta(
            minutes=db_settings.access_token_expire_minutes)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, db_settings.secret_key, algorithm=db_settings.algorithm)


def decode_token(token: str) -> Optional[dict]:
    """Decode and verify a JWT. Returns the payload dict or None if invalid or expired."""
    try:
        payload = jwt.decode(token, db_settings.secret_key,
                             algorithms=[db_settings.algorithm])
        return payload
    except JWTError:
        return None


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    """FastAPI dependency: extract Bearer token and return JWT payload. Raises 401 if missing or invalid."""
    token = credentials.credentials
    payload = decode_token(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )
    return payload


def require_roles(allowed_roles: List[str]):
    """Return a FastAPI dependency that allows only the given roles. Raises 403 if the user's role is not in the list."""

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
