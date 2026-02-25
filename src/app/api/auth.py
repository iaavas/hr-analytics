
from datetime import timedelta

from fastapi import APIRouter, HTTPException, status

from src.app.core.roles import ROLE_VIEWER
from src.app.core.security import create_access_token, verify_password
from src.app.core.user_store import get_user_by_username
from src.app.database import db_settings
from src.app.schemas.auth_schema import LoginRequest, TokenResponse
from src.app.schemas.response_schema import ApiResponse

router = APIRouter(tags=["auth"])


@router.post("/token", response_model=ApiResponse)
def login(login_data: LoginRequest):
    """Authenticate with username and password. Returns a JWT access token for use in the Authorization header."""
    user = get_user_by_username(login_data.username)
    if not user or not verify_password(login_data.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )
    role = user.get("role", ROLE_VIEWER)
    access_token = create_access_token(
        data={"sub": login_data.username, "role": role},
        expires_delta=timedelta(
            minutes=db_settings.access_token_expire_minutes),
    )
    return ApiResponse.ok(
        data=TokenResponse(access_token=access_token).model_dump(),
        message="Authentication successful",
    )
