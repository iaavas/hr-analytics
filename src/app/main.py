from datetime import timedelta

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from src.app.core.config import api_settings
from src.app.core.logging_config import setup_api_logging
from src.app.database import get_db, db_settings
from src.app.core.security import (
    create_access_token,
    verify_password,
    get_password_hash,
)
from src.app.core.exceptions import NotFoundError, ServiceError
from src.app.schemas.employee_schema import LoginRequest, TokenResponse
from src.app.schemas.response_schema import ApiResponse
from src.app.api import employee, timesheet, analytics

setup_api_logging()

app = FastAPI(
    title="HR Insights API",
    description="HR Insights API with Silver layer data. All responses use the format: `success`, `data` (optional), `message`, and `error` (only on failure).",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


def _normalize_error_detail(detail) -> str:
    if detail is None:
        return "An error occurred."
    if isinstance(detail, str):
        return detail
    if isinstance(detail, list):
        parts = []
        for item in detail:
            if isinstance(item, dict) and "msg" in item:
                parts.append(item.get("msg", str(item)))
            else:
                parts.append(str(item))
        return " ".join(parts) if parts else "Validation error."
    if isinstance(detail, dict):
        return detail.get("message", detail.get("detail", str(detail)))
    return str(detail)


@app.exception_handler(HTTPException)
def http_exception_handler(request, exc: HTTPException):
    error_message = _normalize_error_detail(exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content=ApiResponse.fail(
            message=error_message, error=error_message
        ).model_dump(exclude_none=True),
    )


@app.exception_handler(NotFoundError)
def not_found_handler(request, exc: NotFoundError):
    return JSONResponse(
        status_code=404,
        content=ApiResponse.fail(message=exc.message, error=exc.message).model_dump(
            exclude_none=True
        ),
    )


@app.exception_handler(ServiceError)
def service_error_handler(request, exc: ServiceError):
    return JSONResponse(
        status_code=500,
        content=ApiResponse.fail(message=exc.message, error=exc.message).model_dump(
            exclude_none=True
        ),
    )


app.include_router(employee.router)
app.include_router(timesheet.router)
app.include_router(analytics.router)

security = HTTPBearer()

MOCK_USERS = {
    "admin": {
        "username": "admin",
        "password_hash": "$2b$12$p.7gehGxmdSa394iZ63p2u/IuPaJmvGEhuhddEDjEiCbsk1lCAOXm",
    }
}


@app.post("/token", response_model=ApiResponse)
def login(login_data: LoginRequest, db: Session = Depends(get_db)):
    user = MOCK_USERS.get(login_data.username)
    if not user or not verify_password(login_data.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )

    access_token = create_access_token(
        data={"sub": login_data.username},
        expires_delta=timedelta(
            minutes=db_settings.access_token_expire_minutes),
    )
    return ApiResponse.ok(
        data=TokenResponse(access_token=access_token).model_dump(),
        message="Authentication successful",
    )


@app.get("/health", response_model=ApiResponse)
def health_check():
    return ApiResponse.ok(data={"status": "healthy"}, message="Service is running and healthy.")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.app.main:app",
        host=api_settings.api_host,
        port=api_settings.api_port,
        reload=api_settings.reload,
    )
