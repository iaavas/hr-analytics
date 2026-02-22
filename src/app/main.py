from datetime import timedelta

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from src.app.database import get_db, db_settings
from src.app.core.security import (
    create_access_token,
    verify_password,
    get_password_hash,
)
from src.app.schemas.employee_schema import LoginRequest, TokenResponse
from src.app.api import employee, timesheet

app = FastAPI(
    title="HR Insights API",
    description="HR Insights API with Silver layer data",
    version="1.0.0",
)

app.include_router(employee.router)
app.include_router(timesheet.router)

security = HTTPBearer()

MOCK_USERS = {
    "admin": {
        "username": "admin",
        "password_hash": "$2b$12$p.7gehGxmdSa394iZ63p2u/IuPaJmvGEhuhddEDjEiCbsk1lCAOXm",
    }
}


@app.post("/token", response_model=TokenResponse)
def login(login_data: LoginRequest, db: Session = Depends(get_db)):
    user = MOCK_USERS.get(login_data.username)
    if not user or not verify_password(login_data.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )

    access_token = create_access_token(
        data={"sub": login_data.username},
        expires_delta=timedelta(minutes=db_settings.access_token_expire_minutes),
    )
    return TokenResponse(access_token=access_token)


@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.app.main:app", host="0.0.0.0", port=8000, reload=True)
