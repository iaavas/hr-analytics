from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from src.app.core.config import api_settings
from src.app.core.logging_config import setup_api_logging
from src.app.core.exceptions import NotFoundError, ServiceError
from src.app.schemas.response_schema import ApiResponse
from src.app.api import auth, employee, timesheet, analytics

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
    """Turn HTTPException detail (str, list, or dict) into a single string for the API response."""
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


app.include_router(auth.router)
app.include_router(employee.router)
app.include_router(timesheet.router)
app.include_router(analytics.router)


@app.get("/health", response_model=ApiResponse)
def health_check():
    """Return service health status. No authentication required."""
    return ApiResponse.ok(data={"status": "healthy"}, message="Service is running and healthy.")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.app.main:app",
        host=api_settings.api_host,
        port=api_settings.api_port,
        reload=api_settings.reload,
    )
