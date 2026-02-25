"""Standard API response envelope: success, data, message, and optional error. Used by all endpoints."""

from typing import Any, Optional

from pydantic import BaseModel, model_serializer


class ApiResponse(BaseModel):
    """All API responses use this shape. success and message always present; data and error as needed."""
    success: bool
    data: Optional[Any] = None
    message: str = ""
    error: Optional[str] = None

    @model_serializer(mode="plain")
    def _serialize_exclude_none(self) -> dict:
        out: dict = {"success": self.success, "message": self.message}
        if self.data is not None:
            out["data"] = self.data
        if self.error is not None:
            out["error"] = self.error
        return out

    @classmethod
    def ok(cls, data: Any = None, message: str = "Success") -> "ApiResponse":
        return cls(success=True, data=data, message=message, error=None)

    @classmethod
    def fail(cls, message: str, data: Any = None, error: Optional[str] = None) -> "ApiResponse":
        err = error if error is not None else message
        return cls(success=False, data=data, message=message, error=err)
