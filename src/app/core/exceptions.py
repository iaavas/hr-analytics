"""Application exceptions. NotFoundError and ServiceError are handled globally in main.py and returned as JSON."""


class AppException(Exception):
    """Base exception for the API. Subclasses set a message used in error responses."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class NotFoundError(AppException):
    """Raised when a requested resource (e.g. employee by id) does not exist. Mapped to HTTP 404."""

    def __init__(self, resource: str, identifier: str):
        self.resource = resource
        self.identifier = identifier
        message = f"{resource} '{identifier}' not found."
        super().__init__(message)


class ServiceError(AppException):
    """Raised on database or internal failures. Mapped to HTTP 500."""
    pass
