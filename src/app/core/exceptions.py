class AppException(Exception):

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class NotFoundError(AppException):

    def __init__(self, resource: str, identifier: str):
        self.resource = resource
        self.identifier = identifier
        message = f"{resource} '{identifier}' not found."
        super().__init__(message)


class ServiceError(AppException):

    pass
