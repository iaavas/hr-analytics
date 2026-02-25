"""In-memory user store for authentication. Replace with DB lookup in production."""

from typing import Any, Optional

from src.app.core.roles import ROLE_ADMIN, ROLE_VIEWER

_DEFAULT_PASSWORD_HASH = "$2b$12$p.7gehGxmdSa394iZ63p2u/IuPaJmvGEhuhddEDjEiCbsk1lCAOXm"

_USERS: dict[str, dict[str, Any]] = {
    "admin": {
        "username": "admin",
        "password_hash": _DEFAULT_PASSWORD_HASH,
        "role": ROLE_ADMIN,
    },
    "viewer": {
        "username": "viewer",
        "password_hash": _DEFAULT_PASSWORD_HASH,
        "role": ROLE_VIEWER,
    },
}


def get_user_by_username(username: str) -> Optional[dict[str, Any]]:
    return _USERS.get(username)
