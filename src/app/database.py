"""Database engine, session factory, and init. DATABASE_URL and JWT settings from .env / environment."""

from pydantic_settings import BaseSettings
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base


class DatabaseSettings(BaseSettings):
    database_url: str = "postgresql://hr_insights:hr_insights@localhost:5432/hr_insights"
    echo: bool = False

    secret_key: str = "your-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30

    class Config:
        env_file = ".env"
        extra = "ignore"


db_settings = DatabaseSettings()

engine = create_engine(db_settings.database_url, echo=db_settings.echo)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    """FastAPI dependency that yields a DB session and closes it after the request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create schemas and all tables."""

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        conn.commit()

    from src.db.models.bronze import EmployeeBronze, TimesheetBronze
    from src.db.models.silver import (
        Department,
        Organization,
        Employee,
        Timesheet,
    )
    from src.db.models.gold import (
        EmployeeMonthlySnapshot,
        TimesheetDailySummary,
        DepartmentMonthlyMetrics,
        EmployeeAttendanceMetrics,
        HeadcountTrend,
        OrganizationMetrics,
    )

    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    init_db()
    print("Database initialized successfully.")
