from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic_settings import BaseSettings
import os


class DatabaseSettings(BaseSettings):
    database_url: str = "postgresql://hr_insights:hr_insights@localhost:5432/hr_insights"
    echo: bool = False


db_settings = DatabaseSettings()

database_url = os.environ.get("DATABASE_URL", db_settings.database_url)

engine = create_engine(
    database_url,
    echo=db_settings.echo,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database tables."""

    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()
    print("Database initialized successfully.")