import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    # fallback to sqlite for local development
    "sqlite:///./recommender.db",
)

# echo=False to avoid noisy logs in production
engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(
    bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


def init_db():
    """Crea las tablas declaradas por los modelos si no existen."""
    from sqlalchemy import inspect

    inspector = inspect(engine)
    # Import models so they are registered on the Base metadata
    try:
        # local import to avoid circular imports
        import recommender_system.models  # noqa: F401
    except Exception:
        pass

    Base.metadata.create_all(bind=engine)
