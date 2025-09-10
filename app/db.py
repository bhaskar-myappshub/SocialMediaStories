import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import DATABASE_URL

# Create engine and session factory
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

# pool_pre_ping helps with Lambda cold-start re-use
engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def get_session():
    """Yields a new SQLAlchemy session. Use manually in routes: s = get_session(); try: ... finally: s.close()"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
