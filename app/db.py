from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse
# from app.config import DATABASE_URL

encoded_username = urllib.parse.quote("core_payment_user")
encoded_password = urllib.parse.quote("HkdjHjijowejT@43q2")
encoded_dbname = urllib.parse.quote("core_payment_db")

host = "52.207.245.55"
port = 5432

DATABASE_URL = f"postgresql://{encoded_username}:{encoded_password}@{host}:{port}/{encoded_dbname}"

# if not DATABASE_URL:
#     raise RuntimeError("DATABASE_URL environment variable is required")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)