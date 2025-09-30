from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from .config import DATABASE_URL

def get_engine(url: str | None = None) -> Engine:
    return create_engine(url or DATABASE_URL, pool_pre_ping=True, future=True)

def fetch_scalar(engine: Engine, sql: str, params: dict):
    with engine.connect() as conn:
        res = conn.execute(text(sql), params).scalar()
    return res

def fetch_one(engine: Engine, sql: str, params: dict):
    with engine.connect() as conn:
        res = conn.execute(text(sql), params).mappings().first()
    return res

def fetch_all(engine: Engine, sql: str, params: dict):
    with engine.connect() as conn:
        res = conn.execute(text(sql), params).mappings().all()
    return res

def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()
