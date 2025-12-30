import json
from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import DB_URL


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    json_serializer=lambda obj: json.dumps(obj, default=_json_default),
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
