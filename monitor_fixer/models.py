import os
from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Text, Float, JSON, TIMESTAMP, func
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")  # נכניס ב-Render
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Log(Base):
    __tablename__ = "logs"
    id = Column(BigInteger, primary_key=True)
    ts = Column(TIMESTAMP, server_default=func.now())
    level = Column(String(16))          # INFO|WARN|ERROR
    source = Column(String(32))         # bot|monitor|fixer
    event_type = Column(String(32))     # HEARTBEAT|TRADE|ERROR|SYSTEM
    payload = Column(JSON)

class Fix(Base):
    __tablename__ = "fixes"
    id = Column(BigInteger, primary_key=True)
    ts = Column(TIMESTAMP, server_default=func.now())
    issue_type = Column(String(64))     # BadSymbol|IndentationError|ConfigKeyAlias|...
    summary = Column(Text)
    repo_ref = Column(Text)             # commit sha / PR URL
    diff = Column(Text)

class Status(Base):
    __tablename__ = "bot_status"
    id = Column(Integer, primary_key=True)
    status = Column(String(16))         # running|paused|stopped
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
