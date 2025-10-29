# bot/db_writer.py
# ------------------------------------------------------------
# Postgres writer with auto-migrate:
# - Tries psycopg (v3), then psycopg2 (v2), else falls back to NoOp.
# - Auto-creates tables equity_curve & trades if missing.
# - API:
#     DB(os.getenv("DATABASE_URL"))
#     db.write_equity({"time": iso8601/datetime, "equity": float})
#     db.write_trades([[time, connector, symbol, type, side, price, qty, pnl, equity], ...])
# ------------------------------------------------------------

from __future__ import annotations
import os
from typing import Iterable, Any, Optional

DDL_EQUITY = """
CREATE TABLE IF NOT EXISTS equity_curve (
    time   timestamptz PRIMARY KEY,
    equity numeric NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_equity_curve_time ON equity_curve(time);
"""

DDL_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    id        bigserial PRIMARY KEY,
    time      timestamptz NOT NULL,
    connector text NOT NULL,
    symbol    text NOT NULL,
    type      text NOT NULL,
    side      text NOT NULL,
    price     numeric,
    qty       numeric,
    pnl       numeric,
    equity    numeric
);
CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(time);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_connector ON trades(connector);
"""


# -------------------------- Backends --------------------------

class _NoOpDB:
    def __init__(self, err: str = "No DATABASE_URL / driver missing"):
        print(f"[DB] Disabled (CSV-only). Reason: {err}")

    def write_equity(self, row: dict) -> None:
        pass

    def write_trades(self, rows: Iterable[Iterable[Any]]) -> None:
        pass

    def close(self) -> None:
        pass


class _PsycopgV3DB:
    """psycopg (v3) backend"""
    def __init__(self, dsn: str):
        import psycopg
        from psycopg.rows import tuple_row

        self.psycopg = psycopg
        self.conn = psycopg.connect(dsn, autocommit=True, row_factory=tuple_row)
        print("[INFO] Connected to Postgres (psycopg v3).")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(DDL_EQUITY)
                cur.execute(DDL_TRADES)
            print("[INFO] Ensured DB schema (tables exist).")
        except Exception as e:
            print(f"[WARN] Ensuring schema failed: {e}")

    def write_equity(self, row: dict) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO equity_curve (time, equity) VALUES (%s, %s) "
                    "ON CONFLICT (time) DO UPDATE SET equity = EXCLUDED.equity;",
                    (row["time"], row["equity"])
                )
        except Exception as e:
            print(f"[WARN] DB write_equity failed: {e}")

    def write_trades(self, rows: Iterable[Iterable[Any]]) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO trades (time, connector, symbol, type, side, price, qty, pnl, equity) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                    rows
                )
        except Exception as e:
            print(f"[WARN] DB write_trades failed: {e}")

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


class _Psycopg2DB:
    """psycopg2 (v2) backend"""
    def __init__(self, dsn: str):
        import psycopg2
        self.psycopg2 = psycopg2
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True
        print("[INFO] Connected to Postgres (psycopg2 v2).")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(DDL_EQUITY)
                cur.execute(DDL_TRADES)
            print("[INFO] Ensured DB schema (tables exist).")
        except Exception as e:
            print(f"[WARN] Ensuring schema failed: {e}")

    def write_equity(self, row: dict) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO equity_curve (time, equity) VALUES (%s, %s) "
                    "ON CONFLICT (time) DO UPDATE SET equity = EXCLUDED.equity;",
                    (row["time"], row["equity"])
                )
        except Exception as e:
            print(f"[WARN] DB write_equity failed: {e}")

    def write_trades(self, rows: Iterable[Iterable[Any]]) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO trades (time, connector, symbol, type, side, price, qty, pnl, equity) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                    rows
                )
        except Exception as e:
            print(f"[WARN] DB write_trades failed: {e}")

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


# -------------------------- Factory --------------------------

class DB:
    """
    Usage:
        db = DB(os.getenv("DATABASE_URL"))
        db.write_equity({"time": "...", "equity": 123.45})
        db.write_trades([...])
    """
    def __new__(cls, dsn: Optional[str]):
        if not dsn:
            return _NoOpDB(err="No DATABASE_URL set")

        # Try psycopg v3
        try:
            import psycopg  # noqa: F401
            return _PsycopgV3DB(dsn)
        except Exception as e_v3:
            # Try psycopg2
            try:
                import psycopg2  # noqa: F401
                return _Psycopg2DB(dsn)
            except Exception as e_v2:
                return _NoOpDB(err=f"psycopg error: {e_v3}; psycopg2 error: {e_v2}")
