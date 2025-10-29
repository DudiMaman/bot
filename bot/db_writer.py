# bot/db_writer.py
import os
from datetime import datetime, timezone

"""
Fail-safe DB writer:
- Tries psycopg (v3) first (works with Python 3.13).
- If not available, tries psycopg2.
- If neither works, falls back to a No-Op writer so the bot won't crash.
"""

class _NoOpDB:
    def __init__(self, *args, **kwargs):
        err = kwargs.get("err")
        print(f"[DB] Disabled (CSV-only). Reason: {err}")

    def write_trade(self, *args, **kwargs):
        pass

    def write_equity(self, *args, **kwargs):
        pass

    def close(self):
        pass


def _make_psycopg_db(conn_str):
    import psycopg  # v3
    class _DB:
        def __init__(self, dsn):
            self.conn = psycopg.connect(dsn)
            self.conn.autocommit = True

        def write_trade(self, t):
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    insert into trades
                      (time, connector, symbol, type, side, price, qty, pnl, equity)
                    values
                      (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        t["time"], t["connector"], t["symbol"], t["type"],
                        t["side"], t["price"], t["qty"], t["pnl"], t["equity"]
                    )
                )

        def write_equity(self, e):
            with self.conn.cursor() as cur:
                cur.execute(
                    "insert into equity_curve (time, equity) values (%s, %s)",
                    (e["time"], e["equity"])
                )

        def close(self):
            try:
                self.conn.close()
            except Exception:
                pass

    return _DB(conn_str)


def _make_psycopg2_db(conn_str):
    import psycopg2  # v2
    class _DB:
        def __init__(self, dsn):
            self.conn = psycopg2.connect(dsn)
            self.conn.autocommit = True

        def write_trade(self, t):
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    insert into trades
                      (time, connector, symbol, type, side, price, qty, pnl, equity)
                    values
                      (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        t["time"], t["connector"], t["symbol"], t["type"],
                        t["side"], t["price"], t["qty"], t["pnl"], t["equity"]
                    )
                )

        def write_equity(self, e):
            with self.conn.cursor() as cur:
                cur.execute(
                    "insert into equity_curve (time, equity) values (%s, %s)",
                    (e["time"], e["equity"])
                )

        def close(self):
            try:
                self.conn.close()
            except Exception:
                pass

    return _DB(conn_str)


class DB:
    """
    Factory wrapper: tries psycopg (v3), then psycopg2 (v2), otherwise degrades to NoOp.
    Usage: DB(os.getenv('DATABASE_URL'))
    """
    def __new__(cls, dsn: str | None):
        if not dsn:
            return _NoOpDB(err="No DATABASE_URL set")

        try:
            return _make_psycopg_db(dsn)
        except Exception as e_psycopg:
            try:
                return _make_psycopg2_db(dsn)
            except Exception as e_psycopg2:
                return _NoOpDB(err=f"psycopg error: {e_psycopg}; psycopg2 error: {e_psycopg2}")
