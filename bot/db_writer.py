# bot/db_writer.py
import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

class DB:
    def __init__(self):
        self.url = os.environ.get("DATABASE_URL")
        if not self.url:
            raise RuntimeError("DATABASE_URL env var is missing")
        self._ensure_schema()

    def _get_conn(self):
        return psycopg2.connect(self.url)

    def _ensure_schema(self):
        ddl_trades = """
        CREATE TABLE IF NOT EXISTS trades (
            time        TIMESTAMP NOT NULL,
            connector   TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            type        TEXT NOT NULL,   -- ENTER/TP1/TP2/SL/TIME
            side        TEXT NOT NULL,   -- long/short
            price       DOUBLE PRECISION,
            qty         DOUBLE PRECISION,
            pnl         DOUBLE PRECISION,
            equity      DOUBLE PRECISION
        );
        """
        ddl_equity = """
        CREATE TABLE IF NOT EXISTS equity_curve (
            time   TIMESTAMP NOT NULL,
            equity DOUBLE PRECISION NOT NULL
        );
        """
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute(ddl_trades)
            cur.execute(ddl_equity)
            conn.commit()

    # rows_trades: list of rows like we already write to CSV
    # ["time","connector","symbol","type","side","price","qty","pnl","equity"]
    def write_trades(self, rows_trades):
        if not rows_trades:
            return
        ins = """
        INSERT INTO trades(time, connector, symbol, type, side, price, qty, pnl, equity)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.executemany(ins, rows_trades)
            conn.commit()

    # equity_point: (iso_time, equity_string)
    def write_equity(self, equity_point):
        t, eq = equity_point
        ins = "INSERT INTO equity_curve(time, equity) VALUES (%s,%s)"
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute(ins, (t, float(eq)))
            conn.commit()
