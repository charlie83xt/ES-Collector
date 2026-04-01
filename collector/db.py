import os
import psycopg
from psycopg.rows import dict_row

UPSERT_SQL = """
insert into market_bars_1m
(symbol, ts, open, high, low, close, volume, source)
values (%(symbol)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(source)s)
on conflict (symbol, ts) do update set
  open = excluded.open,
  high = excluded.high,
  low  = excluded.low,
  close= excluded.close,
  volume = excluded.volume,
  source = excluded.source,
  ingest_ts = now();
"""

def get_conn():
    url = os.environ["DATABASE_URL"]
    # For Supabase transaction pooler, prepared statements can be problematic;
    # we are using simple statements + autocommit.
    conn = psycopg.connect(url, autocommit=True, row_factory=dict_row, prepare_threshold=0)
    return conn

def upsert_many(conn, rows: list[dict]):
    if not rows:
        return
    # Ensure always a list[dict]
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise TypeError(f"upsert_many expects list[dict], got {type(rows)}: {rows!r}")
    with conn.cursor() as cur:
        for r in rows:
            # prepare=False prevents server-side prepared statements entirely
            cur.execute(UPSERT_SQL, r, prepare=False)
