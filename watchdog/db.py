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
    conn = psycopg.connect(url, autocommit=True, row_factory=dict_row)
    return conn

def get_latest_ts(conn, symbol: str):
    sql = """
    select ts
    from market_bars_1m
    where symbol = %s
    order by ts desc
    limit 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol,))
        row = cur.fetchone()
        return row["ts"] if row else None

def upsert_many(conn, rows: list[dict]):
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
