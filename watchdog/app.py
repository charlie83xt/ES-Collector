import os
import time
from datetime import datetime, timezone, timedelta

import requests
from ib_insync import util

from db import get_conn, get_latest_ts, upsert_many
from ibkr import connect_ib, make_es_contfut

def utc_now():
    return datetime.now(timezone.utc)

def alert(webhook_url: str | None, msg: str):
    print(f"[watchdog] {msg}")
    if not webhook_url:
        return
    try:
        requests.post(webhook_url, json={"content": msg}, timeout=10)
    except Exception as e:
        print(f"[watchdog] alert failed: {e}")

def backfill_1m(ib, contract, end_dt: datetime, minutes: int):
    """
    Pull last N minutes of 1-minute bars ending at end_dt (UTC), return list of bars oldest->newest.
    """
    # IB uses a formatted endDateTime string
    end_str = end_dt.strftime("%Y%m%d %H:%M:%S")
    duration = f"{minutes} M"
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=end_str,
        durationStr=duration,
        barSizeSetting="1 min",
        whatToShow="TRADES",
        useRTH=False,
        formatDate=1,
        keepUpToDate=False,
    )
    return bars or []

def main():
    symbol = os.getenv("SYMBOL", "ES")
    exchange = os.getenv("EXCHANGE", "CME")
    db_symbol = os.getenv("DB_SYMBOL", "ES_CONTFUT")
    source = os.getenv("SOURCE", "ibkr")

    max_gap = int(os.getenv("MAX_GAP_MINUTES", "10"))
    backfill_minutes = int(os.getenv("BACKFILL_MINUTES", "180"))
    interval = int(os.getenv("WATCHDOG_INTERVAL_SECONDS", "120"))
    webhook = os.getenv("ALERT_WEBHOOK_URL")

    conn = get_conn()

    while True:
        try:
            latest = get_latest_ts(conn, db_symbol)
            now = utc_now()

            if latest is None:
                alert(webhook, f"[watchdog] No bars yet for {db_symbol}. Will backfill {backfill_minutes} minutes.")
                need_backfill = True
            else:
                gap_min = (now - latest).total_seconds() / 60.0
                need_backfill = gap_min > max_gap
                if need_backfill:
                    alert(webhook, f"[watchdog] Gap detected for {db_symbol}: {gap_min:.1f} minutes. Backfilling...")

            if need_backfill:
                ib = connect_ib(client_id=202)
                contract = make_es_contfut(symbol, exchange)
                ib.qualifyContracts(contract)

                bars = backfill_1m(ib, contract, end_dt=now, minutes=backfill_minutes)
                rows = []
                for b in bars:
                    # b.date is either str or datetime depending on formatDate; ib_insync usually gives datetime
                    ts = b.date
                    if isinstance(ts, str):
                        # very defensive; should rarely happen
                        ts = datetime.strptime(ts, "%Y%m%d  %H:%M:%S").replace(tzinfo=timezone.utc)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    else:
                        ts = ts.astimezone(timezone.utc)

                    rows.append({
                        "symbol": db_symbol,
                        "ts": ts.replace(second=0, microsecond=0),
                        "open": float(b.open),
                        "high": float(b.high),
                        "low": float(b.low),
                        "close": float(b.close),
                        "volume": float(getattr(b, "volume", 0.0) or 0.0),
                        "source": source,
                    })

                upsert_many(conn, rows)
                alert(webhook, f"[watchdog] Backfill complete: upserted {len(rows)} bars for {db_symbol}.")
                ib.disconnect()

        except Exception as e:
            alert(webhook, f"[watchdog] error: {e}")

        time.sleep(interval)

if __name__ == "__main__":
    util.patchAsyncio()
    main()
