"""Watchdog: finds gaps in market_bars_1m and refills them from IB history.

Fixes compared with the first version:
  * uses the container's own IB_CLIENT_ID (all watchdogs used 202 -> error 326)
  * pages back one day at a time on the real front-month contract, so gaps longer than
    BACKFILL_MINUTES are refilled (an end date on a continuous future gave error 10339)
  * always disconnects, and reports missing market-data permissions clearly
  * at most one alert per ALERT_EVERY_MINUTES for the same problem
"""
import os
import time
from datetime import datetime, timezone, timedelta

import requests
from ib_insync import util

from db import get_conn, get_latest_ts, upsert_many
from ibkr import connect_ib, front_month

PERMISSION_CODES = {354, 420, 10089, 10090, 10167, 10168}
# Four watchdogs share IB's limit of about 60 history requests per 10 minutes, so pause between requests.
PAUSE_SECONDS = int(os.getenv("BACKFILL_PAUSE_SECONDS", "40"))
_last_alert: dict[str, float] = {}


def utc_now():
    return datetime.now(timezone.utc)


def alert(webhook_url: str | None, msg: str, key: str | None = None, every_min: int = 60):
    print(f"[watchdog] {msg}", flush=True)
    if not webhook_url:
        return
    k = key or msg
    if time.time() - _last_alert.get(k, 0) < every_min * 60:
        return
    _last_alert[k] = time.time()
    try:
        requests.post(webhook_url, json={"content": msg, "text": msg}, timeout=10)
    except Exception as e:
        print(f"[watchdog] alert failed: {e}", flush=True)


def to_utc(ts) -> datetime:
    if isinstance(ts, str):
        ts = datetime.strptime(ts[:17], "%Y%m%d %H:%M:%S")
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).replace(second=0, microsecond=0)


def fetch_day(ib, contract, end_dt: datetime):
    """One day of 1-minute bars ending at end_dt (UTC), oldest first."""
    return ib.reqHistoricalData(
        contract,
        endDateTime=end_dt,          # tz-aware datetime; ib_insync sends it as UTC
        durationStr="1 D",
        barSizeSetting="1 min",
        whatToShow="TRADES",
        useRTH=False,
        formatDate=2,                # bar times as UTC datetimes
        keepUpToDate=False,
    ) or []


def backfill(ib, contract, since: datetime, until: datetime, db_symbol: str, source: str, conn,
             max_days: int, log, stop=lambda: False, paced=lambda: False) -> int:
    """Pages backwards a day at a time from `until` to `since`; returns rows written."""
    total = 0
    end = until
    oldest_allowed = until - timedelta(days=max_days)
    while end > since and end > oldest_allowed:
        bars = fetch_day(ib, contract, end)
        if stop():
            break
        if paced():  # IB said "pacing violation": wait, then ask for the same day again
            log("IB pacing limit hit; waiting 10 minutes")
            time.sleep(600)
            continue
        rows = []
        for b in bars:
            ts = to_utc(b.date)
            if ts <= since:
                continue
            rows.append({
                "symbol": db_symbol, "ts": ts,
                "open": float(b.open), "high": float(b.high), "low": float(b.low), "close": float(b.close),
                "volume": float(getattr(b, "volume", 0.0) or 0.0), "source": source,
            })
        upsert_many(conn, rows)
        total += len(rows)
        log(f"backfill {db_symbol}: {len(rows)} bars up to {end:%Y-%m-%d %H:%M} UTC")
        # step to just before the earliest bar received (or one day back on weekends/holidays)
        earliest = to_utc(bars[0].date) if bars else end - timedelta(days=1)
        end = min(earliest - timedelta(minutes=1), end - timedelta(hours=1))
        time.sleep(PAUSE_SECONDS)
    return total


def main():
    symbol = os.getenv("SYMBOL", "ES")
    exchange = os.getenv("EXCHANGE", "CME")
    db_symbol = os.getenv("DB_SYMBOL", "ES_CONTFUT")
    source = os.getenv("SOURCE", "ibkr")

    max_gap = int(os.getenv("MAX_GAP_MINUTES", "10"))
    max_days = int(os.getenv("MAX_BACKFILL_DAYS", "45"))
    interval = int(os.getenv("WATCHDOG_INTERVAL_SECONDS", "120"))
    webhook = os.getenv("ALERT_WEBHOOK_URL")

    conn = get_conn()

    while True:
        ib = None
        permission_error = []
        try:
            latest = get_latest_ts(conn, db_symbol)
            now = utc_now()
            gap_min = None if latest is None else (now - latest).total_seconds() / 60.0

            if latest is None or gap_min > max_gap:
                ib = connect_ib()
                ib.RequestTimeout = 90  # never wait forever on a history request

                pacing = []

                def on_error(req_id, code, msg, contract):
                    if code in PERMISSION_CODES or (code == 162 and "permission" in str(msg).lower()):
                        permission_error.append(f"{code}: {msg}")
                    elif code == 162 and "pacing" in str(msg).lower():
                        pacing.append(msg)

                def paced():
                    hit = bool(pacing)
                    pacing.clear()
                    return hit
                ib.errorEvent += on_error

                contract = front_month(ib, symbol, exchange)
                since = latest or (now - timedelta(days=max_days))
                print(f"[watchdog] gap for {db_symbol}: {gap_min if gap_min is not None else 'no data'} min; "
                      f"refilling from {since:%Y-%m-%d %H:%M} UTC with {contract.localSymbol}", flush=True)
                n = backfill(ib, contract, since, now, db_symbol, source, conn, max_days,
                             lambda m: print(f"[watchdog] {m}", flush=True), stop=lambda: bool(permission_error),
                             paced=paced)

                if permission_error:
                    alert(webhook, f"{db_symbol}: IBKR has no market-data permission for this contract "
                                   f"({permission_error[0]}). Renew the CME market-data subscription.",
                          key="permission")
                elif n:
                    alert(webhook, f"{db_symbol}: refilled {n} bars after a {gap_min or 0:.0f}-minute gap.",
                          key="refill", every_min=0)
                else:
                    alert(webhook, f"{db_symbol}: gap of {gap_min or 0:.0f} minutes and IB returned no bars.",
                          key="nobars")
        except Exception as e:
            alert(webhook, f"{db_symbol} watchdog error: {e!r}", key="error")
        finally:
            if ib is not None:
                try:
                    ib.disconnect()
                except Exception:
                    pass

        time.sleep(interval)


if __name__ == "__main__":
    util.patchAsyncio()
    main()
