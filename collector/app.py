import os
import time
from datetime import datetime, timezone

import requests
from ib_insync import util

from db import get_conn, upsert_many
from ibkr import connect_ib, make_es_contfut
from agg import MinuteAggregator

# IB error codes meaning "your account may not receive this data"
PERMISSION_CODES = {354, 420, 10089, 10090, 10167, 10168}
STALL_SECONDS = int(os.getenv("STALL_SECONDS", "600"))           # no bars this long while the market is open -> reconnect
PERMISSION_RETRY_SECONDS = int(os.getenv("PERMISSION_RETRY_SECONDS", "900"))
_last_alert: dict[str, float] = {}


def utc_now():
    return datetime.now(timezone.utc)


def alert(msg: str, key: str, every_min: int = 60):
    print(f"[collector] {msg}", flush=True)
    url = os.getenv("ALERT_WEBHOOK_URL")
    if not url or time.time() - _last_alert.get(key, 0) < every_min * 60:
        return
    _last_alert[key] = time.time()
    try:
        requests.post(url, json={"content": msg, "text": msg}, timeout=10)
    except Exception as e:
        print(f"[collector] alert failed: {e}", flush=True)


def globex_open(now: datetime) -> bool:
    """CME Globex (ES/MES/NQ/MNQ): Sun 18:00 - Fri 17:00 New York time, paused 17:00-18:00 daily."""
    from zoneinfo import ZoneInfo
    t = now.astimezone(ZoneInfo("America/New_York"))
    m = t.hour * 60 + t.minute
    wd = t.weekday()  # Mon=0 .. Sun=6
    if wd == 5:
        return False
    if wd == 6:
        return m >= 18 * 60
    if wd == 4:
        return m < 17 * 60
    return m < 17 * 60 or m >= 18 * 60


def main():
    symbol = os.getenv("SYMBOL", "ES")
    exchange = os.getenv("EXCHANGE", "CME")
    db_symbol = os.getenv("DB_SYMBOL", "ES_CONTFUT")
    source = os.getenv("SOURCE", "ibkr")

    conn = get_conn()

    while True:
        ib = None
        wait = 10
        try:
            ib = connect_ib()
            permission_error = []

            def on_error(req_id, code, msg, contract):
                if code in PERMISSION_CODES:
                    permission_error.append(f"{code}: {msg}")
            ib.errorEvent += on_error

            contract = make_es_contfut(symbol, exchange)
            ib.qualifyContracts(contract)

            agg = MinuteAggregator()
            pending_rows = []
            last_bar_at = time.time()

            # IB realtime bars are 5-second. We aggregate into 1-minute.
            bars = ib.reqRealTimeBars(contract, 5, "TRADES", False)

            def on_bar_update(_bars, has_new_bar: bool):
                nonlocal last_bar_at
                if not has_new_bar or not _bars:
                    return
                last_bar_at = time.time()
                b5 = _bars[-1]
                emitted = agg.update_from_5s(
                    ts=b5.time,
                    o=float(b5.open_),
                    h=float(b5.high),
                    l=float(b5.low),
                    c=float(b5.close),
                    v=float(getattr(b5, "volume", 0.0) or 0.0),
                )
                if emitted:
                    pending_rows.append({
                        "symbol": db_symbol,
                        "ts": emitted.ts,
                        "open": emitted.open,
                        "high": emitted.high,
                        "low": emitted.low,
                        "close": emitted.close,
                        "volume": emitted.volume,
                        "source": source,
                    })

            bars.updateEvent += on_bar_update
            print(f"[collector] connected, streaming realtime bars for {contract.localSymbol}...", flush=True)

            while ib.isConnected():
                ib.waitOnUpdate(timeout=5)

                if pending_rows:
                    upsert_many(conn, pending_rows)
                    print(f"[collector] upserted {len(pending_rows)} bars; latest={pending_rows[-1]['ts'].isoformat()}", flush=True)
                    pending_rows.clear()

                if permission_error:
                    alert(f"{db_symbol}: IBKR has no market-data permission ({permission_error[0]}). "
                          f"Renew the CME market-data subscription. Retrying in {PERMISSION_RETRY_SECONDS // 60} min.",
                          key="permission")
                    wait = PERMISSION_RETRY_SECONDS
                    break

                if globex_open(utc_now()) and time.time() - last_bar_at > STALL_SECONDS:
                    alert(f"{db_symbol}: no bars for {STALL_SECONDS // 60} min while the market is open; reconnecting.",
                          key="stall")
                    break
                if not globex_open(utc_now()):
                    last_bar_at = time.time()  # quiet market hours are not a stall

        except Exception as e:
            print(f"[collector] error: {e!r}", flush=True)
        finally:
            if ib is not None:
                try:
                    ib.disconnect()
                except Exception:
                    pass

        print(f"[collector] reconnecting in {wait}s...", flush=True)
        time.sleep(wait)


if __name__ == "__main__":
    util.patchAsyncio()
    main()
