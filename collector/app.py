import os
import time
from datetime import datetime, timezone

from ib_insync import util

from db import get_conn, upsert_many
from ibkr import connect_ib, make_es_contfut
from agg import MinuteAggregator

def utc_now():
    return datetime.now(timezone.utc)

def main():
    symbol = os.getenv("SYMBOL", "ES")
    exchange = os.getenv("EXCHANGE", "CME")
    db_symbol = os.getenv("DB_SYMBOL", "ES_CONTFUT")
    source = os.getenv("SOURCE", "ibkr")

    conn = get_conn()

    while True:
        try:
            ib = connect_ib()
            contract = make_es_contfut(symbol, exchange)
            ib.qualifyContracts(contract)

            agg = MinuteAggregator()
            pending_rows = []

            # IB realtime bars are 5-second. We'll aggregate into 1-minute.
            # whatToShow='TRADES' is common for bars.
            bars = ib.reqRealTimeBars(contract, 5, "TRADES", False)

            def on_bar_update(_bars, has_new_bar: bool):
                # When has_new_bar, last item is the newest 5s bar
                if not has_new_bar or not _bars:
                    return
                b5 = _bars[-1]
                # b5.time is datetime
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

                # batch write every few bars
                if len(pending_rows) >= 10:
                    upsert_many(conn, pending_rows)
                    latest_ts = pending_rows[-1]["ts"]
                    print(f"[collector] upserted {len(pending_rows)} bars; latest={latest_ts.isoformat()}")
                    pending_rows.clear()

            bars.updateEvent += on_bar_update

            print("[collector] connected, streaming realtime bars...")

            # Run event loop forever; if gateway disconnects, we'll fall into exception and reconnect.
            while ib.isConnected():
                ib.waitOnUpdate(timeout=5)

                # periodic flush in case batch never reaches threshold
                if pending_rows:
                    upsert_many(conn, pending_rows)
                    latest_ts = pending_rows[-1]["ts"]
                    print(f"[collector] upserted {len(pending_rows)} bars; latest={latest_ts.isoformat()}")
                    pending_rows.clear()

        except Exception as e:
            print(f"[collector] error: {e}")
        finally:
            try:
                ib.disconnect()
            except Exception:
                pass

        print("[collector] reconnecting in 10s...")
        time.sleep(10)

if __name__ == "__main__":
    util.patchAsyncio()
    main()
