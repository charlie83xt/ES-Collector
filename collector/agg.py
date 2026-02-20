from dataclasses import dataclass
from datetime import datetime, timezone

@dataclass
class Bar1m:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

def floor_to_minute(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(second=0, microsecond=0)

class MinuteAggregator:
    def __init__(self):
        self.current_minute_ts = None
        self.bar = None

    def update_from_5s(self, ts: datetime, o: float, h: float, l: float, c: float, v: float):
        m = floor_to_minute(ts)

        # rollover minute -> emit previous
        if self.current_minute_ts is None or m != self.current_minute_ts:
            emitted = self.bar
            self.current_minute_ts = m
            self.bar = Bar1m(ts=m, open=o, high=h, low=l, close=c, volume=v)
            return emitted

        # same minute -> update
        b = self.bar
        b.high = max(b.high, h)
        b.low = min(b.low, l)
        b.close = c
        b.volume += v
        return None

    def flush(self):
        b = self.bar
        self.bar = None
        self.current_minute_ts = None
        return b
