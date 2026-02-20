create table if not exists market_bars_1m (
  symbol text not null,
  ts timestamptz not null,          -- bar open time (UTC)
  open numeric not null,
  high numeric not null,
  low  numeric not null,
  close numeric not null,
  volume numeric null,
  source text not null,             -- e.g. 'ibkr'
  ingest_ts timestamptz not null default now(),
  primary key (symbol, ts)
);

create index if not exists idx_market_bars_1m_symbol_ts
  on market_bars_1m (symbol, ts desc);
