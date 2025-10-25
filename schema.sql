-- schema.sql
CREATE TABLE IF NOT EXISTS orats_oi_gamma (
  ticker           TEXT        NOT NULL,
  trade_date       DATE        NOT NULL,
  expir_date       DATE        NOT NULL,
  dte              INTEGER,
  strike           NUMERIC(14,4) NOT NULL,
  stock_price      NUMERIC(16,6),
  call_oi          INTEGER,
  put_oi           INTEGER,
  gamma            DOUBLE PRECISION,
  gex_call         DOUBLE PRECISION,
  gex_put          DOUBLE PRECISION,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, trade_date, expir_date, strike)
);

-- Optional: aggregate view by expiry
CREATE MATERIALIZED VIEW IF NOT EXISTS orats_gex_by_exp AS
SELECT
  ticker, trade_date, expir_date,
  SUM(gex_call) AS gex_call,
  SUM(gex_put)  AS gex_put,
  SUM(gex_call + gex_put) AS gex_total
FROM orats_oi_gamma
GROUP BY 1,2,3;

-- Helpful index for range queries by trade_date
CREATE INDEX IF NOT EXISTS idx_orats_oi_gamma_trade_date ON orats_oi_gamma(trade_date);
