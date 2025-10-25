# ORATS Nightly Ingest (Render Cron Job)

This mini-project provides a scheduled job (via Render Cron Jobs) to:
- Pull **end-of-day strikes** from ORATS for a given `tradeDate` (defaults to previous trading day).
- Store **Open Interest (calls & puts)** and **Gamma** by strike/expiry into your **Postgres** on Render.
- (Optional) compute and store **per-strike GEX** buckets (`gex_call`, `gex_put` = gamma × S^2 × OI × 100).

## Files
- `job_orats_eod.py`: Main job script (idempotent upsert; simple retries).
- `db.py`: Minimal Postgres helper using `psycopg` (v3) connection pooling.
- `schema.sql`: Table DDL (and an example materialized view).
- `requirements.txt`: Python dependencies for the cron job service.
- `render.yaml` (optional): Example Blueprint to define a Cron Job in IaC.
- `example.env`: Environment variable template (copy values into Render secrets).

## Quick start (local)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# set envs for local testing
export ORATS_TOKEN="your_orats_token"
export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
export TICKER="SPX"                       # default
export DTE_MAX="400"                      # optional filter
export RUN_UTC_HOUR="02"                  # not required locally

# create table(s)
psql "$DATABASE_URL" -f schema.sql

# run for a specific trade date
python job_orats_eod.py --date 2025-10-17

# or just run (it will auto-pick the most recent trade date with data)
python job_orats_eod.py
```

## Deploy on Render (Dashboard)
1. **New → Cron Job** → connect to your repo (that contains these files).
2. **Build Command**: `pip install -r requirements.txt`
3. **Start/Command**: `python job_orats_eod.py`
4. **Schedule (UTC)**: e.g. `30 02 * * MON-FRI` (which is 7:30pm PT during DST).
5. **Environment**: add
   - `ORATS_TOKEN` (secret)
   - `DATABASE_URL` (use your Render Postgres **Internal DB URL**)
   - `TICKER=SPX` (default) or `SPY`, etc.
   - optional: `DTE_MAX=400` to trim far-dated expiries.

> Tip: ORATS EOD is typically complete shortly after the close. Running the job **~1 hour after close** is usually safe.

## Notes
- The job uses `https://api.orats.io/datav2/hist/strikes` and requests only the fields we need for speed (ticker, tradeDate, expirDate, dte, strike, stockPrice, callOpenInterest, putOpenInterest, gamma).
- We upsert by `(ticker, trade_date, expir_date, strike)` so re-runs don't duplicate.
- `gex_call` and `gex_put` are stored for convenience using multiplier **100** (SPX index options). If you prefer a different convention, adjust in code.

