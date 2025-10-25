# job_orats_eod.py
import os, sys, time, json, math, logging, argparse, datetime as dt
from dateutil import tz
from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta
import pytz
import requests

from db import get_conn, executemany_upsert

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("orats_job")

ORATS_TOKEN = os.environ.get("ORATS_TOKEN")
TICKER = os.environ.get("TICKER", "SPX")
DTE_MAX = int(os.environ.get("DTE_MAX", "400"))  # Trim very long-dated expiries
CONTRACT_MULTIPLIER = float(os.environ.get("CONTRACT_MULTIPLIER", "100"))  # SPX index options

BASE_URL = "https://api.orats.io/datav2/hist/strikes"

FIELDS = ",".join([
    "ticker","tradeDate","expirDate","dte","strike","stockPrice",
    "callOpenInterest","putOpenInterest","gamma"
])

def previous_business_day_with_data(session, ticker, max_lookback_days=7):
    # Try yesterday, then step back until data is returned or we hit lookback limit
    et = pytz.timezone("America/New_York")
    day = dt.datetime.now(et).date() - dt.timedelta(days=1)
    attempts = 0
    while attempts < max_lookback_days:
        ok = has_data_for_date(session, ticker, day)
        if ok:
            return day
        day -= dt.timedelta(days=1)
        attempts += 1
    return None

def has_data_for_date(session, ticker, trade_date):
    params = {
        "ticker": ticker,
        "tradeDate": trade_date.isoformat(),
        "fields": "ticker"  # keep tiny
    }
    headers = {"Authorization": ORATS_TOKEN} if ORATS_TOKEN else {}
    r = session.get(BASE_URL, params=params, headers=headers, timeout=60)
    if r.status_code != 200:
        log.warning("ORATS probe %s %s → %s %s", trade_date, ticker, r.status_code, r.text[:200])
        return False
    data = r.json().get("data", [])
    return len(data) > 0

def fetch_eod_strikes(session, ticker, trade_date):
    params = {
        "ticker": ticker,
        "tradeDate": trade_date.isoformat(),
        "fields": FIELDS
    }
    headers = {"Authorization": ORATS_TOKEN} if ORATS_TOKEN else {}
    r = session.get(BASE_URL, params=params, headers=headers, timeout=180)
    r.raise_for_status()
    js = r.json()
    return js.get("data", [])

def compute_gex(row):
    # Gamma in ORATS is per-option (same for calls and puts). A common convention:
    # GEX_side = gamma * S^2 * OI * contract_multiplier
    S = row.get("stockPrice") or 0.0
    gamma = row.get("gamma") or 0.0
    call_oi = row.get("callOpenInterest") or 0
    put_oi  = row.get("putOpenInterest") or 0
    gex_call = gamma * (S**2) * call_oi * CONTRACT_MULTIPLIER
    gex_put  = gamma * (S**2) * put_oi  * CONTRACT_MULTIPLIER
    return gex_call, gex_put

def main():
    if not ORATS_TOKEN:
        log.error("ORATS_TOKEN is not set")
        sys.exit(2)

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Trade date YYYY-MM-DD (defaults to most recent date with data)")
    args = parser.parse_args()

    with requests.Session() as session:
        trade_date = dt.date.fromisoformat(args.date) if args.date else previous_business_day_with_data(session, TICKER)
        if not trade_date:
            log.error("Could not find a recent trade date with data.")
            sys.exit(3)

        log.info("Fetching ORATS EOD strikes for %s %s", TICKER, trade_date.isoformat())
        data = fetch_eod_strikes(session, TICKER, trade_date)
        if not data:
            log.warning("No records returned for %s %s", TICKER, trade_date.isoformat())
            return

        # Filter DTE if requested
        if DTE_MAX is not None:
            data = [d for d in data if d.get("dte") is None or int(d["dte"]) <= DTE_MAX]

        rows = []
        for d in data:
            try:
                gex_call, gex_put = compute_gex(d)
                rows.append({
                    "ticker": d.get("ticker"),
                    "trade_date": d.get("tradeDate"),
                    "expir_date": d.get("expirDate"),
                    "dte": d.get("dte"),
                    "strike": d.get("strike"),
                    "stock_price": d.get("stockPrice"),
                    "call_oi": d.get("callOpenInterest"),
                    "put_oi": d.get("putOpenInterest"),
                    "gamma": d.get("gamma"),
                    "gex_call": gex_call,
                    "gex_put": gex_put
                })
            except Exception as ex:
                log.exception("Row build failure for record: %s", d)

        if not rows:
            log.warning("Nothing to insert.")
            return

        # Insert / Upsert
        with get_conn() as conn:
            executemany_upsert(conn, rows)
            with conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW orats_gex_by_exp;")
            conn.commit()

        log.info("Upserted %s rows for %s", len(rows), trade_date.isoformat())

if __name__ == "__main__":
    main()
