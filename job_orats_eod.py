import os, sys, logging, argparse, datetime as dt
import pytz
import requests

from db import get_conn, executemany_upsert

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("orats_job")

BASE_URL = os.environ.get("ORATS_BASE_URL", "https://api.orats.io/datav2/hist/strikes")
FIELDS = ",".join([
    "ticker","tradeDate","expirDate","dte","strike","stockPrice",
    "callOpenInterest","putOpenInterest","gamma"
])
TICKER = os.environ.get("TICKER", "SPX").strip()
DTE_MAX = int(os.environ.get("DTE_MAX", "400"))
CONTRACT_MULTIPLIER = float(os.environ.get("CONTRACT_MULTIPLIER", "100"))

def _mask(url: str, token: str) -> str:
    return url.replace(token, "***") if token else url

def _get(session: requests.Session, token: str, params: dict) -> requests.Response:
    # Force token on the query string every time
    q = dict(params)
    q["token"] = token
    r = session.get(BASE_URL, params=q, timeout=180)
    log.debug("GET %s -> %s", _mask(r.url, token), r.status_code)
    return r

def has_data_for_date(session, token, ticker, trade_date):
    r = _get(session, token, {"ticker": ticker, "tradeDate": trade_date.isoformat(), "fields": "ticker"})
    if r.status_code != 200:
        log.warning("ORATS probe %s %s → %s %s", trade_date, ticker, r.status_code, r.text[:200])
        return False
    return len(r.json().get("data", [])) > 0

def previous_business_day_with_data(session, token, ticker, max_lookback_days=7):
    et = pytz.timezone("America/New_York")
    day = dt.datetime.now(et).date() - dt.timedelta(days=1)
    for _ in range(max_lookback_days):
        if has_data_for_date(session, token, ticker, day):
            return day
        day -= dt.timedelta(days=1)
    return None

def fetch_eod_strikes(session, token, ticker, trade_date):
    r = _get(session, token, {"ticker": ticker, "tradeDate": trade_date.isoformat(), "fields": FIELDS})
    if r.status_code == 401:
        raise RuntimeError("ORATS auth failed (401). Double-check token and access to datav2/hist/strikes.")
    r.raise_for_status()
    return r.json().get("data", [])

def compute_gex(row):
    S = row.get("stockPrice") or 0.0
    gamma = row.get("gamma") or 0.0
    call_oi = row.get("callOpenInterest") or 0
    put_oi  = row.get("putOpenInterest") or 0
    gex_call = gamma * (S**2) * call_oi * CONTRACT_MULTIPLIER
    gex_put  = gamma * (S**2) * put_oi  * CONTRACT_MULTIPLIER
    return gex_call, gex_put

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="Trade date YYYY-MM-DD (defaults to most recent trade date with data)")
    ap.add_argument("--token", help="ORATS API token (overrides ORATS_TOKEN env var)")
    args = ap.parse_args()

    token = (args.token or os.environ.get("ORATS_TOKEN") or "").strip()
    if not token:
        log.error("No token provided. Use --token <value> or set ORATS_TOKEN env var.")
        sys.exit(2)

    with requests.Session() as session:
        trade_date = dt.date.fromisoformat(args.date) if args.date else previous_business_day_with_data(session, token, TICKER)
        if not trade_date:
            log.error("Could not find a recent trade date with data.")
            sys.exit(3)

        log.info("Fetching ORATS EOD strikes for %s %s", TICKER, trade_date.isoformat())
        data = fetch_eod_strikes(session, token, TICKER, trade_date)

        if DTE_MAX is not None:
            data = [d for d in data if d.get("dte") is None or int(d["dte"]) <= DTE_MAX]

        if not data:
            log.warning("No records returned for %s %s", TICKER, trade_date.isoformat())
            return

        rows = []
        for d in data:
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

        with get_conn() as conn:
            executemany_upsert(conn, rows)
            with conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW orats_gex_by_exp;")
            conn.commit()

        log.info("Upserted %s rows for %s", len(rows), trade_date.isoformat())

if __name__ == "__main__":
    main()
