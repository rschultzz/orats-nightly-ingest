import os, sys, logging, argparse, datetime as dt
import pytz, requests
from math import exp
from db import get_conn, executemany_upsert

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("orats_job")

# Endpoints
STRIKES_URL   = "https://api.orats.io/datav2/hist/strikes"
SUMM_HIST_URL = "https://api.orats.io/datav2/hist/summaries"     # riskFree30 fallback
SUMM_LIVE_URL = "https://api.orats.io/datav2/summaries"
MONIM_HIST_URL= "https://api.orats.io/datav2/hist/monies/implied" # riskFreeRate, yieldRate per expir
MONIM_LIVE_URL= "https://api.orats.io/datav2/monies/implied"

# Fields
STRIKE_FIELDS = ",".join([
    "ticker","tradeDate","expirDate","dte","strike","stockPrice",
    "callOpenInterest","putOpenInterest","gamma"
])

# Config
TICKER = os.environ.get("TICKER", "SPX").strip()
CARRY_TICKER = os.environ.get("CARRY_TICKER", "SPY").strip()      # proxy if SPX yield missing
DTE_MAX = int(os.environ.get("DTE_MAX", "400"))
CONTRACT_MULTIPLIER = float(os.environ.get("CONTRACT_MULTIPLIER", "100"))

def _mask(url: str, token: str) -> str:
    return url.replace(token, "***") if token else url

def _get(session: requests.Session, url: str, token: str, params: dict) -> requests.Response:
    q = dict(params); q["token"] = token
    r = session.get(url, params=q, timeout=120)
    log.debug("GET %s -> %s", _mask(r.url, token), r.status_code)
    return r

def _fetch_monies_map(session, token, ticker, trade_date):
    """Return {expirDate: (riskFreeRate, yieldRate)} from Monies Implied."""
    res = {}
    for url in (MONIM_HIST_URL, MONIM_LIVE_URL):
        r = _get(session, url, token, {
            "ticker": ticker,
            "tradeDate": trade_date.isoformat(),
            "fields": "ticker,tradeDate,expirDate,riskFreeRate,yieldRate"
        })
        if r.status_code >= 400:
            continue
        data = r.json().get("data", [])
        for row in data:
            expd = row.get("expirDate")
            if not expd:
                continue
            rf = row.get("riskFreeRate")
            y  = row.get("yieldRate")
            res[expd] = (rf, y)
        if res:
            break
    return res

def _fetch_rf30(session, token, ticker, trade_date):
    """Fallback riskFree30 from Summaries (single value)."""
    for url in (SUMM_HIST_URL, SUMM_LIVE_URL):
        r = _get(session, url, token, {
            "ticker": ticker, "tradeDate": trade_date.isoformat(),
            "fields": "ticker,tradeDate,riskFree30"
        })
        if r.status_code >= 400:
            continue
        d = r.json().get("data", [])
        if d:
            return d[0].get("riskFree30")
    return None

def has_data_for_date(session, token, ticker, trade_date):
    r = _get(session, STRIKES_URL, token, {"ticker": ticker, "tradeDate": trade_date.isoformat(), "fields": "ticker"})
    return r.status_code == 200 and len(r.json().get("data", [])) > 0

def previous_business_day_with_data(session, token, ticker, max_lookback_days=7):
    et = pytz.timezone("America/New_York")
    day = dt.datetime.now(et).date() - dt.timedelta(days=1)
    for _ in range(max_lookback_days):
        if has_data_for_date(session, token, ticker, day):
            return day
        day -= dt.timedelta(days=1)
    return None

def fetch_eod_strikes(session, token, ticker, trade_date):
    params = {"ticker": ticker, "tradeDate": trade_date.isoformat(), "fields": STRIKE_FIELDS}
    r = _get(session, STRIKES_URL, token, params)
    if r.status_code == 401:
        raise RuntimeError("401 from strikes. Check token/entitlement for datav2/hist/strikes.")
    r.raise_for_status()
    return r.json().get("data", [])

def compute_gex(S, gamma, oi):
    return (gamma or 0.0) * (S or 0.0)**2 * (oi or 0) * CONTRACT_MULTIPLIER

def compute_discounted_level(strike, dte, short_rate, div_yield):
    if strike is None or dte is None or short_rate is None or div_yield is None:
        return None
    t = (int(dte) + 1) / 252.0
    return float(strike) * exp((float(short_rate) - float(div_yield)) * t)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="Trade date YYYY-MM-DD (defaults to most recent date with data)")
    ap.add_argument("--token", help="ORATS API token (overrides ORATS_TOKEN env var)")
    args = ap.parse_args()

    token = (args.token or os.environ.get("ORATS_TOKEN") or "").strip()
    if not token:
        log.error("Provide token via --token or ORATS_TOKEN.")
        sys.exit(2)

    with requests.Session() as session:
        trade_date = dt.date.fromisoformat(args.date) if args.date else previous_business_day_with_data(session, token, TICKER)
        if not trade_date:
            log.error("Could not find a recent trade date with data.")
            sys.exit(3)

        # Carry per-expiration via Monies Implied
        m_spx = _fetch_monies_map(session, token, "SPX", trade_date)
        m_spy = _fetch_monies_map(session, token, "SPY", trade_date)  # proxy if SPX missing yield
        have_spx_yield = sum(1 for _, (_, y) in m_spx.items() if y not in (None, 0, 0.0))
        have_spy_yield = sum(1 for _, (_, y) in m_spy.items() if y not in (None, 0, 0.0))
        log.info("Monies maps %s → SPX expir=%d (yields=%d), SPY expir=%d (yields=%d)",
                 trade_date, len(m_spx), have_spx_yield, len(m_spy), have_spy_yield)

        # Fallback rf30 if an expiry lacks riskFreeRate
        rf30 = _fetch_rf30(session, token, "SPX", trade_date)
        if rf30 is None:
            rf30 = _fetch_rf30(session, token, "SPY", trade_date)

        # Strikes
        data = fetch_eod_strikes(session, token, TICKER, trade_date)
        if DTE_MAX is not None:
            data = [d for d in data if d.get("dte") is None or int(d["dte"]) <= DTE_MAX]
        if not data:
            log.warning("No strike records for %s %s", TICKER, trade_date)
            return

        rows = []
        for d in data:
            expd = d.get("expirDate")
            # prefer SPX monies; use SPY monies if SPX yield missing; else rf30 & 0.0 as last resort
            sr, dy = (None, None)
            if expd and expd in m_spx:
                sr, dy = m_spx[expd]
            if (dy in (None, 0, 0.0)) and expd and expd in m_spy:
                sr2, dy2 = m_spy[expd]
                if sr is None: sr = sr2
                if dy2 not in (None, 0, 0.0): dy = dy2
            if sr is None: sr = rf30
            if dy is None: dy = 0.0  # ensure discounted_level gets computed

            S     = d.get("stockPrice")
            gamma = d.get("gamma")
            coi   = d.get("callOpenInterest")
            poi   = d.get("putOpenInterest")
            gex_call = compute_gex(S, gamma, coi)
            gex_put  = compute_gex(S, gamma, poi)
            disc_lvl = compute_discounted_level(d.get("strike"), d.get("dte"), sr, dy)

            rows.append({
                "ticker": d.get("ticker"),
                "trade_date": d.get("tradeDate"),
                "expir_date": expd,
                "dte": d.get("dte"),
                "strike": d.get("strike"),
                "stock_price": S,
                "call_oi": coi,
                "put_oi": poi,
                "gamma": gamma,
                "gex_call": gex_call,
                "gex_put": gex_put,
                "short_rate": sr,
                "div_yield": dy,
                "discounted_level": disc_lvl
            })

        with get_conn() as conn:
            executemany_upsert(conn, rows)
            with conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW orats_gex_by_exp;")
            conn.commit()

        log.info("Upserted %s rows for %s", len(rows), trade_date.isoformat())

if __name__ == "__main__":
    main()
