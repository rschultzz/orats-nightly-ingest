import os, sys, logging, argparse, datetime as dt
import pytz, requests
from math import exp
from db import get_conn, executemany_upsert

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("orats_job")

# Endpoints
STRIKES_URL    = "https://api.orats.io/datav2/hist/strikes"
SUMM_HIST_URL  = "https://api.orats.io/datav2/hist/summaries"      # riskFree30 fallback
SUMM_LIVE_URL  = "https://api.orats.io/datav2/summaries"
MONIM_HIST_URL = "https://api.orats.io/datav2/hist/monies/implied" # riskFreeRate, yieldRate per expir
MONIM_LIVE_URL = "https://api.orats.io/datav2/monies/implied"

# Fields
STRIKE_FIELDS = ",".join([
    "ticker","tradeDate","expirDate","dte","strike","stockPrice",
    "callOpenInterest","putOpenInterest","gamma"
])

# Config
TICKER = os.environ.get("TICKER", "SPX").strip()
CARRY_TICKER = os.environ.get("CARRY_TICKER", "SPY").strip()  # proxy if SPX yield missing
DTE_MAX = int(os.environ.get("DTE_MAX", "400"))
CONTRACT_MULTIPLIER = float(os.environ.get("CONTRACT_MULTIPLIER", "100"))

# Timezones
TZ_NY = pytz.timezone("America/New_York")
TZ_LA = pytz.timezone("America/Los_Angeles")

def _mask(url: str, token: str) -> str:
    return url.replace(token, "***") if token else url

def _get(session: requests.Session, url: str, token: str, params: dict) -> requests.Response:
    q = dict(params); q["token"] = token
    r = session.get(url, params=q, timeout=120)
    log.debug("GET %s -> %s", _mask(r.url, token), r.status_code)
    return r

def _is_bday(d: dt.date) -> bool:
    # Mon-Fri only (no holiday calendar here)
    return d.weekday() < 5

def _next_bday(d: dt.date) -> dt.date:
    nd = d
    while not _is_bday(nd):
        nd += dt.timedelta(days=1)
    return nd

def _prev_bday(d: dt.date) -> dt.date:
    pd = d
    while not _is_bday(pd):
        pd -= dt.timedelta(days=1)
    return pd

def _fetch_monies_map(session, token, ticker, trade_date):
    """Return {expirDate: (riskFreeRate, yieldRate)} from Monies Implied for a given API trade_date."""
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

def find_prev_bday_with_data_from(session, token, ticker, start_date, max_lookback_days=10):
    """
    Start from (start_date - 1) and walk backward to find the most recent business day with data.
    """
    day = start_date - dt.timedelta(days=1)
    for _ in range(max_lookback_days):
        if _is_bday(day) and has_data_for_date(session, token, ticker, day):
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

def parse_iso_date(s):
    try:
        return dt.date.fromisoformat(s)
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="Store date YYYY-MM-DD (overrides local LA 'today')")
    ap.add_argument("--token", help="ORATS API token (overrides ORATS_TOKEN env var)")
    args = ap.parse_args()

    token = (args.token or os.environ.get("ORATS_TOKEN") or "").strip()
    if not token:
        log.error("Provide token via --token or ORATS_TOKEN.")
        sys.exit(2)

    # 1) Decide the STORED trade date based on Los Angeles "today"
    if args.date:
        store_trade_date = dt.date.fromisoformat(args.date)
    else:
        la_today = dt.datetime.now(TZ_LA).date()
        store_trade_date = _next_bday(la_today) if not _is_bday(la_today) else la_today

    with requests.Session() as session:
        # 2) Find the API trade date = previous business day with data BEFORE the store date
        api_trade_date = find_prev_bday_with_data_from(session, token, TICKER, store_trade_date)
        if not api_trade_date:
            log.error("Could not find a previous business day with data before %s", store_trade_date.isoformat())
            sys.exit(3)

        log.info("Source(API) trade_date=%s  →  Stored trade_date=%s  [LA_today=%s, NY_now=%s]",
                 api_trade_date.isoformat(),
                 store_trade_date.isoformat(),
                 dt.datetime.now(TZ_LA).strftime("%Y-%m-%d %H:%M:%S %Z"),
                 dt.datetime.now(TZ_NY).strftime("%Y-%m-%d %H:%M:%S %Z"))

        # Carry per-expiration via Monies Implied (sourced off api_trade_date)
        m_spx = _fetch_monies_map(session, token, "SPX", api_trade_date)
        m_spy = _fetch_monies_map(session, token, "SPY", api_trade_date)  # proxy if SPX missing yield
        have_spx_yield = sum(1 for _, (_, y) in m_spx.items() if y not in (None, 0, 0.0))
        have_spy_yield = sum(1 for _, (_, y) in m_spy.items() if y not in (None, 0, 0.0))
        log.info("Monies maps %s → SPX expir=%d (yields=%d), SPY expir=%d (yields=%d)",
                 api_trade_date, len(m_spx), have_spx_yield, len(m_spy), have_spy_yield)

        # Fallback rf30 if an expiry lacks riskFreeRate
        rf30 = _fetch_rf30(session, token, "SPX", api_trade_date)
        if rf30 is None:
            rf30 = _fetch_rf30(session, token, "SPY", api_trade_date)

        # Strikes (sourced off api_trade_date)
        data = fetch_eod_strikes(session, token, TICKER, api_trade_date)
        if DTE_MAX is not None:
            data = [d for d in data if d.get("dte") is None or int(d["dte"]) <= DTE_MAX]
        if not data:
            log.warning("No strike records for %s %s", TICKER, api_trade_date)
            return

        # Build rows targeting the forward storage date. Recalculate DTE off the stored trade date.
        rows = []
        for d in data:
            expd = d.get("expirDate")  # 'YYYY-MM-DD'
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

            S      = d.get("stockPrice")
            gamma  = d.get("gamma")
            coi    = d.get("callOpenInterest")
            poi    = d.get("putOpenInterest")
            gex_call = compute_gex(S, gamma, coi)
            gex_put  = compute_gex(S, gamma, poi)

            # Recompute DTE relative to the STORED trade date
            eff_dte = d.get("dte")
            exp_date_obj = parse_iso_date(expd) if isinstance(expd, str) else None
            if exp_date_obj is not None:
                eff_dte = (exp_date_obj - store_trade_date).days

            disc_lvl = compute_discounted_level(d.get("strike"), eff_dte, sr, dy)

            rows.append({
                "ticker": d.get("ticker"),
                "trade_date": store_trade_date.isoformat(),   # <<-- STORED date = LA "today" (Mon-Fri)
                "expir_date": expd,
                "dte": eff_dte,
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

        log.info(
            "Upserted %s rows. source_trade_date=%s stored_trade_date=%s",
            len(rows), api_trade_date.isoformat(), store_trade_date.isoformat()
        )

if __name__ == "__main__":
    main()
