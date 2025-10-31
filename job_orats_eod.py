import os, sys, logging, argparse, datetime as dt
import pytz, requests
from math import exp
from db import get_conn  # NOTE: we bypass executemany_upsert to remove any ambiguity

import datetime as dt

def _next_business_day(d: dt.date) -> dt.date:
    nd = d + dt.timedelta(days=1)
    if nd.weekday() == 5:  # Sat -> Mon
        nd += dt.timedelta(days=2)
    elif nd.weekday() == 6:  # Sun -> Mon
        nd += dt.timedelta(days=1)
    return nd

def _parse_iso_date(s):
    try:
        return dt.date.fromisoformat(str(s)[:10])
    except Exception:
        return None


VERSION = "eod-shift-hard-upsert-2025-10-31b"
log.info("[START %s] file=%s", VERSION, __file__)


VERSION = "eod-shift-hard-upsert-2025-10-31b"

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("orats_job")

# Endpoints
STRIKES_URL    = "https://api.orats.io/datav2/hist/strikes"
SUMM_HIST_URL  = "https://api.orats.io/datav2/hist/summaries"
SUMM_LIVE_URL  = "https://api.orats.io/datav2/summaries"
MONIM_HIST_URL = "https://api.orats.io/datav2/hist/monies/implied"
MONIM_LIVE_URL = "https://api.orats.io/datav2/monies/implied"

# Fields
STRIKE_FIELDS = ",".join([
    "ticker","tradeDate","expirDate","dte","strike","stockPrice",
    "callOpenInterest","putOpenInterest","gamma"
])

# Config
TICKER = os.environ.get("TICKER", "SPX").strip()
DTE_MAX = int(os.environ.get("DTE_MAX", "400"))
CONTRACT_MULTIPLIER = float(os.environ.get("CONTRACT_MULTIPLIER", "100"))
TZ_NY = pytz.timezone("America/New_York")

def _get(session: requests.Session, url: str, token: str, params: dict) -> requests.Response:
    q = dict(params); q["token"] = token
    r = session.get(url, params=q, timeout=120)
    log.debug("GET %s -> %s", url, r.status_code)
    return r

def _fetch_monies_map(session, token, ticker, trade_date):
    res = {}
    for url in (MONIM_HIST_URL, MONIM_LIVE_URL):
        r = _get(session, url, token, {
            "ticker": ticker,
            "tradeDate": trade_date.isoformat(),
            "fields": "ticker,tradeDate,expirDate,riskFreeRate,yieldRate"
        })
        if r.status_code >= 400: 
            continue
        for row in r.json().get("data", []):
            expd = row.get("expirDate")
            if expd: res[expd] = (row.get("riskFreeRate"), row.get("yieldRate"))
        if res: break
    return res

def _fetch_rf30(session, token, ticker, trade_date):
    for url in (SUMM_HIST_URL, SUMM_LIVE_URL):
        r = _get(session, url, token, {
            "ticker": ticker, "tradeDate": trade_date.isoformat(),
            "fields": "ticker,tradeDate,riskFree30"
        })
        if r.status_code >= 400: 
            continue
        d = r.json().get("data", [])
        if d: return d[0].get("riskFree30")
    return None

def has_data_for_date(session, token, ticker, trade_date):
    r = _get(session, STRIKES_URL, token, {"ticker": ticker, "tradeDate": trade_date.isoformat(), "fields": "ticker"})
    return r.status_code == 200 and len(r.json().get("data", [])) > 0

def previous_business_day_with_data(session, token, ticker, max_lookback_days=7):
    day = dt.datetime.now(TZ_NY).date() - dt.timedelta(days=1)
    for _ in range(max_lookback_days):
        if has_data_for_date(session, token, ticker, day):
            return day
        day -= dt.timedelta(days=1)
    return None

def next_business_day(d: dt.date) -> dt.date:
    nd = d + dt.timedelta(days=1)
    if nd.weekday() == 5: nd += dt.timedelta(days=2)  # Sat->Mon
    elif nd.weekday() == 6: nd += dt.timedelta(days=1)  # Sun->Mon
    return nd

def fetch_eod_strikes(session, token, ticker, trade_date):
    r = _get(session, STRIKES_URL, token, {"ticker": ticker, "tradeDate": trade_date.isoformat(), "fields": STRIKE_FIELDS})
    if r.status_code == 401:
        raise RuntimeError("401 from strikes. Check token/entitlement.")
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
        return dt.date.fromisoformat(s) if s else None
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="API trade date YYYY-MM-DD (defaults to most recent date with data)")
    ap.add_argument("--token", help="ORATS API token (overrides ORATS_TOKEN env var)")
    args = ap.parse_args()

    log.info("[START %s] file=%s cwd=%s argv=%s", VERSION, __file__, os.getcwd(), sys.argv)

    token = (args.token or os.environ.get("ORATS_TOKEN") or "").strip()
    if not token:
        log.error("Provide token via --token or ORATS_TOKEN.")
        sys.exit(2)

    with requests.Session() as session:
        api_trade_date = dt.date.fromisoformat(args.date) if args.date else previous_business_day_with_data(session, token, TICKER)
        if not api_trade_date:
            log.error("Could not find a recent trade date with data.")
            sys.exit(3)

        forced = os.environ.get("FORCE_STORE_DATE")
        store_trade_date = dt.date.fromisoformat(forced) if forced else next_business_day(api_trade_date)

        log.info("API trade_date=%s  ->  STORED trade_date=%s  (forced=%s)",
                 api_trade_date.isoformat(), store_trade_date.isoformat(), bool(forced))

        # inputs
        m_spx = _fetch_monies_map(session, token, "SPX", api_trade_date)
        m_spy = _fetch_monies_map(session, token, "SPY", api_trade_date)
        rf30  = _fetch_rf30(session, token, "SPX", api_trade_date) or _fetch_rf30(session, token, "SPY", api_trade_date)
        data = fetch_eod_strikes(session, token, TICKER, api_trade_date)
        if DTE_MAX is not None:
            data = [d for d in data if d.get("dte") is None or int(d["dte"]) <= DTE_MAX]
        if not data:
            log.warning("No strike records for %s %s", TICKER, api_trade_date)
            return

        # build rows (with STORED trade_date and dte recomputed vs stored date)
        rows = []
        for d in data:
            expd_s = d.get("expirDate")
            expd = parse_iso_date(expd_s)

            # carry
            sr, dy = (None, None)
            if expd_s and expd_s in m_spx: sr, dy = m_spx[expd_s]
            if (dy in (None, 0, 0.0)) and expd_s and expd_s in m_spy:
                sr2, dy2 = m_spy[expd_s]
                if sr is None: sr = sr2
                if dy2 not in (None, 0, 0.0): dy = dy2
            if sr is None: sr = rf30
            if dy is None: dy = 0.0

            S, gamma = d.get("stockPrice"), d.get("gamma")
            coi, poi = d.get("callOpenInterest"), d.get("putOpenInterest")
            gex_call, gex_put = compute_gex(S, gamma, coi), compute_gex(S, gamma, poi)

            eff_dte = (expd - store_trade_date).days if expd else d.get("dte")
            disc_lvl = compute_discounted_level(d.get("strike"), eff_dte, sr, dy)

            rows.append((
                d.get("ticker"),
                store_trade_date,     # <- THIS is what we store
                expd,                 # Date object (matches your PK type)
                eff_dte,
                d.get("strike"),
                S,
                coi,
                poi,
                gamma,
                gex_call,
                gex_put,
                sr,
                dy,
                disc_lvl
            ))

        upsert_sql = """
        INSERT INTO orats_oi_gamma
            (ticker, trade_date, expir_date, dte, strike, stock_price, call_oi, put_oi,
             gamma, gex_call, gex_put, short_rate, div_yield, discounted_level)
        VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ticker, trade_date, expir_date, strike)
        DO UPDATE SET
            dte = EXCLUDED.dte,
            stock_price = EXCLUDED.stock_price,
            call_oi = EXCLUDED.call_oi,
            put_oi = EXCLUDED.put_oi,
            gamma = EXCLUDED.gamma,
            gex_call = EXCLUDED.gex_call,
            gex_put = EXCLUDED.gex_put,
            short_rate = EXCLUDED.short_rate,
            div_yield = EXCLUDED.div_yield,
            discounted_level = EXCLUDED.discounted_level;
        """

        with get_conn() as conn:
            with conn.cursor() as cur:
                # clear today (stored) first to avoid any ambiguity
                cur.execute("DELETE FROM orats_oi_gamma WHERE ticker=%s AND trade_date=%s", (TICKER, store_trade_date))
                log.info("Deleted existing rows for (%s, %s): %s", TICKER, store_trade_date.isoformat(), cur.rowcount)

                # explicit UPSERT (no helper)
                cur.executemany(upsert_sql, rows)

                # refresh MV
                try:
                    cur.execute("REFRESH MATERIALIZED VIEW orats_gex_by_exp;")
                except Exception as e:
                    log.warning("Refresh MV failed (non-fatal): %s", e)

                conn.commit()

                # post-commit verification
                cur.execute("SELECT COUNT(*) FROM orats_oi_gamma WHERE ticker=%s AND trade_date=%s", (TICKER, store_trade_date))
                cnt = cur.fetchone()[0]
                log.info("POST-COMMIT: rowcount for (%s, %s) = %s", TICKER, store_trade_date.isoformat(), cnt)

    log.info("[DONE %s] API=%s → STORED=%s | attempted_rows=%s",
             VERSION, api_trade_date.isoformat(), store_trade_date.isoformat(), len(rows))

if __name__ == "__main__":
    main()
