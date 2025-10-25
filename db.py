import os, contextlib
import psycopg
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set.")

pool = ConnectionPool(
    DATABASE_URL,
    min_size=1,
    max_size=4,
    kwargs={"connect_timeout": 20}
)

@contextlib.contextmanager
def get_conn():
    with pool.connection() as conn:
        yield conn

def executemany_upsert(conn, rows):
    """
    Upsert rows into orats_oi_gamma. Ensures all named parameters exist
    on every row dict so psycopg doesn't raise 'query parameter missing'.
    """
    if not rows:
        return

    expected = (
        "ticker","trade_date","expir_date","dte","strike","stock_price",
        "call_oi","put_oi","gamma","gex_call","gex_put",
        "short_rate","div_yield","discounted_level"
    )

    # normalize rows → every key present (None if missing)
    normalized = []
    for r in rows:
        base = {k: None for k in expected}
        if r:
            base.update(r)
        normalized.append(base)

    sql = '''
    INSERT INTO orats_oi_gamma
      (ticker, trade_date, expir_date, dte, strike, stock_price,
       call_oi, put_oi, gamma, gex_call, gex_put,
       short_rate, div_yield, discounted_level)
    VALUES
      (%(ticker)s, %(trade_date)s, %(expir_date)s, %(dte)s, %(strike)s, %(stock_price)s,
       %(call_oi)s, %(put_oi)s, %(gamma)s, %(gex_call)s, %(gex_put)s,
       %(short_rate)s, %(div_yield)s, %(discounted_level)s)
    ON CONFLICT (ticker, trade_date, expir_date, strike) DO UPDATE SET
       stock_price       = EXCLUDED.stock_price,
       call_oi           = EXCLUDED.call_oi,
       put_oi            = EXCLUDED.put_oi,
       gamma             = EXCLUDED.gamma,
       gex_call          = EXCLUDED.gex_call,
       gex_put           = EXCLUDED.gex_put,
       short_rate        = EXCLUDED.short_rate,
       div_yield         = EXCLUDED.div_yield,
       discounted_level  = EXCLUDED.discounted_level,
       updated_at        = NOW();
    '''
    with conn.cursor() as cur:
        cur.executemany(sql, normalized)
