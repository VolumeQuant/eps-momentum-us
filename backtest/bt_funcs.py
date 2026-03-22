import sys, sqlite3, copy
import numpy as np
sys.stdout.reconfigure(encoding='utf-8')

DB = r'C:\dev\claude-code\eps-momentum-us\eps_momentum_data.db'

def get_eligible(cur, date):
    cur.execute("""SELECT ticker, adj_gap, rev_growth, price, ma60, adj_score,
               num_analysts, rev_up30, rev_down30
        FROM ntm_screening WHERE date = ?
          AND adj_score > 9 AND adj_gap IS NOT NULL
          AND price IS NOT NULL AND price >= 10
          AND ma60 IS NOT NULL AND price > ma60
          AND rev_growth IS NOT NULL AND rev_growth >= 0.10""", (date,))
    out = []
    for r in cur.fetchall():
        tk, ag, rg, pr, m60, asc, na, ru, rd = r
        if na is not None and na < 3: continue
        if ru is not None and rd is not None:
            tot = ru + rd
            if tot > 0 and rd / tot > 0.3: continue
        out.append(dict(ticker=tk, adj_gap=ag, rev_growth=rg, price=pr))
    return out

def rank_stocks(stocks, method):
    if not stocks: return []
    g = np.array([s['adj_gap'] for s in stocks])
    r = np.array([s['rev_growth'] for s in stocks])
    gm, gs = np.mean(g), np.std(g)
    rm, rs = np.mean(r), np.std(r)
    zg = (g - gm)/gs if gs > 0 else np.zeros_like(g)
    zr = (r - rm)/rs if rs > 0 else np.zeros_like(r)
    comp = (-zg)*0.7 + zr*0.3 if method == 'A' else (-zg)*1.0
    for i, s in enumerate(stocks):
        s['comp'] = comp[i]; s['zg'] = zg[i]; s['zr'] = zr[i]
    ranked = sorted(stocks, key=lambda x: x['comp'], reverse=True)
    for i, s in enumerate(ranked): s['rank'] = i+1
    return ranked

def price_at(cur, tk, dt):
    cur.execute('SELECT price FROM ntm_screening WHERE date=? AND ticker=?', (dt, tk))
    r = cur.fetchone()
    return r[0] if r else None
def fp(v, w=8):
    s = '+' if v >= 0 else ''
    return f'{s}{v:.3%}'.rjust(w)

def fp4(v, w=20):
    s = '+' if v >= 0 else ''
    return f'{s}{v:.4%}'.rjust(w)
