"""Top20 종목을 가장 잘 반영하는 ETF 탐색 — 발행사 full holdings 기반 순위가중 커버리지.
소스: SSGA(xlsx), First Trust(html), iShares(session csv), stockanalysis API(fallback top25).
"""
import requests, io, json, re, sqlite3
import pandas as pd

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'}

# --- Top20 순위가중치 (DB 2026-06-10) ---
c = sqlite3.connect('eps_momentum_data.db')
top20 = list(c.execute('SELECT ticker,part2_rank FROM ntm_screening WHERE date="2026-06-10" AND part2_rank<=20 ORDER BY part2_rank'))
W = {tk: (21 - pr) for tk, pr in top20}
_s = sum(W.values()); W = {k: v / _s for k, v in W.items()}
T20 = set(W)

def cov(held_weights):
    """held_weights: {ticker: etf_weight%}. 반환: (적중수, 순위가중커버%, ETF내비중합%)"""
    hits = T20 & set(held_weights)
    rwc = sum(W[t] for t in hits) * 100
    inw = sum(held_weights[t] for t in hits)
    return len(hits), rwc, inw, hits

# --- 소스별 fetch ---
def fetch_ssga(ticker):
    url = f'https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{ticker.lower()}.xlsx'
    r = requests.get(url, headers=UA, timeout=40)
    df = pd.read_excel(io.BytesIO(r.content), header=4)
    df = df[['Ticker', 'Weight']].dropna()
    out = {}
    for _, row in df.iterrows():
        t = str(row['Ticker']).strip().upper()
        try: out[t] = float(row['Weight'])
        except: pass
    return out

def fetch_firsttrust(ticker):
    url = f'https://www.ftportfolios.com/retail/etf/etfholdings.aspx?Ticker={ticker}'
    html = requests.get(url, headers=UA, timeout=40).text
    out = {}
    try:
        tables = pd.read_html(io.StringIO(html))
        for tb in tables:
            cols = [str(x).lower() for x in tb.columns]
            tcol = next((tb.columns[i] for i, x in enumerate(cols) if 'ticker' in x or 'symbol' in x), None)
            wcol = next((tb.columns[i] for i, x in enumerate(cols) if 'weight' in x or '%' in x), None)
            if tcol is not None and wcol is not None:
                for _, row in tb.iterrows():
                    t = str(row[tcol]).strip().upper()
                    w = str(row[wcol]).replace('%', '').replace(',', '').strip()
                    try: out[t] = float(w)
                    except: pass
    except Exception as e:
        out['_err'] = str(e)[:60]
    return out

def fetch_ishares(prod_id, ticker, slug):
    """세션으로 product 페이지 방문 후 ajax csv."""
    s = requests.Session(); s.headers.update(UA)
    s.get(f'https://www.ishares.com/us/products/{prod_id}/{slug}', timeout=40)
    url = f'https://www.ishares.com/us/products/{prod_id}/{slug}/1467271812596.ajax?fileType=csv&fileName={ticker}_holdings&dataType=fund'
    r = s.get(url, timeout=40)
    txt = r.text
    if '<html' in txt[:200].lower():
        return {'_err': 'html_wall'}
    # CSV: skip 헤더 메타행, 'Ticker' 헤더 찾기
    lines = txt.splitlines()
    hi = next((i for i, l in enumerate(lines) if l.lower().startswith('"ticker"') or l.lower().startswith('ticker')), None)
    if hi is None: return {'_err': 'no_header'}
    df = pd.read_csv(io.StringIO('\n'.join(lines[hi:])))
    tcol = next((co for co in df.columns if 'ticker' in co.lower()), None)
    wcol = next((co for co in df.columns if 'weight' in co.lower()), None)
    out = {}
    for _, row in df.iterrows():
        t = str(row[tcol]).strip().upper()
        try: out[t] = float(str(row[wcol]).replace('%', '').replace(',', ''))
        except: pass
    return out

def fetch_api(ticker):
    """stockanalysis API (top25 캡)."""
    r = requests.get(f'https://stockanalysis.com/api/symbol/e/{ticker}/holdings', headers=UA, timeout=30).json()
    out = {}
    for h in r.get('data', {}).get('holdings', []):
        t = h['s'].replace('$', '').strip().upper()
        try: out[t] = float(h['as'].replace('%', ''))
        except: pass
    return out

# --- 후보 ETF ---
JOBS = [
    ('MDY', 'ssga', '중형주 S&P400'),
    ('XLI', 'ssga', '산업재'),
    ('XLK', 'ssga', '기술'),
    ('XLY', 'ssga', '경기소비'),
    ('XLF', 'ssga', '금융'),
    ('XLV', 'ssga', '헬스케어'),
    ('FTXL', 'ft', '반도체(FT)'),
    ('AIRR', 'ft', '산업르네상스(FT)'),
    ('GRID', 'ft', '스마트그리드(FT)'),
    ('FXR', 'ft', '산업AlphaDEX(FT)'),
    ('FXL', 'ft', '기술AlphaDEX(FT)'),
    ('FXD', 'ft', '경기소비AlphaDEX(FT)'),
    ('IWP', 'ish', '중형성장'),
    ('SOXX', 'ish', '반도체'),
    ('ITA', 'ish', '방산'),
    ('IWO', 'ish', '소형성장'),
]
ISH_IDS = {
    'IWP': ('239713', 'ishares-russell-mid-cap-growth-etf'),
    'SOXX': ('239705', 'ishares-semiconductor-etf'),
    'ITA': ('239502', 'ishares-us-aerospace-defense-etf'),
    'IWO': ('239712', 'ishares-russell-2000-growth-etf'),
}

results = []
for tk, src, desc in JOBS:
    try:
        if src == 'ssga': h = fetch_ssga(tk)
        elif src == 'ft': h = fetch_firsttrust(tk)
        elif src == 'ish':
            pid, slug = ISH_IDS[tk]; h = fetch_ishares(pid, tk, slug)
        if '_err' in h:
            # iShares 실패 시 API fallback
            h2 = fetch_api(tk)
            if h2:
                n, rwc, inw, hits = cov(h2)
                results.append((tk, desc, src + '/api25', len(h2), n, rwc, inw, hits)); continue
            results.append((tk, desc, src + '_ERR:' + h.get('_err', ''), 0, 0, 0, 0, set())); continue
        n, rwc, inw, hits = cov(h)
        results.append((tk, desc, src, len(h), n, rwc, inw, hits))
    except Exception as e:
        results.append((tk, desc, 'EXC:' + str(e)[:40], 0, 0, 0, 0, set()))

results.sort(key=lambda x: -x[5])
print(f'{"ETF":<6}{"설명":<16}{"소스":<11}{"보유수":>6}{"적중":>5}{"순위가중커버":>11}{"ETF내비중":>9}  적중종목')
for tk, desc, src, nh, n, rwc, inw, hits in results:
    dets = ' '.join(sorted(hits, key=lambda t: -W[t]))
    print(f'{tk:<6}{desc:<16}{src:<11}{nh:>6}{n:>5}{rwc:>10.1f}%{inw:>8.1f}%  {dets}')
