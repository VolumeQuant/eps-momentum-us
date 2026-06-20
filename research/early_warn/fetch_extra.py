# -*- coding: utf-8 -*-
"""추가 매크로 시리즈 수집 → early_warn/extra.parquet (오프라인 BT용).
FRED는 CSV 직다운(rate limit 없음), 그 외는 yfinance(순차, burst 회피)."""
import sys, io, time, urllib.request
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path
OUT = Path(__file__).resolve().parent / 'extra.parquet'

def fred(series):
    url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    raw = urllib.request.urlopen(req, timeout=30).read().decode()
    df = pd.read_csv(io.StringIO(raw))
    df.columns = ['date', series]
    df['date'] = pd.to_datetime(df['date'])
    df[series] = pd.to_numeric(df[series], errors='coerce')
    return df.set_index('date')[series].dropna()

cols = {}
# --- FRED (긴 히스토리, 전체 커버) ---
for sid, nm in [('T10Y3M', 'term_10y3m'), ('T10Y2Y', 'term_10y2y'),
                ('BAMLH0A0HYM2', 'hy_oas'), ('NFCI', 'nfci'),
                ('DGS10', 'y10'), ('DTB3', 'y3m')]:
    try:
        s = fred(sid); cols[nm] = s
        print(f'FRED {sid:14} -> {nm:12} {s.index.min().date()}~{s.index.max().date()} n={len(s)}')
    except Exception as e:
        print(f'FRED {sid} ERR {e}')

# --- yfinance (ETF/지수, 순차) ---
import yfinance as yf
def yget(tk):
    for attempt in range(3):
        try:
            df = yf.download(tk, period='max', auto_adjust=True, progress=False, threads=False)
            cl = df['Close']
            if hasattr(cl, 'columns'):
                cl = cl.iloc[:, 0]
            cl = cl.dropna()
            if len(cl) > 50:
                return cl
        except Exception as e:
            print(f'  {tk} attempt{attempt} {e}')
        time.sleep(5)
    return None

yfmap = [('DX-Y.NYB', 'dxy'), ('GC=F', 'gold'), ('HG=F', 'copper'),
         ('RSP', 'rsp'), ('SPY', 'spy'), ('XLU', 'xlu'), ('XLY', 'xly'),
         ('XLP', 'xlp'), ('GLD', 'gld'), ('TLT', 'tlt'), ('HYG', 'hyg'), ('LQD', 'lqd'),
         ('^TNX', 'tnx'), ('^IRX', 'irx'), ('^SKEW', 'skew')]
for tk, nm in yfmap:
    s = yget(tk)
    if s is not None:
        s.index = pd.to_datetime(s.index).tz_localize(None)
        cols[nm] = s
        print(f'yf   {tk:10} -> {nm:8} {s.index.min().date()}~{s.index.max().date()} n={len(s)}')
    else:
        print(f'yf   {tk} FAILED')
    time.sleep(3)

df = pd.DataFrame(cols).sort_index()
df.to_parquet(OUT)
print(f'\n저장: {OUT}  shape={df.shape}  cols={list(df.columns)}')
