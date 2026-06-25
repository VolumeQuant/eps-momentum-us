# -*- coding: utf-8 -*-
import sqlite3, yfinance as yf, pandas as pd, pickle, os
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
SP=r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad'
c=sqlite3.connect(DB); cur=c.cursor()
tks=sorted({r[0] for r in cur.execute("SELECT DISTINCT ticker FROM ntm_screening WHERE composite_rank IS NOT NULL")})
print(f"{len(tks)} tickers fetch")
df=yf.download(tks,start='2026-01-15',end='2026-06-25',auto_adjust=False,progress=False)['Close']
df.to_pickle(SP+r'\px137.pkl')
print('px saved', df.shape, 'ok cols', int(df.iloc[-1].notna().sum()))
# trailing EPS (PIT) for these
pit={}
miss=[]
for tk in tks:
    try:
        qi=yf.Ticker(tk).quarterly_income_stmt
        if qi is None or qi.empty: miss.append(tk); continue
        row=None
        for k in ['Diluted EPS','Basic EPS']:
            if k in qi.index: row=qi.loc[k]; break
        if row is None: miss.append(tk); continue
        q=row.dropna().sort_index(); qe=list(q.items())
        rec=[((qe[j][0]+pd.Timedelta(days=45)).strftime('%Y-%m-%d'),float(sum(qe[j-k][1] for k in range(4)))) for j in range(3,len(qe))]
        if rec: pit[tk]=rec
        else: miss.append(tk)
    except Exception: miss.append(tk)
pickle.dump(pit,open(SP+r'\pit137.pkl','wb'))
print(f'PIT EPS: {len(pit)}/{len(tks)}, missing {len(miss)}')
