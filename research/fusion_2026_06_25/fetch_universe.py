# -*- coding: utf-8 -*-
import sqlite3, yfinance as yf, pandas as pd
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c=sqlite3.connect(DB); cur=c.cursor()
# 회전 관련 종목: 언젠가 part2_rank<=12였던 모든 종목 (매수가능+보유가능 풀)
tks=sorted({r[0] for r in cur.execute("SELECT DISTINCT ticker FROM ntm_screening WHERE part2_rank<=12").fetchall()})
print(f"top12 ever 종목수: {len(tks)}")
print(tks)
df=yf.download(tks,start='2026-02-01',end='2026-06-25',auto_adjust=False,progress=False)['Close']
df.to_pickle(r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad\pxU.pkl')
print('saved', df.shape, 'cols ok:', sum(df.iloc[-1].notna()), '/', df.shape[1])
