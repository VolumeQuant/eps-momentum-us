# -*- coding: utf-8 -*-
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c=sqlite3.connect(DB); cur=c.cursor()
# SNDK/STX(보유) vs MU/FORM/TTMI(놓침) 순위 — 3월말~5월초
print(f"{'date':10} {'SNDK':>5} {'STX':>5} {'MU':>5} {'FORM':>5} {'TTMI':>5}  (part2_rank, 못 들면 -)")
dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE date>='2026-03-25' AND date<='2026-05-05' ORDER BY date")]
for d in dates:
    row={}
    for tk in ['SNDK','STX','MU','FORM','TTMI']:
        r=cur.execute("SELECT part2_rank FROM ntm_screening WHERE ticker=? AND date=?",(tk,d)).fetchone()
        row[tk]=r[0] if (r and r[0] is not None) else None
    f=lambda v: f"{v}" if v is not None else "-"
    print(f"{d:10} {f(row['SNDK']):>5} {f(row['STX']):>5} {f(row['MU']):>5} {f(row['FORM']):>5} {f(row['TTMI']):>5}")
