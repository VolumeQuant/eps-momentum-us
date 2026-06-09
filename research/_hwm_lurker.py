# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
con=sqlite3.connect(DB); cur=con.cursor()

def hist(tk):
    rows=cur.execute("SELECT date,part2_rank,composite_rank,price FROM ntm_screening WHERE ticker=? ORDER BY date",(tk,)).fetchall()
    return rows

def summarize(tk):
    rows=hist(tk)
    in20=[r for r in rows if r[1] is not None and r[1]<=20]
    inTop=[r for r in rows if r[1] is not None and r[1]<=2]   # 진입권(part2<=2)
    days_total=len(rows)
    days_in20=len(in20)
    days_entry=len(inTop)
    # 첫 Top20 ~ 마지막 가격 수익
    prices=[r[3] for r in rows if r[3]]
    first_p=prices[0] if prices else None
    last_p=prices[-1] if prices else None
    # Top20 처음 든 날 가격 → 최신 가격
    p20_first=in20[0][3] if in20 else None
    print(f"\n=== {tk} ===")
    print(f"  관측일수 {days_total} | Top20일수 {days_in20} | 진입권(part2<=2)일수 {days_entry}")
    if p20_first and last_p:
        print(f"  Top20 첫등장({in20[0][0]}) 가격 {p20_first:.1f} → 최신 {last_p:.1f} = {last_p/p20_first-1:+.1%}")
    if first_p and last_p:
        print(f"  전체기간 가격 {first_p:.1f} → {last_p:.1f} = {last_p/first_p-1:+.1%}")
    # 최고/최저 part2
    p2s=[r[1] for r in rows if r[1] is not None]
    if p2s:
        print(f"  part2 best {min(p2s)} worst {max(p2s)}")

for tk in ['HWM','KEYS','MU','SNDK','STX','VIRT','FORM']:
    summarize(tk)

# HWM 상세 rank 궤적 (Top20 든 구간만)
print('\n\n=== HWM part2_rank 궤적 (Top30 이내인 날만) ===')
for d,p2,cr,px in hist('HWM'):
    if p2 is not None and p2<=30:
        print(f"  {d} part2={int(p2):2d} price={px:.1f}")
