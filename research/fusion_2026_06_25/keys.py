# -*- coding: utf-8 -*-
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; cur=c.cursor()
dates=['2026-06-11','2026-06-12','2026-06-15','2026-06-16','2026-06-17','2026-06-18','2026-06-22','2026-06-23','2026-06-24']
print(f"{'date':10} {'KEYS':>8} {'LITE':>8} {'NVDA':>8} {'AVGO':>8}")
base={}
for d in dates:
    row={}
    for tk in ['KEYS','LITE','NVDA','AVGO']:
        r=cur.execute("SELECT price FROM ntm_screening WHERE ticker=? AND date=?",(tk,d)).fetchone()
        row[tk]=r[0] if r else None
        if d==dates[0]: base[tk]=row[tk]
    print(f"{d:10} "+" ".join(f"{row[tk]:>8.1f}" if row[tk] else f"{'NA':>8}" for tk in ['KEYS','LITE','NVDA','AVGO']))
print("\n=== 9거래일 누적 수익률 (06-11 종가 기준) ===")
for tk in ['KEYS','LITE','NVDA','AVGO']:
    last=cur.execute("SELECT price FROM ntm_screening WHERE ticker=? AND date='2026-06-24'",(tk,)).fetchone()[0]
    print(f"  {tk}: {(last/base[tk]-1)*100:+.1f}%  ({base[tk]:.1f} → {last:.1f})")
# 실제 보유 진입가 기준 (트레이스: NVDA 06-15 진입 212.4, LITE 06-18 진입 850.0)
print("\n=== 우리가 실제 산 것 (진입가 기준, 06-24) ===")
nvda=cur.execute("SELECT price FROM ntm_screening WHERE ticker='NVDA' AND date='2026-06-24'").fetchone()[0]
lite=cur.execute("SELECT price FROM ntm_screening WHERE ticker='LITE' AND date='2026-06-24'").fetchone()[0]
print(f"  NVDA: 진입 212.4(06-15) → 199.0 = {(nvda/212.4-1)*100:+.1f}%")
print(f"  LITE: 진입 850.0(06-18) → 842.5 = {(lite/850.0-1)*100:+.1f}%")
print(f"  KEYS 만약 06-11 샀으면: {(cur.execute(chr(34)+'SELECT price FROM ntm_screening WHERE ticker=' + chr(39)+'KEYS'+chr(39)+' AND date='+chr(39)+'2026-06-24'+chr(39)).fetchone()[0]/base['KEYS']-1)*100:+.1f}%")
