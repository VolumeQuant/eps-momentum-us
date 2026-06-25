# -*- coding: utf-8 -*-
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; cur=c.cursor()

def minseg(r):
    nc,n7,n30,n60,n90=r['ntm_current'],r['ntm_7d'],r['ntm_30d'],r['ntm_60d'],r['ntm_90d']
    segs=[]
    for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
        segs.append((a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0)
    return segs, min(segs)

for tk in ['LITE','NVDA']:
    print(f"\n===== {tk} =====")
    rows=cur.execute("""SELECT date,composite_rank,part2_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,
      adj_gap,rev_up30,rev_down30,high30 FROM ntm_screening WHERE ticker=? AND date>='2026-05-20' ORDER BY date""",(tk,)).fetchall()
    print(f"{'date':10} {'cr':>3} {'p2':>3} {'price':>8} {'ntm_cur':>8} {'ntm_90d':>8} {'eps90Δ%':>7} {'adj_gap':>8} {'min_seg':>7} {'up/dn':>6}")
    for r in rows:
        segs,ms=minseg(r)
        eps90=(r['ntm_current']-r['ntm_90d'])/abs(r['ntm_90d'])*100 if r['ntm_90d'] else 0
        cr = r['composite_rank'] if r['composite_rank'] is not None else '-'
        p2 = r['part2_rank'] if r['part2_rank'] is not None else '-'
        print(f"{r['date']:10} {str(cr):>3} {str(p2):>3} {r['price']:>8.1f} "
              f"{r['ntm_current']:>8.2f} {r['ntm_90d']:>8.2f} {eps90:>7.1f} {r['adj_gap']:>8.2f} {ms:>7.1f} {str(r['rev_up30'])+'/'+str(r['rev_down30']):>6}")
