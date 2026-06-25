import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c = sqlite3.connect(DB); c.row_factory = sqlite3.Row; cur = c.cursor()
D = '2026-06-24'

def price_30d_ago(tk):
    r = cur.execute("SELECT price FROM ntm_screening WHERE ticker=? AND date<? AND price IS NOT NULL ORDER BY date DESC LIMIT 1 OFFSET 29",(tk,D)).fetchone()
    cnt = cur.execute("SELECT COUNT(*) FROM ntm_screening WHERE ticker=? AND date<? AND price IS NOT NULL",(tk,D)).fetchone()[0]
    return (r[0] if r else None), cnt

rows = cur.execute("""SELECT ticker, part2_rank, composite_rank, price, eps_chg_weighted, adj_gap,
  ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening
  WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank""",(D,)).fetchall()

def seg(new,old):
    if abs(new)<0.01 or abs(old)<0.01: return 0
    return max(-100,min(100,(new-old)/abs(old)*100))

print(f"{'p2':>3} {'tkr':6} {'epsΔw%':>7} {'px':>8} {'px30d':>8} {'pxΔ30%':>7} {'dir':>6} {'class':>5}  note")
counts={'C1':0,'C2':0,'C3':0,'C4':0,'?':0}
for r in rows:
    tk=r['ticker']; ec=r['eps_chg_weighted']; px=r['price']
    p30,cnt=price_30d_ago(tk)
    pxchg=(px-p30)/p30*100 if (p30 and p30>0) else None
    s1=seg(r['ntm_current'],r['ntm_7d']); s2=seg(r['ntm_7d'],r['ntm_30d'])
    s3=seg(r['ntm_30d'],r['ntm_60d']); s4=seg(r['ntm_60d'],r['ntm_90d'])
    if any(abs(s)>=100 for s in (s1,s2,s3,s4)): direction=9.0
    else: direction=(s1+s2)/2-(s3+s4)/2
    # quadrant by EPS revision (eps_chg_weighted) x price 30d change
    cls='?'
    if ec is not None and pxchg is not None:
        if ec>0 and pxchg>0: cls='C1'
        elif ec>0 and pxchg<0: cls='C2'
        elif ec<0 and pxchg>0: cls='C3'
        elif ec<0 and pxchg<0: cls='C4'
    counts[cls]+=1
    note='' if cnt>=30 else f'hist={cnt}(부족)'
    ecs=f'{ec:+.2f}' if ec is not None else 'None'
    p30s=f'{p30:.1f}' if p30 else 'NA'
    pxs=f'{pxchg:+.1f}' if pxchg is not None else 'NA'
    print(f"{r['part2_rank']:>3} {tk:6} {ecs:>7} {px:>8.1f} {p30s:>8} {pxs:>7} {direction:>6.1f} {cls:>5}  {note}")
print("\ncounts:",counts)
