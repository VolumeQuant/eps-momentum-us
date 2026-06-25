# -*- coding: utf-8 -*-
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; cur=c.cursor()
D='2026-06-24'

def seg(new,old):
    if old is None or abs(old)<0.01 or new is None: return 0.0
    return max(-100,min(100,(new-old)/abs(old)*100))

# MU recent presence
print("=== MU 최근 DB 기록 ===")
mu=cur.execute("SELECT date,composite_rank,part2_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_90d,rev_up30,rev_down30 FROM ntm_screening WHERE ticker='MU' AND date>='2026-06-01' ORDER BY date",).fetchall()
if not mu: print("  MU 없음 (유니버스/EPS상향 미충족)")
for r in mu:
    print(f"  {r['date']} cr={r['composite_rank']} p2={r['part2_rank']} px={r['price']} ntm={r['ntm_current']} up/dn={r['rev_up30']}/{r['rev_down30']}")

# All stocks on 06-24: recent revision strength
rows=cur.execute("""SELECT ticker,composite_rank,part2_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,
  rev_up30,rev_down30,num_analysts FROM ntm_screening WHERE date=? AND ntm_current IS NOT NULL""",(D,)).fetchall()
data=[]
for r in rows:
    s1=seg(r['ntm_current'],r['ntm_7d'])  # last 7d
    s2=seg(r['ntm_7d'],r['ntm_30d'])      # 7-30d
    s3=seg(r['ntm_30d'],r['ntm_60d'])
    recent30=s1+s2                         # last ~30d revision
    eps90=(r['ntm_current']-r['ntm_90d'])/abs(r['ntm_90d'])*100 if r['ntm_90d'] else 0
    data.append((r['ticker'],r['composite_rank'],r['part2_rank'],s1,s2,recent30,eps90,
                 r['rev_up30'],r['rev_down30'],r['num_analysts']))

print("\n=== 최근 7일(seg1) EPS 상향 상위 15 ===")
print(f"{'tkr':6} {'cr':>4} {'p2':>4} {'7d%':>6} {'30d%':>6} {'recent30%':>9} {'90d%':>6} {'up/dn':>7}")
for t in sorted(data,key=lambda x:-x[3])[:15]:
    print(f"{t[0]:6} {str(t[1]):>4} {str(t[2]):>4} {t[3]:>6.1f} {t[4]:>6.1f} {t[5]:>9.1f} {t[6]:>6.1f} {str(t[7])+'/'+str(t[8]):>7}")

print("\n=== 최근 30일(seg1+seg2) EPS 상향 상위 15 ===")
for t in sorted(data,key=lambda x:-x[5])[:15]:
    print(f"{t[0]:6} {str(t[1]):>4} {str(t[2]):>4} {t[3]:>6.1f} {t[4]:>6.1f} {t[5]:>9.1f} {t[6]:>6.1f} {str(t[7])+'/'+str(t[8]):>7}")
