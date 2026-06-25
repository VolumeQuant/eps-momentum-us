# -*- coding: utf-8 -*-
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; cur=c.cursor()
D='2026-06-24'
def seg(new,old):
    if old is None or abs(old)<0.01 or new is None: return 0.0
    return max(-100,min(100,(new-old)/abs(old)*100))

rows=cur.execute("""SELECT ticker,composite_rank,part2_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,
  rev_up30,rev_down30,num_analysts,adj_gap FROM ntm_screening WHERE date=? AND ntm_current IS NOT NULL""",(D,)).fetchall()
out=[]
for r in rows:
    s1=seg(r['ntm_current'],r['ntm_7d']); s2=seg(r['ntm_7d'],r['ntm_30d'])
    recent30=s1+s2
    eps90=(r['ntm_current']-r['ntm_90d'])/abs(r['ntm_90d'])*100 if r['ntm_90d'] else 0
    # 품질: 애널 5+, 상향 3+, 하향이 상향보다 적음, 턴어라운드(저베이스) 배제 |ntm|>=2
    if (r['num_analysts'] or 0)>=5 and (r['rev_up30'] or 0)>=3 and (r['rev_down30'] or 0)<=(r['rev_up30'] or 0) and abs(r['ntm_current'])>=2:
        out.append((r['ticker'],r['composite_rank'],r['part2_rank'],s1,s2,recent30,eps90,
                    r['rev_up30'],r['rev_down30'],r['num_analysts'],r['adj_gap']))

print("품질 통과 종목 중 '최근 30일 EPS 상향' 상위 20 (애널5+ · 상향3+ · 하향<=상향 · 베이스>=$2)")
print(f"{'tkr':6} {'cr':>4} {'p2':>4} {'7d%':>6} {'30d%':>6} {'recent%':>7} {'90d%':>6} {'up/dn':>7} {'adj_gap':>8}")
for t in sorted(out,key=lambda x:-x[5])[:20]:
    p2=t[2] if t[2] is not None else '-'
    print(f"{t[0]:6} {str(t[1]):>4} {str(p2):>4} {t[3]:>6.1f} {t[4]:>6.1f} {t[5]:>7.1f} {t[6]:>6.1f} {str(t[7])+'/'+str(t[8]):>7} {t[10]:>8.2f}")
