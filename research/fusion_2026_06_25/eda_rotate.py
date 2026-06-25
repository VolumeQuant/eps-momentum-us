# -*- coding: utf-8 -*-
"""Phase2: 보유종목 vs 미매수 상위종목 순위 비교 — 순위우선 교체 여지 검증"""
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c=sqlite3.connect(DB); cur=c.cursor()
EXIT_RANK=12; PE_HOLD=30
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date").fetchall()]
idx={d:i for i,d in enumerate(all_dates)}
prices={}; data={}
for d in all_dates:
    prices[d]={r[0]:r[1] for r in cur.execute("SELECT ticker,price FROM ntm_screening WHERE date=?",(d,)).fetchall()}
    data[d]={r[0]:{'p2':r[2],'nc':r[3],'n7':r[4],'n30':r[5],'n60':r[6],'n90':r[7],'dv':r[8]}
             for r in cur.execute("SELECT ticker,price,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,)).fetchall()}
def minseg(nc,n7,n30,n60,n90):
    return min((a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0 for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)])

# faithful replay, 매일 보유종목 순위 vs 미매수 top-ranked 비교 기록
portfolio={}; conflicts=[]
for i in range(2,len(all_dates)):
    date=all_dates[i]; prev=all_dates[i-1]
    d=data.get(date,{}); pr=prices.get(date,{}); ppr=prices.get(prev,{})
    ms={tk:minseg(v['nc'],v['n7'],v['n30'],v['n60'],v['n90']) for tk,v in d.items()}
    wrank={tk:v['p2'] for tk,v in d.items() if v.get('p2')}
    elig=sorted([(tk,v['p2']) for tk,v in d.items() if ms.get(tk,0)>=-2 and v.get('p2')],key=lambda x:x[1])
    for tk in list(portfolio.keys()):
        it=d.get(tk); cp=pr.get(tk)
        if it is None or cp is None: continue
        rk=wrank.get(tk); m=ms.get(tk,0); nc=it.get('nc')
        sell=False
        if m<-2: sell=True
        elif rk is None or rk>EXIT_RANK:
            pe=(cp/nc) if (cp and nc and nc>0) else 999
            if pe>=PE_HOLD: sell=True
        if sell: del portfolio[tk]
    # 교체 충돌 기록: 슬롯 full인데 미매수 종목이 보유종목보다 순위 높음
    if len(portfolio)>=2:
        held_ranks={tk:wrank.get(tk,999) for tk in portfolio}
        worst_held=max(held_ranks.values())
        worst_tk=max(held_ranks,key=held_ranks.get)
        for tk,rk in elig:
            if tk in portfolio: continue
            if (d.get(tk,{}).get('dv') or 0)<1000: continue
            if ms.get(tk,-9)<0: continue
            if rk < worst_held - 1:  # 미매수 종목이 최악 보유보다 2칸+ 상위
                # forward: 교체 시 이득? 미매수종목 - 보유종목 향후10d
                def f10(t):
                    i0=idx[date]; p0=pr.get(t)
                    if not p0: return None
                    for j in range(min(i0+10,len(all_dates)-1),i0,-1):
                        pj=prices[all_dates[j]].get(t)
                        if pj: return (pj-p0)/p0*100
                    return None
                fin=f10(tk); fout=f10(worst_tk)
                conflicts.append({'date':date,'in':tk,'in_rk':rk,'out':worst_tk,'out_rk':worst_held,
                                  'fin':fin,'fout':fout})
                break  # 하루 1건만

print(f"=== 순위교체 충돌 (미매수 종목이 보유 최약체보다 2칸+ 상위, $1B+✅) ===")
print(f"총 {len(conflicts)}일\n")
print(f"{'date':10} {'들어올':6} {'rk':>3} {'나갈':6} {'rk':>3} {'들어올10d':>9} {'나갈10d':>8} {'교체이득':>8}")
gains=[]
for x in conflicts:
    fin=x['fin']; fout=x['fout']
    g = (fin-fout) if (fin is not None and fout is not None) else None
    if g is not None: gains.append(g)
    fs=lambda v: f"{v:+.1f}%" if v is not None else "NA"
    print(f"{x['date']:10} {x['in']:6} {x['in_rk']:>3} {x['out']:6} {x['out_rk']:>3} {fs(fin):>9} {fs(fout):>8} {fs(g):>8}")
import statistics as st
if gains:
    print(f"\n교체 시 평균 10일 이득(들어올-나갈): {st.mean(gains):+.1f}%, 중앙 {st.median(gains):+.1f}%, 양수 {sum(1 for g in gains if g>0)}/{len(gains)}")
from collections import Counter
print("들어올 종목 분포:", Counter(x['in'] for x in conflicts))
print("나갈 종목 분포:", Counter(x['out'] for x in conflicts))
