# -*- coding: utf-8 -*-
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c = sqlite3.connect(DB); cur=c.cursor()
EPOCH='2026-06-11'; EXIT_RANK=12; PE_HOLD=30

all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL AND date>=? ORDER BY date",(EPOCH,)).fetchall()]
print("dates(>=epoch):",all_dates)

prices={}; data={}
for d in all_dates:
    prices[d]={r[0]:r[1] for r in cur.execute("SELECT ticker,price FROM ntm_screening WHERE date=?",(d,)).fetchall()}
    data[d]={r[0]:{'price':r[1],'p2':r[2],'nc':r[3],'n7':r[4],'n30':r[5],'n60':r[6],'n90':r[7],'dv':r[8]}
             for r in cur.execute("SELECT ticker,price,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,)).fetchall()}

def minseg(nc,n7,n30,n60,n90):
    segs=[]
    for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
        segs.append((a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0)
    return min(segs)

portfolio={}; nav=1.0
print(f"\n{'date':10} {'dayret%':>8} {'nav':>7}  holdings(entry→today)")
for i in range(2,len(all_dates)):
    date=all_dates[i]; prev=all_dates[i-1]
    d=data.get(date,{}); pr=prices.get(date,{}); ppr=prices.get(prev,{})
    ms={tk:minseg(v['nc'],v['n7'],v['n30'],v['n60'],v['n90']) for tk,v in d.items()}
    wrank={tk:v['p2'] for tk,v in d.items() if v.get('p2')}
    elig=sorted([(tk,v['p2']) for tk,v in d.items() if ms.get(tk,0)>=-2 and v.get('p2')],key=lambda x:x[1])
    # day return
    dayret=0.0
    for tk,info in portfolio.items():
        w=info['weight']/100.0; cu=pr.get(tk); pv=ppr.get(tk)
        if cu and pv and pv>0: dayret+=w*(cu-pv)/pv*100
    nav*=(1+dayret/100)
    # sells
    for tk in list(portfolio.keys()):
        ep=portfolio[tk]['entry_price']; it=d.get(tk); cp=pr.get(tk)
        if it is None or cp is None: continue
        rk=wrank.get(tk); m=ms.get(tk,0); nc=it.get('nc')
        sell=False
        if m<-2: sell=True
        elif rk is None or rk>EXIT_RANK:
            pe=(cp/nc) if (cp and nc and nc>0) else 999
            if pe>=PE_HOLD: sell=True
        if sell: del portfolio[tk]
    # buys
    if len(portfolio)<2:
        used={info['slot_idx'] for info in portfolio.values()}
        free=sorted([s for s in range(2) if s not in used])
        cands=[tk for tk,_ in elig if tk not in portfolio and ms.get(tk,-999)>=0
               and wrank.get(tk,999)<=5 and (d.get(tk,{}).get('dv') or 0)>=1000]
        cands.sort(key=lambda t:wrank.get(t,999))
        for tk in cands:
            if len(portfolio)>=2: break
            idx=free.pop(0) if free else len(portfolio)
            portfolio[tk]={'entry_price':pr.get(tk),'slot_idx':idx,'weight':50}
        pn=len(portfolio)
        for info in portfolio.values(): info['weight']=100 if pn==1 else 50
    hold=",".join(f"{tk}({(pr.get(tk)/portfolio[tk]['entry_price']-1)*100:+.1f}%)" for tk in portfolio)
    print(f"{date:10} {dayret:>8.2f} {nav:>7.4f}  {hold}")

print(f"\n시스템 누적 = {(nav-1)*100:+.2f}%  (n_days={len(all_dates)-2-1})")
