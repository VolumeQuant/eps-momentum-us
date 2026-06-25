# -*- coding: utf-8 -*-
"""거래대금 필터 완화 BT — faithful production-replay (_get_system_performance 엔진 복제)
변경점은 매수후보 dollar_volume 게이트 단 하나. 순위(part2_rank)는 DB 그대로(재랭킹 안 함)."""
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c=sqlite3.connect(DB); cur=c.cursor()
EXIT_RANK=12; PE_HOLD=30

all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date").fetchall()]
print(f"DB part2_rank 기간: {all_dates[0]} ~ {all_dates[-1]} ({len(all_dates)}거래일)")

prices={}; data={}
for d in all_dates:
    prices[d]={r[0]:r[1] for r in cur.execute("SELECT ticker,price FROM ntm_screening WHERE date=?",(d,)).fetchall()}
    data[d]={r[0]:{'p2':r[2],'nc':r[3],'n7':r[4],'n30':r[5],'n60':r[6],'n90':r[7],'dv':r[8],'mcap':r[9]}
             for r in cur.execute("SELECT ticker,price,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d,market_cap FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,)).fetchall()}

def minseg(nc,n7,n30,n60,n90):
    segs=[]
    for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
        segs.append((a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0)
    return min(segs)

def gate(variant, dv, mcap):
    dv=dv or 0; mcap=mcap or 0
    if variant=='base_1000': return dv>=1000
    if variant=='relax_500': return dv>=500
    if variant=='relax_250': return dv>=250
    if variant=='no_filter': return True
    if variant=='size_cond':  # 시총 $10B+면 $500M, 아니면 $1B
        return dv>=500 if mcap>=10_000_000_000 else dv>=1000
    return True

def run(variant, start_idx=2):
    portfolio={}; nav=1.0; peak=1.0; mdd=0.0; trades=0
    bought=set()
    for i in range(start_idx,len(all_dates)):
        date=all_dates[i]; prev=all_dates[i-1]
        d=data.get(date,{}); pr=prices.get(date,{}); ppr=prices.get(prev,{})
        ms={tk:minseg(v['nc'],v['n7'],v['n30'],v['n60'],v['n90']) for tk,v in d.items()}
        wrank={tk:v['p2'] for tk,v in d.items() if v.get('p2')}
        elig=sorted([(tk,v['p2']) for tk,v in d.items() if ms.get(tk,0)>=-2 and v.get('p2')],key=lambda x:x[1])
        dayret=0.0
        for tk,info in portfolio.items():
            w=info['weight']/100.0; cu=pr.get(tk); pv=ppr.get(tk)
            if cu and pv and pv>0: dayret+=w*(cu-pv)/pv*100
        nav*=(1+dayret/100)
        peak=max(peak,nav); mdd=min(mdd,nav/peak-1)
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
            if sell: del portfolio[tk]; trades+=1
        # buys
        if len(portfolio)<2:
            used={info['slot_idx'] for info in portfolio.values()}
            free=sorted([s for s in range(2) if s not in used])
            cands=[tk for tk,_ in elig if tk not in portfolio and ms.get(tk,-999)>=0
                   and wrank.get(tk,999)<=5 and gate(variant, d.get(tk,{}).get('dv'), d.get(tk,{}).get('mcap'))]
            cands.sort(key=lambda t:wrank.get(t,999))
            for tk in cands:
                if len(portfolio)>=2: break
                idx=free.pop(0) if free else len(portfolio)
                portfolio[tk]={'entry_price':pr.get(tk),'slot_idx':idx,'weight':50}
                bought.add(tk); trades+=1
            pn=len(portfolio)
            for info in portfolio.values(): info['weight']=100 if pn==1 else 50
    return (nav-1)*100, mdd*100, trades, bought

variants=['base_1000','relax_500','relax_250','no_filter','size_cond']
print("\n=== 전체기간 faithful replay ===")
print(f"{'variant':12} {'누적%':>8} {'MDD%':>7} {'거래':>4}  {'KEYS샀나':>8}")
base_bought=None
for v in variants:
    cum,mdd,tr,bought=run(v)
    if v=='base_1000': base_bought=bought
    keys='O' if 'KEYS' in bought else 'X'
    print(f"{v:12} {cum:>8.1f} {mdd:>7.1f} {tr:>4}  {keys:>8}")

# 완화로 새로 매수된 종목들 (size_cond 기준)
_,_,_,bs=run('size_cond')
print(f"\nsize_cond에서 새로 매수된 종목(base 대비): {sorted(bs - base_bought)}")
print(f"no_filter에서 새로: {sorted(run('no_filter')[3] - base_bought)}")
