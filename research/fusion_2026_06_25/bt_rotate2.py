# -*- coding: utf-8 -*-
import sqlite3, pandas as pd
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
PX = pd.read_pickle(r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad\px.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
EXIT_RANK=12; PE_HOLD=30
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date").fetchall()]
data={}
for d in all_dates:
    data[d]={r[0]:{'p2':r[1],'nc':r[2],'n7':r[3],'n30':r[4],'n60':r[5],'n90':r[6],'dv':r[7]}
             for r in cur.execute("SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,)).fetchall()}
pxidx={d.strftime('%Y-%m-%d'):i for i,d in enumerate(PX.index)}
def px(tk,d):
    i=pxidx.get(d)
    if i is None: return None
    try:
        v=PX[tk].iloc[i]; return float(v) if pd.notna(v) else None
    except Exception: return None
def minseg(v):
    return min((a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0 for a,b in [(v['nc'],v['n7']),(v['n7'],v['n30']),(v['n30'],v['n60']),(v['n60'],v['n90'])])

def run(ROT_CAP=999, d_lo=None, d_hi=None, log=False):
    portfolio={}; nav=1.0; peak=1.0; mdd=0.0; tlog=[]
    tradable=set(PX.columns)
    rng=[d for d in all_dates if (d_lo is None or d>=d_lo) and (d_hi is None or d<=d_hi)]
    for k in range(2,len(rng)):
        d=rng[k]; pv=rng[k-1]; dd=data.get(d,{})
        ms={tk:minseg(v) for tk,v in dd.items()}
        wrank={tk:v['p2'] for tk,v in dd.items() if v.get('p2')}
        elig=sorted([(tk,v['p2']) for tk,v in dd.items() if ms.get(tk,0)>=-2 and v.get('p2')],key=lambda x:x[1])
        dayret=0.0
        for tk,info in portfolio.items():
            w=info['weight']/100; cu=px(tk,d); pp=px(tk,pv)
            if cu and pp and pp>0: dayret+=w*(cu-pp)/pp*100
        nav*=(1+dayret/100); peak=max(peak,nav); mdd=min(mdd,nav/peak-1)
        for tk in list(portfolio.keys()):
            cp=px(tk,d)
            if cp is None: continue
            it=dd.get(tk); sell=False; tag=''
            if it is None:
                if ROT_CAP>=999: continue
                sell=True; tag=f'회전(top30밖)'
            else:
                rk=it['p2']; nc=it['nc']; m=minseg(it)
                if m<-2: sell=True; tag='EPS꺾임'
                elif rk>EXIT_RANK:
                    pe=(cp/nc) if (nc and nc>0) else 999
                    if pe>=PE_HOLD: sell=True; tag=f'순위{rk}+PER{pe:.0f}'
                    elif rk>ROT_CAP: sell=True; tag=f'회전(rank{rk})'
            if sell:
                ep=portfolio[tk]['entry_price']
                if log: tlog.append((d,'SELL',tk,f"{(cp/ep-1)*100:+.0f}%",tag))
                del portfolio[tk]
        if len(portfolio)<2:
            used={info['slot_idx'] for info in portfolio.values()}
            free=sorted([s for s in range(2) if s not in used])
            cands=[tk for tk,_ in elig if tk not in portfolio and tk in tradable
                   and ms.get(tk,-9)>=0 and wrank.get(tk,999)<=5 and (dd.get(tk,{}).get('dv') or 0)>=1000]
            cands.sort(key=lambda t:wrank.get(t,999))
            for tk in cands:
                if len(portfolio)>=2: break
                ip=px(tk,d); ix=free.pop(0) if free else len(portfolio)
                if ip:
                    portfolio[tk]={'entry_price':ip,'slot_idx':ix,'weight':50}
                    if log: tlog.append((d,'BUY ',tk,f"rank{wrank.get(tk)}",''))
            for info in portfolio.values(): info['weight']=100 if len(portfolio)==1 else 50
    return (nav-1)*100, mdd*100, tlog

print("=== CAP30 실제 매매 로그 (SNDK winner 버리는지 확인) ===")
_,_,tl=run(30,log=True)
for e in tl: print("  ",*e)

print("\n=== 시기 분할 robustness (baseline vs CAP30) ===")
mid='2026-04-21'
for lo,hi,lbl in [(None,mid,'전반(2/12~4/21)'),(mid,None,'후반(4/21~6/24)')]:
    b=run(999,lo,hi)[0]; r=run(30,lo,hi)[0]
    print(f"  {lbl:18}: baseline {b:>7.1f}% | CAP30 {r:>7.1f}% | {r-b:>+7.1f}p")
