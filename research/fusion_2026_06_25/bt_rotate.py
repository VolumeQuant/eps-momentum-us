# -*- coding: utf-8 -*-
"""저평가 보유에 순위상한(ROT_CAP) 추가 BT — 실제 yfinance 가격으로 mark-to-market.
가설: 보유종목이 top30 밖(또는 rank>CAP)으로 밀리면 PER 무관 매도→슬롯 회전(MU 포착)."""
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
        v=PX[tk].iloc[i]
        return float(v) if pd.notna(v) else None
    except Exception: return None
def minseg(v):
    nc,n7,n30,n60,n90=v['nc'],v['n7'],v['n30'],v['n60'],v['n90']
    return min((a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0 for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)])

def run(ROT_CAP=999, ban=()):
    portfolio={}; nav=1.0; peak=1.0; mdd=0.0; rot_events=[]
    # 유니버스: yfinance에 있는 종목만 사고팔 수 있음(BT 한계). DB rank는 전체 사용.
    tradable=set(PX.columns)
    for i in range(2,len(all_dates)):
        d=all_dates[i]; pv=all_dates[i-1]
        dd=data.get(d,{})
        ms={tk:minseg(v) for tk,v in dd.items()}
        wrank={tk:v['p2'] for tk,v in dd.items() if v.get('p2')}
        elig=sorted([(tk,v['p2']) for tk,v in dd.items() if ms.get(tk,0)>=-2 and v.get('p2')],key=lambda x:x[1])
        # mark to market
        dayret=0.0
        for tk,info in portfolio.items():
            w=info['weight']/100; cu=px(tk,d); pp=px(tk,pv)
            if cu and pp and pp>0: dayret+=w*(cu-pp)/pp*100
        nav*=(1+dayret/100); peak=max(peak,nav); mdd=min(mdd,nav/peak-1)
        # sells
        for tk in list(portfolio.keys()):
            cp=px(tk,d)
            if cp is None: continue
            it=dd.get(tk)
            sell=False; is_rot=False
            if it is None:
                # top30 밖: production=carryover 보유(baseline). 회전변형은 매도.
                if ROT_CAP>=999:
                    continue  # baseline 보유
                else:
                    sell=True; is_rot=True
            else:
                rk=it['p2']; nc=it['nc']; m=minseg(it)
                if m<-2: sell=True  # EPS꺾임
                elif rk>EXIT_RANK:
                    pe=(cp/nc) if (nc and nc>0) else 999
                    if pe>=PE_HOLD: sell=True
                    elif rk>ROT_CAP: sell=True; is_rot=True  # 싸도 순위 CAP밖→회전
            if sell:
                if is_rot: rot_events.append((d,tk,it['p2'] if it else None))
                del portfolio[tk]
        # buys
        if len(portfolio)<2:
            used={info['slot_idx'] for info in portfolio.values()}
            free=sorted([s for s in range(2) if s not in used])
            cands=[tk for tk,_ in elig if tk not in portfolio and tk not in ban and tk in tradable
                   and ms.get(tk,-9)>=0 and wrank.get(tk,999)<=5 and (dd.get(tk,{}).get('dv') or 0)>=1000]
            cands.sort(key=lambda t:wrank.get(t,999))
            for tk in cands:
                if len(portfolio)>=2: break
                ip=px(tk,d); ix=free.pop(0) if free else len(portfolio)
                if ip: portfolio[tk]={'entry_price':ip,'slot_idx':ix,'weight':50}
            for info in portfolio.values(): info['weight']=100 if len(portfolio)==1 else 50
    return (nav-1)*100, mdd*100, portfolio, rot_events

print("=== 저평가보유 순위상한(ROT_CAP) BT — yfinance mark-to-market ===")
print("(주의: 매매가능 종목=yfinance 9종목으로 제한된 근사 BT, baseline 절대값은 DB전체와 다름. 변형간 비교가 핵심)\n")
print(f"{'ROT_CAP':>9} {'누적%':>9} {'MDD%':>7}  최종보유")
for cap in [999,30,20,15,12]:
    cum,mdd,pf,rot=run(cap)
    lbl='baseline' if cap==999 else f'{cap}'
    print(f"{lbl:>9} {cum:>9.1f} {mdd:>7.1f}  {list(pf.keys())}  회전{len(rot)}건")

print("\n=== LOWO (MU/STX 착시 검증, ROT_CAP=30) ===")
for ban,l in [((),'전체'),(('MU',),'-MU'),(('STX',),'-STX'),(('SNDK',),'-SNDK'),(('MU','STX','SNDK'),'-MU-STX-SNDK')]:
    b=run(999,ban)[0]; r=run(30,ban)[0]
    print(f"  {l:14}: baseline {b:>8.1f}% | CAP30 {r:>8.1f}% | 회전이득 {r-b:>+8.1f}p")

_,_,_,rot=run(30)
print("\n=== CAP30에서 회전 매도된 이벤트(상위) ===")
for e in rot[:10]: print("  ",e)
