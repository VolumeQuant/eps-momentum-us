# -*- coding: utf-8 -*-
"""실험 배터리 — 저평가보유 순위상한 + 창의적 변형. faithful replay(DB랭크+yfinance MtM, 71종목)."""
import sqlite3, pandas as pd, statistics as st
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
PX = pd.read_pickle(r'C:\Users\user\AppData\Local\Temp\claude\C--dev-claude-code-eps-momentum-us\ea493dab-b81b-40fd-af7e-c266716091da\scratchpad\pxU.pkl')
c=sqlite3.connect(DB); cur=c.cursor()
EXIT_RANK=12
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
SEMI={'NVDA','AVGO','MU','LITE','AMAT','ASML','LRCX','MPWR','MCHP','ADI','TER','MKSI','FORM','COHR','CRDO','LSCC','NVMI','SIMO','AEIS','MTSI','KEYS','TSM','SNDK','STX','CGNX','CIEN','ANET'}

def run(ROT_CAP=999, PE_HOLD=30, rot_minseg=None, sector_guard=False, weak_only=False,
        entry_rank=5, slots=2, ban=(), d_lo=None, d_hi=None, log=False):
    tradable=set(PX.columns)
    rng=[d for d in all_dates if (d_lo is None or d>=d_lo) and (d_hi is None or d<=d_hi)]
    portfolio={}; nav=1.0; peak=1.0; mdd=0.0; nrot=0; tlog=[]
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
        # 최약체 슬롯 식별(weak_only용): 보유 중 순위 최악
        if portfolio:
            hr={tk:(wrank.get(tk) or 999) for tk in portfolio}
            weak_tk=max(hr,key=hr.get)
        else: weak_tk=None
        for tk in list(portfolio.keys()):
            cp=px(tk,d)
            if cp is None: continue
            it=dd.get(tk); sell=False; tag=''
            cap_applies = (not weak_only) or (tk==weak_tk)
            if it is None:
                if ROT_CAP>=999 or not cap_applies: continue
                sell=True; tag='회전(top30밖)'; nrot+=1
            else:
                rk=it['p2']; nc=it['nc']; m=minseg(it)
                if m<-2: sell=True; tag='EPS꺾임'
                elif rk>EXIT_RANK:
                    pe=(cp/nc) if (nc and nc>0) else 999
                    if pe>=PE_HOLD: sell=True; tag=f'순위{rk}+PER'
                    elif rk>ROT_CAP and cap_applies: sell=True; tag=f'회전(rank{rk})'; nrot+=1
            if sell:
                ep=portfolio[tk]['entry_price']
                if log: tlog.append((d,'SELL',tk,f"{(cp/ep-1)*100:+.0f}%",tag))
                del portfolio[tk]
        if len(portfolio)<slots:
            used={info['slot_idx'] for info in portfolio.values()}
            free=sorted([s for s in range(slots) if s not in used])
            held_sectors={'SEMI'} if (sector_guard and any(t in SEMI for t in portfolio)) else set()
            cands=[tk for tk,_ in elig if tk not in portfolio and tk in tradable and tk not in ban
                   and ms.get(tk,-9)>=0 and wrank.get(tk,999)<=entry_rank and (dd.get(tk,{}).get('dv') or 0)>=1000]
            cands.sort(key=lambda t:wrank.get(t,999))
            for tk in cands:
                if len(portfolio)>=slots: break
                if sector_guard and (tk in SEMI) and ('SEMI' in held_sectors): continue
                ip=px(tk,d); ix=free.pop(0) if free else len(portfolio)
                if ip:
                    portfolio[tk]={'entry_price':ip,'slot_idx':ix,'weight':0}
                    if tk in SEMI: held_sectors.add('SEMI')
                    if log: tlog.append((d,'BUY ',tk,f"rank{wrank.get(tk)}",''))
            n=len(portfolio)
            for info in portfolio.values(): info['weight']=100/n if n else 0
        else:
            n=len(portfolio)
            for info in portfolio.values(): info['weight']=100/n if n else 0
    return {'cum':(nav-1)*100,'mdd':mdd*100,'nrot':nrot,'hold':sorted(portfolio.keys()),'log':tlog}

def line(lbl,r): print(f"  {lbl:22} {r['cum']:>8.1f}% | MDD {r['mdd']:>6.1f}% | 회전 {r['nrot']:>2} | {r['hold']}")

print("="*70)
print("SANITY: baseline 재현 (목표 ~+216.9% DB값)")
print("="*70)
line('baseline', run())

print("\n"+"="*70); print("EXP-A: ROT_CAP 스윕 (저평가보유 순위상한)"); print("="*70)
base=run()['cum']
for cap in [999,40,30,25,20,15,12]:
    r=run(ROT_CAP=cap); print(f"  CAP {cap if cap<999 else 'base':>4}: {r['cum']:>8.1f}% (vs base {r['cum']-base:>+7.1f}p) | MDD {r['mdd']:>6.1f}% | 회전 {r['nrot']}")

print("\n"+"="*70); print("EXP-A2: walk-forward (3블록) — CAP25"); print("="*70)
splits=[(None,'2026-03-20','블록1'),('2026-03-20','2026-05-08','블록2'),('2026-05-08',None,'블록3')]
for lo,hi,lbl in splits:
    b=run(d_lo=lo,d_hi=hi)['cum']; r=run(ROT_CAP=25,d_lo=lo,d_hi=hi)['cum']
    print(f"  {lbl}: base {b:>7.1f}% | CAP25 {r:>7.1f}% | {r-b:>+7.1f}p")

print("\n"+"="*70); print("EXP-A3: LOWO (CAP25) — winner 착시 검증"); print("="*70)
for ban,l in [((),'전체'),(('MU',),'-MU'),(('SNDK',),'-SNDK'),(('STX',),'-STX'),(('NVDA',),'-NVDA'),(('MU','SNDK','STX','NVDA'),'-4winner')]:
    b=run(ban=ban)['cum']; r=run(ROT_CAP=25,ban=ban)['cum']
    print(f"  {l:12}: base {b:>8.1f}% | CAP25 {r:>8.1f}% | {r-b:>+7.1f}p")

print("\n"+"="*70); print("EXP-B: 회전 품질필터 (회전은 하되 신규는 그대로 ✅) — CAP25 기준 변형"); print("="*70)
line('CAP25 기본', run(ROT_CAP=25))
line('CAP25 + weak슬롯만', run(ROT_CAP=25, weak_only=True))
line('CAP25 + PE_HOLD20', run(ROT_CAP=25, PE_HOLD=20))
line('CAP25 + PE_HOLD40', run(ROT_CAP=25, PE_HOLD=40))

print("\n"+"="*70); print("EXP-C: 섹터 집중 가드 (반도체 2개 동시보유 금지) — 사용자 직감 측정"); print("="*70)
line('base (집중 허용)', run())
line('sector_guard', run(sector_guard=True))
line('CAP25', run(ROT_CAP=25))
line('CAP25 + sector_guard', run(ROT_CAP=25, sector_guard=True))

print("\n"+"="*70); print("EXP-D: 슬롯수 재검토 (ROT_CAP 하에서 slot3 부활하나?)"); print("="*70)
line('slot2 base', run(slots=2))
line('slot3 base', run(slots=3))
line('slot2 CAP25', run(ROT_CAP=25, slots=2))
line('slot3 CAP25', run(ROT_CAP=25, slots=3))

print("\n"+"="*70); print("EXP-E: 진입폭 (entry_rank) × CAP25"); print("="*70)
for er in [3,5,8]:
    line(f'entry≤{er} CAP25', run(ROT_CAP=25, entry_rank=er))
