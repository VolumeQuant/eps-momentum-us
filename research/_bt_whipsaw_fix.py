# -*- coding: utf-8 -*-
"""휩쏘 해결 BT — MA12 매도 confirm-days / buffer 변형.
established 기준: paired 100×3 random subwindow + LOWO robustness.
exit rule = f(state). baseline N=1 (현행). 통과 기준 = LOWO 양수(과적합 아님)."""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import sqlite3, json
import numpy as np

DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
conn = sqlite3.connect(DB); cur = conn.cursor()
alld=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE price IS NOT NULL ORDER BY date')]
didx={d:i for i,d in enumerate(alld)}
pxh={}
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pxh.setdefault(tk,{})[d]=p
def ma12(tk,d):
    i=didx.get(d)
    if i is None or i-11<0: return None
    v=[pxh.get(tk,{}).get(alld[j]) for j in range(i-11,i+1)]; v=[x for x in v if x]
    return sum(v)/len(v) if len(v)>=6 else None
dts=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
# precompute per-date info
DINFO={}
for d in dts:
    info={}
    for tk,p2,nc,n7,n30,n60,n90 in cur.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',(d,)):
        segs=[(a-b)/abs(b)*100 if b and abs(b)>0.01 else 0 for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]]
        info[tk]=(p2,min(segs))
    DINFO[d]=info

def replay(exit_mode, i0, i1, exclude=()):
    """exit_mode: ('confirm',N) or ('buffer',b). i0..i1 inclusive subwindow index into dts."""
    typ,par = exit_mode
    port={}  # tk -> consecutive breakdown count
    daily=[]; trades=0
    for k in range(i0, i1+1):
        d=dts[k]; info=DINFO[d]
        # exits
        for tk in list(port):
            it=info.get(tk)
            if tk in exclude: port.pop(tk,None); continue
            if it is not None and it[1]<-2:  # EPS꺾임 = 즉시(안전망, 불변)
                port.pop(tk,None); trades+=1; continue
            p2 = it[0] if it else None
            if p2 is None or p2>10:
                cp=pxh.get(tk,{}).get(d)
                if cp is None: continue  # carryover
                m=ma12(tk,d)
                if m is None: continue
                if typ=='confirm':
                    broken = cp<=m
                    if broken:
                        port[tk]+=1
                        if port[tk]>=par: port.pop(tk,None); trades+=1
                    else:
                        port[tk]=0
                elif typ=='buffer':
                    if cp <= m*(1-par): port.pop(tk,None); trades+=1
                    # else hold (no counter)
                elif typ=='gapconfirm':
                    # 깨졌을 때: 하루 -par% 이상 급락이면 1일 유예, 아니면 즉시매도
                    if cp<=m:
                        i=didx.get(d); pprev=pxh.get(tk,{}).get(alld[i-1]) if i and i>0 else None
                        dayret=(cp/pprev-1) if pprev else 0.0
                        if dayret < -par and port[tk]==0:
                            port[tk]=1  # 급락 첫날 = 유예
                        else:
                            port.pop(tk,None); trades+=1
                    else:
                        port[tk]=0
            else:
                port[tk]=0  # back in favor, reset counter
        # entries (rank<=2, 빈슬롯)
        if len(port)<2:
            for tk,(p2,ms) in sorted([(t,v) for t,v in info.items() if t not in port and v[0]<=2 and t not in exclude], key=lambda x:x[1][0]):
                if len(port)>=2: break
                port[tk]=0; trades+=1
        # next-day eq-weight return
        if k+1<=i1 and port:
            d1=dts[k+1]; rr=[]
            for tk in port:
                p0=pxh.get(tk,{}).get(d); p1=pxh.get(tk,{}).get(d1)
                if p0 and p1: rr.append(p1/p0-1)
            daily.append(np.mean(rr) if rr else 0.0)
        elif k+1<=i1:
            daily.append(0.0)
    cum=np.prod([1+x for x in daily])-1 if daily else 0.0
    return cum, trades

BASE=('confirm',1)
VARIANTS=[('gapconfirm',0.05),('gapconfirm',0.06),('gapconfirm',0.07),('gapconfirm',0.08),('gapconfirm',0.10),('gapconfirm',0.12)]
NMA=len(dts)

def paired(variant, exclude, n=300, seed=0):
    rng=np.random.RandomState(seed)
    wins=0; lifts=[]
    for _ in range(n):
        L=rng.randint(25, 60)  # subwindow length
        if L>=NMA: L=NMA-1
        i0=rng.randint(0, NMA-L)
        i1=i0+L
        b,_=replay(BASE,i0,i1,exclude)
        v,_=replay(variant,i0,i1,exclude)
        lifts.append(v-b)
        if v>b+1e-9: wins+=1
    return wins, n, np.mean(lifts)*100

print('=== 전체 윈도(79일) 단일 + 회전수 ===')
fb,tb=replay(BASE,0,NMA-1)
print(f'  baseline(confirm1)  : {fb*100:+.1f}%  trades={tb}')
for v in VARIANTS:
    fc,tc=replay(v,0,NMA-1)
    print(f'  {str(v):18s}: {fc*100:+.1f}%  trades={tc}  (lift {(fc-fb)*100:+.1f}%p)')

print('\n=== paired 300 subwindow (vs baseline) ===')
for v in VARIANTS:
    w,n,lf=paired(v,(),300,seed=42)
    print(f'  {str(v):18s}: wins {w}/{n}  meanlift {lf:+.2f}%p')

print('\n=== LOWO: -MU-SNDK (robustness 핵심) ===')
for v in VARIANTS:
    w,n,lf=paired(v,('MU','SNDK'),300,seed=42)
    print(f'  {str(v):18s}: wins {w}/{n}  meanlift {lf:+.2f}%p')

print('\n=== LOWO: -MU-SNDK-STX ===')
for v in VARIANTS:
    w,n,lf=paired(v,('MU','SNDK','STX'),300,seed=42)
    print(f'  {str(v):18s}: wins {w}/{n}  meanlift {lf:+.2f}%p')
