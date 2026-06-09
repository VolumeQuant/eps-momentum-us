# -*- coding: utf-8 -*-
"""staleness/tenure entry 필터 BT — "오래 Top20에 머물렀는데 진입 못함 + 정체"(HWM류) 거르기.
hold=baseline(MA12) 고정, entry만 변경. paired 300 + LOWO."""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import sqlite3
import numpy as np

DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
conn=sqlite3.connect(DB); cur=conn.cursor()
alld=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE price IS NOT NULL ORDER BY date')]
didx={d:i for i,d in enumerate(alld)}
pxh={}
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pxh.setdefault(tk,{})[d]=p
def _ma12(tk,d):
    i=didx.get(d)
    if i is None or i-11<0: return None
    v=[pxh.get(tk,{}).get(alld[j]) for j in range(i-11,i+1)]; v=[x for x in v if x]
    return sum(v)/len(v) if len(v)>=6 else None
def _mom(tk,d,n):
    i=didx.get(d)
    if i is None or i-n<0: return None
    p0=pxh.get(tk,{}).get(d); pn=pxh.get(tk,{}).get(alld[i-n])
    return (p0/pn-1) if (p0 and pn) else None

dts=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
ddx={d:i for i,d in enumerate(dts)}
DINFO={}; TOP20={}
for d in dts:
    info={}; t20=set()
    for tk,p2,nc,n7,n30,n60,n90 in cur.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',(d,)):
        segs=[(a-b)/abs(b)*100 if b and abs(b)>0.01 else 0 for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]]
        info[tk]=(p2,min(segs))
        if p2<=20: t20.add(tk)
    DINFO[d]=info; TOP20[d]=t20

def tenure(tk, k, T=20):
    """최근 T 거래일 중 Top20에 있었던 일수"""
    lo=max(0,k-T+1)
    return sum(1 for j in range(lo,k+1) if tk in TOP20[dts[j]])

def blocked(gate, tk, k, d):
    if gate is None: return False
    typ,par = gate
    if typ=='tenure':   # Top20 머문 일수 >= thr → stale 차단
        return tenure(tk,k,par[0]) >= par[1]
    if typ=='staleflat':# 오래 머뭄 + 가격 정체(mom<=0) → 차단
        T,thr=par; m=_mom(tk,d,T)
        return tenure(tk,k,T) >= thr and (m is not None and m<=0)
    return False

def replay(gate, i0, i1, exclude=()):
    port=set(); daily=[]
    for k in range(i0,i1+1):
        d=dts[k]; info=DINFO[d]
        for tk in list(port):
            it=info.get(tk)
            if tk in exclude: port.discard(tk); continue
            if it is not None and it[1]<-2: port.discard(tk); continue
            p2=it[0] if it else None
            if p2 is None or p2>10:
                cp=pxh.get(tk,{}).get(d)
                if cp is None: continue
                m=_ma12(tk,d)
                if m is not None and cp>m: continue
                port.discard(tk)
        if len(port)<2:
            for tk,(p2,ms) in sorted([(t,v) for t,v in info.items() if t not in port and v[0]<=2 and t not in exclude], key=lambda x:x[1][0]):
                if len(port)>=2: break
                if blocked(gate, tk, k, d): continue
                port.add(tk)
        if k+1<=i1 and port:
            d1=dts[k+1]; rr=[]
            for tk in port:
                p0=pxh.get(tk,{}).get(d); p1=pxh.get(tk,{}).get(d1)
                if p0 and p1: rr.append(p1/p0-1)
            daily.append(np.mean(rr) if rr else 0.0)
        elif k+1<=i1:
            daily.append(0.0)
    return (np.prod([1+x for x in daily])-1) if daily else 0.0

BASE=None
VARIANTS=[
    ('tenure_t20>=10', ('tenure',(20,10))),
    ('tenure_t20>=14', ('tenure',(20,14))),
    ('tenure_t30>=18', ('tenure',(30,18))),
    ('staleflat20_10', ('staleflat',(20,10))),
    ('staleflat20_8',  ('staleflat',(20,8))),
]
NMA=len(dts)
def paired(gate, exclude, n=300, seed=42):
    rng=np.random.RandomState(seed); wins=0; lifts=[]
    for _ in range(n):
        L=rng.randint(25,60)
        if L>=NMA: L=NMA-1
        i0=rng.randint(0,NMA-L); i1=i0+L
        b=replay(BASE,i0,i1,exclude); v=replay(gate,i0,i1,exclude)
        lifts.append(v-b); wins+= 1 if v>b+1e-9 else 0
    return wins,n,np.mean(lifts)*100

print('=== 전체 윈도(79일) ===')
fb=replay(BASE,0,NMA-1)
print(f'  baseline: {fb*100:+.1f}%')
for nm,g in VARIANTS:
    print(f'  {nm:16s}: {replay(g,0,NMA-1)*100:+.1f}%  (lift {(replay(g,0,NMA-1)-fb)*100:+.1f}%p)')

for label,exc in [('paired 300 (전체)',()),('LOWO -MU-SNDK',('MU','SNDK')),('LOWO -MU-SNDK-STX',('MU','SNDK','STX'))]:
    print(f'\n=== {label} ===')
    for nm,g in VARIANTS:
        w,n,lf=paired(g,exc,300)
        print(f'  {nm:16s}: wins {w}/{n}  meanlift {lf:+.2f}%p')

print('\n=== sanity: 06-05 HWM/KEYS tenure & 차단여부 ===')
k=ddx['2026-06-05']
for tk in ['HWM','KEYS','MU','SNDK']:
    t20=tenure(tk,k,20); t30=tenure(tk,k,30); m20=_mom(tk,'2026-06-05',20)
    print(f'  {tk}: Top20머문(20d)={t20} (30d)={t30} mom20={m20 and round(m20,3)} | tenure>=10차단={t20>=10} staleflat차단={t20>=10 and (m20 is not None and m20<=0)}')
