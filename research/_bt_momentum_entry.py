# -*- coding: utf-8 -*-
"""가격 모멘텀 entry 필터 BT — "깔짝이"(HWM류) 거르기.
진입(part2<=2)에 가격 모멘텀 조건 추가. hold 로직은 baseline 고정(MA12, valve 무관)으로
entry 효과만 격리. paired 300 + LOWO."""
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
def _ma(tk,d,n):
    i=didx.get(d)
    if i is None or i-(n-1)<0: return None
    v=[pxh.get(tk,{}).get(alld[j]) for j in range(i-(n-1),i+1)]; v=[x for x in v if x]
    return sum(v)/len(v) if len(v)>=max(6,n//2) else None
def _mom(tk,d,n):
    i=didx.get(d)
    if i is None or i-n<0: return None
    p0=pxh.get(tk,{}).get(d); pn=pxh.get(tk,{}).get(alld[i-n])
    return (p0/pn-1) if (p0 and pn) else None
def _ma12(tk,d): return _ma(tk,d,12)

dts=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
DINFO={}
for d in dts:
    info={}
    for tk,p2,nc,n7,n30,n60,n90 in cur.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',(d,)):
        segs=[(a-b)/abs(b)*100 if b and abs(b)>0.01 else 0 for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]]
        info[tk]=(p2,min(segs))
    DINFO[d]=info

def passes(gate, tk, d):
    if gate is None: return True
    typ,par = gate
    if typ=='mom':
        m=_mom(tk,d,par[0]); return (m is not None and m > par[1])
    if typ=='ma':
        ma=_ma(tk,d,par); cp=pxh.get(tk,{}).get(d); return (ma is not None and cp is not None and cp>ma)
    return True

def replay(gate, i0, i1, exclude=()):
    port=set()
    daily=[]
    for k in range(i0,i1+1):
        d=dts[k]; info=DINFO[d]
        # exit: baseline MA12 (valve 무관, entry효과 격리)
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
        # entry: part2<=2 + 모멘텀 게이트
        if len(port)<2:
            for tk,(p2,ms) in sorted([(t,v) for t,v in info.items() if t not in port and v[0]<=2 and t not in exclude], key=lambda x:x[1][0]):
                if len(port)>=2: break
                if not passes(gate, tk, d): continue
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
    ('mom20>0',  ('mom',(20,0.0))),
    ('mom20>5%', ('mom',(20,0.05))),
    ('mom10>0',  ('mom',(10,0.0))),
    ('price>MA20',('ma',20)),
    ('price>MA10',('ma',10)),
]
NMA=len(dts)

def paired(gate, exclude, n=300, seed=42):
    rng=np.random.RandomState(seed); wins=0; lifts=[]
    for _ in range(n):
        L=rng.randint(25,60);
        if L>=NMA: L=NMA-1
        i0=rng.randint(0,NMA-L); i1=i0+L
        b=replay(BASE,i0,i1,exclude); v=replay(gate,i0,i1,exclude)
        lifts.append(v-b); wins+= 1 if v>b+1e-9 else 0
    return wins,n,np.mean(lifts)*100

print('=== 전체 윈도(79일) ===')
fb=replay(BASE,0,NMA-1)
print(f'  baseline: {fb*100:+.1f}%')
for nm,g in VARIANTS:
    fv=replay(g,0,NMA-1)
    print(f'  {nm:12s}: {fv*100:+.1f}%  (lift {(fv-fb)*100:+.1f}%p)')

for label,exc in [('paired 300 (전체)',()),('LOWO -MU-SNDK',('MU','SNDK')),('LOWO -MU-SNDK-STX',('MU','SNDK','STX'))]:
    print(f'\n=== {label} ===')
    for nm,g in VARIANTS:
        w,n,lf=paired(g,exc,300)
        print(f'  {nm:12s}: wins {w}/{n}  meanlift {lf:+.2f}%p')

# sanity: HWM이 06-05에 모멘텀 게이트 통과하나?
print('\n=== sanity: 06-05 HWM/KEYS 모멘텀 ===')
for tk in ['HWM','KEYS']:
    print(f'  {tk}: mom20={_mom(tk,"2026-06-05",20)}  mom10={_mom(tk,"2026-06-05",10)} price>MA20={pxh[tk]["2026-06-05"]>_ma(tk,"2026-06-05",20) if _ma(tk,"2026-06-05",20) else None}')
