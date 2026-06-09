# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import sqlite3, json
from datetime import datetime, timedelta
import numpy as np
from scipy.stats import spearmanr

DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
tgt = json.load(open(r'C:\dev\claude code\eps-momentum-us\research\_targets_cache.json'))
conn = sqlite3.connect(DB); cur = conn.cursor()
dts=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
didx={d:i for i,d in enumerate(dts)}
px={}
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    px.setdefault(tk,{})[d]=p
def dparse(s): return datetime.strptime(s,'%Y-%m-%d')
def tgt_mag(tk,d,lb=30):
    d0=dparse(d); lo=d0-timedelta(days=lb); mag=0.0; net=0
    for r in tgt.get(tk,[]):
        rd=dparse(r['date'])
        if lo<rd<=d0:
            pta=(r['pta'] or '').lower()
            if r['prior']>0 and r['cur']>0 and 'rais' in pta: mag+=(r['cur']-r['prior'])/r['prior']; net+=1
            elif r['prior']>0 and r['cur']>0 and 'lower' in pta: mag+=(r['cur']-r['prior'])/r['prior']; net-=1
    return mag,net

def fwd(tk,d,k=5):
    i=didx.get(d)
    if i is None or i+k>=len(dts): return None
    p0=px.get(tk,{}).get(d); p1=px.get(tk,{}).get(dts[i+k])
    return (p1/p0-1) if (p0 and p1) else None

# IC among TOP candidates (part2_rank<=10) — 결정이 일어나는 지점
mags=[]; fwds=[]; p2s=[]
for d in dts:
    for tk,p2 in cur.execute('SELECT ticker,part2_rank FROM ntm_screening WHERE date=? AND part2_rank<=10',(d,)):
        f=fwd(tk,d,5)
        if f is None: continue
        m,_=tgt_mag(tk,d)
        mags.append(m); fwds.append(f); p2s.append(p2)
mags=np.array(mags); fwds=np.array(fwds); p2s=np.array(p2s)
print(f'상위후보(part2<=10) 표본 n={len(mags)}')
print('목표가 mag30 vs fwd5 IC :', spearmanr(mags,fwds))
print('part2_rank  vs fwd5 IC :', spearmanr(p2s,fwds), '(음수=낮은순위가 고수익, 현행신호 유효성)')
# 목표가 모멘텀 있음/없음 그룹 fwd5
hi = mags>0.02
print(f'  목표가 상향 강함(mag>0.02) fwd5 평균: {fwds[hi].mean()*100:+.2f}%  n={hi.sum()}')
print(f'  목표가 잠잠(mag<=0.02)     fwd5 평균: {fwds[~hi].mean()*100:+.2f}%  n={(~hi).sum()}')

# 진입 재정렬 BT: 매일 ✅후보(part2<=10) 중 진입을 (A)part2 top2 vs (B)목표가mag top2 vs (C)blend
def run_entry(mode):
    port=[]; daily=[]
    for k,d in enumerate(dts):
        cand=[]
        for tk,p2,nc,n7,n30,n60,n90 in cur.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',(d,)):
            segs=[(a-b)/abs(b)*100 if b and abs(b)>0.01 else 0 for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]]
            cand.append((tk,p2,min(segs)))
        cinfo={c[0]:(c[1],c[2]) for c in cand}
        # exit: rank>10 or minseg<-2 (단순 — 진입효과 격리 위해 MA12 생략, 동일 조건 비교)
        port=[tk for tk in port if cinfo.get(tk) and cinfo[tk][0]<=10 and cinfo[tk][1]>=-2]
        if len(port)<2:
            pool=[(tk,p2) for tk,p2,ms in cand if tk not in port and p2<=10 and ms>=0]
            if mode=='part2': pool.sort(key=lambda x:x[1])
            elif mode=='tgt': pool.sort(key=lambda x:-tgt_mag(x[0],d)[0])
            elif mode=='blend': pool.sort(key=lambda x: x[1]-tgt_mag(x[0],d)[0]*5)  # 목표가 보너스
            for tk,_ in pool:
                if len(port)>=2: break
                if tk not in port: port.append(tk)
        if k+1<len(dts) and port:
            d1=dts[k+1]; rr=[]
            for tk in port:
                p0=px.get(tk,{}).get(d); p1=px.get(tk,{}).get(d1)
                if p0 and p1: rr.append(p1/p0-1)
            daily.append(np.mean(rr) if rr else 0.0)
        else: daily.append(0.0)
    return np.prod([1+x for x in daily])-1

print('\n=== 진입 선정 방식 비교 (exit 동일, 진입만 변경) ===')
for mode in ['part2','tgt','blend']:
    print(f'  {mode:6s}: {run_entry(mode)*100:+.1f}%')
