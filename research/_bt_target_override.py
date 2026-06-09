# -*- coding: utf-8 -*-
"""목표가 홀드-오버라이드 BT (replay 기반, equal-weight daily 수익 cumulate).
baseline = 현행 MA12 추세홀드. override = MA12 깨져도 최근30일 목표가 강상향이면 보유 유지.
leave-MU/SNDK-out robustness 포함."""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import sqlite3, json
from datetime import datetime, timedelta
import numpy as np

DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
tgt = json.load(open(r'C:\dev\claude code\eps-momentum-us\research\_targets_cache.json'))
conn = sqlite3.connect(DB); cur = conn.cursor()
alld = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE price IS NOT NULL ORDER BY date')]
didx = {d:i for i,d in enumerate(alld)}
pxh={}
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pxh.setdefault(tk,{})[d]=p
def ma12(tk,d):
    i=didx.get(d)
    if i is None or i-11<0: return None
    v=[pxh.get(tk,{}).get(alld[j]) for j in range(i-11,i+1)]; v=[x for x in v if x]
    return sum(v)/len(v) if len(v)>=6 else None
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

dts=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]

def run(use_override, mag_thr=0.05, net_thr=2, exclude=()):
    port=set(); daily=[]; hold_log=[]
    for k,d in enumerate(dts):
        rows=cur.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',(d,)).fetchall()
        info={}
        for tk,p2,nc,n7,n30,n60,n90 in rows:
            if tk in exclude: continue
            segs=[]
            for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
                segs.append((a-b)/abs(b)*100 if b and abs(b)>0.01 else 0)
            info[tk]=dict(p2=p2,minseg=min(segs) if segs else 0)
        # exits
        for tk in list(port):
            it=info.get(tk)
            if it is not None and it['minseg']<-2: port.discard(tk); continue  # EPS꺾임은 오버라이드 안함(안전망)
            p2=it['p2'] if it else None
            if p2 is None or p2>10:
                cp=pxh.get(tk,{}).get(d)
                if cp is None: continue
                m=ma12(tk,d)
                if m is not None and cp>m: continue
                # 추세붕괴 — override 체크
                if use_override:
                    mag,net=tgt_mag(tk,d)
                    if mag>=mag_thr or net>=net_thr:
                        continue  # 목표가 강상향 → 보유 유지
                port.discard(tk)
        # entries (rank<=2, 빈슬롯)
        if len(port)<2:
            for tk,p2 in sorted([(tk,it['p2']) for tk,it in info.items() if tk not in port and it['p2']<=2], key=lambda x:x[1]):
                if len(port)>=2: break
                port.add(tk)
        hold_log.append((d,sorted(port)))
        # next-day equal-weight return
        if k+1<len(dts) and port:
            d1=dts[k+1]; rets=[]
            for tk in port:
                p0=pxh.get(tk,{}).get(d); p1=pxh.get(tk,{}).get(d1)
                if p0 and p1: rets.append(p1/p0-1)
            daily.append(np.mean(rets) if rets else 0.0)
        else:
            daily.append(0.0)
    cum=np.prod([1+x for x in daily])-1
    return cum, hold_log

base,bl = run(False)
ovr, ol = run(True)
print('=== 전체 (equal-weight cumulative, 79일) ===')
print(f'  baseline(현행 MA12)     : {base*100:+.1f}%')
print(f'  +목표가 홀드오버라이드   : {ovr*100:+.1f}%   (lift {(ovr-base)*100:+.1f}%p)')
print('  06-05 hold baseline:', dict(bl).get('2026-06-05'))
print('  06-05 hold override:', dict(ol).get('2026-06-05'))

print('\n=== leave-MU/SNDK-out (robustness) ===')
b2,_=run(False, exclude=('MU','SNDK'))
o2,_=run(True, exclude=('MU','SNDK'))
print(f'  baseline : {b2*100:+.1f}%')
print(f'  override : {o2*100:+.1f}%   (lift {(o2-b2)*100:+.1f}%p)')

print('\n=== leave-MU/SNDK/STX-out ===')
b3,_=run(False, exclude=('MU','SNDK','STX'))
o3,_=run(True, exclude=('MU','SNDK','STX'))
print(f'  baseline : {b3*100:+.1f}%')
print(f'  override : {o3*100:+.1f}%   (lift {(o3-b3)*100:+.1f}%p)')
