# -*- coding: utf-8 -*-
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
def ma12(tk,d):
    i=didx.get(d)
    if i is None or i-11<0: return None
    v=[pxh.get(tk,{}).get(alld[j]) for j in range(i-11,i+1)]; v=[x for x in v if x]
    return sum(v)/len(v) if len(v)>=6 else None
dts=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
DINFO={}
for d in dts:
    info={}
    for tk,p2,nc,n7,n30,n60,n90 in cur.execute('SELECT ticker,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',(d,)):
        segs=[(a-b)/abs(b)*100 if b and abs(b)>0.01 else 0 for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]]
        info[tk]=(p2,min(segs))
    DINFO[d]=info

# baseline replay, log every exit + whether it was a >=10% single-day gap (valve target)
port=set(); ever_held=set(); valve_events=[]; all_exits=[]
for k,d in enumerate(dts):
    info=DINFO[d]
    for tk in list(port):
        it=info.get(tk)
        if it is not None and it[1]<-2:
            all_exits.append((d,tk,'EPS꺾임',None)); port.discard(tk); continue
        p2=it[0] if it else None
        if p2 is None or p2>10:
            cp=pxh.get(tk,{}).get(d)
            if cp is None: continue
            m=ma12(tk,d)
            if m is not None and cp>m: continue
            # exit (MA12 break). check if single-day gap
            i=didx.get(d); pprev=pxh.get(tk,{}).get(alld[i-1]) if i>0 else None
            dayret=(cp/pprev-1)*100 if pprev else None
            all_exits.append((d,tk,'MA12깨짐',dayret))
            if dayret is not None and dayret <= -10:
                valve_events.append((d,tk,dayret))
            port.discard(tk)
    if len(port)<2:
        for tk,(p2,ms) in sorted([(t,v) for t,v in info.items() if t not in port and v[0]<=2], key=lambda x:x[1][0]):
            if len(port)>=2: break
            port.add(tk); ever_held.add(tk)

print('=== 윈도 전체에서 보유된 적 있는 종목 ===')
print(sorted(ever_held))
print('\nAEIS 보유된 적 있나?:', 'AEIS' in ever_held)

print('\n=== 모든 MA12깨짐 매도 (하루변동% 포함) ===')
for d,tk,why,dr in all_exits:
    if why=='MA12깨짐':
        flag = ' <== 보험밸브 발동(−10%+)' if (dr is not None and dr<=-10) else ''
        print(f'  {d} {tk:5s} 하루{dr:+.1f}%{flag}' if dr is not None else f'  {d} {tk:5s} 하루N/A')

print('\n=== 보험밸브(−10%+ 급락) 실제 수혜 이벤트 ===')
if valve_events:
    for d,tk,dr in valve_events:
        print(f'  {d} {tk:5s} 하루 {dr:+.1f}%')
else:
    print('  (없음)')
