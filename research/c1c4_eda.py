# -*- coding: utf-8 -*-
"""C1~C4 사분면 EDA + 검증 (자율주행 2026-06-13).
C1 EPS↑가격↑ / C2 EPS↑가격↓(buy-dip) / C3 EPS↓가격↑(고평가) / C4 EPS↓가격↓(약세).
분류: eps_chg_weighted × price_30d. 검증: 사분면별 전진수익률(5d/20d)·승률·IC.
6/12 Top20 분포 + 일자별 분포 추이."""
import sqlite3, statistics as st
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'
con=sqlite3.connect(DB); cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
didx={d:i for i,d in enumerate(dates)}
# 전체 가격 이력 (price_30d, forward return용)
px=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    px[tk][d]=p
# 랭킹 유니버스 데이터 (top30)
rows=cur.execute('SELECT date,ticker,part2_rank,composite_rank,eps_chg_weighted,price FROM ntm_screening WHERE part2_rank IS NOT NULL').fetchall()
con.close()

def price_chg(tk,d,lb=30):
    i=didx.get(d)
    if i is None or i-lb<0: return None
    p0=px[tk].get(dates[i-lb]); p1=px[tk].get(d)
    return (p1/p0-1)*100 if (p0 and p1) else None

def fwd_ret(tk,d,k):
    i=didx.get(d)
    if i is None or i+k>=len(dates): return None
    p0=px[tk].get(d); p1=px[tk].get(dates[i+k])
    return (p1/p0-1)*100 if (p0 and p1) else None

def classify(eps_w,p30):
    if eps_w is None or p30 is None: return None
    if eps_w>0 and p30>0: return 'C1'
    if eps_w>0 and p30<0: return 'C2'
    if eps_w<0 and p30>0: return 'C3'
    if eps_w<0 and p30<0: return 'C4'
    return None

# 분류 + 전진수익률 집계
buckets=defaultdict(lambda: {'n':0,'f5':[],'f20':[]})
by_date_q=defaultdict(lambda: defaultdict(int))
top20_612=[]
for d,tk,p2,cr,epsw,price in rows:
    p30=price_chg(tk,d,30)
    q=classify(epsw,p30)
    if q is None: continue
    by_date_q[d][q]+=1
    f5=fwd_ret(tk,d,5); f20=fwd_ret(tk,d,20)
    b=buckets[q]; b['n']+=1
    if f5 is not None: b['f5'].append(f5)
    if f20 is not None: b['f20'].append(f20)
    if d==dates[-1] and p2 is not None and p2<=20:
        top20_612.append((p2,tk,q,round(epsw,1) if epsw else None,round(p30,1) if p30 else None))

print(f'데이터 {dates[0]}~{dates[-1]} ({len(dates)}일)\n')
print('='*70)
print(f'■ Part A: 최신일({dates[-1]}) Top20 C1~C4 분포')
print('='*70)
qcnt=defaultdict(list)
for p2,tk,q,ew,p30 in sorted(top20_612):
    qcnt[q].append(tk)
    print(f'  {p2:>2}위 {tk:<6} {q}  (EPS_w={ew}, 가격30d={p30}%)')
print('  분포:', {q:len(v) for q,v in sorted(qcnt.items())})
print()
print('='*70)
print('■ Part B: 사분면별 전진수익률 (전 기간 top30 유니버스 — 정의 검증)')
print('='*70)
print(f'{"사분면":<22}{"표본":>6}{"5일평균":>9}{"5일승률":>8}{"20일평균":>10}{"20일승률":>9}')
desc={'C1':'EPS↑가격↑(추세)','C2':'EPS↑가격↓(buy-dip)','C3':'EPS↓가격↑(고평가)','C4':'EPS↓가격↓(약세)'}
for q in ['C1','C2','C3','C4']:
    b=buckets[q]
    f5=b['f5']; f20=b['f20']
    if not f5: continue
    w5=sum(1 for x in f5 if x>0)/len(f5)*100
    w20=sum(1 for x in f20 if x>0)/len(f20)*100 if f20 else 0
    print(f'{q} {desc[q]:<18}{b["n"]:>6}{st.mean(f5):>+8.2f}%{w5:>7.0f}%{st.mean(f20):>+9.2f}%{w20:>8.0f}%')
print('\n해석: 우리 정리(C2 알파/C3 위험)가 맞으면 → C2 전진수익 최고, C3 최저여야.')
print()
print('='*70)
print('■ 일자별 C1~C4 분포 추이 (월별 비중%)')
print('='*70)
mon=defaultdict(lambda: defaultdict(int))
for d,qd in by_date_q.items():
    for q,n in qd.items(): mon[d[:7]][q]+=n
print(f'{"월":<9}{"C1%":>7}{"C2%":>7}{"C3%":>7}{"C4%":>7}{"총":>6}')
for m in sorted(mon):
    qd=mon[m]; tot=sum(qd.values())
    print(f'{m:<9}'+''.join(f'{qd.get(q,0)/tot*100:>6.0f}%' for q in ['C1','C2','C3','C4'])+f'{tot:>6}')
