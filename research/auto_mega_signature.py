# -*- coding: utf-8 -*-
"""B 트랙: 메가종목(>+50% 대박) 조기 탐지 시그니처 연구
질문: MU(+103%)/SNDK(+126%)가 터지기 '전에' 감지 가능한 특징이 있었나?
가설: rev_growth(매출)가 아니라 NTM EPS 추정치 자체의 폭발적 상향(분석가 대규모 업그레이드).
방법:
 1. 각 종목 실현 최대 forward 수익(20d) + 전 기간 가격상승 → mega 판별
 2. 진입권(part2≤5) 시점 특징: NTM revision 배율, rev_growth, PEG, fwd_PE, 분석가 breadth
 3. mega vs 비mega 특징 분리도
 4. 조기 탐지: 종목 '첫 진입' 시점에 이미 시그널이 있었나 (look-ahead 없이)
 5. False positive: 높은 시그널인데 mega 안 된 종목
 ⚠️ N=2~3 = 과적합 위험 극도. 탐색용, 배포 불가.
"""
import sys, sqlite3, statistics
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
ROOT=Path(__file__).parent.parent
con=sqlite3.connect(ROOT/'eps_momentum_data.db');cur=con.cursor()
all_dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE price IS NOT NULL ORDER BY date')]
didx={d:i for i,d in enumerate(all_dates)};px=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): px[tk][d]=p
def fwd(tk,d,n=20):
    i=didx.get(d)
    if i is None or i+n>=len(all_dates): return None
    p0=px[tk].get(d);p1=px[tk].get(all_dates[i+n]); return (p1/p0-1)*100 if p0 and p1 else None

# 종목별 전 기간 최대 가격상승 (첫 등장 대비 peak)
def max_gain(tk):
    ps=[px[tk][d] for d in sorted(px[tk])]
    if len(ps)<2: return None
    return (max(ps)/ps[0]-1)*100

# 진입권 관측 + 특징
rows=cur.execute('''SELECT date,ticker,part2_rank,price,ntm_current,ntm_90d,rev_growth,num_analysts,rev_up30 FROM ntm_screening WHERE part2_rank<=5 ORDER BY date''').fetchall()
obs=defaultdict(list)
for d,tk,p2,price,nc,n90,rg,na,up in rows:
    if not price or not nc or nc<=0: continue
    ntm_rev=(nc/n90-1)*100 if n90 and n90>0 else None
    fwd_pe=price/nc
    peg=fwd_pe/(rg*100) if rg and rg>0 else None
    obs[tk].append(dict(d=d,p2=p2,ntm_rev=ntm_rev,rg=rg,fwd_pe=fwd_pe,peg=peg,na=na,up=up,fr20=fwd(tk,d,20)))

# mega 판별: 최대 가격상승 >50%
print('='*95)
print('B트랙: 메가종목 시그니처 — 진입권 도달 종목 전수 (최대상승 내림차순)')
print('='*95)
print(f'{"tk":<6}{"최대상승":>8}{"NTM상향배율":>11}{"rev_g":>7}{"fwd_PE":>7}{"PEG":>6}{"분석가↑":>7}{"등장":>5}')
recs=[]
for tk,lst in obs.items():
    mg=max_gain(tk)
    # 첫 진입 시점 특징 (조기 탐지: 가장 이른 등장)
    first=min(lst,key=lambda x:x['d'])
    # 최대 NTM 상향 (전 등장 중)
    ntmmax=max((x['ntm_rev'] for x in lst if x['ntm_rev'] is not None),default=None)
    recs.append((tk,mg,first['ntm_rev'],ntmmax,first['rg'],first['fwd_pe'],first['peg'],first['up'],len(lst)))
recs.sort(key=lambda x:(x[1] if x[1] is not None else -1),reverse=True)
for tk,mg,ntm_first,ntmmax,rg,pe,peg,up,n in recs:
    if n<1: continue
    mgs=f'{mg:+.0f}%' if mg is not None else '   -'
    nfs=f'{ntm_first:+.0f}%' if ntm_first is not None else '  -'
    rgs=f'{rg*100:.0f}%' if rg is not None else ' -'
    pegs=f'{peg:.2f}' if peg is not None else '  -'
    print(f'{tk:<6}{mgs:>8}{nfs:>11}{rgs:>7}{pe:>7.1f}{pegs:>6}{up if up else 0:>7}{n:>5}')

# mega(>50%) vs 비mega 특징 평균
mega=[r for r in recs if r[1] is not None and r[1]>50]
nonmega=[r for r in recs if r[1] is not None and r[1]<=50]
print(f'\n--- mega(최대상승>50%, n={len(mega)}) vs 비mega(n={len(nonmega)}) 첫진입 특징 평균 ---')
def avg(lst,idx):
    v=[r[idx] for r in lst if r[idx] is not None]; return statistics.mean(v) if v else None
print(f'  {"특징":<16}{"mega":>10}{"비mega":>10}')
for label,idx in [('첫진입 NTM상향',2),('최대 NTM상향',3),('rev_growth',4),('fwd_PE',5),('PEG',6),('분석가↑수',7)]:
    m=avg(mega,idx);nm=avg(nonmega,idx)
    ms=f'{m:.2f}' if m is not None else '-'; nms=f'{nm:.2f}' if nm is not None else '-'
    print(f'  {label:<16}{ms:>10}{nms:>10}')

# 조기 탐지 시뮬: NTM상향 임계로 mega 잡으면 precision/recall
print('\n--- 조기탐지 룰 후보: 첫진입 NTM상향배율 ≥ 임계 → "메가후보" precision/recall ---')
print(f'  {"임계":<8}{"잡힌종목":<28}{"그중mega":>8}{"precision":>10}{"recall":>8}')
for thr in [40,60,80,100,120]:
    flagged=[r for r in recs if r[2] is not None and r[2]>=thr]
    tp=[r for r in flagged if r[1] is not None and r[1]>50]
    prec=len(tp)/len(flagged)*100 if flagged else 0
    rec=len(tp)/len(mega)*100 if mega else 0
    names=','.join(r[0] for r in flagged[:6])
    print(f'  ≥{thr:<6}{names:<28}{len(tp):>8}{prec:>9.0f}%{rec:>7.0f}%')

# NTM상향 vs 최대상승 상관
print('\n--- NTM상향배율(최대) vs 실현 최대상승 상관 ---')
import math
pairs=[(r[3],r[1]) for r in recs if r[3] is not None and r[1] is not None]
if len(pairs)>=5:
    xs=[a for a,b in pairs];ys=[b for a,b in pairs]
    mx=statistics.mean(xs);my=statistics.mean(ys)
    num=sum((a-mx)*(b-my) for a,b in pairs)
    dx=math.sqrt(sum((a-mx)**2 for a in xs));dy=math.sqrt(sum((b-my)**2 for b in ys))
    print(f'  corr = {num/(dx*dy):+.3f} (n={len(pairs)})')
con.close()
