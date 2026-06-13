# -*- coding: utf-8 -*-
"""Part D: 종목 특성 추출 + 클러스터링 (자율주행 2026-06-13).
C1~C4(2축) 대신 다차원 특성(밸류/성장/마진/모멘텀/리스크)으로 KMeans → 자연 군집 발견.
각 클러스터의 전진20일 수익으로 '더 유용한 분류인가' 평가."""
import sqlite3
from collections import defaultdict
import numpy as np
import sys; sys.stdout.reconfigure(encoding='utf-8')
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
DB='eps_momentum_data.db'
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
didx={d:i for i,d in enumerate(dates)}
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
# 특성 추출: 랭킹 유니버스(top30)
cols=['rev_growth','operating_margin','gross_margin','roe','debt_to_equity','beta','market_cap','num_analysts','rev_up30','eps_chg_weighted','adj_gap','ntm_current']
q='SELECT date,ticker,part2_rank,price,'+','.join(cols)+' FROM ntm_screening WHERE part2_rank IS NOT NULL'
rows=cur.execute(q).fetchall()
con.close()
def price_chg(tk,d,lb=30):
    i=didx.get(d)
    if i is None or i-lb<0: return None
    p0=pf[dates[i-lb]].get(tk); p1=pf[d].get(tk)
    return (p1/p0-1)*100 if (p0 and p1) else None
def fwd(tk,d,k=20):
    i=didx.get(d)
    if i is None or i+k>=len(dates): return None
    p0=pf[d].get(tk); p1=pf[dates[i+k]].get(tk)
    return (p1/p0-1)*100 if (p0 and p1) else None

# feature matrix 구성
feat_names=['매출성장','영업마진','매출총이익','ROE','부채비율','베타','로그시총','애널수','상향수','EPS변화','괴리','fwd_PE','가격30d']
X=[]; meta=[]  # meta: (date,ticker,fwd20,c2flag)
for r in rows:
    d,tk,p2,price=r[0],r[1],r[2],r[3]
    vals=r[4:]
    rec=dict(zip(cols,vals))
    nc=rec['ntm_current']
    fpe=(price/nc) if (nc and nc>0) else None
    p30=price_chg(tk,d,30)
    f20=fwd(tk,d,20)
    row=[rec['rev_growth'],rec['operating_margin'],rec['gross_margin'],rec['roe'],rec['debt_to_equity'],
         rec['beta'], (np.log(rec['market_cap']) if rec['market_cap'] else None), rec['num_analysts'],
         rec['rev_up30'], rec['eps_chg_weighted'], rec['adj_gap'], fpe, p30]
    if any(v is None for v in row) or f20 is None: continue
    if fpe>500: continue  # outlier 제거
    X.append(row); meta.append((d,tk,f20,(rec['eps_chg_weighted']>0 and p30<0)))
X=np.array(X,dtype=float)
print(f'클러스터링 표본: {len(X)} (특성 {len(feat_names)}개)')
Xs=StandardScaler().fit_transform(X)
for k in [3,4,5]:
    km=KMeans(n_clusters=k,n_init=10,random_state=42).fit(Xs)
    lab=km.labels_
    print(f'\n=== K={k} 클러스터 ===')
    for c in range(k):
        idx=np.where(lab==c)[0]
        f20s=[meta[i][2] for i in idx]
        c2frac=sum(1 for i in idx if meta[i][3])/len(idx)*100
        # 클러스터 특성 평균(원본 스케일)
        prof=X[idx].mean(axis=0)
        avg20=np.mean(f20s); win=sum(1 for x in f20s if x>0)/len(f20s)*100
        # 대표 특성 3개 (z-score 절대값 큰 것)
        z=km.cluster_centers_[c]
        top=sorted(range(len(feat_names)),key=lambda j:-abs(z[j]))[:4]
        sig=', '.join(f'{feat_names[j]}{"↑" if z[j]>0 else "↓"}' for j in top)
        print(f'  C{c}: n={len(idx):>4} 전진20일 {avg20:>+6.1f}% 승률{win:>3.0f}% | C2비율{c2frac:>3.0f}% | 특징: {sig}')
    # 클러스터 간 수익 분산 (분류가 수익을 가르는가)
    cl_means=[np.mean([meta[i][2] for i in np.where(lab==c)[0]]) for c in range(k)]
    print(f'  → 클러스터간 전진수익 범위: {min(cl_means):+.1f}% ~ {max(cl_means):+.1f}% (spread {max(cl_means)-min(cl_means):.1f}p)')
print('\n해석: 어떤 클러스터가 전진수익 뚜렷이 높/낮으면 = C1/C2보다 유용한 분류 후보.')
print('비교 기준: C1/C2 spread는 ~3p (C2 buy-dip 우위). 클러스터가 이보다 크게 가르면 가치 있음.')
