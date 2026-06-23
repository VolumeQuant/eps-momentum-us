# -*- coding: utf-8 -*-
"""실제 매수 종목의 진입일 볼륨서지 실측. '시스템이 저볼륨만 산다' 주장 검증."""
import random, statistics as st, numpy as np, pandas as pd, yfinance as yf, warnings, sys
warnings.filterwarnings('ignore'); sys.stdout.reconfigure(encoding='utf-8')
exec(open('research/_bt_universe_research.py',encoding='utf-8').read().split('elig_s=')[0].replace('cd "','#'))
crF,p2F=build_ranks(0,0)
# 결정적 전기간 sim에서 진입 로그
entries=[]
def sim_log(E=5,H=12,S=2,P=30):
    held={};prev=None
    for i in range(len(dates)):
        d=dates[i];o_d=raw[d];p2d=p2F[d]
        for tk in list(held):
            o=o_d.get(tk)
            if o and minseg(o)<-2: del held[tk];continue
            if o is None: continue
            p2=p2d.get(tk)
            if not (p2 is None or p2>H): continue
            _pe=o['price']/o['nc'] if o.get('nc',0)>0 else 999
            if _pe>=P: del held[tk]
        if len(held)<S:
            cands=[]
            for tk,p2 in p2d.items():
                if tk in held or p2>E: continue
                o=o_d[tk]
                if minseg(o)<0 or not o['price'] or not verified(crF,tk,i): continue
                if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
                if (o['dv'] or 0)<1000: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:S-len(held)]:
                held[tk]=1.0/S; entries.append((d,tk,i))
        prev=dict(held)
sim_log()
print(f'전기간 실제 진입 {len(entries)}건:')
etk=sorted({e[1] for e in entries})
vol=yf.download(etk,start='2026-01-01',end='2026-06-20',progress=False,auto_adjust=True,threads=2)['Volume']
vol.index=pd.to_datetime(vol.index)
def vsurge(tk,dt):
    if tk not in vol.columns: return None
    s=vol[tk].dropna();idx=s.index[s.index<=pd.Timestamp(dt)]
    if len(idx)<21: return None
    j=s.index.get_loc(idx[-1])
    return s.iloc[j]/s.iloc[j-20:j].mean() if j>=20 else None
print(f'{"날짜":<12}{"종목":<7}{"진입일볼륨서지":>12}  판정')
surges=[]
for d,tk,i in entries:
    vr=vsurge(tk,d)
    if vr is not None:
        surges.append(vr)
        tag='저볼륨' if vr<0.8 else ('보통' if vr<1.5 else '★고볼륨(서지!)')
        print(f'{d:<12}{tk:<7}{vr:>11.2f}x  {tag}')
print(f'\n진입 {len(surges)}건 볼륨서지: 평균 {st.mean(surges):.2f}x, 중앙값 {st.median(surges):.2f}x')
print(f'  저볼륨(<0.8x): {sum(1 for v in surges if v<0.8)}건')
print(f'  보통(0.8~1.5): {sum(1 for v in surges if 0.8<=v<1.5)}건')
print(f'  ★고볼륨(>1.5x): {sum(1 for v in surges if v>=1.5)}건')
print(f'  >1.0x(평균이상): {sum(1 for v in surges if v>=1.0)}건')
