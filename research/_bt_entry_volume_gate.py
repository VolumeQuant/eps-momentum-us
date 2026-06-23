# -*- coding: utf-8 -*-
"""진입 볼륨게이트: 물량털기 가설(상승+고볼륨=분산) 트레이딩 테스트.
top2 진입 시 그 종목 볼륨서지(당일/20일평균)가 높으면 회피(=저볼륨 매집만 진입).
대조군: 고볼륨 요구(통념 볼륨확인)도 같이. 순위 고정, 진입 게이트만 변형.
판정=paired(100×3)+LOWO최악+walk-forward+MDD."""
import random, statistics as st, numpy as np, pandas as pd, yfinance as yf, warnings, sys
warnings.filterwarnings('ignore'); sys.stdout.reconfigure(encoding='utf-8')
exec(open('research/_bt_universe_research.py',encoding='utf-8').read().split('elig_s=')[0].replace('cd "','#'))
crF,p2F=build_ranks(0,0)
# 진입 가능권(p2<=15 한번이라도) 종목 볼륨 fetch
ent_tk=sorted({tk for d in dates for tk,o in raw[d].items() if p2F[d].get(tk,99)<=15})
print(f'진입권 종목 {len(ent_tk)}개 볼륨 수집...',flush=True)
vol=yf.download(ent_tk,start='2026-01-01',end='2026-06-20',progress=False,auto_adjust=True,threads=2)['Volume']
vol.index=pd.to_datetime(vol.index)
dstr=[pd.Timestamp(d) for d in dates]
volr={}  # (tk,i)->볼륨서지
for tk in ent_tk:
    if tk not in vol.columns: continue
    s=vol[tk].dropna()
    for i,ts in enumerate(dstr):
        idx=s.index[s.index<=ts]
        if len(idx)<21: continue
        j=s.index.get_loc(idx[-1])
        if j>=20: volr[(tk,i)]=s.iloc[j]/s.iloc[j-20:j].mean()
def sim_g(exclude=(),start=0,end=None,E=5,H=12,S=2,P=30,vgate=None,vreq=None):
    if end is None: end=len(dates)
    held={};prev=None;val=1.0;peak=1.0;mdd=0
    for i in range(start,end):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,w in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        o_d=raw[d]; p2d=p2F[d]
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
                if tk in held or tk in exclude or p2>E: continue
                o=o_d[tk]
                if minseg(o)<0 or not o['price'] or not verified(crF,tk,i): continue
                if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
                if (o['dv'] or 0)<1000: continue
                vr=volr.get((tk,i))
                # 볼륨게이트: 서지 높으면 회피(물량털기), 또는 저볼륨이면 요구(통념)
                if vgate is not None and vr is not None and vr>vgate: continue
                if vreq is not None and (vr is None or vr<vreq): continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:S-len(held)]: held[tk]=1.0/S
        prev=dict(held)
    return (val-1)*100,mdd
elig_s=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig_s,SAMP) for s in range(N)]
def widx(mm): return [i for i,d in enumerate(dates) if d[5:7] in mm]
def batt(lbl,**kw):
    cs=[sim_g(start=s,**kw)[0] for ch in seeds for s in ch];ms=[sim_g(start=s,**kw)[1] for ch in seeds for s in ch]
    full,fmdd=sim_g(**kw)
    worst=st.mean(cs)
    for w in WINNERS: worst=min(worst,st.mean([sim_g(exclude=(w,),start=s,**kw)[0] for ch in seeds for s in ch]))
    seg=[sim_g(start=widx(mm)[0],end=widx(mm)[-1]+1,**kw)[0] for mm in (['02','03'],['04','05'],['06'])]
    print(f'{lbl:<22}{full:>+8.0f}%{fmdd:>+6.0f}%{st.mean(cs):>+8.0f}%{worst:>+8.0f}%{seg[0]:>+5.0f}{seg[1]:>+6.0f}{seg[2]:>+6.0f}',flush=True)
print(f'\n[volr 커버 {len(volr)}건]')
print(f'{"진입 볼륨게이트":<22}{"전기간":>8}{"MDD":>6}{"paired":>8}{"LOWO":>8}{"2-3월":>5}{"4-5월":>6}{"6월":>6}')
batt('현행(게이트X)')
print('--- 물량털기 회피: 고볼륨 진입 차단 ---')
batt(' 서지>2.0 차단',vgate=2.0); batt(' 서지>1.5 차단',vgate=1.5); batt(' 서지>1.2 차단',vgate=1.2)
print('--- 대조군: 고볼륨 요구(통념 볼륨확인) ---')
batt(' 서지>=1.0 요구',vreq=1.0); batt(' 서지>=1.3 요구',vreq=1.3)
print('\n판정: 고볼륨차단이 paired·LOWO 현행↑ + MDD↓면 = 물량털기 가설 트레이딩 유효.')
print('      대조군(고볼륨요구)이 더 나으면 = 통념(볼륨확인)이 맞고 가설 기각.')
