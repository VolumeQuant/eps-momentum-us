# -*- coding: utf-8 -*-
"""#3 이탈 로직 재최적화: 현행(rank>12 AND PE>=30, OR min_seg<-2) 외 변형.
min_seg임계·트레일링스탑·PE스파이크·EPS감속 매도. full universe 순위 고정, 이탈만 변형.
판정=paired+LOWO+walk-forward."""
import sqlite3, random, statistics as st, numpy as np, json
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
exec(open('research/_bt_universe_research.py',encoding='utf-8').read().split('elig_s=')[0].replace("cd \"","#"))
# 위 exec로 raw/dates/pf/minseg/conv/eligible/build_ranks/verified/sim 일부 로드 — sim은 새로 정의
import math
crF,p2F=build_ranks(0,0)
elig_s=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig_s,SAMP) for s in range(N)]
def widx(mm): return [i for i,d in enumerate(dates) if d[5:7] in mm]
def sim2(exclude=(),start=0,end=None,E=5,H=12,S=2,P=30,ms_thr=-2,tstop=None,pe_spike=None):
    if end is None: end=len(dates)
    held={};entry={};peak={};prev=None;val=1.0;pk=1.0;mdd=0
    for i in range(start,end):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,w in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);pk=max(pk,val);mdd=min(mdd,(val/pk-1)*100)
        o_d=raw[d]; p2d=p2F[d]
        for tk in list(held):
            o=o_d.get(tk)
            if o is None: continue
            px=o['price']
            if px: peak[tk]=max(peak.get(tk,px),px)
            # EPS꺾임
            if minseg(o)<ms_thr: del held[tk];continue
            # 트레일링스탑
            if tstop and px and peak.get(tk) and px/peak[tk]-1<tstop: del held[tk];continue
            _pe=px/o['nc'] if o.get('nc',0)>0 else 999
            # PE스파이크(순위 무관 과열매도)
            if pe_spike and _pe>=pe_spike: del held[tk];continue
            # 순위>H AND PE>=P
            p2=p2d.get(tk)
            if not (p2 is None or p2>H): continue
            if _pe>=P: del held[tk]
        if len(held)<S:
            cands=[]
            for tk,p2 in p2d.items():
                if tk in held or tk in exclude or p2>E: continue
                o=o_d[tk]
                if minseg(o)<0 or not o['price'] or not verified(crF,tk,i): continue
                if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
                if (o['dv'] or 0)<1000: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:S-len(held)]:
                held[tk]=1.0/S; entry[tk]=o_d[tk]['price']; peak[tk]=o_d[tk]['price']
        prev=dict(held)
    return (val-1)*100,mdd
def batt(lbl,**kw):
    cs=[];ms=[]
    for ch in seeds:
        for s in ch: c,m=sim2(start=s,**kw);cs.append(c);ms.append(m)
    full,fmdd=sim2(**kw)
    worst=st.mean(cs)
    for w in WINNERS:
        a=st.mean([sim2(exclude=(w,),start=s,**kw)[0] for ch in seeds for s in ch])
        worst=min(worst,a)
    seg=[sim2(start=widx(mm)[0],end=widx(mm)[-1]+1,**kw)[0] for mm in (['02','03'],['04','05'],['06'])]
    print(f'{lbl:<24}{full:>+8.0f}%{fmdd:>+7.0f}%{st.mean(cs):>+8.0f}%{st.mean(ms):>+6.0f}%{worst:>+8.0f}%{seg[0]:>+5.0f}{seg[1]:>+6.0f}',flush=True)
print(f'{"이탈 로직":<24}{"전기간":>8}{"전MDD":>7}{"paired":>8}{"pMDD":>6}{"LOWO":>8}{"2-3월":>6}{"4-5월":>6}')
batt('현행(H12·PE30·ms-2)')
print('--- min_seg(EPS꺾임) 임계 ---')
batt(' ms -3 (느슨)',ms_thr=-3); batt(' ms -1 (엄격)',ms_thr=-1); batt(' ms 0 (즉시)',ms_thr=0)
print('--- 트레일링스탑 추가 ---')
batt(' +TS -25%',tstop=-0.25); batt(' +TS -20%',tstop=-0.20); batt(' +TS -15%',tstop=-0.15)
print('--- PE스파이크(순위무관 과열매도) ---')
batt(' +PE>60 매도',pe_spike=60); batt(' +PE>50 매도',pe_spike=50); batt(' +PE>40 매도',pe_spike=40)
print('--- 조합 ---')
batt(' ms-1 +TS-20',ms_thr=-1,tstop=-0.20); batt(' +TS-20 +PE60',tstop=-0.20,pe_spike=60)
print('\n판정: paired·LOWO·전기간 현행보다 robust↑면 후보. MDD↓+perf유지면 가치.')
