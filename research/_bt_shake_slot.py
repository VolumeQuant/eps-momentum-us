# -*- coding: utf-8 -*-
"""개미털기 전용 3번 슬롯: winner 2슬롯(p2F 정상) + 슬롯3은 AMZN프로파일 저볼륨이탈 반등주 전용.
대조군: 일반 slot3(아무 후보, 과거 -90p). 슬롯비면 cash(winner 안 건드림). weight=1/보유수.
판정=paired+LOWO+walk-forward+MDD vs 현행 2슬롯."""
import random, statistics as st, numpy as np, pandas as pd, yfinance as yf, warnings, sys
warnings.filterwarnings('ignore'); sys.stdout.reconfigure(encoding='utf-8')
exec(open('research/_bt_universe_research.py',encoding='utf-8').read().split('elig_s=')[0].replace('cd "','#'))
crF,p2F=build_ranks(0,0)
ranked=sorted({tk for d in dates for tk,o in raw[d].items() if o['ads'] and o['ads']>9})
print(f'볼륨 수집 {len(ranked)}종목...',flush=True)
vol=yf.download(ranked,start='2026-01-01',end='2026-06-20',progress=False,auto_adjust=True,threads=2)['Volume']
vol.index=pd.to_datetime(vol.index);dstr=[pd.Timestamp(d) for d in dates]
streak={};shake_ok={};shake_rank={}  # AMZN프로파일 이탈 + 이탈직전순위 (streak 동안 carry)
for tk in ranked:
    s=vol[tk].dropna() if tk in vol.columns else pd.Series(dtype=float);st_=0;ok=False;rk=None
    for i,d in enumerate(dates):
        o=raw[d].get(tk)
        if o is None or o['ma'] is None: st_=0;ok=False;rk=None;streak[(tk,i)]=0;shake_ok[(tk,i)]=False;continue
        if o['price']<=o['ma']:
            if st_==0:
                bvr=None;idx=s.index[s.index<=dstr[i]] if len(s) else []
                if len(idx)>=21:
                    j=s.index.get_loc(idx[-1]); bvr=s.iloc[j]/s.iloc[j-20:j].mean() if j>=20 else None
                pre=p2F[dates[i-1]].get(tk) if i>0 else None
                pre4=p2F[dates[i-4]].get(tk) if i>=4 else None
                ok=(bvr is not None and bvr<1.0 and pre is not None and pre<=8 and pre4 is not None and pre<pre4)
                rk=pre
            st_+=1
        else: st_=0;ok=False;rk=None
        streak[(tk,i)]=st_;shake_ok[(tk,i)]=ok;shake_rank[(tk,i)]=rk
def sim3(exclude=(),start=0,end=None,E=5,H=12,P=30,mode='base',grace=20):
    """mode: base(2슬롯) / shake(2+개미털기전용3) / gen3(일반 3슬롯)"""
    if end is None: end=len(dates)
    main={};shake={};prev=None;val=1.0;peak=1.0;mdd=0
    for i in range(start,end):
        d=dates[i]
        allh={**main,**shake}
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,w in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        o_d=raw[d];p2d=p2F[d]
        # main 매도
        for tk in list(main):
            o=o_d.get(tk)
            if o and minseg(o)<-2: del main[tk];continue
            if o is None: continue
            p2=p2d.get(tk)
            if not (p2 is None or p2>H): continue
            _pe=o['price']/o['nc'] if o.get('nc',0)>0 else 999
            if _pe>=P: del main[tk]
        # shake 매도: EPS꺾임 / 유예만료(반등실패) / 회복후 순위>H
        for tk in list(shake):
            o=o_d.get(tk)
            if o is None: continue
            if minseg(o)<-2: del shake[tk];continue
            below=o['price']<=(o['ma'] or 0)
            if below and streak.get((tk,i),99)>grace: del shake[tk];continue   # 유예만료 실패
            if not below:
                p2=p2d.get(tk)
                if p2 is not None and p2>H: del shake[tk]                       # 회복했는데 순위 이탈
        Smain=3 if mode=='gen3' else 2
        # main 진입(정상 후보)
        if len(main)<Smain:
            cands=[]
            for tk,p2 in p2d.items():
                if tk in main or tk in shake or tk in exclude or p2>E: continue
                o=o_d[tk]
                if minseg(o)<0 or not o['price'] or not verified(crF,tk,i): continue
                if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
                if (o['dv'] or 0)<1000: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:Smain-len(main)]: main[tk]=1
        # shake 진입(개미털기 전용)
        if mode=='shake' and len(shake)<1:
            sc=[(shake_rank.get((tk,i),99),tk) for tk in ranked
                if shake_ok.get((tk,i),False) and tk not in main and tk not in shake and tk not in exclude]
            sc.sort()
            if sc: shake[sc[0][1]]=1
        allh={**main,**shake};n=len(allh)
        prev={tk:1.0/n for tk in allh} if n else {}
    return (val-1)*100,mdd
elig_s=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig_s,SAMP) for s in range(N)]
def widx(mm): return [i for i,d in enumerate(dates) if d[5:7] in mm]
def batt(lbl,**kw):
    cs=[sim3(start=s,**kw)[0] for ch in seeds for s in ch];ms=[sim3(start=s,**kw)[1] for ch in seeds for s in ch]
    full,fmdd=sim3(**kw)
    worst=st.mean(cs)
    for w in WINNERS: worst=min(worst,st.mean([sim3(exclude=(w,),start=s,**kw)[0] for ch in seeds for s in ch]))
    seg=[sim3(start=widx(mm)[0],end=widx(mm)[-1]+1,**kw)[0] for mm in (['02','03'],['04','05'],['06'])]
    print(f'{lbl:<24}{full:>+8.0f}%{fmdd:>+6.0f}%{st.mean(cs):>+8.0f}%{worst:>+8.0f}%{seg[0]:>+5.0f}{seg[1]:>+6.0f}{seg[2]:>+6.0f}',flush=True)
print(f'\n{"구성":<24}{"전기간":>8}{"MDD":>6}{"paired":>8}{"LOWO":>8}{"2-3월":>5}{"4-5월":>6}{"6월":>6}')
batt('현행 2슬롯',mode='base')
batt('일반 3슬롯(대조군)',mode='gen3')
batt('2슬롯+개미털기 슬롯3',mode='shake')
print('\n판정: 개미털기슬롯3가 현행2슬롯 paired·LOWO↑(또는 동급+MDD↓)면 = 신호 적용가치(구현).')
print('      일반3슬롯보다만 나으면 신호는 의미있으나 winner희석이 더 큼. 둘 다 현행미만이면 = 못먹음 확정.')
