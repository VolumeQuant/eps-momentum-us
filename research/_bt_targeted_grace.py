# -*- coding: utf-8 -*-
"""타겟 유예: AMZN프로파일(이탈직전 p2<=8 + 올라오던중 + 저볼륨이탈)만 eligible 유지.
실제 grace매수(이탈상태서 매수) 로그 + forward + 전체 battery. 슬롯경합까지 실측.
판정=paired+LOWO+walk-forward+MDD + grace매수 net."""
import random, statistics as st, numpy as np, pandas as pd, yfinance as yf, warnings, sys
warnings.filterwarnings('ignore'); sys.stdout.reconfigure(encoding='utf-8')
exec(open('research/_bt_universe_research.py',encoding='utf-8').read().split('elig_s=')[0].replace('cd "','#'))
crF,p2F=build_ranks(0,0)  # 베이스라인 순위(이탈전 순위 참조용)
ranked=sorted({tk for d in dates for tk,o in raw[d].items() if o['ads'] and o['ads']>9})
print(f'볼륨 수집 {len(ranked)}종목...',flush=True)
vol=yf.download(ranked,start='2026-01-01',end='2026-06-20',progress=False,auto_adjust=True,threads=2)['Volume']
vol.index=pd.to_datetime(vol.index);dstr=[pd.Timestamp(d) for d in dates]
streak={};brkok={}  # brkok=이 이탈이 AMZN프로파일인가
for tk in ranked:
    s=vol[tk].dropna() if tk in vol.columns else pd.Series(dtype=float);st_=0;ok=False
    for i,d in enumerate(dates):
        o=raw[d].get(tk)
        if o is None or o['ma'] is None: st_=0;ok=False;streak[(tk,i)]=0;brkok[(tk,i)]=False;continue
        if o['price']<=o['ma']:
            if st_==0:  # 이탈 첫날: 프로파일 판정
                bvr=None;idx=s.index[s.index<=dstr[i]] if len(s) else []
                if len(idx)>=21:
                    j=s.index.get_loc(idx[-1]); bvr=s.iloc[j]/s.iloc[j-20:j].mean() if j>=20 else None
                pre=p2F[dates[i-1]].get(tk) if i>0 else None       # 이탈 직전 순위
                pre4=p2F[dates[i-4]].get(tk) if i>=4 else None      # 3일전 순위
                climbing=(pre is not None and pre4 is not None and pre<pre4)  # 순위 개선중
                ok=(bvr is not None and bvr<1.0 and pre is not None and pre<=8 and climbing)
            st_+=1
        else: st_=0;ok=False
        streak[(tk,i)]=st_;brkok[(tk,i)]=ok
def elig_g(o,i,grace):
    if is_commodity(o['tk']) or o['ads'] is None or o['ads']<=9 or o['ag'] is None: return False
    if not o['nc'] or o['nc']<=0 or not o['price'] or o['price']<10: return False
    if o['n90'] and o['nc']/o['n90']-1<=0: return False
    if o['ma'] is None: return False
    if o['price']<=o['ma']:
        if not (streak.get((o['tk'],i),99)<=grace and brkok.get((o['tk'],i),False)): return False
    if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: return False
    if o['rg'] is None or o['rg']<0.10 or o['na'] is None or o['na']<3 or o['ru'] is None or o['ru']<3: return False
    tot=(o['ru'] or 0)+(o['rd'] or 0)
    if tot>0 and (o['rd'] or 0)/tot>0.3: return False
    if o['om'] is not None and o['gm'] is not None and o['om']<0.10 and o['gm']<0.30: return False
    if o['om'] is not None and o['om']<0.05 or minseg(o)<-2: return False
    return True
def build_g(grace):
    cr_by={};score_by={};p2_by={};p2top30={}
    for i,d in enumerate(dates):
        eg={tk:o for tk,o in raw[d].items() if elig_g(o,i,grace)}
        cr_by[d]={tk:j+1 for j,tk in enumerate(sorted(eg,key=lambda t:eg[t]['ag']))}
        cg={tk:conv(o) for tk,o in eg.items()};v=list(cg.values())
        score_by[d]=({tk:max(30.,65+(-(x-np.mean(v))/np.std(v))*15) for tk,x in cg.items()} if len(v)>=2 and np.std(v)>0 else {tk:65 for tk in cg})
    for idx,d in enumerate(dates):
        rc=dates[max(0,idx-2):idx+1];w=[0.2,0.3,0.5][-len(rc):] if len(rc)==3 else([0.4,0.6] if len(rc)==2 else[1.])
        wg={tk:sum((score_by[d].get(tk,30) if dd==d else (score_by[dd].get(tk,30) if tk in p2top30.get(dd,set()) else 30))*w[k] for k,dd in enumerate(rc)) for tk in score_by[d]}
        order=sorted(wg,key=lambda t:-wg[t]);p2_by[d]={tk:j+1 for j,tk in enumerate(order)};p2top30[d]=set(order[:30])
    return cr_by,p2_by
def sim(cr_by,p2_by,exclude=(),start=0,end=None,E=5,H=12,S=2,P=30,log=None):
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
        o_d=raw[d];p2d=p2_by[d]
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
                if minseg(o)<0 or not o['price'] or not verified(cr_by,tk,i): continue
                if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: continue
                if (o['dv'] or 0)<1000: continue
                cands.append((p2,tk))
            cands.sort()
            for _,tk in cands[:S-len(held)]:
                held[tk]=1.0/S
                if log is not None and raw[d][tk]['price']<=(raw[d][tk]['ma'] or 0):  # 이탈상태서 매수=grace매수
                    fwd=None
                    for k in range(i+1,min(i+21,len(dates))):
                        pn=pf[dates[k]].get(tk)
                        if pn: fwd=(pn/raw[d][tk]['price']-1)*100
                    log.append((d,tk,fwd))
        prev=dict(held)
    return (val-1)*100,mdd
elig_s=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig_s,SAMP) for s in range(N)]
def widx(mm): return [i for i,d in enumerate(dates) if d[5:7] in mm]
def batt(lbl,cr,p2):
    cs=[sim(cr,p2,(),s)[0] for ch in seeds for s in ch];ms=[sim(cr,p2,(),s)[1] for ch in seeds for s in ch]
    full,fmdd=sim(cr,p2)
    worst=st.mean(cs)
    for w in WINNERS: worst=min(worst,st.mean([sim(cr,p2,(w,),s)[0] for ch in seeds for s in ch]))
    seg=[sim(cr,p2,(),widx(mm)[0],widx(mm)[-1]+1)[0] for mm in (['02','03'],['04','05'],['06'])]
    print(f'{lbl:<20}{full:>+8.0f}%{fmdd:>+6.0f}%{st.mean(cs):>+8.0f}%{worst:>+8.0f}%{seg[0]:>+5.0f}{seg[1]:>+6.0f}{seg[2]:>+6.0f}',flush=True)
cr0,p20=build_ranks(0,0)
print(f'\n{"타겟유예":<20}{"전기간":>8}{"MDD":>6}{"paired":>8}{"LOWO":>8}{"2-3월":>5}{"4-5월":>6}{"6월":>6}')
batt('현행(유예X)',cr0,p20)
for g in [10,20,40]:
    crg,p2g=build_g(g); batt(f'타겟유예 {g}일',crg,p2g)
# grace매수 로그(결정적경로 + 몇몇 random)
crg,p2g=build_g(40);log=[]
sim(crg,p2g,log=log)
for s in [seeds[0][0],seeds[1][0],seeds[2][0]]: sim(crg,p2g,start=s,log=log)
print(f'\n실제 grace매수(이탈상태서 매수) {len(log)}건:')
for d,tk,fwd in log[:20]: print(f'  {d} {tk} → 20일 {fwd:+.1f}%' if fwd is not None else f'  {d} {tk} → fwd없음')
if log:
    fws=[f for _,_,f in log if f is not None]
    if fws: print(f'  grace매수 평균 fwd20: {st.mean(fws):+.1f}% ({sum(1 for f in fws if f>0)}/{len(fws)} 양수)')
print('\n판정: 타겟유예가 paired·LOWO 현행 명확↑ + grace매수 net양수면 = 가설 적용가치(구현). 아니면 슬롯제약으로 못먹음(데이터확정).')
