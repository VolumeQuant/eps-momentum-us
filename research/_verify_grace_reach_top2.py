# -*- coding: utf-8 -*-
"""저볼륨 MA120이탈 반등주가 유예 시 실제 매수권(top2)에 도달하나 검증.
유예 랭킹(저볼륨 이탈=계속 eligible) 빌드 후, 알려진 반등주들의 이탈후 최고순위 추적.
top2 도달=유예가 진짜 매수 만듦. 미도달=구조적 못먹음(이번엔 데이터로)."""
import random, statistics as st, numpy as np, pandas as pd, yfinance as yf, warnings, sys
warnings.filterwarnings('ignore'); sys.stdout.reconfigure(encoding='utf-8')
exec(open('research/_bt_universe_research.py',encoding='utf-8').read().split('elig_s=')[0].replace('cd "','#'))
# 랭킹풀 종목 볼륨
ranked=sorted({tk for d in dates for tk,o in raw[d].items() if o['ads'] and o['ads']>9})
print(f'볼륨 수집 {len(ranked)}종목...',flush=True)
vol=yf.download(ranked,start='2026-01-01',end='2026-06-20',progress=False,auto_adjust=True,threads=2)['Volume']
vol.index=pd.to_datetime(vol.index);dstr=[pd.Timestamp(d) for d in dates]
streak={};brkvolr={}
for tk in ranked:
    s=vol[tk].dropna() if tk in vol.columns else pd.Series(dtype=float);st_=0;bvr=None
    for i,d in enumerate(dates):
        o=raw[d].get(tk)
        if o is None or o['ma'] is None: st_=0;bvr=None;streak[(tk,i)]=0;continue
        if o['price']<=o['ma']:
            if st_==0:
                idx=s.index[s.index<=dstr[i]] if len(s) else []
                if len(idx)>=21:
                    j=s.index.get_loc(idx[-1]); bvr=s.iloc[j]/s.iloc[j-20:j].mean() if j>=20 else None
                else: bvr=None
            st_+=1
        else: st_=0;bvr=None
        streak[(tk,i)]=st_;brkvolr[(tk,i)]=bvr
def elig_gv(o,i,grace=20,vthr=1.0):
    if is_commodity(o['tk']) or o['ads'] is None or o['ads']<=9 or o['ag'] is None: return False
    if not o['nc'] or o['nc']<=0 or not o['price'] or o['price']<10: return False
    if o['n90'] and o['nc']/o['n90']-1<=0: return False
    if o['ma'] is None: return False
    if o['price']<=o['ma']:
        st_=streak.get((o['tk'],i),99);bvr=brkvolr.get((o['tk'],i))
        if not (st_<=grace and bvr is not None and bvr<vthr): return False
    if o['h30'] and (o['price']-o['h30'])/o['h30']<-0.25: return False
    if o['rg'] is None or o['rg']<0.10 or o['na'] is None or o['na']<3 or o['ru'] is None or o['ru']<3: return False
    tot=(o['ru'] or 0)+(o['rd'] or 0)
    if tot>0 and (o['rd'] or 0)/tot>0.3: return False
    if o['om'] is not None and o['gm'] is not None and o['om']<0.10 and o['gm']<0.30: return False
    if o['om'] is not None and o['om']<0.05 or minseg(o)<-2: return False
    return True
# 유예 랭킹
cr_by={};score_by={};p2_by={};p2top30={}
for i,d in enumerate(dates):
    eg={tk:o for tk,o in raw[d].items() if elig_gv(o,i)}
    cr_by[d]={tk:j+1 for j,tk in enumerate(sorted(eg,key=lambda t:eg[t]['ag']))}
    cg={tk:conv(o) for tk,o in eg.items()};v=list(cg.values())
    score_by[d]=({tk:max(30.,65+(-(x-np.mean(v))/np.std(v))*15) for tk,x in cg.items()} if len(v)>=2 and np.std(v)>0 else {tk:65 for tk in cg})
for idx,d in enumerate(dates):
    rc=dates[max(0,idx-2):idx+1];w=[0.2,0.3,0.5][-len(rc):] if len(rc)==3 else([0.4,0.6] if len(rc)==2 else[1.])
    wg={tk:sum((score_by[d].get(tk,30) if dd==d else (score_by[dd].get(tk,30) if tk in p2top30.get(dd,set()) else 30))*w[k] for k,dd in enumerate(rc)) for tk in score_by[d]}
    order=sorted(wg,key=lambda t:-wg[t]);p2_by[d]={tk:j+1 for j,tk in enumerate(order)};p2top30[d]=set(order[:30])
# 반등주 이탈일 + 이탈후 유예랭킹 최고순위
events=[('AEIS','2026-06-05'),('WWD','2026-05-22'),('APH','2026-06-05'),('WWD','2026-05-15'),
        ('LLY','2026-05-18'),('TPR','2026-06-01'),('BWXT','2026-05-27'),('FHN','2026-03-06'),('NYT','2026-05-20')]
print(f'\n저볼륨 이탈 반등주 — 유예 시 이탈후 20일 최고순위 도달:')
print(f'{"종목":<6}{"이탈일":<12}{"이탈직전p2":>10}{"유예후최고p2":>12}{"top2도달":>9}{"top5도달":>9}')
reach2=0;reach5=0
for tk,bd in events:
    if bd not in dates: continue
    bi=dates.index(bd)
    pre=p2_by[dates[bi-1]].get(tk) if bi>0 else None  # 이탈 직전 순위
    post=[p2_by[dates[j]].get(tk) for j in range(bi,min(bi+20,len(dates))) if p2_by[dates[j]].get(tk)]
    best=min(post) if post else None
    r2='✅' if best and best<=2 else '—';r5='✅' if best and best<=5 else '—'
    if best and best<=2: reach2+=1
    if best and best<=5: reach5+=1
    print(f'{tk:<6}{bd:<12}{str(pre):>10}{str(best):>12}{r2:>9}{r5:>9}')
print(f'\n→ top2(매수권) 도달 {reach2}/9, top5(후보권) 도달 {reach5}/9')
print('  top2 다수 도달=유예가 진짜 매수 만듦(가설 적용가치). 0~소수=유예해도 매수권 못옴(이번엔 데이터로).')
