# -*- coding: utf-8 -*-
"""v117 재검증 — 거래량 임계 × Top3/5 스윕 + LOWO + 회전/빈손
DB dollar_volume_30d 사용. production-replay sim_v114(MA12 추세홀드 + EPS꺾임 + 휩쏘 보험)."""
import sys, sqlite3, random, statistics as st
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10; GAP=-0.10
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER'}  # LOWO 대상 (어제 set + NVDA/TER)
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,dollar_volume_30d,rev_growth FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[5:10]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],score=r[4] or 0,min_seg=min(segs),seg1=segs[0],high30=r[10],dv=r[11],ntm=nc,rg=r[12] or 0)
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()
# yfinance 거래대금 fetch (DB dv 불완전 → 정확도 확정)
import yfinance as yf, pandas as pd
all_tk=sorted({tk for d in dates for tk,info in data[d].items() if info['cr'] and info['cr']<=30})
ddv={}
for i in range(0,len(all_tk),50):
    batch=all_tk[i:i+50]
    try:
        dl=yf.download(' '.join(batch),start='2025-08-01',end='2026-06-11',auto_adjust=False,progress=False,threads=True,group_by='ticker')
        mi=isinstance(dl.columns,pd.MultiIndex)
        for tk in batch:
            try:
                df=(dl[tk] if (mi and tk in dl.columns.get_level_values(0)) else (None if mi else dl))
                if df is not None and not df.empty:
                    dv=(df['Volume']*df['Close'])/1e6
                    ddv[tk]={dt.strftime('%Y-%m-%d'):v for dt,v in zip(df.index,dv.values) if not pd.isna(v)}
            except: pass
    except: pass
print(f'yfinance fetch dv: {len(ddv)}/{len(all_tk)}')
def avg_vol_at(tk,td,lb=30):
    if tk not in ddv: return 0
    dv=ddv[tk];sd=sorted([x for x in dv if x<td])
    if len(sd)<5: return 0
    rec=sd[-lb:]; return sum(dv[x] for x in rec)/len(rec)
def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True
def above_ma12(tk,i):
    if i<6: return True
    ps=[pf[dates[j]].get(tk) for j in range(max(0,i-11),i+1)];ps=[p for p in ps if p]
    if len(ps)<6: return True
    cp=pf[dates[i]].get(tk)
    return cp>sum(ps)/len(ps) if cp else True
def gap(tk,i):
    if i<1: return 0
    a=pf[dates[i]].get(tk);b=pf[dates[i-1]].get(tk)
    return a/b-1 if a and b else 0
def _peg(info):
    pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
    return pe/(info['rg']*100) if info.get('rg',0)>0 else 999
def sim(vol_thr,entry_r=3,exclude=(),start=0,ma12_hold=True,eps_sell=True,peg_hold=None,pe_hold=None,slots=2,entry_pe=False,dd_filter=True,trend_thr=None,track=False):
    held={};prev=None;val=1.0;peak=1.0;mdd=0;series=[]
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,(ed,ep,w,g) in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk);ed,ep,w,gr=held[tk]
            if eps_sell and info and info.get('min_seg',0)<-2: del held[tk];continue
            if info is None: continue
            p2=info.get('p2');rank_out=(p2 is None or p2>10)
            if not rank_out:
                continue  # 10위 안 = 보유
            if pe_hold is not None:  # B: fwd_PE 단독 — PE 낮으면(저평가) 보유, 고PE면 매도
                _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
                if _pe >= pe_hold: del held[tk]
                continue
            if peg_hold is not None:  # A: 매출 PEG — 저평가면 보유, 고평가면 매도
                if _peg(info) >= peg_hold: del held[tk]
                continue
            if not ma12_hold:
                del held[tk]   # 단순: 10위 밖이면 매도
                continue
            below=not above_ma12(tk,i)
            if below:
                if gap(tk,i)<=GAP and not gr: held[tk]=(ed,ep,w,True);continue
                del held[tk]
        if len(held)<slots:
            cands=[]
            for tk,info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if trend_thr is not None and info.get('seg1',0)<trend_thr: continue  # 둔화 진입차단
                if dd_filter and info.get('high30') and info['price']/info['high30']-1<-0.25: continue
                if vol_thr>0 and avg_vol_at(tk,d)<vol_thr: continue
                p2=info.get('p2')
                if p2 is None: continue
                _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
                if p2<=entry_r:
                    cands.append((0,p2,tk))   # 순위 Top 진입 (우선)
                elif entry_pe and _pe<15:
                    cands.append((1,p2,tk))   # (2)variant: 저평가(PER<15) winner 신규진입 (순위 무관)
            cands.sort();pick=cands[:slots-len(held)]
            for _,_,tk in pick: held[tk]=(d,dd[tk]['price'],1.0/slots,False)
        if track: series.append(frozenset(held.keys()))
        prev=dict(held)
    if track: return (val-1)*100,mdd,series
    return (val-1)*100,mdd
elig=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def run(vol_thr,entry_r,exclude=(),ma12=True,eps=True,peg=None,pe=None,slots=2,entry_pe=False,dd_filter=True,trend_thr=None):
    cs,ms=[],[]
    for ch in seeds:
        for s in ch:
            c,m=sim(vol_thr,entry_r,exclude,s,ma12,eps,peg,pe,slots,entry_pe,dd_filter,trend_thr);cs.append(c);ms.append(m)
    return cs,ms
def track_metrics(vol_thr,entry_r):
    _,_,series=sim(vol_thr,entry_r,track=True)
    empty=sum(1 for s in series if not s)
    swap=0;days=0
    for a,b in zip(series,series[1:]):
        if a and b:
            days+=1
            if not (a&b): swap+=1
    return empty,swap,days,len(series)
_lastd=dates[-1]; _cov=sum(1 for tk in data[_lastd] if data[_lastd][tk]['dv'] is not None)
print(f'dates={len(dates)}, dv 커버리지: {_cov}/{len(data[_lastd])}')
# === 시장(SPY/QQQ) — "강세장이었나" 검증 ===
print(f'\n=== 시장 검증: BT 기간 {dates[0]} ~ {dates[-1]} ({len(dates)}일) ===')
try:
    mk=yf.download('SPY QQQ',start=dates[0],end='2026-06-11',auto_adjust=False,progress=False,group_by='ticker')
    for idx in ['SPY','QQQ']:
        try:
            cl=mk[idx]['Close'].dropna()
            tot=(cl.iloc[-1]/cl.iloc[0]-1)*100
            mdd=((cl/cl.cummax()-1)*100).min()
            lo=cl.idxmin().strftime('%Y-%m-%d'); loval=cl.min()
            print(f'{idx}: {cl.iloc[0]:.0f} -> {cl.iloc[-1]:.0f}  총 {tot:+.1f}%  MDD {mdd:.1f}%  (최저 {lo} {loval:.0f})')
            # 월별 종가
            mser=cl.resample('M').last()
            print('   월말: '+' '.join(f'{i.strftime("%y-%m")}:{v:.0f}' for i,v in mser.items()))
        except Exception as e: print(idx,'err',repr(e))
except Exception as e: print('market fetch err',repr(e))

# === slot 분산 검증: 제3방안(PE<15) × slot 2/3/4 ===
print('\n=== 보유 PER 기준 재검증: PE_HOLD 15 vs 30 (Top5, $1B, LOWO로 노이즈 판별) ===')
print(f'{"PE_HOLD":<9}{"전기간":>9}{"전MDD":>8}{"paired":>9}{"pMDD":>8}{"LOWO":>10}')
resd={}
for ph in [15,20,25,30,40]:
    c,m,ser=sim(1000,5,(),0,False,True,None,ph,2,False,True,None,True)
    cs,ms=run(1000,5,(),False,True,None,ph,2,False,True)
    pavg=st.mean(cs);pmdd=st.mean(ms)
    worst=pavg;wj=''
    for w in WINNERS:
        a=st.mean(run(1000,5,(w,),False,True,None,ph,2,False,True)[0])
        if a<worst: worst=a;wj=w
    resd[ph]=cs
    print(f'{ph:<9}{c:>+8.1f}%{m:>+7.1f}%{pavg:>+8.1f}%{pmdd:>+7.1f}%{worst:>+7.1f}%(-{wj})',flush=True)
d=[a-b for a,b in zip(resd[30],resd[15])]
w=sum(1 for x in d if x>0.01);t=sum(1 for x in d if abs(x)<=0.01);l=sum(1 for x in d if x<-0.01)
print(f'\nPE30 - PE15 (paired 300, 같은 시작일): 평균 {st.mean(d):+.2f}%p, PE30승 {w}/똑같음 {t}/PE15승 {l}')
print('해석: PE30 LOWO(괄호=worst 제외종목)가 PE15보다 높으면 robust(30 채택). 특정 winner 제외시 이득 사라지면 노이즈(15 유지).')
