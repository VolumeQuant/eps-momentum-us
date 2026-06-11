# -*- coding: utf-8 -*-
"""Task A: fwd_PER 진입 캡 검증 — 진입 시 fwd_PE < cap 종목만 매수 허용.
v119 production-replay(sim) 기반. DB dollar_volume_30d 사용(yfinance fetch 불필요).
캡 스윕 + 전기간/MDD/paired(100x3)/LOWO/빈손 + 캡에 막히는 종목 추적."""
import sqlite3, random, statistics as st
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10; GAP=-0.10
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER','BE'}
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

def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True

def sim(entry_pe_cap=None,exclude=(),start=0,slots=2,track=False,blocked=None):
    """v119: vol$1B, entry Top5, eps_sell, fwd_PE<15 hold, dd_30_25, slots=2.
    entry_pe_cap: 진입 시 fwd_PE<cap 인 종목만 매수 허용(None=무제한=baseline)."""
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
        for tk in list(held):  # 매도: EPS꺾임 / (10위밖 AND fwd_PE>=15)
            info=dd.get(tk);ed,ep,w,gr=held[tk]
            if info and info.get('min_seg',0)<-2: del held[tk];continue
            if info is None: continue
            p2=info.get('p2')
            if not (p2 is None or p2>10): continue  # 10위 안 보유
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=15: del held[tk]  # 비싸짐 → 매도 (PE<15면 저평가 보유)
        if len(held)<slots:  # 진입: Top5 + (캡 적용 시 fwd_PE<cap)
            cands=[]
            for tk,info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price']/info['high30']-1<-0.25: continue  # dd_30_25
                if (info.get('dv') or 0)<1000: continue  # $1B
                p2=info.get('p2')
                if p2 is None or p2>5: continue
                _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
                if entry_pe_cap is not None and _pe>=entry_pe_cap:
                    if blocked is not None: blocked.append((d,tk,round(_pe,1),p2))  # 캡에 막힌 후보 기록
                    continue
                cands.append((p2,tk))
            cands.sort();pick=cands[:slots-len(held)]
            for _,tk in pick: held[tk]=(d,dd[tk]['price'],1.0/slots,False)
        if track: series.append(frozenset(held.keys()))
        prev=dict(held)
    if track: return (val-1)*100,mdd,series
    return (val-1)*100,mdd

elig=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def run(cap,exclude=()):
    cs,ms=[],[]
    for ch in seeds:
        for s in ch:
            c,m=sim(cap,exclude,s);cs.append(c);ms.append(m)
    return cs,ms

print(f'BT 기간 {dates[0]} ~ {dates[-1]} ({len(dates)}일), v119 base\n')
print(f'{"진입캡":<12}{"전기간":>9}{"전MDD":>8}{"paired평균":>10}{"pMDD":>8}{"LOWO최악":>10}{"빈손일":>7}')
caps=[None,50,40,30,25,20,15]
for cap in caps:
    blocked=[]
    c,m,ser=sim(cap,track=True,blocked=blocked)
    cs,ms=run(cap)
    pavg=st.mean(cs);pmdd=st.mean(ms)
    worst=pavg;wW=None
    for w in WINNERS:
        a=st.mean(run(cap,(w,))[0])
        if a<worst: worst=a;wW=w
    empty=sum(1 for s in ser if not s)
    lbl='없음(baseline)' if cap is None else f'fwd_PE<{cap}'
    print(f'{lbl:<12}{c:>+8.1f}%{m:>+7.1f}%{pavg:>+9.1f}%{pmdd:>+7.1f}%{worst:>+9.1f}%{empty:>6}일',flush=True)

# 캡이 막는 실제 진입후보 (baseline 경로에서)
print('\n=== 캡별로 차단되는 진입후보 (deterministic 경로, 상위) ===')
for cap in [15,20,25,30]:
    blocked=[]
    sim(cap,track=True,blocked=blocked)
    # 종목별 첫 차단 + PE
    seen={}
    for d,tk,pe,p2 in blocked:
        if tk not in seen: seen[tk]=(d,pe,p2)
    items=sorted(seen.items(),key=lambda x:x[1][1],reverse=True)
    s=', '.join(f'{tk}(PE{pe},{p2}위)' for tk,(d,pe,p2) in items[:8])
    print(f'  fwd_PE<{cap} 차단: {len(seen)}종목 — {s}')
print('\n해석: 캡 낮출수록 빈손↑·winner 차단(BE PER83 등). baseline 대비 수익/LOWO 유지·개선이면 채택, 악화면 기각.')
