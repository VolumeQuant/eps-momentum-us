# -*- coding: utf-8 -*-
"""1차 방향 BT: 거래대금 $1B+ 종목만 남겨 '순위 단계'에서 재랭킹 → 진입/이탈에 반영.
baseline(현행: 전체 랭킹, $1B는 진입에서만) vs variant($1B+ 선거른 뒤 재랭킹).
둘 다 현행 PE_HOLD=30 / entry Top5 / slots2 / dd_30_25 / eps_sell 동일.
재랭킹 = 순서 보존, 미달주 제거로 갭만 당김(new_rank = $1B+ 중 w_gap 우월 개수+1)."""
import sqlite3, random, statistics as st
from collections import defaultdict
import sys; sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'; N=100; SAMP=3; MINH=10; PE_HOLD=30.0
WINNERS={'MU','SNDK','STX','LITE','TTMI','NVDA','TER','BE'}
con=sqlite3.connect(DB);cur=con.cursor()
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data={}
for d in dates:
    data[d]={}
    for r in cur.execute('SELECT ticker,part2_rank,composite_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,dollar_volume_30d FROM ntm_screening WHERE date=?',(d,)):
        tk=r[0];nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[4:9]);segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],min_seg=min(segs),high30=r[9],dv=r[10],ntm=nc)
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()

# variant 재랭킹: 각 날짜 $1B+ 종목만 원래 p2 순서 보존해 1..K 재번호
rank_var={}
for d in dates:
    liquid=sorted([(info['p2'],tk) for tk,info in data[d].items()
                   if info['p2'] is not None and (info['dv'] or 0)>=1000])
    rank_var[d]={tk:i+1 for i,(p2,tk) in enumerate(liquid)}

def get_rank(mode,d,tk):
    if mode=='base': return data[d][tk].get('p2')
    return rank_var[d].get(tk)  # variant: 미달주는 None(유니버스 밖)

def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True

def sim(mode,exclude=(),start=0,slots=2,track=False):
    held={};prev=None;val=1.0;peak=1.0;mdd=0;series=[]
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,(ed,ep,w) in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);peak=max(peak,val);mdd=min(mdd,(val/peak-1)*100)
        dd=data[d]
        for tk in list(held):  # 매도: EPS꺾임 / (10위밖 AND fwd_PE>=PE_HOLD)
            info=dd.get(tk)
            if info and info.get('min_seg',0)<-2: del held[tk];continue
            if info is None: continue
            p2=get_rank(mode,d,tk)
            if not (p2 is None or p2>10): continue  # 10위 안 보유
            _pe=info['price']/info['ntm'] if info.get('ntm',0)>0 else 999
            if _pe>=PE_HOLD: del held[tk]  # 비싸짐 → 매도
        if len(held)<slots:  # 진입: Top5(해당 순위체계) + min_seg>=0 + verified + dd_30_25 + $1B
            cands=[]
            for tk,info in dd.items():
                if tk in held or tk in exclude: continue
                if info.get('min_seg',0)<0 or not info['price'] or not verified(tk,i): continue
                if info.get('high30') and info['price']/info['high30']-1<-0.25: continue
                if (info.get('dv') or 0)<1000: continue
                p2=get_rank(mode,d,tk)
                if p2 is None or p2>5: continue
                cands.append((p2,tk))
            cands.sort();pick=cands[:slots-len(held)]
            for _,tk in pick: held[tk]=(d,dd[tk]['price'],1.0/slots)
        if track: series.append(frozenset(held.keys()))
        prev=dict(held)
    if track: return (val-1)*100,mdd,series
    return (val-1)*100,mdd

elig=list(range(2,len(dates)-MINH));seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def run(mode,exclude=()):
    cs,ms=[],[]
    for ch in seeds:
        for s in ch:
            c,m=sim(mode,exclude,s);cs.append(c);ms.append(m)
    return cs,ms

print(f'BT 기간 {dates[0]} ~ {dates[-1]} ({len(dates)}일), PE_HOLD={PE_HOLD:.0f}\n')
print(f'{"모드":<22}{"전기간":>9}{"전MDD":>8}{"paired평균":>11}{"pMDD":>8}{"LOWO최악":>11}')
res={}
for mode,lbl in [('base','baseline(현행 전체랭킹)'),('var','variant($1B+ 재랭킹)')]:
    c,m,ser=sim(mode,track=True)
    cs,ms=run(mode)
    pavg=st.mean(cs);pmdd=st.mean(ms)
    worst=pavg;wW=None
    for w in WINNERS:
        a=st.mean(run(mode,(w,))[0])
        if a<worst: worst=a;wW=w
    res[mode]=dict(full=c,mdd=m,pavg=pavg,pmdd=pmdd,worst=worst,wW=wW,ser=ser)
    print(f'{lbl:<22}{c:>+8.1f}%{m:>+7.1f}%{pavg:>+10.1f}%{pmdd:>+7.1f}%{worst:>+10.1f}% (-{wW})',flush=True)

b,v=res['base'],res['var']
print('\n=== 차이 (variant - baseline) ===')
print(f'  전기간 {v["full"]-b["full"]:+.1f}%p / paired {v["pavg"]-b["pavg"]:+.1f}%p / LOWO최악 {v["worst"]-b["worst"]:+.1f}%p / MDD {v["mdd"]-b["mdd"]:+.1f}%p')

# 보유 차이 (결정적 경로 마지막 5일)
print('\n=== 결정적 경로 보유 비교 (마지막 6일) ===')
for k in range(max(0,len(dates)-6),len(dates)):
    sb=b['ser'][k] if k<len(b['ser']) else set()
    sv=v['ser'][k] if k<len(v['ser']) else set()
    print(f'  {dates[k]}  base={sorted(sb)}  var={sorted(sv)}')
print('\n해석: variant가 전기간·paired·LOWO 모두 +이고 MDD 비악화면 2·3단계 진행. marginal/노이즈/LOWO음수면 기각.')
