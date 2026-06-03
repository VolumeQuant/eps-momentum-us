# -*- coding: utf-8 -*-
"""옵션 B: 순위 무관 메가홀드 BT
A (현재/v86e+): 메가홀드는 Top30 안(part2_rank 존재)에서만. Top30 밖 이탈 → 매도.
B (확장): 메가(PEG<0.22)면 Top30 밖(part2_rank=None)이어도 펀더 유지 시 계속 보유.
  매도: min_seg<-2 OR rev_growth<0.25 OR PEG≥0.22 (순위 무관).
검증: paired 100×3 + MDD + LOWO(MU/SNDK 제외) + 부분기간. B의 edge가 MU 착시인지.
진입: part2_rank≤2 (양 옵션 동일). 가중치: 균등(양 옵션 동일 → 비교 공정).
"""
import sys, sqlite3, random, statistics
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB='eps_momentum_data.db'
N=100; SAMP=3; MINH=10
con=sqlite3.connect(DB); cur=con.cursor()
# 전체 eligible(composite_rank) 행 — 순위밖 메가도 추적 가능
dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')]
data={}
for d in dates:
    data[d]={}
    for tk,p2,cr,price,nc,n7,n30,n60,n90,rg in cur.execute(
        'SELECT ticker,part2_rank,composite_rank,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,rev_growth FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',(d,)):
        segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append((a-b)/abs(b)*100 if b and abs(b)>0.01 else 0)
        peg=(price/nc)/(rg*100) if (price and nc and nc>0 and rg and rg>0) else None
        data[d][tk]=dict(p2=p2,price=price,peg=peg,minseg=min(segs) if segs else 0,rg=rg)
pf=defaultdict(dict)
for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
con.close()

def sim(option, exclude=(), start=0):
    # held: tk -> entry_price. 균등비중.
    held={}; prev=None; val=1.0; peak=1.0; mdd=0.0
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1]; ret=0; w=1.0/len(prev) if prev else 0
            for tk in prev:
                pp=pf[dp].get(tk); pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret); peak=max(peak,val); mdd=max(mdd,(peak-val)/peak)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info is None:  # eligible 자체 탈락 → 양 옵션 매도
                del held[tk]; continue
            mega=info['peg'] is not None and info['peg']<0.22
            if info['minseg']<-2:
                del held[tk]; continue
            if mega and info['rg'] is not None and info['rg']<0.25:
                del held[tk]; continue
            if not mega:
                if info['p2'] is None or info['p2']>10:
                    del held[tk]; continue
            else:
                # 메가
                if option=='A' and info['p2'] is None:
                    del held[tk]; continue  # A: Top30 밖 이탈 시 매도
                # B: 순위 무관 보유 (이미 위 min_seg/rev/PEG 통과)
        if len(held)<2:
            cands=sorted([(tk,info['p2']) for tk,info in dd.items()
                          if tk not in held and tk not in exclude and info['p2'] is not None and info['p2']<=2],
                         key=lambda x:x[1])
            for tk,_ in cands:
                if len(held)>=2: break
                held[tk]=dd[tk]['price']
        prev=dict(held)
    return (val-1)*100, mdd*100

elig=list(range(len(dates)-MINH)); seeds=[random.Random(s).sample(elig,SAMP) for s in range(N)]
def paired(option, exclude=()):
    cums=[]; mdds=[]; sav=[]
    for ch in seeds:
        sr=[]
        for s in ch:
            cu,md=sim(option,exclude,s); cums.append(cu); mdds.append(md); sr.append(cu)
        sav.append(sum(sr)/len(sr))
    return cums,mdds,sav

print('='*80); print('옵션 B: 순위 무관 메가홀드 vs A(현재) — paired 100×3'); print('='*80)
def med(x): return statistics.median(x)
ac,am,asav=paired('A'); bc,bm,bsav=paired('B')
print(f'\n{"":12}{"avg":>10}{"med":>10}{"MDD중앙":>9}{"MDD최대":>9}')
print(f'  A(현재)   {statistics.mean(ac):>+9.1f}%{med(ac):>+9.1f}%{med(am):>8.1f}%{max(am):>8.1f}%')
print(f'  B(확장)   {statistics.mean(bc):>+9.1f}%{med(bc):>+9.1f}%{med(bm):>8.1f}%{max(bm):>8.1f}%')
lifts=[b-a for a,b in zip(asav,bsav)]; wins=sum(1 for l in lifts if l>0)
print(f'\n  B-A lift: {statistics.mean(lifts):+.1f}p  ({wins}/100 wins)')

print('\n--- LOWO: B-A lift (MU/SNDK 제외 시 edge 유지?) ---')
for exn,ex in [('전체',()),('-MU',('MU',)),('-SNDK',('SNDK',)),('-둘다',('MU','SNDK'))]:
    _,_,a=paired('A',ex); _,_,b=paired('B',ex)
    lf=[y-x for x,y in zip(a,b)]; w=sum(1 for l in lf if l>0)
    print(f'  [{exn:<6}] B-A {statistics.mean(lf):>+7.1f}p ({w:>3}/100)')

print('\n--- 부분기간: B-A lift (전반/후반) ---')
half=len(elig)//2
for lbl,lo,hi in [('전반',0,half),('후반(최근)',half,len(elig))]:
    sub=[random.Random(s+7).sample(range(lo,hi),min(SAMP,hi-lo)) for s in range(N)]
    av=[];bv=[]
    for ch in sub:
        av.append(statistics.mean([sim('A',(),s)[0] for s in ch]))
        bv.append(statistics.mean([sim('B',(),s)[0] for s in ch]))
    lf=[y-x for x,y in zip(av,bv)]; w=sum(1 for l in lf if l>0)
    print(f'  [{lbl:<8}] A {statistics.mean(av):+.0f}% / B {statistics.mean(bv):+.0f}% / lift {statistics.mean(lf):+.1f}p ({w}/100)')

# full 단일경로: MU 보유 여부
print('\n--- Full BT(start=0): A vs B 종목별 (MU 잡나?) ---')
for opt in ['A','B']:
    held={}; prev=None; val=1.0; first_buy={}
    for i in range(len(dates)):
        d=dates[i]
        if prev and i>0:
            dp=dates[i-1]; w=1.0/len(prev) if prev else 0
            for tk in prev:
                pp=pf[dp].get(tk); pn=pf[d].get(tk,pp)
                if pp and pn: val*=(1+w*(pn/pp-1)) if False else 1
        # (간략: 누적은 paired에서 봤으니 여기선 보유종목만 추적)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk)
            if info is None: del held[tk]; continue
            mega=info['peg'] is not None and info['peg']<0.22
            if info['minseg']<-2: del held[tk]; continue
            if mega and info['rg'] is not None and info['rg']<0.25: del held[tk]; continue
            if not mega and (info['p2'] is None or info['p2']>10): del held[tk]; continue
            if mega and opt=='A' and info['p2'] is None: del held[tk]; continue
        if len(held)<2:
            for tk,_ in sorted([(tk,info['p2']) for tk,info in dd.items() if tk not in held and info['p2'] is not None and info['p2']<=2],key=lambda x:x[1]):
                if len(held)>=2: break
                held[tk]=1; first_buy.setdefault(tk,d)
        prev=dict(held)
    print(f'  [{opt}] 마지막 보유: {sorted(held.keys())}')
