# -*- coding: utf-8 -*-
"""자율 Stage3: 섹터 블록리스트 진입필터 BT
가설: WMG를 거르는 유일한 robust lever = 업종(숫자 필터 전부 실패/착시).
기존 COMMODITY_INDUSTRIES 선례와 동일 메커니즘.
검증: 소비/레저 업종 제외가 (a)과거 winning trade 0개 차단 (b)WMG/FIVE 차단.
leave-winner-out 불필요: 블록리스트가 MU/SNDK(반도체/하드웨어)를 안 건드림 → 독립적.
"""
import sys, sqlite3, random, statistics, json
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'
N_SEEDS=100; SAMPLES=3; MIN_HOLD=10

# 소비/레저/미디어 — 구조적 비(非)복리성장 업종 (사용자 "압도적 성장기업" 목적 외)
BLOCK_SETS = {
    'WMG만(엔터)': {'엔터'},
    '엔터+전문소매': {'엔터','전문소매'},
    '소비레저': {'엔터','전문소매','외식','리조트카지노','백화점','의류제조','신발','숙박','레저','담배','음료'},
}

cache=json.load(open(ROOT/'ticker_info_cache.json',encoding='utf-8'))
IND={t:(v.get('industry') or '기타') for t,v in cache.items()}

def load():
    con=sqlite3.connect(DB); cur=con.cursor()
    dates=[r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
    data={}
    for d in dates:
        data[d]={}
        for r in cur.execute('''SELECT ticker,part2_rank,composite_rank,price,score,
                ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30 FROM ntm_screening WHERE date=?''',(d,)):
            tk=r[0]; nc,n7,n30,n60,n90=(float(x) if x else 0 for x in r[5:10]); segs=[]
            for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
                segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
            data[d][tk]=dict(p2=r[1],cr=r[2],price=r[3],score=r[4] or 0,min_seg=min(segs) if segs else 0,high30=r[10])
    pf=defaultdict(dict)
    for tk,d,p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'): pf[d][tk]=p
    con.close(); return dates,data,pf

def verified(t,i,dates,data):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True

def sim(dates,data,pf,block=frozenset(),start=0):
    held={};prev=None;val=1.0;peak=1.0;mdd=0.0;rets=[];trades=[];blocked=[]
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,(ed,ep,si,w) in prev.items():
                pp=pf[dp].get(tk);pn=pf[d].get(tk,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);rets.append(ret);peak=max(peak,val);mdd=max(mdd,(peak-val)/peak)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk);ep=held[tk][1];p2=info.get('p2') if info else None
            if info is None or p2 is None or p2>10:
                sp=(info.get('price') if info else None) or pf[d].get(tk,ep); trades.append(dict(tk=tk,ret=(sp/ep-1)*100,ind=IND.get(tk,'?'))); del held[tk]
            elif info.get('min_seg') is not None and info['min_seg']<-2:
                sp=info['price'] or ep; trades.append(dict(tk=tk,ret=(sp/ep-1)*100,ind=IND.get(tk,'?'))); del held[tk]
        if len(held)<2:
            cands=[]
            for tk,info in dd.items():
                if info['p2'] is None or info['p2']>3: continue
                if tk in held: continue
                if info.get('min_seg') is not None and info['min_seg']<0: continue
                if not info['price']: continue
                if not verified(tk,i,dates,data): continue
                if info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25: continue
                if IND.get(tk,'기타') in block:
                    blocked.append((d,tk,IND.get(tk))); continue
                cands.append((info['p2'],info['score'],tk))
            cands.sort(key=lambda x:x[0]);pk=cands[:2]
            if len(pk)==1: held[pk[0][2]]=(d,dd[pk[0][2]]['price'],0,1.0)
            elif len(pk)>=2:
                w=[1.0,0.0] if (pk[0][1]-pk[1][1])>=15 else [0.5,0.5]
                for si,(_,_,tk) in enumerate(pk[:2]):
                    if w[si]>0: held[tk]=(d,dd[tk]['price'],si,w[si])
        prev=dict(held)
    return dict(cum=(val-1)*100,mdd=mdd*100,trades=trades,rets=rets,blocked=blocked)

def main():
    dates,data,pf=load()
    print('='*100); print('Stage3: 섹터 블록리스트 BT (v84 sim, 100×3 paired)'); print('='*100)
    elig=dates[:-MIN_HOLD]; seeds=[]
    for s in range(N_SEEDS): random.seed(s); seeds.append(random.sample(range(len(elig)),SAMPLES))
    def pavg(block):
        return [statistics.mean([sim(dates,data,pf,block=block,start=s)['cum'] for s in ch]) for ch in seeds]
    base=pavg(frozenset())
    print(f'\n{"블록리스트":<18}{"avg":>9}{"med":>9}{"mdd":>9}{"lift":>9}{"wins":>9}  비고')
    print('-'*82)
    allr=[];allm=[]
    for ch in seeds:
        for s in ch: r=sim(dates,data,pf,start=s); allr.append(r['cum']);allm.append(r['mdd'])
    print(f' ★{"none":<16}{sum(allr)/len(allr):>+8.1f}%{sorted(allr)[len(allr)//2]:>+8.1f}%{max(allm):>+8.1f}%')
    for name,block in BLOCK_SETS.items():
        b=frozenset(block); avgs=pavg(b)
        ar=[];am=[]
        for ch in seeds:
            for s in ch: r=sim(dates,data,pf,block=b,start=s); ar.append(r['cum']);am.append(r['mdd'])
        avg=sum(ar)/len(ar); med=sorted(ar)[len(ar)//2]; mdd=max(am)
        lifts=[y-x for x,y in zip(base,avgs)]; wins=sum(1 for l in lifts if l>0)
        nonzero=sum(1 for l in lifts if abs(l)>0.01)
        note=f'{nonzero}개 seed에서만 변화' if nonzero else '전 seed 동일(비용0)'
        print(f'  {name:<16}{avg:>+8.1f}%{med:>+8.1f}%{mdd:>+8.1f}%{sum(lifts)/len(lifts):>+8.2f}p{wins:>6}/100  {note}')

    # full BT: 어떤 trade가 차단되나 + winning trade 손실 여부
    print('\n--- Full BT(start=0): 블록리스트별 차단된 진입 & 거래 손익 ---')
    base_tr={(t['tk']) for t in sim(dates,data,pf,start=0)['trades']}
    for name,block in BLOCK_SETS.items():
        r=sim(dates,data,pf,block=frozenset(block),start=0)
        bl=r['blocked']
        bl_tks=sorted(set(f'{tk}({ind})' for d,tk,ind in bl))
        wins=sum(1 for t in r['trades'] if t['ret']>0); n=len(r['trades'])
        print(f'  [{name}] cum {r["cum"]:+.1f}% 거래{n} 승{wins}/{n}  차단진입: {bl_tks}')
    # WMG 6/1 차단 확인
    print('\n--- 6/1 WMG 진입 차단 확인 ---')
    r=sim(dates,data,pf,block=frozenset({'엔터'}),start=0)
    wmg_blocked=[x for x in r['blocked'] if x[1]=='WMG']
    print(f'  엔터 블록 시 WMG 차단 발생: {wmg_blocked}')

if __name__=='__main__':
    main()
