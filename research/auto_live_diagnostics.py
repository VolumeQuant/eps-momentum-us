# -*- coding: utf-8 -*-
"""전문가 권고 라이브 진단 — "지금 운인가 실력인가" 판별
1. 상위2종목 P&L 비중 (>50% = breadth 없음 = 운)
2. MU/SNDK 제외 시 승률·평균수익 (진짜 신호)
3. drift 곡선 (+1/3/5/10/20일) — 진짜=지속 drift, 운=스파이크 후 무
4. IC: composite_rank(신호) vs 실현 20일수익 일별 Spearman (전 cross-section)
5. 슬롯 2/3/4/5 재검증 (전체 vs MU/SNDK 제외) — 집중우위가 LOWO 착시인가
6. PIT/생존편향: 유니버스 breadth 추이
"""
import sys, sqlite3, random, statistics, math
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'

con = sqlite3.connect(DB); cur = con.cursor()
all_dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE price IS NOT NULL ORDER BY date')]
didx = {d: i for i, d in enumerate(all_dates)}
px = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    px[tk][d] = p

def fwd(tk, d, n):
    i = didx.get(d)
    if i is None or i + n >= len(all_dates): return None
    p0 = px[tk].get(d); p1 = px[tk].get(all_dates[i+n])
    return (p1/p0 - 1)*100 if p0 and p1 else None

# 매매 가능일
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data = {}
for d in dates:
    data[d] = {}
    for r in cur.execute('''SELECT ticker,part2_rank,composite_rank,price,score,
            ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,adj_gap FROM ntm_screening WHERE date=?''', (d,)):
        tk = r[0]; nc,n7,n30,n60,n90 = (float(x) if x else 0 for x in r[5:10]); segs=[]
        for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)]:
            segs.append(max(-100,min(100,(a-b)/abs(b)*100)) if b and abs(b)>0.01 else 0)
        data[d][tk] = dict(p2=r[1],cr=r[2],price=r[3],score=r[4] or 0,min_seg=min(segs) if segs else 0,high30=r[10],adj_gap=r[11])

def verified(t,i):
    for j in (i,i-1,i-2):
        if j<0: return False
        x=data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr']>30: return False
    return True

def sim(slots=2, exclude=(), start=0):
    held={};prev=None;val=1.0;peak=1.0;mdd=0.0;rets=[];trades=[]
    for i in range(start,len(dates)):
        d=dates[i]
        if prev and i>start:
            dp=dates[i-1];ret=0
            for tk,(ed,ep,w) in prev.items():
                pp=px[tk].get(dp);pn=px[tk].get(d,pp)
                if pp and pn: ret+=w*(pn/pp-1)
            val*=(1+ret);rets.append(ret);peak=max(peak,val);mdd=max(mdd,(peak-val)/peak)
        dd=data[d]
        for tk in list(held):
            info=dd.get(tk);ep=held[tk][1];p2=info.get('p2') if info else None
            if info is None or p2 is None or p2>10:
                sp=(info.get('price') if info else None) or px[tk].get(d,ep)
                trades.append(dict(tk=tk,ret=(sp/ep-1)*100,bd=held[tk][0],sd=d)); del held[tk]
            elif info.get('min_seg') is not None and info['min_seg']<-2:
                sp=info['price'] or ep; trades.append(dict(tk=tk,ret=(sp/ep-1)*100,bd=held[tk][0],sd=d)); del held[tk]
        if len(held)<slots:
            cands=[]
            for tk,info in dd.items():
                if info['p2'] is None or info['p2']>slots+1: continue
                if tk in held or tk in exclude: continue
                if info.get('min_seg') is not None and info['min_seg']<0: continue
                if not info['price']: continue
                if not verified(tk,i): continue
                if info.get('high30') and info['price'] and info['price']/info['high30']-1<-0.25: continue
                cands.append((info['p2'],info['score'],tk))
            cands.sort(key=lambda x:x[0]); pick=cands[:slots]
            n=len(pick)
            if n>=1:
                if n>=2 and slots==2:
                    s1,s2=pick[0][1],pick[1][1]; w=[1.0,0.0] if (s1-s2)>=15 else [0.5,0.5]
                    for si,(_,_,tk) in enumerate(pick[:2]):
                        if w[si]>0: held[tk]=(d,dd[tk]['price'],w[si])
                else:
                    w=1.0/n
                    for _,_,tk in pick: held[tk]=(d,dd[tk]['price'],w)
        prev=dict(held)
    return dict(cum=(val-1)*100,mdd=mdd*100,trades=trades)

def spearman(xs, ys):
    pairs=[(a,b) for a,b in zip(xs,ys) if a is not None and b is not None]
    if len(pairs)<5: return None,len(pairs)
    def rank(v):
        s=sorted(range(len(v)),key=lambda i:v[i]); r=[0]*len(v)
        for pos,i in enumerate(s): r[i]=pos
        return r
    xa=[a for a,b in pairs]; ya=[b for a,b in pairs]
    rx=rank(xa); ry=rank(ya)
    mx=statistics.mean(rx);my=statistics.mean(ry)
    num=sum((a-mx)*(b-my) for a,b in zip(rx,ry))
    dx=math.sqrt(sum((a-mx)**2 for a in rx));dy=math.sqrt(sum((b-my)**2 for b in ry))
    return (num/(dx*dy) if dx*dy>0 else None),len(pairs)

print('='*90)
print('라이브 진단 — 운 vs 실력 판별')
print('='*90)

# === 진단 1 & 2: 종목별 P&L 비중 + MU/SNDK 제외 ===
r=sim(slots=2,start=0)
tr=r['trades']
print(f'\n[1] 전체 BT 거래 {len(tr)}건, cum {r["cum"]:+.1f}%')
bytk=defaultdict(float); cnt=defaultdict(int)
for t in tr: bytk[t['tk']]+=t['ret']; cnt[t['tk']]+=1
print('  종목별 P&L 합 (거래수):')
for tk,s in sorted(bytk.items(),key=lambda x:-x[1]):
    print(f'    {tk:6} {s:+7.1f}%p ({cnt[tk]}거래)')
top2=sorted(bytk.values(),reverse=True)[:2]; total=sum(abs(v) for v in bytk.values())
musndk=bytk.get('MU',0)+bytk.get('SNDK',0); allsum=sum(bytk.values())
print(f'  → MU+SNDK P&L 기여: {musndk:+.1f}%p / 전체 {allsum:+.1f}%p = {musndk/allsum*100 if allsum else 0:.0f}%')

print('\n[2] MU/SNDK 제외 시 나머지 거래 (진짜 신호):')
rest=[t for t in tr if t['tk'] not in ('MU','SNDK')]
if rest:
    wr=sum(1 for t in rest if t['ret']>0)/len(rest)*100
    print(f'    거래 {len(rest)}건  승률 {wr:.0f}%  평균 {statistics.mean([t["ret"] for t in rest]):+.1f}%  중앙값 {statistics.median([t["ret"] for t in rest]):+.1f}%')

# === 진단 3: drift 곡선 ===
print('\n[3] drift 곡선 — 진입권(part2≤2) 진입 후 평균 수익 (지속 vs 스파이크):')
print(f'    {"horizon":<10}{"전체":>10}{"MU/SNDK제외":>14}')
ez=[(d,tk) for d in dates for tk,info in data[d].items() if info['p2'] and info['p2']<=2]
for n in [1,3,5,10,20]:
    allf=[fwd(tk,d,n) for d,tk in ez]; allf=[x for x in allf if x is not None]
    exf=[fwd(tk,d,n) for d,tk in ez if tk not in ('MU','SNDK')]; exf=[x for x in exf if x is not None]
    a=statistics.mean(allf) if allf else 0; e=statistics.mean(exf) if exf else 0
    print(f'    +{n:<9}{a:>+9.1f}%{e:>+13.1f}%')

# === 진단 4: IC (composite_rank vs 실현 20일수익) ===
print('\n[4] IC — 일별 Spearman(신호순위 vs 실현20일), 양수=신호가 수익 예측:')
for label,excl in [('전체',()),('MU/SNDK제외',('MU','SNDK'))]:
    ics=[]
    for d in dates:
        ranks=[];frs=[]
        for tk,info in data[d].items():
            if tk in excl: continue
            if info.get('cr') is None: continue
            f=fwd(tk,d,20)
            if f is None: continue
            ranks.append(-info['cr']); frs.append(f)  # -cr: 높을수록 좋은 신호
        ic,n=spearman(ranks,frs)
        if ic is not None: ics.append(ic)
    if ics:
        print(f'    {label:<12} 평균IC {statistics.mean(ics):+.3f}  (양수일수 {sum(1 for x in ics if x>0)}/{len(ics)})')

# === 진단 5: 슬롯 재검증 (LOWO) ===
print('\n[5] 슬롯 2/3/4/5 재검증 — 전체 vs MU/SNDK 제외 (집중우위가 착시인가):')
print(f'    {"슬롯":<6}{"전체 cum":>12}{"전체 MDD":>10}   {"MU/SNDK제외 cum":>16}{"제외 MDD":>10}')
for s in [2,3,4,5]:
    rf=sim(slots=s,start=0); re=sim(slots=s,exclude=('MU','SNDK'),start=0)
    print(f'    {s:<6}{rf["cum"]:>+11.1f}%{rf["mdd"]:>9.1f}%   {re["cum"]:>+15.1f}%{re["mdd"]:>9.1f}%')

# === 진단 6: PIT/생존편향 — 유니버스 breadth 추이 ===
print('\n[6] PIT/생존편향 점검 — 일자별 eligible 종목수 추이 (수축=생존편향 의심):')
counts=[]
for d in dates:
    n=cur.execute('SELECT COUNT(*) FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',(d,)).fetchone()[0]
    counts.append((d,n))
import statistics as st
nums=[n for d,n in counts]
print(f'    eligible 종목수: 최소 {min(nums)} / 최대 {max(nums)} / 평균 {st.mean(nums):.0f} / 표준편차 {st.pstdev(nums):.0f}')
print(f'    첫 5일: {[n for d,n in counts[:5]]}  /  마지막 5일: {[n for d,n in counts[-5:]]}')
# 고유 등장 종목수 (universe 폭)
uniq=cur.execute('SELECT COUNT(DISTINCT ticker) FROM ntm_screening WHERE composite_rank IS NOT NULL').fetchone()[0]
uniq_top3=cur.execute('SELECT COUNT(DISTINCT ticker) FROM ntm_screening WHERE part2_rank<=3').fetchone()[0]
print(f'    전 기간 eligible 고유종목 {uniq}개, 그 중 진입권(part2≤3) 도달 {uniq_top3}개')
con.close()
