# -*- coding: utf-8 -*-
"""Phase1: 실제 매매 이력 복원 + 놓친 종목 후보 수집 (faithful replay)"""
import sqlite3
DB = r'C:\dev\claude code\eps-momentum-us\eps_momentum_data.db'
c=sqlite3.connect(DB); cur=c.cursor()
EXIT_RANK=12; PE_HOLD=30
all_dates=[r[0] for r in cur.execute("SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date").fetchall()]
idx={d:i for i,d in enumerate(all_dates)}
prices={}; data={}
for d in all_dates:
    prices[d]={r[0]:r[1] for r in cur.execute("SELECT ticker,price FROM ntm_screening WHERE date=?",(d,)).fetchall()}
    data[d]={r[0]:{'p2':r[2],'nc':r[3],'n7':r[4],'n30':r[5],'n60':r[6],'n90':r[7],'dv':r[8]}
             for r in cur.execute("SELECT ticker,price,part2_rank,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,dollar_volume_30d FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL",(d,)).fetchall()}
def minseg(nc,n7,n30,n60,n90):
    return min((a-b)/abs(b)*100 if (b and abs(b)>0.01) else 0 for a,b in [(nc,n7),(n7,n30),(n30,n60),(n60,n90)])

def fwd(tk, sell_date, n):
    """매도 후 n거래일 뒤 DB가격으로 forward return (없으면 마지막 가용)"""
    i0=idx[sell_date]; p0=prices[sell_date].get(tk)
    if not p0: return None
    tgt=min(i0+n, len(all_dates)-1)
    for j in range(tgt, i0, -1):
        pj=prices[all_dates[j]].get(tk)
        if pj: return (pj-p0)/p0*100, all_dates[j]
    return None

portfolio={}; trades=[]; missed=[]
for i in range(2,len(all_dates)):
    date=all_dates[i]; prev=all_dates[i-1]
    d=data.get(date,{}); pr=prices.get(date,{}); ppr=prices.get(prev,{})
    ms={tk:minseg(v['nc'],v['n7'],v['n30'],v['n60'],v['n90']) for tk,v in d.items()}
    wrank={tk:v['p2'] for tk,v in d.items() if v.get('p2')}
    elig=sorted([(tk,v['p2']) for tk,v in d.items() if ms.get(tk,0)>=-2 and v.get('p2')],key=lambda x:x[1])
    # update peak/trough of held
    for tk,info in portfolio.items():
        cp=pr.get(tk)
        if cp:
            info['peak']=max(info['peak'],cp); info['trough']=min(info['trough'],cp)
    # sells
    for tk in list(portfolio.keys()):
        ep=portfolio[tk]['entry_price']; it=d.get(tk); cp=pr.get(tk)
        if it is None or cp is None: continue
        rk=wrank.get(tk); m=ms.get(tk,0); nc=it.get('nc')
        sell=False; reason=None
        if m<-2: sell=True; reason='EPS꺾임'
        elif rk is None or rk>EXIT_RANK:
            pe=(cp/nc) if (cp and nc and nc>0) else 999
            if pe>=PE_HOLD: sell=True; reason=f'순위{rk}+PER{pe:.0f}'
        if sell:
            info=portfolio[tk]; ret=(cp-ep)/ep*100
            maxg=(info['peak']-ep)/ep*100; maxd=(info['trough']-ep)/ep*100
            trades.append({'tk':tk,'entry':info['edate'],'exit':date,'ret':ret,'days':idx[date]-idx[info['edate']],
                           'reason':reason,'maxgain':maxg,'maxdd':maxd,
                           'fwd10':fwd(tk,date,10),'fwd20':fwd(tk,date,20)})
            del portfolio[tk]
    # buys (+ record misses: top-ranked ✅ candidates not bought)
    buy_cands=[tk for tk,_ in elig if tk not in portfolio and ms.get(tk,-999)>=0 and wrank.get(tk,999)<=5
               and (d.get(tk,{}).get('dv') or 0)>=1000]
    buy_cands.sort(key=lambda t:wrank.get(t,999))
    if len(portfolio)<2:
        used={info['slot_idx'] for info in portfolio.values()}
        free=sorted([s for s in range(2) if s not in used])
        for tk in buy_cands:
            if len(portfolio)>=2: break
            ipx=pr.get(tk); idxs=free.pop(0) if free else len(portfolio)
            portfolio[tk]={'entry_price':ipx,'slot_idx':idxs,'weight':50,'edate':date,'peak':ipx,'trough':ipx}
        for info in portfolio.values(): info['weight']=100 if len(portfolio)==1 else 50
    # misses: rank<=3, ✅조건 충족인데 슬롯full or 필터로 못산 종목
    bought_today=set(portfolio.keys())
    for tk,_ in elig:
        rk=wrank.get(tk,999)
        if rk<=3 and tk not in bought_today and ms.get(tk,-9)>=0:
            why='슬롯full' if len(portfolio)>=2 and (d.get(tk,{}).get('dv') or 0)>=1000 else ('거래대금' if (d.get(tk,{}).get('dv') or 0)<1000 else '기타')
            f10=fwd(tk,date,10)
            missed.append({'date':date,'tk':tk,'rank':rk,'why':why,'fwd10':f10})

print(f"=== 실제 매매 이력 ({all_dates[0]}~{all_dates[-1]}, {len(all_dates)}일) ===")
print(f"{'종목':6} {'진입':10} {'청산':10} {'일':>3} {'수익%':>7} {'최대이익':>7} {'최대손실':>7} {'사유':14} {'청산후10d':>9}")
for t in trades:
    f10=f"{t['fwd10'][0]:+.1f}%" if t['fwd10'] else 'NA'
    print(f"{t['tk']:6} {t['entry']:10} {t['exit']:10} {t['days']:>3} {t['ret']:>7.1f} {t['maxgain']:>7.1f} {t['maxdd']:>7.1f} {t['reason']:14} {f10:>9}")
# 미청산 보유
print("미청산 보유:", {tk:f"{(prices[all_dates[-1]].get(tk,0)/info['entry_price']-1)*100:+.1f}%" for tk,info in portfolio.items()})

import statistics as st
rets=[t['ret'] for t in trades]
print(f"\n=== 매매 통계 ===")
print(f"  총 {len(trades)}거래, 승률 {sum(1 for r in rets if r>0)}/{len(rets)}, 평균 {st.mean(rets):+.1f}%, 중앙 {st.median(rets):+.1f}%")
print(f"  최고 {max(rets):+.1f}% / 최악 {min(rets):+.1f}%")
losers=[t for t in trades if t['ret']<0]
print(f"  손실거래 {len(losers)}건: " + ", ".join(f"{t['tk']}({t['ret']:+.0f}%,{t['days']}d)" for t in losers))
prem=[t for t in trades if t['fwd20'] and t['fwd20'][0]>5]
print(f"  조기청산 의심(청산후20d +5%↑): " + ", ".join(f"{t['tk']}({t['fwd20'][0]:+.0f}%)" for t in prem))

print(f"\n=== 놓친 종목 (rank≤3, ✅, 미매수) — 사유별 ===")
from collections import Counter
print("  사유 분포:", Counter(m['why'] for m in missed))
# 슬롯full로 놓친 것 중 forward+ 큰 것
sf=[m for m in missed if m['fwd10']]
big=sorted([m for m in sf if m['fwd10'][0]>3],key=lambda m:-m['fwd10'][0])[:12]
print("  놓친 후 10일 크게 오른 것:")
for m in big:
    print(f"    {m['date']} {m['tk']:6} rank{m['rank']} {m['why']:8} fwd10={m['fwd10'][0]:+.1f}%")
