# -*- coding: utf-8 -*-
"""자율 진단 Stage1: 최근 drawdown + winner/loser 성장·밸류 EDA
질문:
 (A) 최근 시스템이 망가졌나? 변경(v83.2 5/27, v84 5/30) 탓인가?
 (B) 대박 winner = 압도적 성장+저평가? 물린 종목 = 그 반대?
지표:
 - rev_growth (YoY 매출성장)
 - fwd_pe = price / ntm_current
 - ntm_rev = (ntm_current/ntm_90d -1) (90일 EPS 전망 상향폭)
 - peg = fwd_pe / (rev_growth*100)
 - fwd_ret_20 = 진입 후 20거래일 수익률 (실현 성과)
"""
import sys, sqlite3, statistics
from pathlib import Path
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
con = sqlite3.connect(ROOT / 'eps_momentum_data.db'); cur = con.cursor()

# 전체 가격 캘린더
all_dates = [r[0] for r in cur.execute(
    'SELECT DISTINCT date FROM ntm_screening WHERE price IS NOT NULL ORDER BY date')]
didx = {d: i for i, d in enumerate(all_dates)}
px = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker, date, price FROM ntm_screening WHERE price IS NOT NULL'):
    px[tk][d] = p

def fwd_ret(tk, d, n=20):
    i = didx.get(d)
    if i is None or i + n >= len(all_dates): return None
    p0 = px[tk].get(d); p1 = px[tk].get(all_dates[i+n])
    if not p0 or not p1: return None
    return (p1/p0 - 1) * 100

# part2_rank ≤ 5 (진입권) 모든 (ticker,date) 수집
rows = cur.execute('''
    SELECT date, ticker, part2_rank, price, ntm_current, ntm_90d, rev_growth,
           market_cap, ev, ebitda, roe, operating_margin
    FROM ntm_screening WHERE part2_rank IS NOT NULL AND part2_rank <= 5
    ORDER BY date''').fetchall()

recs = []
for d, tk, p2, price, nc, n90, rg, mc, ev, ebitda, roe, om in rows:
    if not price or not nc or nc <= 0: continue
    fwd_pe = price / nc
    ntm_rev = (nc/n90 - 1)*100 if n90 and n90 > 0 else None
    peg = fwd_pe/(rg*100) if rg and rg > 0 else None
    evebitda = ev/ebitda if ev and ebitda and ebitda > 0 else None
    fr20 = fwd_ret(tk, d, 20)
    recs.append(dict(date=d, tk=tk, p2=p2, price=price, fwd_pe=fwd_pe, rg=rg,
                     ntm_rev=ntm_rev, peg=peg, evebitda=evebitda, roe=roe, om=om, fr20=fr20))

print('='*100)
print('Stage1: 진입권(part2≤5) 종목 EDA  —  성장·밸류 vs 실현 20일 수익률')
print('='*100)
print(f'관측치: {len(recs)} (ticker-date)  기간 {rows[0][0]}~{rows[-1][0]}')

# 종목별 집계: 평균 rev_growth, 평균 fwd_pe, 진입권 등장수, 평균 fwd20
byt = defaultdict(list)
for r in recs: byt[r['tk']].append(r)
print('\n--- 종목별 (진입권 등장 ≥2회) : 성장/밸류/실현성과 ---')
print(f'{"tk":<6}{"n":>3}{"rev_g":>7}{"fwdPE":>7}{"PEG":>6}{"ntm_rev%":>9}{"avg_fwd20%":>11}')
agg = []
for tk, rs in byt.items():
    n = len(rs)
    rg = statistics.mean([x['rg'] for x in rs if x['rg'] is not None]) if any(x['rg'] is not None for x in rs) else None
    pe = statistics.mean([x['fwd_pe'] for x in rs])
    pegs = [x['peg'] for x in rs if x['peg'] is not None]
    peg = statistics.mean(pegs) if pegs else None
    nrev = statistics.mean([x['ntm_rev'] for x in rs if x['ntm_rev'] is not None]) if any(x['ntm_rev'] is not None for x in rs) else None
    frs = [x['fr20'] for x in rs if x['fr20'] is not None]
    fr = statistics.mean(frs) if frs else None
    agg.append((tk, n, rg, pe, peg, nrev, fr))
# sort by realized fwd20 desc
agg.sort(key=lambda x: (x[6] if x[6] is not None else -999), reverse=True)
for tk, n, rg, pe, peg, nrev, fr in agg:
    if n < 2: continue
    rgs = f'{rg*100:.0f}%' if rg is not None else '  -'
    pegs = f'{peg:.2f}' if peg is not None else '  -'
    nrevs = f'{nrev:+.1f}' if nrev is not None else '   -'
    frs = f'{fr:+.1f}' if fr is not None else '   -'
    print(f'{tk:<6}{n:>3}{rgs:>7}{pe:>7.1f}{pegs:>6}{nrevs:>9}{frs:>11}')

# 상관 분석: fwd20 vs 각 지표 (관측치 레벨, fr20 존재하는 것만)
print('\n--- 지표 vs 실현 20일수익률 상관 (관측치 레벨) ---')
import math
def corr(xs, ys):
    pairs = [(a,b) for a,b in zip(xs,ys) if a is not None and b is not None]
    if len(pairs) < 5: return None, len(pairs)
    xa = [a for a,b in pairs]; ya = [b for a,b in pairs]
    mx=statistics.mean(xa); my=statistics.mean(ya)
    num=sum((a-mx)*(b-my) for a,b in pairs)
    dx=math.sqrt(sum((a-mx)**2 for a in xa)); dy=math.sqrt(sum((b-my)**2 for b in ya))
    return (num/(dx*dy) if dx*dy>0 else None), len(pairs)
fr20s=[r['fr20'] for r in recs]
for name in ['rg','fwd_pe','peg','ntm_rev','evebitda','roe','om']:
    c,n = corr([r[name] for r in recs], fr20s)
    print(f'  {name:<10} corr={c:+.3f}  (n={n})' if c is not None else f'  {name:<10} n/a')

# 고성장 vs 저성장 분위 비교
print('\n--- rev_growth 분위별 실현 20일수익률 ---')
valid=[r for r in recs if r['rg'] is not None and r['fr20'] is not None]
valid.sort(key=lambda r:r['rg'])
q=len(valid)//4
for label,seg in [('하위25%(저성장)',valid[:q]),('중위50%',valid[q:3*q]),('상위25%(고성장)',valid[3*q:])]:
    if not seg: continue
    rgm=statistics.mean([r['rg'] for r in seg])*100
    frm=statistics.mean([r['fr20'] for r in seg])
    wr=sum(1 for r in seg if r['fr20']>0)/len(seg)*100
    print(f'  {label:<14} n={len(seg):>3}  rev_g평균 {rgm:>5.0f}%  fwd20평균 {frm:>+6.1f}%  승률 {wr:.0f}%')

# 저평가 분위 (fwd_pe)
print('\n--- forward PE 분위별 실현 20일수익률 ---')
valid2=[r for r in recs if r['fr20'] is not None]
valid2.sort(key=lambda r:r['fwd_pe'])
q=len(valid2)//4
for label,seg in [('저PE25%(저평가)',valid2[:q]),('중위50%',valid2[q:3*q]),('고PE25%(고평가)',valid2[3*q:])]:
    if not seg: continue
    pem=statistics.mean([r['fwd_pe'] for r in seg])
    frm=statistics.mean([r['fr20'] for r in seg])
    wr=sum(1 for r in seg if r['fr20']>0)/len(seg)*100
    print(f'  {label:<14} n={len(seg):>3}  fwdPE평균 {pem:>5.1f}  fwd20평균 {frm:>+6.1f}%  승률 {wr:.0f}%')

con.close()
