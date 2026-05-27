"""top-20 섹터 구성 + ETF가 섹터 전반 대표하는지 + 개별픽 중복 체크

취지: ETF = 섹터 전반 분산 노출 (개별 50%와 중복 아닌 보완).
"""
import sys
import json
from pathlib import Path
from collections import Counter
import sqlite3

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
etf = json.load(open(ROOT / 'etf_holdings_cache_v2.json', encoding='utf-8'))
con = sqlite3.connect(ROOT / 'eps_momentum_data.db')
d = con.execute('SELECT MAX(date) FROM ntm_screening WHERE part2_rank IS NOT NULL').fetchone()[0]
tinfo = json.load(open(ROOT / 'ticker_info_cache.json', encoding='utf-8'))

rows = [(tk, p2, tinfo.get(tk, {}).get('industry', '-')) for tk, p2 in con.execute(
    'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank<=20 ORDER BY part2_rank', (d,)).fetchall()]
print(f'기준일 {d} — top-20 구성')
print('순위 | 종목 | 업종')
for tk, p2, ind in rows:
    print(f'  {p2:>2} | {tk:<6} | {ind or "-"}')

print('\n=== 업종 분포 ===')
for ind, c in Counter(r[2] or '-' for r in rows).most_common():
    print(f'  {ind}: {c}종목')

top20 = [r[0] for r in rows]
top2 = [r[0] for r in rows[:2]]

# 매칭 ETF (max overlap)
def match(tickers):
    ts = set(tickers)
    best = None
    for sym, info in etf.items():
        h = info.get('holdings', {})
        mt = {t: h[t] for t in ts if t in h}
        if not mt or sum(mt.values()) / len(mt) < 0.01:
            continue
        c = (len(mt), sum(mt.values()), sym, mt)
        if best is None or c[:2] > best[:2]:
            best = c
    return best

b = match(top20)
sym, mt = b[2], b[3]
hold = etf[sym]['holdings']
print(f'\n=== 매칭 ETF: {sym} ({etf[sym]["name"]}) — 총 {len(hold)} 종목 보유 ===')
print(f'우리 top-20 중 {b[0]}종목 겹침 (합산 {b[1]*100:.1f}%)')
print(f'→ ETF가 {len(hold)}종목 분산 보유 = 섹터 전반 대표. 우리 top-20 밖 {len(hold)-b[0]}종목이 추가 분산.')

print(f'\n=== 중복 체크: 개별픽 2종목이 ETF에 있나 ===')
for tk in top2:
    inn = tk in hold
    print(f'  {tk}: ETF 포함 {"O ("+str(round(hold[tk]*100,1))+"%)" if inn else "X"}')
print('→ 개별픽이 ETF에 없거나 비중 작으면 = 중복 아닌 순수 분산 (취지 부합).')

# 업종 키워드 → 대표 섹터ETF 매핑 제안 (참고)
print('\n=== 참고: 업종 분포 → 대표 섹터ETF ===')
print('  반도체/반도체장비 → SOXX (광범위 반도체) / 기술 전반 → XLK')
print('  방산 → ITA/XAR / 건설 → 별도 / 혼합이면 dominant 섹터 ETF')
