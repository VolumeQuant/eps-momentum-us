"""블렌드가 산 ETF가 뭔지 투명하게 — 일별 매칭 ETF + 겹친 top-20 종목

find_etf_recommendations 동일 로직(매칭수→비중, 평균비중<1% 희석필터).
"""
import sys
import json
from pathlib import Path
from collections import Counter
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / 'research'))
from regime_eda_market import fetch_close  # noqa
import sqlite3

etf = json.load(open(ROOT / 'etf_holdings_cache_v2.json', encoding='utf-8'))
con = sqlite3.connect(ROOT / 'eps_momentum_data.db')
dates = [r[0] for r in con.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]


def top20(d):
    return [r[0] for r in con.execute(
        'SELECT ticker FROM ntm_screening WHERE date=? AND part2_rank<=20 ORDER BY part2_rank', (d,))]


def match(tickers):
    ts = set(tickers)
    best = None
    for sym, info in etf.items():
        h = info.get('holdings', {})
        matched = {t: h[t] for t in ts if t in h}
        if not matched:
            continue
        if sum(matched.values()) / len(matched) < 0.01:  # 희석필터
            continue
        cand = (len(matched), sum(matched.values()), sym, matched, info.get('name', sym))
        if best is None or cand[:2] > best[:2]:
            best = cand
    return best  # (count, overlap, sym, matched_dict, name)


# 일별 매칭 → 연속 구간으로 묶어 timeline
runs = []
for d in dates:
    b = match(top20(d))
    sym = b[2] if b else None
    if runs and runs[-1][0] == sym:
        runs[-1][2] = d
        runs[-1][3] += 1
    else:
        runs.append([sym, d, d, 1])

print('=== 매칭 ETF 타임라인 (연속 구간) ===')
for sym, s, e, n in runs:
    nm = etf.get(sym, {}).get('name', '')
    print(f'  {s} ~ {e} ({n}일): {sym}  {nm}')

print(f'\n분포: {dict(Counter(r[0] for d in dates for r in [match(top20(d))] if True).most_common()) }')
# 위 한 줄이 복잡 → 간단히 재계산
dist = Counter(match(top20(d))[2] for d in dates)
print('분포:', dict(dist))

# 대표일 상세: 최신일 + 각 ETF 처음 등장일
print('\n=== 왜 그 ETF인가 — 겹친 top-20 종목 (비중) ===')
shown = set()
for d in [dates[-1]] + [r[1] for r in runs]:
    b = match(top20(d))
    if not b:
        continue
    sym = b[2]
    if sym in shown:
        continue
    shown.add(sym)
    cnt, ov, _, matched, nm = b
    items = sorted(matched.items(), key=lambda x: -x[1])
    detail = ', '.join(f'{t} {w*100:.1f}%' for t, w in items[:10])
    print(f'\n[{d}] {sym} ({nm})')
    print(f'  top-20 중 {cnt}종목 포함, 합산비중 {ov*100:.1f}%')
    print(f'  {detail}' + (' ...' if len(items) > 10 else ''))


# 매칭 ETF 가격 수익 (참고)
print('\n=== 매칭 ETF 기간 수익 (참고) ===')
for sym in dist:
    p = fetch_close(sym)
    seg = p.loc[dates[0]:dates[-1]]
    if len(seg) > 1:
        print(f'  {sym}: {(seg.iloc[-1]/seg.iloc[0]-1)*100:+.1f}%')
