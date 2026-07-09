# -*- coding: utf-8 -*-
"""KR vs US 애널리스트 리비전 정규화 연구 (2026-07-09).

문제: 통합 top5는 rev90(=90일 전망상향폭) 순으로 뽑는데, KR 애널리스트와
US 애널리스트가 같은 자로 잰 게 아니다. LG이노텍(+50.6%)이 HPE(+49.2%)를
1.4%p 차로 눌러 HPE를 밀어냈으나 HPE가 PER·gap·유동성·분석가 전원상향 등
객관 지표 전부 우위 → rev90 절대값 비교의 결함.

EDA + 정규화 방법 비교로 "무엇으로 재야 공정한가"를 결정한다.
"""
import os, sqlite3, statistics as st, math

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
US_DB = os.path.join(HERE, 'eps_momentum_data.db')
KR_DB = os.path.join(HERE, 'research', 'kr_db_snapshot_2026_07_09.db')


def seg(a, b):
    return (a - b) / abs(b) * 100 if (a and b and b > 0) else None


def load_universe(db, market):
    """유니버스 전체의 rev90 + 유동성 프록시. 게이트 미적용(원시 분포)."""
    c = sqlite3.connect(db)
    last = c.execute("SELECT MAX(date) FROM ntm_screening").fetchone()[0]
    cols = [r[1] for r in c.execute("PRAGMA table_info(ntm_screening)")]
    dv_col = 'dollar_volume_30d' if 'dollar_volume_30d' in cols else 'NULL'
    rows = c.execute(
        f"SELECT ticker, price, ntm_current, ntm_90d, {dv_col}, market_cap, num_analysts "
        f"FROM ntm_screening WHERE date=? AND ntm_current>0 AND ntm_90d>0.1", (last,)).fetchall()
    c.close()
    out = []
    for tk, p, nc, n90, dv, mc, na in rows:
        r = seg(nc, n90)
        if r is None:
            continue
        out.append(dict(ticker=tk, market=market, rev90=r, price=p, dv=dv,
                        mc=mc, na=na, fwd_per=(p / nc if nc else None)))
    return last, out


def dist_stats(vals):
    vals = sorted(vals)
    n = len(vals)
    med = st.median(vals)
    mad = st.median([abs(v - med) for v in vals]) or 1e-9
    mean = st.mean(vals)
    sd = st.pstdev(vals) or 1e-9

    def pct(q):
        return vals[min(n - 1, int(q * n))]
    return dict(n=n, median=med, mad=mad, mean=mean, sd=sd,
                p50=pct(.50), p75=pct(.75), p90=pct(.90), p95=pct(.95), p99=pct(.99),
                pmax=vals[-1])


def pctile_of(x, vals):
    return sum(1 for v in vals if v < x) / len(vals) * 100


def robust_z(x, s):
    # 0.6745: MAD→sigma 환산 상수 (정규분포 가정)
    return (x - s['median']) / s['mad'] * 0.6745


def std_z(x, s):
    return (x - s['mean']) / s['sd']


print("=" * 72)
print("STEP 1. 유니버스 원시 분포 EDA (게이트 미적용, rev90 %)")
print("=" * 72)
us_last, us_uni = load_universe(US_DB, 'US')
kr_last, kr_uni = load_universe(KR_DB, 'KR')
us_r = [d['rev90'] for d in us_uni]
kr_r = [d['rev90'] for d in kr_uni]
S_us_raw = dist_stats(us_r)
S_kr_raw = dist_stats(kr_r)
print(f"US({us_last}) N={S_us_raw['n']}  KR({kr_last}) N={S_kr_raw['n']}")
print(f"{'':10}{'median':>9}{'MAD':>8}{'mean':>8}{'sd':>8}{'p75':>8}{'p90':>8}{'p95':>8}{'p99':>8}{'max':>9}")
for lbl, S in [('US raw', S_us_raw), ('KR raw', S_kr_raw)]:
    print(f"{lbl:10}{S['median']:>9.1f}{S['mad']:>8.1f}{S['mean']:>8.1f}{S['sd']:>8.1f}"
          f"{S['p75']:>8.1f}{S['p90']:>8.1f}{S['p95']:>8.1f}{S['p99']:>8.1f}{S['pmax']:>9.1f}")
print()
print(f"→ KR 리비전이 구조적으로 뜨거운가? 중앙값 US {S_us_raw['median']:+.1f} vs KR {S_kr_raw['median']:+.1f}, "
      f"p90 US {S_us_raw['p90']:.1f} vs KR {S_kr_raw['p90']:.1f}")
print(f"→ ★유니버스 크기 비대칭: US {S_us_raw['n']}개 vs KR {S_kr_raw['n']}개 "
      f"(KR은 애널리스트 커버 종목만 수집 = 이미 엘리트 집합)")

print()
print("=" * 72)
print("STEP 2. 유동성 필터 유니버스 (US $1B / KR $0.3B 프록시)로 재-EDA")
print("=" * 72)
# US: dv >= $1000M
us_liq = [d for d in us_uni if d['dv'] and d['dv'] >= 1000.0]
# KR: dv 없음 → market_cap 프록시. $0.3B/일 ≈ 회전율 가정 필요.
#   KR dv는 후속 yf 조회로만 가능. 여기선 market_cap 상위로 US $1B 체급($1B/일≈시총 수조) 근사.
#   보수적으로 시총 상위 그룹을 여러 컷으로 관찰.
kr_mc = sorted([d for d in kr_uni if d['mc']], key=lambda d: -d['mc'])
print(f"US $1B 필터: {len(us_liq)}/{len(us_uni)} ({len(us_liq)/len(us_uni)*100:.1f}%)")
if us_liq:
    S_us_liq = dist_stats([d['rev90'] for d in us_liq])
    print(f"  US $1B: median {S_us_liq['median']:+.1f}  MAD {S_us_liq['mad']:.1f}  "
          f"p90 {S_us_liq['p90']:.1f}  p95 {S_us_liq['p95']:.1f}  max {S_us_liq['pmax']:.1f}")
else:
    S_us_liq = S_us_raw
# KR 여러 시총 컷
for frac in (1.0, 0.5, 0.3):
    k = max(10, int(len(kr_mc) * frac))
    sub = kr_mc[:k]
    S = dist_stats([d['rev90'] for d in sub])
    print(f"  KR 시총상위 {frac*100:.0f}% (N={S['n']}): median {S['median']:+.1f}  "
          f"MAD {S['mad']:.1f}  p90 {S['p90']:.1f}  max {S['pmax']:.1f}")
# KR 유동성 유니버스 = 전체 73 (이미 엘리트) 채택
S_kr_liq = dist_stats(kr_r)

print()
print("=" * 72)
print("STEP 3. 우리 후보를 각 정규화로 재랭킹 → top5 & LG이노텍 vs HPE")
print("=" * 72)
# 로그에서 오늘 후보 20개 로드
import csv
LOG = os.path.join(HERE, 'data_cache', 'unified_vm_log.csv')
rows = list(csv.DictReader(open(LOG, encoding='utf-8')))
today = rows[-1]['run_date']
cand = [r for r in rows if r['run_date'] == today]
# 마지막 실행 블록만
starts = [i for i, r in enumerate(cand) if r['rank'] == '1']
if starts:
    cand = cand[starts[-1]:]
NAME = {'011070.KS': 'LG이노텍', '000660.KS': 'SK하이닉스', '005930.KS': '삼성전자',
        '066570.KS': 'LG전자', '009150.KS': '삼성전기'}


def disp(tk):
    return NAME.get(tk, tk)


for c in cand:
    c['rev90'] = float(c['rev90'])
    c['fwd_per'] = float(c['fwd_per'])
    c['market'] = c['market']
    S = S_us_liq if c['market'] == 'US' else S_kr_liq
    liq_vals = [d['rev90'] for d in us_liq] if c['market'] == 'US' else kr_r
    raw_vals = us_r if c['market'] == 'US' else kr_r
    c['pct_raw'] = pctile_of(c['rev90'], raw_vals)
    c['pct_liq'] = pctile_of(c['rev90'], liq_vals)
    c['rz'] = robust_z(c['rev90'], S)
    c['sz'] = std_z(c['rev90'], S)

methods = [('절대 rev90', lambda c: c['rev90']),
           ('원시백분위', lambda c: c['pct_raw']),
           ('유동성백분위', lambda c: c['pct_liq']),
           ('robust-z(MAD)', lambda c: c['rz']),
           ('표준z', lambda c: c['sz'])]

for name, key in methods:
    ranked = sorted(cand, key=lambda c: -key(c))
    top5 = [disp(c['ticker']) for c in ranked[:5]]
    # LG이노텍 vs HPE 상대순위
    order = [c['ticker'] for c in ranked]
    lg = order.index('011070.KS') + 1 if '011070.KS' in order else None
    hpe = order.index('HPE') + 1 if 'HPE' in order else None
    verdict = ''
    if lg and hpe:
        verdict = f"  [이노텍 {lg}위 vs HPE {hpe}위 → {'HPE 우위' if hpe < lg else '이노텍 우위'}]"
    print(f"\n{name:16} top5: {', '.join(top5)}{verdict}")

print()
print("=" * 72)
print("STEP 4. 방법별 raw 점수표 (핵심 종목)")
print("=" * 72)
key_tk = ['SNDK', '000660.KS', 'MU', '005930.KS', '011070.KS', 'HPE', 'FLEX']
print(f"{'종목':14}{'rev90':>8}{'pct_raw':>9}{'pct_liq':>9}{'robZ':>7}{'stdZ':>7}{'PER':>6}")
for tk in key_tk:
    c = next((x for x in cand if x['ticker'] == tk), None)
    if not c:
        continue
    print(f"{disp(tk):14}{c['rev90']:>8.1f}{c['pct_raw']:>9.1f}{c['pct_liq']:>9.1f}"
          f"{c['rz']:>7.1f}{c['sz']:>7.1f}{c['fwd_per']:>6.1f}")
