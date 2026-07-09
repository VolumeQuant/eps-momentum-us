# -*- coding: utf-8 -*-
"""ntm_90d glitch 교정 후 통합 top5 재분석 (2026-07-09 자율주행).

발견: 삼성전자 ntm_90d(90일전 기준선)가 7/8 +20% spurious jump → rev90 92%→64% 붕괴
→ 정규화 top5에서 삼성 부당 탈락. 기준선은 원래 day-to-day 안정해야 함.
교정: rev90 = (ntm_current - median6(ntm_90d)) / median6(ntm_90d), US·KR 동일 적용.
"""
import os, sqlite3, statistics as st

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
US_DB = os.path.join(HERE, 'eps_momentum_data.db')
KR_DB = os.path.join(HERE, 'research', 'kr_db_snapshot_2026_07_09.db')
DV_MIN = 1000.0


def robust_rev90_universe(db, dv_min=None, lookback=6):
    """유니버스 전체의 rev90 (오늘값)과 robust rev90(중앙값 기준선) 둘 다."""
    c = sqlite3.connect(db)
    last = c.execute("SELECT MAX(date) FROM ntm_screening").fetchone()[0]
    cols = [r[1] for r in c.execute("PRAGMA table_info(ntm_screening)")]
    has_dv = 'dollar_volume_30d' in cols
    days = [r[0] for r in c.execute("SELECT DISTINCT date FROM ntm_screening ORDER BY date DESC LIMIT ?", (lookback,))]
    dmin = min(days)
    # 종목별 ntm_90d 히스토리
    hist = {}
    for tk, d, n90 in c.execute("SELECT ticker, date, ntm_90d FROM ntm_screening WHERE date>=?", (dmin,)):
        if n90 and n90 > 0.1:
            hist.setdefault(tk, []).append(n90)
    dvq = ", dollar_volume_30d" if has_dv else ", NULL"
    rows = c.execute(f"SELECT ticker, ntm_current, ntm_90d{dvq} FROM ntm_screening "
                     f"WHERE date=? AND ntm_current>0 AND ntm_90d>0.1", (last,)).fetchall()
    c.close()
    out = []
    for tk, nc, n90, dv in rows:
        if dv_min and (dv is None or dv < dv_min):
            continue
        base_robust = st.median(hist.get(tk, [n90]))
        rev_t = (nc - n90) / n90 * 100
        rev_r = (nc - base_robust) / base_robust * 100
        out.append(dict(ticker=tk, rev_today=rev_t, rev_robust=rev_r,
                        n90_today=n90, n90_robust=base_robust, nc=nc))
    return last, out


def pct(x, vals):
    return sum(1 for v in vals if v < x) / len(vals) * 100 if vals else 0


us_last, us_uni = robust_rev90_universe(US_DB, dv_min=DV_MIN)
kr_last, kr_uni = robust_rev90_universe(KR_DB)
NAME = {'005930.KS': '삼성전자', '000660.KS': 'SK하이닉스', '011070.KS': 'LG이노텍',
        '009150.KS': '삼성전기', '066570.KS': 'LG전자'}


def dn(tk):
    return NAME.get(tk, tk)


# 후보(오늘 통합 로그 상위 20, 게이트 통과분) — DB에서 직접 못 뽑으니 알려진 후보 사용
# 대신 유니버스에서 rev90_robust>0로 필터 + 시장 태그
CAND_US = ['SNDK', 'MU', 'HPE', 'FLEX', 'MCHP', 'NVDA', 'AVGO', 'AAOI']
CAND_KR = ['000660.KS', '005930.KS', '011070.KS', '066570.KS', '009150.KS']
us_map = {d['ticker']: d for d in us_uni}
kr_map = {d['ticker']: d for d in kr_uni}
us_rev_t = [d['rev_today'] for d in us_uni]
us_rev_r = [d['rev_robust'] for d in us_uni]
kr_rev_t = [d['rev_today'] for d in kr_uni]
kr_rev_r = [d['rev_robust'] for d in kr_uni]

print("=" * 78)
print("후보별 rev90: 오늘값(glitch포함) vs robust(중앙값 기준선) + 백분위")
print("=" * 78)
print(f"{'종목':12}{'시장':>4}{'rev_today':>10}{'rev_robust':>11}{'pct_today':>10}{'pct_robust':>11}")
merged = []
for tk in CAND_US:
    d = us_map.get(tk)
    if not d:
        continue
    pt = pct(d['rev_today'], us_rev_t)
    pr = pct(d['rev_robust'], us_rev_r)
    merged.append((dn(tk), tk, 'US', d['rev_today'], d['rev_robust'], pt, pr))
for tk in CAND_KR:
    d = kr_map.get(tk)
    if not d:
        continue
    pt = pct(d['rev_today'], kr_rev_t)
    pr = pct(d['rev_robust'], kr_rev_r)
    merged.append((dn(tk), tk, 'KR', d['rev_today'], d['rev_robust'], pt, pr))
for nm, tk, mk, rt, rr, pt, pr in merged:
    flag = ' ★glitch' if abs(rr - rt) > 15 else ''
    print(f"{nm:12}{mk:>4}{rt:>10.1f}{rr:>11.1f}{pt:>10.1f}{pr:>11.1f}{flag}")

print("\n" + "=" * 78)
print("top5 비교")
print("=" * 78)
by_today = sorted(merged, key=lambda x: -x[5])[:5]
by_robust = sorted(merged, key=lambda x: -x[6])[:5]
print("현행(glitch) 백분위 top5:", [x[0] for x in by_today])
print("교정(robust) 백분위 top5:", [x[0] for x in by_robust])
