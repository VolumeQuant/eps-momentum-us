# -*- coding: utf-8 -*-
"""백분위 정규화 역사적 검증 (2026-07-09).

세 질문:
  Q1. KR>US 리비전 갭이 26일 내내 안정적인가? (안정=구조적 정규화 정당)
  Q2. 정규화가 top5를 얼마나 자주 바꾸는가? (영향 빈도)
  Q3. 절대 top5 vs 백분위 top5 — 전방 수익 비교 (얇지만 방향성)

랭킹 방법만 바꿔 비교(동일 후보풀). 게이트는 light-common(rev90>0·유동성)만.
통화는 종목 내 수익률이라 상쇄.
"""
import os, sqlite3, statistics as st

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
US_DB = os.path.join(HERE, 'eps_momentum_data.db')
KR_DB = os.path.join(HERE, 'research', 'kr_db_snapshot_2026_07_09.db')
DV_MIN = 1000.0  # US $1B


def seg(a, b):
    return (a - b) / abs(b) * 100 if (a and b and b > 0) else None


def load_day(db, date, market, liq_proxy=False):
    c = sqlite3.connect(db)
    cols = [r[1] for r in c.execute("PRAGMA table_info(ntm_screening)")]
    dv_col = 'dollar_volume_30d' if 'dollar_volume_30d' in cols else 'NULL'
    rows = c.execute(
        f"SELECT ticker, price, ntm_current, ntm_90d, {dv_col}, market_cap "
        f"FROM ntm_screening WHERE date=? AND ntm_current>0 AND ntm_90d>0.1", (date,)).fetchall()
    c.close()
    out = []
    for tk, p, nc, n90, dv, mc in rows:
        r = seg(nc, n90)
        if r is None or p is None:
            continue
        out.append(dict(ticker=tk, market=market, rev90=r, price=p, dv=dv, mc=mc))
    return out


def fwd_price(db, ticker, d0, d1):
    c = sqlite3.connect(db)
    r = c.execute("SELECT price FROM ntm_screening WHERE ticker=? AND date=?", (ticker, d1)).fetchone()
    c.close()
    return r[0] if r else None


def us_dates():
    c = sqlite3.connect(US_DB)
    ds = [r[0] for r in c.execute("SELECT DISTINCT date FROM ntm_screening ORDER BY date")]
    c.close()
    return ds


def kr_dates():
    c = sqlite3.connect(KR_DB)
    ds = [r[0] for r in c.execute("SELECT DISTINCT date FROM ntm_screening ORDER BY date")]
    c.close()
    return ds


USD = us_dates()
KRD = set(kr_dates())
# 겹치는 날 (KR 기준)
common = [d for d in USD if d in KRD]
print(f"겹치는 거래일 {len(common)}: {common[0]}~{common[-1]}")


def build_pool(date):
    us = [d for d in load_day(US_DB, date, 'US') if d['dv'] and d['dv'] >= DV_MIN and d['rev90'] > 0]
    kr = [d for d in load_day(KR_DB, date, 'KR') if d['rev90'] > 0]
    return us, kr


def rank_absolute(us, kr):
    return sorted(us + kr, key=lambda d: -d['rev90'])


def rank_pctile(us, kr):
    uv = [d['rev90'] for d in load_day(US_DB, cur_date, 'US') if d['dv'] and d['dv'] >= DV_MIN]
    kv = [d['rev90'] for d in load_day(KR_DB, cur_date, 'KR')]
    for d in us:
        d['pct'] = sum(1 for v in uv if v < d['rev90']) / max(len(uv), 1) * 100
    for d in kr:
        d['pct'] = sum(1 for v in kv if v < d['rev90']) / max(len(kv), 1) * 100
    return sorted(us + kr, key=lambda d: (-d['pct'], -d['rev90']))


# ── Q1. KR-US 갭 안정성 ──
print("\n=== Q1. KR vs US 리비전 갭 (일별 중앙값) ===")
print(f"{'date':12}{'US_med':>8}{'KR_med':>8}{'US_MAD':>8}{'KR_MAD':>8}{'gap배수':>8}")
gaps = []
for d in common:
    us = [x['rev90'] for x in load_day(US_DB, d, 'US') if x['dv'] and x['dv'] >= DV_MIN]
    kr = [x['rev90'] for x in load_day(KR_DB, d, 'KR')]
    if len(us) < 10 or len(kr) < 10:
        continue
    um, km = st.median(us), st.median(kr)
    umad = st.median([abs(v - um) for v in us]) or 1e-9
    kmad = st.median([abs(v - km) for v in kr]) or 1e-9
    ratio = km / um if um else float('nan')
    gaps.append(ratio)
    print(f"{d:12}{um:>8.1f}{km:>8.1f}{umad:>8.1f}{kmad:>8.1f}{ratio:>8.1f}")
if gaps:
    print(f"\n갭배수(KR중앙값/US중앙값): 평균 {st.mean(gaps):.1f}x, 중앙값 {st.median(gaps):.1f}x, "
          f"최소 {min(gaps):.1f}x 최대 {max(gaps):.1f}x, 표준편차 {st.pstdev(gaps):.2f}")
    print(f"→ 26일 내내 KR>US 유지? {'예' if min(gaps) > 1 else '아니오'} "
          f"(최소 갭배수 {min(gaps):.1f}x > 1이면 항상 KR이 더 뜨거움)")

# ── Q2 & Q3. top5 변화 빈도 + 전방수익 ──
print("\n=== Q2/Q3. top5 변화 + 전방수익 (rebal 5거래일) ===")
REBAL = 5
common_idx = {d: i for i, d in enumerate(common)}
abs_ret, pct_ret = [], []
n_diff = 0
for i in range(0, len(common) - REBAL, REBAL):
    cur_date = common[i]
    nxt_date = common[i + REBAL]
    us, kr = build_pool(cur_date)
    if len(us) + len(kr) < 6:
        continue
    a5 = rank_absolute([dict(x) for x in us], [dict(x) for x in kr])[:5]
    p5 = rank_pctile([dict(x) for x in us], [dict(x) for x in kr])[:5]
    aset = {x['ticker'] for x in a5}
    pset = {x['ticker'] for x in p5}
    changed = aset != pset

    def port_ret(picks):
        rs = []
        for x in picks:
            db = US_DB if x['market'] == 'US' else KR_DB
            p1 = fwd_price(db, x['ticker'], cur_date, nxt_date)
            if p1 and x['price']:
                rs.append((p1 / x['price'] - 1) * 100)
        return st.mean(rs) if rs else None
    ar, pr = port_ret(a5), port_ret(p5)
    if ar is not None and pr is not None:
        abs_ret.append(ar)
        pct_ret.append(pr)
        if changed:
            n_diff += 1
        diff_names = (pset - aset)
        print(f"{cur_date}→{nxt_date}: 절대 {ar:+5.1f}% | 백분위 {pr:+5.1f}% | "
              f"{'교체' if changed else '동일'} {'+'.join(t.replace('.KS','') for t in diff_names) if changed else ''}")

if abs_ret:
    print(f"\n절대 top5 누적: {sum(abs_ret):+.1f}%  (기간평균 {st.mean(abs_ret):+.2f}%/rebal)")
    print(f"백분위 top5 누적: {sum(pct_ret):+.1f}%  (기간평균 {st.mean(pct_ret):+.2f}%/rebal)")
    print(f"차이(백분위-절대): {sum(pct_ret) - sum(abs_ret):+.1f}%p, top5 달랐던 rebal {n_diff}/{len(abs_ret)}")
    print("⚠️ N=%d rebal·26일·단일regime — 방향성만. 안정성(Q1)이 더 강한 근거." % len(abs_ret))
