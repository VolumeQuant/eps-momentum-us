# -*- coding: utf-8 -*-
"""B안 프로토타입: 자체 아카이브 point-in-time rev90 vs yfinance rolling-column rev90.

현재 rev90 = (ntm_current − ntm_90d[yfinance '90daysAgo' 컬럼]) / ntm_90d.
'90daysAgo'가 rolling이라 glitch(삼성 7/8). 대안 = 우리가 매일 저장한 ntm_current로
rev90(t) = ntm_current(t)/ntm_current(t−~90거래일) − 1. rolling 컬럼 안 씀 → glitch 면역.

US 150일 히스토리로 두 방식을 비교, 괴리(=rolling 컬럼 오염) 빈도를 정량화한다.
"""
import os, sqlite3, statistics as st

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
US_DB = os.path.join(HERE, 'eps_momentum_data.db')

c = sqlite3.connect(US_DB)
dates = [r[0] for r in c.execute("SELECT DISTINCT date FROM ntm_screening ORDER BY date")]
print(f"US 히스토리 {len(dates)}일: {dates[0]} ~ {dates[-1]}")

# ntm_current 아카이브: {ticker: {date: nc}}
arch = {}
for tk, d, nc in c.execute("SELECT ticker, date, ntm_current FROM ntm_screening WHERE ntm_current>0"):
    arch.setdefault(tk, {})[d] = nc
c.close()

LB = 62  # ~90 캘린더일 ≈ 62 거래일
if len(dates) <= LB + 5:
    print("히스토리 부족"); raise SystemExit
# 최근 30일에 대해 두 방식 비교
test_dates = dates[LB:]
diffs = []  # |pit - rolling| (절대 rev90 %p)
big = 0     # 15%p+ 괴리 = rolling 오염 의심
n = 0
per_ticker_big = {}
c = sqlite3.connect(US_DB)
for i, d in enumerate(dates):
    if i < LB:
        continue
    d90 = dates[i - LB]
    rows = c.execute("SELECT ticker, ntm_current, ntm_90d FROM ntm_screening "
                     "WHERE date=? AND ntm_current>0 AND ntm_90d>0.1", (d,)).fetchall()
    for tk, nc, n90 in rows:
        rolling = (nc - n90) / abs(n90) * 100
        nc_past = arch.get(tk, {}).get(d90)
        if not nc_past or nc_past <= 0:
            continue
        pit = (nc - nc_past) / nc_past * 100
        gap = abs(pit - rolling)
        diffs.append(gap)
        n += 1
        if gap > 15:
            big += 1
            per_ticker_big[tk] = per_ticker_big.get(tk, 0) + 1
c.close()

print(f"\n비교 표본 {n}건 (종목×날짜, 최근 {len(test_dates)}일)")
print(f"|point-in-time − rolling| rev90 괴리: 중앙값 {st.median(diffs):.1f}%p, "
      f"평균 {st.mean(diffs):.1f}%p, 90%tile {sorted(diffs)[int(len(diffs)*0.9)]:.1f}%p, 최대 {max(diffs):.0f}%p")
print(f"15%p+ 괴리(rolling 오염 의심): {big}건 ({big/n*100:.1f}%)")
print(f"\n괴리 잦은 종목 top10 (rolling 컬럼 불안정 종목):")
for tk, cnt in sorted(per_ticker_big.items(), key=lambda x: -x[1])[:10]:
    print(f"  {tk}: {cnt}회")

# 삼성 유형(대형 winner)이 US에도 있는지 — 최근일 대형 괴리 상위
print(f"\n최근일({dates[-1]}) rolling vs pit 대형 괴리 종목:")
c = sqlite3.connect(US_DB)
d = dates[-1]; d90 = dates[-1 - LB]
rows = c.execute("SELECT ticker, ntm_current, ntm_90d FROM ntm_screening "
                 "WHERE date=? AND ntm_current>0 AND ntm_90d>0.1", (d,)).fetchall()
tab = []
for tk, nc, n90 in rows:
    ncp = arch.get(tk, {}).get(d90)
    if not ncp or ncp <= 0:
        continue
    rolling = (nc - n90) / abs(n90) * 100
    pit = (nc - ncp) / ncp * 100
    tab.append((tk, rolling, pit, abs(pit - rolling)))
for tk, ro, pi, g in sorted(tab, key=lambda x: -x[3])[:8]:
    print(f"  {tk}: rolling {ro:+.0f}% vs point-in-time {pi:+.0f}%  (괴리 {g:.0f}%p)")
c.close()
