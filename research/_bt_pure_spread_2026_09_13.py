# -*- coding: utf-8 -*-
"""순수 괴리(spread) 반사실 백테스트 — 2026-09-13 사용자 질문
"복잡한 규칙 없이 처음 의도(EPS-가격 괴리)만으로 갔으면 어땠을까"

공통 하네스: 5거래일 리밸(위상 0~4 평균), exec_lag=1(신호 익일 종가 체결),
비용 10bp x 편도 회전, N5 동일비중, 기간 2026-02-17 ~ 2026-09-11 (DB 전체).

전략:
  PURE    : adj_gap(낮을수록 좋음) top5. 게이트 0개.
  PURE_DV : + 거래대금 $300M만 (체결 가능성, 알파 아님).
  CUR     : 현행 규칙 근사 — dv300 + PER<=30 + rev_up30>=3 + na>=6(missing=pass)
            + min_seg>=-2 + 가치함정(rev30<=2 & px20<0 컷) + 급락컷(px20<=-20)
            + 보유밴드7. (업종제외·dedup·동전주 등 소소한 것 생략 = 근사)
adj_gap = DB 저장값(v80 산식, 전 기간 동일 가중 = 균일).
"""
import sqlite3, sys
import numpy as np
import pandas as pd

DB = 'eps_momentum_data.db'
COST = 0.0010          # 10bp per one-way turnover
N = 5
R = 5                  # rebalance every 5 trading days
START = '2026-02-17'

conn = sqlite3.connect(DB)
df = pd.read_sql("""
  SELECT date, ticker, adj_gap, price, dollar_volume_30d dv,
         ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
         rev_up30, num_analysts na
  FROM ntm_screening""", conn)
df = df[df.price.notna() & (df.price > 0)]
dates = sorted(df.date.unique())
didx = {d: i for i, d in enumerate(dates)}

px = df.pivot_table(index='date', columns='ticker', values='price', aggfunc='last')
px = px.reindex(dates).ffill()

# 파생 지표
def seg(a, b):
    with np.errstate(divide='ignore', invalid='ignore'):
        return np.where((b > 0) & (a > 0), (a / b - 1) * 100, np.nan)

df['fwd_per'] = np.where(df.ntm_current > 0, df.price / df.ntm_current, np.nan)
df['rev30'] = seg(df.ntm_current, df.ntm_30d)
s1 = seg(df.ntm_current, df.ntm_7d); s2 = seg(df.ntm_7d, df.ntm_30d)
s3 = seg(df.ntm_30d, df.ntm_60d);   s4 = seg(df.ntm_60d, df.ntm_90d)
df['min_seg'] = np.nanmin(np.vstack([s1, s2, s3, s4]), axis=0)

px20 = px.pct_change(20) * 100  # 20거래일 가격 변화율(%)

by_date = {d: g.set_index('ticker') for d, g in df.groupby('date')}

def select(date, mode, held, ban=None):
    g = by_date[date]
    g = g[g.adj_gap.notna()]
    if ban:
        g = g[~g.index.isin(ban)]
    if mode in ('PURE_DV', 'CUR'):
        g = g[g.dv.fillna(0) >= 300]
    if mode == 'CUR':
        g = g[(g.fwd_per > 0) & (g.fwd_per <= 30)]
        g = g[g.rev_up30.fillna(99) >= 3]
        g = g[g.na.isna() | (g.na == 0) | (g.na >= 6)]
        g = g[g.min_seg.fillna(0) >= -2]
        p20 = px20.loc[date] if date in px20.index else pd.Series(dtype=float)
        c20 = g.index.map(lambda t: p20.get(t, np.nan))
        c20 = pd.Series(c20, index=g.index)
        trap = (g.rev30.fillna(99) <= 2) & (c20 < 0)
        crash = c20 <= -20
        g = g[~(trap | crash)]
    g = g.sort_values('adj_gap')
    ranked = list(g.index)
    top = ranked[:N]
    if mode == 'CUR' and held:  # 보유밴드 7
        band = set(ranked[:7])
        keep = [t for t in held if t in band]
        out = keep + [t for t in top if t not in keep]
        return out[:N]
    return top

def run(mode, phase, ban=None):
    i0 = didx[START] if START in didx else 0
    days = dates[i0:]
    nav = 1.0; peak = 1.0; mdd = 0.0
    w = {}          # ticker -> weight
    pend = None     # (target list) to execute next day
    navs = []
    contrib = {}
    first_reb = phase
    for k, d in enumerate(days):
        # 1) 오늘 수익 반영 (전일 종가 -> 오늘 종가)
        if w and k > 0:
            r = 0.0; neww = {}
            for t, wt in w.items():
                p1 = px.at[days[k-1], t] if t in px.columns else np.nan
                p2 = px.at[d, t] if t in px.columns else np.nan
                rt = (p2 / p1 - 1) if (p1 and p2 and p1 > 0 and not np.isnan(p1) and not np.isnan(p2)) else 0.0
                r += wt * rt
                neww[t] = wt * (1 + rt)
                contrib[t] = contrib.get(t, 0.0) + nav * wt * rt
            nav *= (1 + r)
            tot = sum(neww.values())
            w = {t: v / tot for t, v in neww.items()} if tot > 0 else {}
        # 2) 어제 신호 체결 (오늘 종가)
        if pend is not None:
            tgt = {t: 1.0 / len(pend) for t in pend} if pend else {}
            to = sum(abs(tgt.get(t, 0) - w.get(t, 0)) for t in set(tgt) | set(w)) / 2
            nav *= (1 - COST * to * 2)   # 편도 10bp x 매수+매도
            w = tgt; pend = None
        # 3) 리밸일이면 신호 산출 (내일 체결)
        if (k - first_reb) % R == 0 and k >= first_reb and k < len(days) - 1:
            pend = select(d, mode, list(w.keys()), ban)
        peak = max(peak, nav); mdd = min(mdd, nav / peak - 1)
        navs.append((d, nav))
    return nav - 1, mdd, navs, contrib

def summary(mode, ban=None, label=None):
    rets, mdds, allc = [], [], {}
    for ph in range(R):
        r, m, _, c = run(mode, ph, ban)
        rets.append(r); mdds.append(m)
        for t, v in c.items():
            allc[t] = allc.get(t, 0) + v / R
    top3 = sorted(allc.items(), key=lambda x: -x[1])[:3]
    bot3 = sorted(allc.items(), key=lambda x: x[1])[:3]
    print('%-10s 위상평균 %+7.1f%%  (범위 %+.1f ~ %+.1f)  MDD평균 %5.1f%%  최악 %5.1f%%'
          % (label or mode, np.mean(rets) * 100, min(rets) * 100, max(rets) * 100,
             np.mean(mdds) * 100, min(mdds) * 100))
    print('           기여 top3: %s | 최악: %s'
          % (', '.join('%s %+.1f%%p' % (t, v * 100) for t, v in top3),
             ', '.join('%s %+.1f%%p' % (t, v * 100) for t, v in bot3)))
    return np.mean(rets), allc

print('기간 %s ~ %s (%d거래일), R%d 위상평균, 비용 %dbp, N%d EW, exec_lag=1'
      % (START, dates[-1], len(dates) - didx[START], R, COST * 10000, N))
print()
r_pure, c_pure = summary('PURE', label='순수괴리')
r_dv, c_dv = summary('PURE_DV', label='괴리+dv300')
r_cur, c_cur = summary('CUR', label='현행근사')

# LOWO: 각 전략의 최대 기여 종목 금지 후 재실행
print()
for mode, c, label in [('PURE', c_pure, '순수괴리'), ('PURE_DV', c_dv, '괴리+dv300'), ('CUR', c_cur, '현행근사')]:
    w1 = max(c.items(), key=lambda x: x[1])[0]
    summary(mode, ban={w1}, label='%s -%s' % (label, w1))
