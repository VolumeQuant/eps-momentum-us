# -*- coding: utf-8 -*-
"""7~8월 낙폭 방어 규칙 반사실 — 2026-09-13
베이스 = 현행 근사(CUR, _bt_pure_spread_2026_09_13.py와 동일 하네스).
후보: 트레일링스탑 10/15/20% · 고정손절 15% · MA60/MA120 이탈 매도 ·
      포트 DD −10% 전량현금(2리밸 후 복귀) · 업종 캡 2종목.
지표: 전기간(2/17~9/11) 위상평균 수익·MDD + 7/1~8/31 구간 수익·구간내 낙폭.
⚠️사후 표본(7~8월)을 보고 규칙을 고르는 행위 자체가 과적합 — 결과 보고 전용.
"""
import sqlite3, json
import numpy as np
import pandas as pd

DB = 'eps_momentum_data.db'
COST = 0.0010; N = 5; R = 5; START = '2026-02-17'
W0, W1 = '2026-07-01', '2026-08-31'   # 관심 구간

conn = sqlite3.connect(DB)
df = pd.read_sql("""SELECT date, ticker, adj_gap, price, dollar_volume_30d dv,
    ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, rev_up30, num_analysts na,
    ma60, ma120 FROM ntm_screening""", conn)
df = df[df.price.notna() & (df.price > 0)]
dates = sorted(df.date.unique()); didx = {d: i for i, d in enumerate(dates)}
px = df.pivot_table(index='date', columns='ticker', values='price', aggfunc='last').reindex(dates).ffill()
ma60 = df.pivot_table(index='date', columns='ticker', values='ma60', aggfunc='last').reindex(dates)
ma120 = df.pivot_table(index='date', columns='ticker', values='ma120', aggfunc='last').reindex(dates)

def seg(a, b):
    with np.errstate(divide='ignore', invalid='ignore'):
        return np.where((b > 0) & (a > 0), (a / b - 1) * 100, np.nan)
df['fwd_per'] = np.where(df.ntm_current > 0, df.price / df.ntm_current, np.nan)
df['rev30'] = seg(df.ntm_current, df.ntm_30d)
s1 = seg(df.ntm_current, df.ntm_7d); s2 = seg(df.ntm_7d, df.ntm_30d)
s3 = seg(df.ntm_30d, df.ntm_60d);   s4 = seg(df.ntm_60d, df.ntm_90d)
with np.errstate(all='ignore'):
    df['min_seg'] = np.nanmin(np.vstack([s1, s2, s3, s4]), axis=0)
px20 = px.pct_change(20) * 100
by_date = {d: g.set_index('ticker') for d, g in df.groupby('date')}
IND = {t: (v.get('industry') or '?') for t, v in
       json.load(open('ticker_info_cache.json', encoding='utf-8')).items()}

def select(date, held, indcap=None):
    g = by_date[date]
    g = g[g.adj_gap.notna() & (g.dv.fillna(0) >= 300)]
    g = g[(g.fwd_per > 0) & (g.fwd_per <= 30)]
    g = g[g.rev_up30.fillna(99) >= 3]
    g = g[g.na.isna() | (g.na == 0) | (g.na >= 6)]
    g = g[g.min_seg.fillna(0) >= -2]
    p20 = px20.loc[date] if date in px20.index else pd.Series(dtype=float)
    c20 = pd.Series(g.index.map(lambda t: p20.get(t, np.nan)), index=g.index)
    g = g[~(((g.rev30.fillna(99) <= 2) & (c20 < 0)) | (c20 <= -20))]
    ranked = list(g.sort_values('adj_gap').index)
    if indcap:
        capped, cnt = [], {}
        for t in ranked:
            i = IND.get(t, '?')
            if cnt.get(i, 0) < indcap:
                capped.append(t); cnt[i] = cnt.get(i, 0) + 1
        ranked = capped
    top = ranked[:N]
    band = set(ranked[:7])
    keep = [t for t in held if t in band]
    return (keep + [t for t in top if t not in keep])[:N]

def run(phase, ts=None, fixstop=None, maexit=None, portdd=None, indcap=None):
    i0 = didx[START]; days = dates[i0:]
    nav = 1.0; peak = 1.0; mdd = 0.0
    w = {}; pend = None; entry = {}; hi = {}
    cash_until = -1   # portdd 복귀 대기
    navs = {}
    reb_count = 0; reentry_at = None
    for k, d in enumerate(days):
        if w and k > 0:
            r = 0.0; neww = {}
            for t, wt in w.items():
                p1 = px.at[days[k-1], t]; p2 = px.at[d, t]
                rt = (p2 / p1 - 1) if (p1 and p2 and p1 > 0 and not np.isnan(p1) and not np.isnan(p2)) else 0.0
                r += wt * rt; neww[t] = wt * (1 + rt)
            nav *= (1 + r)
            tot = sum(neww.values()) + (1 - sum(w.values()))  # 현금 포함 정규화
            cashw = 1 - sum(w.values())
            w = {t: v / (tot) for t, v in neww.items()} if tot > 0 else {}
            # 정규화: 현금 비중은 (cashw/tot)로 암묵 유지
        # 스탑류 체크 (오늘 종가 기준, 내일 반영이 정확하나 종가 매도로 근사)
        if w:
            drop = []
            for t in list(w):
                p = px.at[d, t]
                if np.isnan(p): continue
                hi[t] = max(hi.get(t, p), p)
                if ts and p < hi[t] * (1 - ts): drop.append(t)
                elif fixstop and t in entry and p < entry[t] * (1 - fixstop): drop.append(t)
                elif maexit:
                    m = (ma60 if maexit == 60 else ma120).at[d, t] if t in ma60.columns else np.nan
                    if not np.isnan(m) and p < m: drop.append(t)
            for t in drop:
                nav *= (1 - COST * w[t]); del w[t]   # 현금화
        # 포트 DD 가드
        peak = max(peak, nav)
        if portdd and w and nav / peak - 1 <= -portdd:
            nav *= (1 - COST * sum(w.values())); w = {}; pend = None
            reentry_at = reb_count + 2   # 2번째 리밸부터 복귀
        # 체결
        if pend is not None:
            tgt = {t: 1.0 / len(pend) for t in pend} if pend else {}
            to = sum(abs(tgt.get(t, 0) - w.get(t, 0)) for t in set(tgt) | set(w)) / 2
            nav *= (1 - COST * to * 2)
            for t in tgt:
                if t not in w: entry[t] = px.at[d, t]; hi[t] = px.at[d, t]
            w = tgt; pend = None
        # 리밸 신호
        if (k - phase) % R == 0 and k >= phase and k < len(days) - 1:
            reb_count += 1
            if reentry_at is None or reb_count >= reentry_at:
                reentry_at = None
                pend = select(d, list(w.keys()), indcap)
        mdd = min(mdd, nav / peak - 1)
        navs[d] = nav
    return nav - 1, mdd, navs

def summary(label, **kw):
    rets, mdds, wrets, wdds = [], [], [], []
    for ph in range(R):
        r, m, navs = run(ph, **kw)
        rets.append(r); mdds.append(m)
        s = pd.Series(navs)
        wnav = s[(s.index >= W0) & (s.index <= W1)]
        wrets.append(wnav.iloc[-1] / wnav.iloc[0] - 1)
        wdds.append((wnav / wnav.cummax() - 1).min())
    print('%-22s 전기간 %+7.1f%% (MDD %5.1f/최악 %5.1f) | 7~8월 %+6.1f%% 구간낙폭 %5.1f%%'
          % (label, np.mean(rets)*100, np.mean(mdds)*100, min(mdds)*100,
             np.mean(wrets)*100, np.mean(wdds)*100))

summary('베이스(현행근사)')
summary('TS10', ts=0.10)
summary('TS15', ts=0.15)
summary('TS20', ts=0.20)
summary('고정손절15', fixstop=0.15)
summary('MA60이탈매도', maexit=60)
summary('MA120이탈매도', maexit=120)
summary('포트DD10 현금', portdd=0.10)
summary('업종캡2', indcap=2)
summary('업종캡2+TS15', indcap=2, ts=0.15)
