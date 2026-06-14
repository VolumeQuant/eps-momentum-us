# -*- coding: utf-8 -*-
"""Phase 12 — 초단기 V자 급락(2020 COVID·2025-4월) 더 잘 잡을 수 있나?
아이디어: 일반 약세장은 15일 유지(휩쏘 방지), V자 시그니처(VIX 폭등→급락 + 가격 급반등)일 때만
빠른 재진입. 데이터 VIX/가격(+HY). 26년 QQQ프록시, 방어=발행어음3%.
목표: 2020/2025 반등 더 잡으면서 2008/2022 휩쏘 안 늘리기 = robust 개선이어야 채택."""
import sys, numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
A = pd.read_parquet('research/regime_assets.parquet'); A.index = pd.to_datetime(A.index)
qqq, spx, vix = (A[c].dropna() for c in ['QQQ', '^GSPC', '^VIX'])
idx = qqq.index[qqq.index >= '1999-06-01']
q = qqq.reindex(idx).ffill(); s = spx.reindex(idx).ffill(); v = vix.reindex(idx).ffill()
ma = s.rolling(200).mean(); NOTE = (1.03)**(1/252)-1
sv = s.values; vv = v.values; mav = ma.values

def build(reentry='base'):
    """defense bool. 진입=SPX<MA200 15일 OR VIX>36 2일. 재진입=15일 MA200회복 OR (모드별 V자 빠른재진입)."""
    n = len(idx); st = False; below = abovec = vspk = 0
    ep_low = np.inf; ep_vmax = 0; out = np.zeros(n, bool)
    for i in range(n):
        b = sv[i] < mav[i] if not np.isnan(mav[i]) else False
        below = below+1 if b else 0
        abovec = abovec+1 if (not b) else 0
        vspk = vspk+1 if vv[i] > 36 else 0
        if st:
            ep_low = min(ep_low, sv[i]); ep_vmax = max(ep_vmax, vv[i])
        # 진입
        if not st and (below >= 15 or vspk >= 2):
            st = True; ep_low = sv[i]; ep_vmax = vv[i]
        elif st:
            # 표준 재진입
            exit_now = abovec >= 15
            # V자 빠른 재진입 (모드별)
            if reentry == 'vix_norm':
                # VIX가 에피소드중 45+ 폭등했고, 지금 28 미만 + 가격 바닥대비 +5%
                if ep_vmax >= 45 and vv[i] < 28 and sv[i] >= ep_low*1.05: exit_now = True
            elif reentry == 'thrust':
                # 가격이 에피소드 바닥대비 +10% 급반등
                if sv[i] >= ep_low*1.10: exit_now = True
            elif reentry == 'combo':
                if ep_vmax >= 45 and vv[i] < 28 and sv[i] >= ep_low*1.05: exit_now = True
                if sv[i] >= ep_low*1.12: exit_now = True
            if exit_now: st = False
        out[i] = st
    return pd.Series(out, index=idx)

def perf(dfn):
    pos = (~dfn).shift(1, fill_value=False)
    r = pd.Series(np.where(pos.values, q.pct_change().fillna(0).values, NOTE), index=idx)
    nav = (1+r).cumprod(); yrs = (nav.index[-1]-nav.index[0]).days/365.25
    cagr = nav.iloc[-1]**(1/yrs)-1; mdd = ((nav-nav.cummax())/nav.cummax()).min()
    return cagr*100, mdd*100, (cagr/abs(mdd) if mdd < 0 else 0)

def reentry_date(dfn, after, before):
    seg = dfn[(dfn.index >= after) & (dfn.index <= before)]
    tr = seg[seg].index
    if len(tr) == 0: return None
    end = tr[-1]
    nxt = dfn.index[dfn.index > end]
    return nxt[0] if len(nxt) else None

print('=== V자 급락 재진입 개선 테스트 (26년 QQQ프록시) ===')
print(f'{"재진입 모드":<16}{"CAGR%":>8}{"MDD%":>8}{"Calmar":>8}{"2020재진입":>12}{"2025재진입":>12}')
for mode, lbl in [('base', '현행(15일)'), ('vix_norm', 'VIX정상화'), ('thrust', '가격급반등+10%'), ('combo', '복합')]:
    d = build(mode); c, m, cal = perf(d)
    r20 = reentry_date(d, '2020-02-25', '2020-12-31'); r25 = reentry_date(d, '2025-04-01', '2025-08-31')
    print(f'{lbl:<16}{c:>+8.1f}{m:>+8.1f}{cal:>8.2f}{(str(r20.date()) if r20 else "-"):>12}{(str(r25.date()) if r25 else "-"):>12}')

# 2020 V자: 바닥 3/23. 각 모드 재진입일 + 그때까지 놓친 반등
print('\n=== 2020 코로나 V자 상세 (바닥 2020-03-23) ===')
low20 = s[(s.index >= '2020-03-15') & (s.index <= '2020-03-31')].min()
for mode, lbl in [('base', '현행(15일)'), ('vix_norm', 'VIX정상화'), ('thrust', '급반등'), ('combo', '복합')]:
    d = build(mode); rd = reentry_date(d, '2020-02-25', '2020-12-31')
    if rd:
        missed = (s[rd]/low20 - 1)*100
        print(f'  {lbl:<14} 재진입 {rd.date()} — 바닥대비 +{missed:.0f}% 오른 뒤 진입(놓친반등)')
print('\n해석: V자모드가 2020/2025 재진입을 앞당기면서 전체 Calmar도 ≥현행이면 채택가치. 2008/2022 깨지면 기각.')
