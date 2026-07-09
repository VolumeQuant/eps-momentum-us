# -*- coding: utf-8 -*-
# 찐 사이클 하락 감지기 — 실측 (메모리 클러스터 5종, 2015~2026)
# 신호: 클러스터 브레드스(자기 MA 아래 비율) + 낙폭 지속. 판정: 2018/2022/2024 천장 리드 vs 조정 헛울림 vs 오버레이 BT
import sys
import numpy as np
import pandas as pd
import yfinance as yf
sys.stdout.reconfigure(encoding='utf-8')

TICKS = ['MU', 'WDC', 'STX', '005930.KS', '000660.KS']
px = yf.download(TICKS, start='2015-01-01', end='2026-07-09', progress=False, auto_adjust=True, threads=2)['Close']
px = px.dropna(how='all').ffill()
print('data:', px.index[0].date(), '->', px.index[-1].date(), px.shape)

ret = px.pct_change().fillna(0)
cluster_ret = ret.mean(axis=1)          # 동일가중 클러스터 (우리 시스템 메모리 노출 프록시)
nav_bh = (1 + cluster_ret).cumprod()

# 진짜 천장 (클러스터 NAV로 확정)
def peak_in(a, b):
    w = nav_bh.loc[a:b]
    return w.idxmax()
TOPS = {
    '2018': (peak_in('2018-01-01', '2018-12-31'), '2018-09-20'),   # 리비전 컷 개시(가이던스 컷 시즌)
    '2022': (peak_in('2021-06-01', '2022-06-30'), '2022-06-30'),   # MU 워닝
    '2024': (peak_in('2024-01-01', '2024-12-31'), '2024-09-25'),   # MU 실적 발표 전후 컷
}
# 거짓양성 시험창 (조정 — 울리면 감점): 2020 COVID는 시장전체(국면 오버레이 담당)라 별도 표기
FP_WINDOWS = [('2019-04-01', '2019-08-31'), ('2023-01-01', '2023-12-31'),
              ('2026-03-01', '2026-04-15'), ('2026-06-01', '2026-07-08')]

def episodes(sig):
    eps = []
    on = False; s = None
    for d, v in sig.items():
        if v and not on: on, s = True, d
        elif not v and on: on = False; eps.append((s, d))
    if on: eps.append((s, sig.index[-1]))
    return eps

def eval_signal(name, raw, confirm=3, clear=15):
    # confirm일 연속 진행시 발동, clear일 연속 해제시 복귀
    fire = raw.rolling(confirm).sum() == confirm
    off = (~raw).rolling(clear).sum() == clear
    state = pd.Series(False, index=raw.index)
    on = False
    for d in raw.index:
        if not on and fire.loc[d]: on = True
        elif on and off.loc[d]: on = False
        state.loc[d] = on
    eps = episodes(state)
    # 천장 리드 평가
    rows = []
    for label, (pk, revcut) in TOPS.items():
        rc = pd.Timestamp(revcut)
        after = [s for s, e in eps if s > pk and s < rc + pd.Timedelta(days=200)]
        first = min(after) if after else None
        if first is not None:
            rows.append(f"{label}: 고점+{(first-pk).days}d, 리비전컷 {(rc-first).days}d 전 발동")
        else:
            rows.append(f"{label}: 미발동!")
    # 거짓양성
    fp = 0
    for a, b in FP_WINDOWS:
        fp += sum(1 for s, e in eps if pd.Timestamp(a) <= s <= pd.Timestamp(b))
    # 전체 on 비율(휩쏘 비용 프록시)
    on_frac = state.mean() * 100
    # 오버레이 BT: 발동시 노출 50%
    exp = np.where(state.shift(1).fillna(False), 0.5, 1.0)
    nav = (1 + cluster_ret * exp).cumprod()
    def stats(n):
        yrs = (n.index[-1] - n.index[0]).days / 365.25
        cagr = n.iloc[-1] ** (1/yrs) - 1
        mdd = (n / n.cummax() - 1).min()
        return cagr*100, mdd*100, (cagr/abs(mdd)) if mdd else 0
    c1, m1, k1 = stats(nav)
    print(f'\n[{name}] on {on_frac:.0f}% | FP(조정창 발동) {fp}회 | ' + ' | '.join(rows))
    print(f'   오버레이: CAGR {c1:+.1f}% MDD {m1:+.1f} Calmar {k1:.2f}')
    return state

c0, m0, k0 = None, None, None
yrs = (nav_bh.index[-1] - nav_bh.index[0]).days / 365.25
cagr0 = nav_bh.iloc[-1] ** (1/yrs) - 1
mdd0 = (nav_bh / nav_bh.cummax() - 1).min()
print(f'\n기준(매수보유 클러스터): CAGR {cagr0*100:+.1f}% MDD {mdd0*100:+.1f} Calmar {cagr0/abs(mdd0):.2f}')

# 신호 1: 브레드스 — MA100 아래 비율 >= thr
for ma, thr in [(100, 0.6), (100, 0.8), (50, 0.8)]:
    below = (px < px.rolling(ma).mean()).mean(axis=1)
    eval_signal(f'브레드스 MA{ma} {int(thr*100)}%+ 하회', below >= thr)

# 신호 2: 낙폭 지속 — 클러스터 중앙 낙폭(120d 고점 대비) < -X%가 지속
for dd_thr in [-15, -20]:
    dd = (px / px.rolling(120).max() - 1).median(axis=1) * 100
    eval_signal(f'중앙낙폭 {dd_thr}% 지속', dd < dd_thr, confirm=10, clear=20)

# 신호 3: 결합 (브레드스 MA100 80% AND 낙폭 -15%)
below = (px < px.rolling(100).mean()).mean(axis=1)
dd = (px / px.rolling(120).max() - 1).median(axis=1) * 100
eval_signal('결합(브레드스80% & 낙폭-15%)', (below >= 0.8) & (dd < -15), confirm=5, clear=20)
