# -*- coding: utf-8 -*-
"""미국 지수 적립식 전략 백테스트 (2026-07-22, 사용자 스펙).

A: 단순 적립 (63/27/10, 리밸 없음, 현금 적치)
B: 가중적립 + 연1회 밴드(35/25) 리밸 + 폭락 실탄(-20% 50%, -30% 전액, 사이클당 1회) + 버퍼 복원(25%)
C: B - 실탄            (가중적립+밴드)
D: A + 밴드            (매월 70:30 분할매수 + 밴드만; 실탄·가중적립 없음)

분해: 밴드 = D-A, 가중적립 = C-D, 실탄 = B-C  (합 = B-A)
데이터: SPY/QQQ Adj Close(auto_adjust, 배당 포함). QQQ 상장(1999-03-10) 이전은 ^NDX
가격수익률을 QQQ 첫값에 스케일해 접합(리포트 명시). 폭락 트리거 기준 = SPY 조정종가 ATH.
룩어헤드 방지: 트리거는 당일 종가 확인 → 익일 종가 매수.
"""
import io
import sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import numpy as np
import pandas as pd
import yfinance as yf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
CONTRIB = 6000.0
CASH_W, SP_W, NQ_W = 0.10, 0.63, 0.27      # 총자산 목표
EQ_SP, EQ_NQ = 0.70, 0.30                  # 주식 내부 목표
BAND_HI, BAND_LO = 0.35, 0.25
FEE = 0.001
CASH_RATE_D = 0.03 / 365.0
START, END = '1999-01-01', '2025-12-31'

# ── 데이터 ──────────────────────────────────────────────
raw = yf.download(['SPY', 'QQQ', '^NDX'], start='1998-12-01', end='2026-01-01',
                  auto_adjust=True, progress=False)['Close']
raw = raw.dropna(subset=['SPY'])
qqq = raw['QQQ'].copy()
ndx = raw['^NDX'].ffill()
first_qqq = qqq.first_valid_index()
pre = ndx.loc[:first_qqq]
splice = pre / pre.iloc[-1] * qqq.loc[first_qqq]     # ^NDX 가격수익률로 접합
qqq = pd.concat([splice.iloc[:-1], qqq.loc[first_qqq:]])
px = pd.DataFrame({'SP': raw['SPY'], 'NQ': qqq}).dropna()
px = px.loc[START:END]
dates = px.index
month_ends = px.groupby(px.index.to_period('M')).tail(1).index

# ── 시뮬 엔진 ────────────────────────────────────────────
def simulate(mode, start_date=None):
    """mode: 'A','B','C','D'. 반환: value 시리즈, flows dict, 이벤트 로그들."""
    d0 = dates[0] if start_date is None else dates[dates.searchsorted(start_date)]
    idx = dates[dates.searchsorted(d0):]
    sh = {'SP': 0.0, 'NQ': 0.0}
    cash = 0.0
    restore_mode = False                      # B: 실탄 사용 후 현금 25% 적립
    ath = -np.inf
    below20 = below30 = False                 # 전일 종가의 상태 (돌파 감지용)
    spent20 = spent30 = False                 # 사이클 내 발동 여부
    pending = 0.0                             # 익일 집행 실탄 ($)
    vals, flows = [], {}
    deploy_log, band_log = [], []

    def eqv(d):
        return sh['SP'] * px.at[d, 'SP'], sh['NQ'] * px.at[d, 'NQ']

    def buy_split(amount, d, weighted):
        """amount를 주식에 투입. weighted=True면 70:30 대비 미달자산 우선."""
        spv, nqv = eqv(d)
        net = amount * (1 - FEE)
        if not weighted:
            sh['SP'] += net * EQ_SP / px.at[d, 'SP']
            sh['NQ'] += net * EQ_NQ / px.at[d, 'NQ']
            return
        tot_after = spv + nqv + net
        gap_sp = max(0.0, tot_after * EQ_SP - spv)
        gap_nq = max(0.0, tot_after * EQ_NQ - nqv)
        to_sp = min(net, gap_sp) if gap_sp >= gap_nq else max(0.0, net - min(net, gap_nq))
        to_nq = net - to_sp
        sh['SP'] += to_sp / px.at[d, 'SP']
        sh['NQ'] += to_nq / px.at[d, 'NQ']

    for d in idx:
        cash *= (1 + CASH_RATE_D)
        # 실탄 익일 집행 (B만 pending이 생김)
        if pending > 0:
            buy_split(pending, d, weighted=True)
            deploy_log[-1]['exec_date'] = d
            deploy_log[-1]['price_sp'] = px.at[d, 'SP']
            pending = 0.0
        # 월말 적립
        if d in month_ends:
            cw = 0.25 if (mode == 'B' and restore_mode) else CASH_W
            cash += CONTRIB * cw
            eq_amt = CONTRIB * (1 - cw)
            buy_split(eq_amt, d, weighted=(mode in ('B', 'C')))
            flows[d] = flows.get(d, 0.0) - CONTRIB
            # 버퍼 복원 판정
            if mode == 'B' and restore_mode:
                spv, nqv = eqv(d)
                if cash >= 0.10 * (spv + nqv + cash):
                    restore_mode = False
            # 연말 밴드 리밸 (B/C/D)
            if mode in ('B', 'C', 'D') and d.month == 12:
                spv, nqv = eqv(d)
                if spv + nqv > 0:
                    nq_sh = nqv / (spv + nqv)
                    if nq_sh > BAND_HI or nq_sh < BAND_LO:
                        tgt_nq = (spv + nqv) * EQ_NQ
                        delta = nqv - tgt_nq        # 양수면 NQ 과체중 → NQ 매도/SP 매수
                        src, dst = ('NQ', 'SP') if delta > 0 else ('SP', 'NQ')
                        amt = abs(delta)
                        sh[src] -= amt / px.at[d, src]
                        sh[dst] += amt * (1 - 2 * FEE) / px.at[d, dst]
                        band_log.append((d.year, nq_sh, amt))
        # 폭락 트리거 (B만; 당일 종가 판정 → 익일 집행)
        sp_px = px.at[d, 'SP']
        if sp_px > ath:
            ath = sp_px
            spent20 = spent30 = False
        dd = sp_px / ath - 1
        cross20 = (dd <= -0.20) and not below20
        cross30 = (dd <= -0.30) and not below30
        below20, below30 = dd <= -0.20, dd <= -0.30
        if mode == 'B':
            if cross30 and not spent30 and cash > 1:
                pending = cash * (1 - FEE) / (1 - FEE)  # 전액
                pending = cash
                cash = 0.0
                spent30 = spent20 = True
                restore_mode = True
                deploy_log.append({'trigger': '-30%', 'signal_date': d, 'amount': pending})
            elif cross20 and not spent20 and cash > 1:
                pending = cash * 0.5
                cash -= pending
                spent20 = True
                restore_mode = True
                deploy_log.append({'trigger': '-20%', 'signal_date': d, 'amount': pending})
        spv, nqv = eqv(d)
        vals.append(spv + nqv + cash)
    v = pd.Series(vals, index=idx)
    return v, flows, deploy_log, band_log

# ── 지표 ────────────────────────────────────────────────
def xirr(flows, final_date, final_value):
    cfs = sorted(flows.items()) + [(final_date, final_value)]
    t0 = cfs[0][0]
    yrs = np.array([(d - t0).days / 365.25 for d, _ in cfs])
    amts = np.array([a for _, a in cfs])
    def npv(r):
        return (amts / (1 + r) ** yrs).sum()
    lo, hi = -0.5, 1.0
    for _ in range(100):
        mid = (lo + hi) / 2
        if npv(mid) > 0:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2

def metrics(v, flows):
    fl = pd.Series(0.0, index=v.index)
    for d, a in flows.items():
        fl[d] += -a                       # 투입액(양수)
    twr = (v - fl) / v.shift(1) - 1
    twr = twr.replace([np.inf, -np.inf], np.nan).dropna()
    twr = twr[v.shift(1) > 1000]  # 초기 0~소액 구간 제외
    mdd = ((v / v.cummax()) - 1).min()
    vol = twr.std() * np.sqrt(252)
    r = xirr(flows, v.index[-1], v.iloc[-1])
    return {'final': v.iloc[-1], 'xirr': r, 'mdd': mdd, 'vol': vol,
            'calmar': r / abs(mdd) if mdd else np.nan}

# ── 본 실행 ─────────────────────────────────────────────
res, curves, logs = {}, {}, {}
for m in 'ABCD':
    v, fl, dep, band = simulate(m)
    res[m] = metrics(v, fl)
    curves[m] = v
    logs[m] = {'deploy': dep, 'band': band}

print('=' * 72)
print(f'기간 {curves["A"].index[0].date()} ~ {curves["A"].index[-1].date()} | 월 $6,000 적립'
      f' | 총 투입 ${6000 * len(month_ends):,.0f}')
print(f'(주의) 1999-01~03 나스닥 leg는 ^NDX 가격수익률 접합 = 그 두 달만 배당 미포함')
print('=' * 72)
print(f'{"전략":<4} {"최종자산($M)":>12} {"XIRR":>7} {"MDD":>7} {"연변동성":>8} {"Calmar":>7}')
NAME = {'A': 'A 단순적립', 'B': 'B 제안전략', 'C': 'C 밴드+가중', 'D': 'D 밴드만'}
for m in 'ABCD':
    r = res[m]
    print(f'{NAME[m]:<10} {r["final"]/1e6:>10.3f} {r["xirr"]*100:>6.2f}% '
          f'{r["mdd"]*100:>6.1f}% {r["vol"]*100:>7.1f}% {r["calmar"]:>7.2f}')

print('\n── 기여 분해 (XIRR %p / 최종자산 $k) ──')
pairs = [('밴드 리밸 (D−A)', 'D', 'A'), ('가중적립 (C−D)', 'C', 'D'),
         ('폭락 실탄 (B−C)', 'B', 'C'), ('합계 (B−A)', 'B', 'A')]
for label, x, y in pairs:
    dx = (res[x]['xirr'] - res[y]['xirr']) * 100
    dv = (res[x]['final'] - res[y]['final']) / 1e3
    dm = (res[x]['mdd'] - res[y]['mdd']) * 100
    print(f'  {label:<18} XIRR {dx:+.3f}%p | 최종 {dv:+,.0f}k | MDD {dm:+.2f}%p')

print('\n── 실탄 발동 로그 (B) — 발동일·투입액·이후 3년 기여 ──')
for e in logs['B']['deploy']:
    d_exec = e.get('exec_date')
    if d_exec is None:
        continue
    i3 = dates.searchsorted(d_exec + pd.DateOffset(years=3))
    d3 = dates[min(i3, len(dates) - 1)]
    # 실탄을 미달자산 우선으로 넣지만 기여 추정은 70:30 혼합수익 근사
    ret3 = 0.7 * (px.at[d3, 'SP'] / px.at[d_exec, 'SP']) + 0.3 * (px.at[d3, 'NQ'] / px.at[d_exec, 'NQ'])
    cash3 = (1 + 0.03) ** ((d3 - d_exec).days / 365.25)
    contrib = e['amount'] * (ret3 - cash3)
    print(f"  {e['trigger']:>5} 신호 {e['signal_date'].date()} → 집행 {d_exec.date()}"
          f" ${e['amount']:>9,.0f} | 3년 주식수익 {ret3-1:+.1%} vs 현금 {cash3-1:+.1%}"
          f" → 기여 ${contrib:+,.0f}")

print('\n── 밴드 리밸 발동 (전략별 횟수·연도) ──')
for m in 'BCD':
    bl = logs[m]['band']
    yrs = [f"{y}({s:.0%})" for y, s, _ in bl]
    print(f'  {m}: {len(bl)}회 — {", ".join(yrs) if yrs else "없음"}')

print('\n── 시작연도 롤링 (1999~2015): B−A XIRR 차이 분포 ──')
diffs = []
for y in range(1999, 2016):
    sd = f'{y}-01-01'
    va, fa, _, _ = simulate('A', sd)
    vb, fb, _, _ = simulate('B', sd)
    da = xirr(fa, va.index[-1], va.iloc[-1])
    db = xirr(fb, vb.index[-1], vb.iloc[-1])
    diffs.append((y, (db - da) * 100))
arr = np.array([d for _, d in diffs])
for y, d in diffs:
    print(f'  {y}: {d:+.3f}%p')
print(f'  평균 {arr.mean():+.3f}%p | 중앙값 {np.median(arr):+.3f}%p | '
      f'승률 {(arr > 0).mean():.0%} | 최악 {arr.min():+.3f}%p ({diffs[arr.argmin()][0]}년 시작)')

# ── 판정 ────────────────────────────────────────────────
print('\n── 판정 ──')
ba = (res['B']['xirr'] - res['A']['xirr']) * 100
sd = arr.std(ddof=1)
print(f'B−A XIRR {ba:+.3f}%p, 롤링 분포 std {sd:.3f}%p, 승률 {(arr>0).mean():.0%}.')
if abs(arr.mean()) < sd and (arr > 0).mean() < 0.75:
    print('→ 수익 차이는 통계적으로 노이즈 수준(평균 < 1 std, 승률 낮음).')
else:
    print('→ 수익 차이가 분포상 일관됨(평균 ≥ 1 std 또는 승률 ≥ 75%).')
print(f'Calmar: A {res["A"]["calmar"]:.2f} vs B {res["B"]["calmar"]:.2f}, '
      f'MDD: A {res["A"]["mdd"]*100:.1f}% vs B {res["B"]["mdd"]*100:.1f}% — 판정 기준은 여기.')

# ── 그림 ────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(11, 5.5))
for m in 'ABCD':
    ax.plot(curves[m].index, curves[m] / 1e6, label=f'{m} ({res[m]["xirr"]*100:.2f}%)', lw=1.1)
ax.set_yscale('log')
ax.set_ylabel('Portfolio value ($M, log)')
ax.set_title('DCA strategies A/B/C/D — 1999~2025, $6k/month')
ax.legend()
ax.grid(alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, 'dca_band_bullet_curves.png'), dpi=120)

fig2, ax2 = plt.subplots(figsize=(11, 4))
for m in ('A', 'B'):
    v = curves[m]
    ax2.plot(v.index, (v / v.cummax() - 1) * 100, label=m, lw=1.0)
ax2.set_ylabel('Drawdown (%)')
ax2.set_title('Drawdown: A vs B')
ax2.legend()
ax2.grid(alpha=0.3)
fig2.tight_layout()
fig2.savefig(os.path.join(OUT_DIR, 'dca_band_bullet_dd.png'), dpi=120)
print('\nPNG 저장: dca_band_bullet_curves.png, dca_band_bullet_dd.png')
