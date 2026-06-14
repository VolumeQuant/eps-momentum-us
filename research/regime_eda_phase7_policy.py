# -*- coding: utf-8 -*-
"""Regime Phase 7 — 현금정책 3종 검증 (사용자 요청, 메인 EPS 기준, 26년 QQQ 프록시).
A) 현금버퍼 비율: 얼마까지 낮춰도(주식 얼마까지 올려도) 되나
B) 비대칭 재진입: 약세진입 15d 고정, 강세재진입 속도 변화 (회복 빨리 잡기)
C) HY 스프레드 + VIX 추가: 신용스트레스로 약세장 더 잘 잡나
프록시=QQQ(공격 성장 대용), 방어자산=현금(연 2.5% 가정). 데이터 캐시만 사용(fetch 0)."""
import sys, numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
A = pd.read_parquet('research/regime_assets.parquet'); A.index = pd.to_datetime(A.index)
HY = pd.read_parquet('data_cache/hy_spread.parquet', engine='fastparquet')
HY.index = pd.to_datetime(HY.index); hy = HY.iloc[:, 0]
qqq = A['QQQ'].dropna(); spx = A['^GSPC'].dropna(); vix = A['^VIX'].dropna()
START = qqq.index[0]  # 1999~ (QQQ 시작)
idx = qqq.index
spx = spx.reindex(idx).ffill(); vix = vix.reindex(idx).ffill(); hyr = hy.reindex(idx).ffill()
ma200 = spx.rolling(200).mean()
CASH_Y = (1 + 0.025) ** (1/252) - 1  # 현금/단기채 연 2.5% 가정

def confirm_asym(raw, n_enter, n_exit):
    state = False; sd = sb = 0; out = []
    for d in raw.values:
        if d: sd += 1; sb = 0
        else: sb += 1; sd = 0
        if not state and sd >= n_enter: state = True
        elif state and sb >= n_exit: state = False
        out.append(state)
    return pd.Series(out, index=raw.index)

def metrics(port_ret):
    nav = (1 + port_ret).cumprod()
    yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = nav.iloc[-1] ** (1/yrs) - 1
    mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    vol = port_ret.std() * np.sqrt(252)
    sh = (port_ret.mean()*252) / vol if vol > 0 else 0
    return cagr*100, mdd*100, (cagr/abs(mdd) if mdd < 0 else 0), sh

def run(defense, eq_ratio=1.0):
    """defense(bool) 시 시스템슬리브→현금. 총 = eq_ratio·슬리브 + (1-eq_ratio)·현금."""
    pos = (~defense).shift(1, fill_value=False)
    ret_eq = qqq.pct_change().fillna(0)
    sleeve = np.where(pos.values, ret_eq.values, CASH_Y)
    port = eq_ratio * sleeve + (1 - eq_ratio) * CASH_Y
    return metrics(pd.Series(port, index=idx))

# 기본 raw 약세조건: SPX<MA200 OR VIX>36
raw_base = (spx < ma200)
vix_def = confirm_asym(vix > 36, 2, 2)   # VIX>36 2일 → 강제방어
def base_defense(n_enter=15, n_exit=15, raw=None):
    r = raw_base if raw is None else raw
    d = confirm_asym(r, n_enter, n_exit) | vix_def
    return d

print(f'=== 26년 QQQ 프록시 ({idx[0].date()}~{idx[-1].date()}) ===\n')

print('[A] 현금버퍼 비율 — 주식 얼마까지 올려도 되나 (국면=SPX200d 15/15 + VIX36)')
print(f'{"주식%":>6}{"현금버퍼%":>9}{"CAGR%":>8}{"MDD%":>8}{"Calmar":>8}{"Sharpe":>8}')
dbase = base_defense()
for eq in [0.6, 0.7, 0.8, 0.9, 1.0]:
    c, m, cal, sh = run(dbase, eq)
    print(f'{eq*100:>5.0f}{(1-eq)*100:>9.0f}{c:>+8.1f}{m:>+8.1f}{cal:>8.2f}{sh:>8.2f}')
print('  cf) 주식 100%·국면없음:', '%.1f%% MDD %.1f%% Cal %.2f' % run(pd.Series(False, index=idx), 1.0)[:3])

print('\n[B] 비대칭 재진입 — 약세진입 15d 고정, 강세재진입(n_exit) 속도별 (주식100%)')
print(f'{"재진입":>8}{"CAGR%":>8}{"MDD%":>8}{"Calmar":>8}{"방어일%":>8}')
for nx in [1, 3, 5, 8, 15, 25]:
    d = base_defense(15, nx)
    c, m, cal, sh = run(d, 1.0)
    print(f'{nx:>5}일{c:>+8.1f}{m:>+8.1f}{cal:>8.2f}{d.mean()*100:>7.0f}%')

print('\n[C] HY 스프레드 추가 — SPX200d/VIX 에 HY>임계 OR 추가 (주식100%, 15/15)')
print(f'{"HY규칙":>16}{"CAGR%":>8}{"MDD%":>8}{"Calmar":>8}{"방어일%":>8}')
c, m, cal, sh = run(base_defense(), 1.0)
print(f'{"기본(HY없음)":>16}{c:>+8.1f}{m:>+8.1f}{cal:>8.2f}{base_defense().mean()*100:>7.0f}%')
for thr in [5.0, 5.5, 6.0, 7.0]:
    raw = raw_base | (hyr > thr)
    d = confirm_asym(raw, 15, 15) | vix_def
    c, m, cal, sh = run(d, 1.0)
    print(f'{("HY>"+str(thr)+"%"):>16}{c:>+8.1f}{m:>+8.1f}{cal:>8.2f}{d.mean()*100:>7.0f}%')
# HY 급등(자기 MA 대비) 버전
hy_ma = hyr.rolling(126).mean()
for mult in [1.3, 1.5]:
    raw = raw_base | (hyr > hy_ma * mult)
    d = confirm_asym(raw, 15, 15) | vix_def
    c, m, cal, sh = run(d, 1.0)
    print(f'{("HY>6M평균x"+str(mult)):>16}{c:>+8.1f}{m:>+8.1f}{cal:>8.2f}{d.mean()*100:>7.0f}%')
print('\n해석: A=버퍼 낮출수록 수익↑MDD↑(국면이 막아주면 100%도 견딜만한지) / B=재진입 빠를수록 좋나 / C=HY가 Cal·MDD 개선하면 채택가치.')
