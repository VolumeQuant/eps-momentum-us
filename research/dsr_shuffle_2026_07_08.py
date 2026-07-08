# -*- coding: utf-8 -*-
"""다중검정 보정(DSR) + MinTRL + 셔플드 시그널 사기탐지 (2026-07-08).

대상: 채택안 top4 R5 fwd_PER<=30 gap>=2.5 (rev90 내림차순, 고정 1/4 가중+현금).
BT 산식은 research/per_gap_grid_2026_07_04.py run()을 그대로 재현하되,
빈 슬롯은 고정 1/N 가중 + 현금(잔여종목 집중 금지) 처리.

1) 위상 0~4 일별 수익률 재현 (기준 위상=2)
2) Deflated Sharpe Ratio (Bailey & Lopez de Prado 2014) — N_trials {10,50,100,200} 민감도
3) MinTRL — SPY(동기간) 벤치마크 대비 95% 유의에 필요한 최소 트랙 길이
4) 셔플드 시그널: 리밸일 게이트 통과자들의 rev90을 종목 간 무작위 셔플 -> top4, 500회

프로덕션 코드/데이터 변경 없음. 재현: python research/dsr_shuffle_2026_07_08.py
"""
import sys, os, json, sqlite3, math, random
import numpy as np
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
import daily_runner as dr

# ---------------------------------------------------------------- 데이터 로드 (per_gap_grid_2026_07_04.py 동일)
conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
AD_END = '2026-07-02'  # 검증 대상 구간 고정 (91일 성과 주장 시점의 데이터 컷오프, 이후 추가된 07-06/07 제외)
ad = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date') if r[0] <= AD_END]
FULL = {}
for tk, d, px, nc, n7, n30, n60, n90 in c.execute(
        'SELECT ticker,date,price,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d '
        'FROM ntm_screening WHERE price IS NOT NULL AND ntm_current>0'):
    FULL.setdefault(d, {})[tk] = dict(px=px, nc=nc, n7=n7, n30=n30, n60=n60, n90=n90)
conn.close()
AP = {d: {t: v['px'] for t, v in FULL.get(d, {}).items()} for d in ad}
DVF = pd.read_parquet(os.path.join(BASE, 'research', 'dv_full_2026_07_04.parquet'))
DVF.index = pd.to_datetime(DVF.index).strftime('%Y-%m-%d')
DV = {d: {t: (None if pd.isna(DVF.loc[d, t]) else float(DVF.loc[d, t])) for t in DVF.columns} for d in DVF.index if d in set(ad)}
TC = json.load(open(os.path.join(BASE, 'ticker_info_cache.json'), encoding='utf-8'))
BAD_IND = dr.COMMODITY_INDUSTRIES | dr.OFF_STRATEGY_INDUSTRIES
BAD_TK = set(dr.COMMODITY_TICKERS)

def industry_ok(tk):
    if tk in BAD_TK: return False
    v = TC.get(tk)
    ind = v.get('industry') if isinstance(v, dict) else (v[0] if isinstance(v, (list, tuple)) else v)
    return not (isinstance(ind, str) and ind in BAD_IND)

TE = json.load(open(os.path.join(BASE, 'data_cache', 'trailing_eps_ttm.json'), encoding='utf-8'))

def pit_te(tk, d):
    r = TE.get(tk); v = None
    if not r: return None
    for rd, e in r:
        if rd <= d: v = e
        else: break
    return v

def ms(v):
    o = []
    for a, b in [(v['nc'], v['n7']), (v['n7'], v['n30']), (v['n30'], v['n60']), (v['n60'], v['n90'])]:
        o.append((a - b) / abs(b) * 100 if (b and abs(b) > 0.01) else 0)
    return min(o)

def rev90(v):
    return (v['nc'] - v['n90']) / abs(v['n90']) * 100 if (v['n90'] and abs(v['n90']) > 0.01) else 0

# ---------------------------------------------------------------- 게이트 통과자 사전계산 (날짜별, 셔플 재사용)
def gate_pass(d, pe_max, gap_thr, exclude=frozenset()):
    out = []
    for tk, v in FULL.get(d, {}).items():
        if tk in exclude or not industry_ok(tk): continue
        dv = DV.get(d, {}).get(tk)
        if dv is None or dv < 1000: continue
        if ms(v) < 0: continue
        if v['nc'] <= 0 or (v['n90'] or 0) <= 0.1: continue
        if v['px'] / v['nc'] > pe_max: continue
        if gap_thr:
            te = pit_te(tk, d); g = (v['nc'] / te) if (te and te > 0) else None
            if g is not None and g < gap_thr: continue
        out.append((tk, rev90(v)))
    return out

# ---------------------------------------------------------------- BT 엔진 (고정 1/N + 현금)
def run_daily(pe_max=30, gap_thr=2.5, phase=2, N=4, R=5, start=2, picker=None, cand_cache=None):
    """일별 수익률 시계열 반환. picker(cand)->hold list 로 선택 로직 주입(기본=rev90 top N).
    가중은 고정 1/N + 현금(빈 슬롯/가격 결측=현금 0%)."""
    hold = []; rets = []; dates = []
    for i in range(start, len(ad)):
        d, pv = ad[i], ad[i - 1]; px = AP.get(d, {}); ppx = AP.get(pv, {})
        drr = 0.0
        for t in hold:
            cu, pp = px.get(t), ppx.get(t)
            if cu and pp and pp > 0: drr += (1.0 / N) * (cu - pp) / pp
        rets.append(drr); dates.append(d)
        if i % R == phase:
            cand = cand_cache[d] if cand_cache is not None else gate_pass(d, pe_max, gap_thr)
            if picker is None:
                sc = sorted(cand, key=lambda x: -x[1]); hold = [t for t, _ in sc[:N]]
            else:
                hold = picker(cand)
    return dates, np.array(rets)

def stats_from(rets):
    nav = np.cumprod(1 + rets); peak = np.maximum.accumulate(nav)
    mdd = float((nav / peak - 1).min()) * 100
    tot = float(nav[-1] - 1) * 100
    sr_d = float(rets.mean() / rets.std(ddof=1)) if rets.std(ddof=1) > 0 else 0.0
    return tot, mdd, sr_d, sr_d * math.sqrt(252)

# ---------------------------------------------------------------- 정규분포 primitives (직접 구현, 의존성 없음)
def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def norm_ppf(p):
    """Acklam rational approximation (|err|<1.15e-9)."""
    if not (0 < p < 1): raise ValueError(p)
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    cc = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
          -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    dd = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
          3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((cc[0]*q+cc[1])*q+cc[2])*q+cc[3])*q+cc[4])*q+cc[5]) / ((((dd[0]*q+dd[1])*q+dd[2])*q+dd[3])*q+1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((cc[0]*q+cc[1])*q+cc[2])*q+cc[3])*q+cc[4])*q+cc[5]) / ((((dd[0]*q+dd[1])*q+dd[2])*q+dd[3])*q+1)
    q = p - 0.5; r = q * q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)

# ---------------------------------------------------------------- DSR / PSR / MinTRL (Bailey & Lopez de Prado 2014)
EULER_GAMMA = 0.5772156649015329

def skew_kurt(rets):
    m = rets.mean(); s = rets.std(ddof=0)
    g3 = float(((rets - m) ** 3).mean() / s ** 3)
    g4 = float(((rets - m) ** 4).mean() / s ** 4)  # non-excess (정규=3)
    return g3, g4

def psr(sr, sr0, T, g3, g4):
    denom = math.sqrt(max(1 - g3 * sr + (g4 - 1) / 4 * sr * sr, 1e-12))
    return norm_cdf((sr - sr0) * math.sqrt(T - 1) / denom)

def expected_max_sr(n_trials, var_trials):
    """E[max SR] under N independent trials with cross-trial SR variance var_trials."""
    return math.sqrt(var_trials) * ((1 - EULER_GAMMA) * norm_ppf(1 - 1.0 / n_trials)
                                    + EULER_GAMMA * norm_ppf(1 - 1.0 / (n_trials * math.e)))

def min_trl(sr, sr_bench, g3, g4, conf=0.95):
    z = norm_ppf(conf)
    return 1 + (1 - g3 * sr + (g4 - 1) / 4 * sr * sr) * (z / (sr - sr_bench)) ** 2

# ================================================================ 실행
if __name__ == '__main__':
    PE, GAP, N, R = 30, 2.5, 4, 5
    print(f'=== 1) 일별 수익 시계열 재현 (top{N} R{R} PER<={PE} gap>={GAP}, 고정 1/{N} 가중+현금) ===')
    # 게이트 통과자 캐시 (전 날짜 — 위상별 리밸일이 다르므로 전부)
    CAND = {d: gate_pass(d, PE, GAP) for d in ad}
    phase_rets = {}
    for p in range(5):
        dts, rets = run_daily(PE, GAP, phase=p, N=N, R=R, cand_cache=CAND)
        phase_rets[p] = (dts, rets)
        tot, mdd, sr_d, sr_a = stats_from(rets)
        print(f'  위상{p}: 수익 {tot:+7.1f}%  MDD {mdd:+6.1f}%  Sharpe(연율) {sr_a:5.2f}  (T={len(rets)})')
    avg_tot = np.mean([stats_from(phase_rets[p][1])[0] for p in range(5)])
    print(f'  위상평균 수익: {avg_tot:+.1f}%  (기대: +110% 부근)')

    dts, rets = phase_rets[2]                      # 기준 위상=2
    T = len(rets)
    tot, mdd, sr_d, sr_a = stats_from(rets)
    g3, g4 = skew_kurt(rets)
    print(f'\n[기준 위상2] 수익 {tot:+.1f}%  MDD {mdd:+.1f}%  일별SR {sr_d:.4f} (연율 {sr_a:.2f})  skew {g3:+.2f}  kurt {g4:.2f}  T={T}')

    # ---------------- 시행 분산 추정: 실제 스윕한 그리드(PER x gap, 위상2)의 일별 SR 분산
    PES = [15, 18, 20, 25, 30, 999]
    GAPS = [None, 2.0, 2.5, 3.0]
    trial_srs = []
    for pe in PES:
        for g in GAPS:
            _, rr = run_daily(pe, g, phase=2, N=N, R=R)
            s = rr.std(ddof=1)
            trial_srs.append(float(rr.mean() / s) if s > 0 else 0.0)
    var_trials = float(np.var(trial_srs, ddof=1))
    print(f'\n=== 2) Deflated Sharpe Ratio ===')
    print(f'  시행 SR 분산 추정: 그리드 {len(trial_srs)}개 변형(일별 SR) -> var={var_trials:.6f} (std={math.sqrt(var_trials):.4f})')
    print(f'  {"N_trials":>8} | {"E[maxSR]_d":>10} | {"연율환산":>8} | {"DSR":>7}')
    dsr_rows = []
    for nt in [10, 50, 100, 200]:
        sr0 = expected_max_sr(nt, var_trials)
        d_val = psr(sr_d, sr0, T, g3, g4)
        dsr_rows.append((nt, sr0, d_val))
        print(f'  {nt:>8} | {sr0:>10.4f} | {sr0*math.sqrt(252):>8.2f} | {d_val:>7.4f}')
    psr0 = psr(sr_d, 0.0, T, g3, g4)
    print(f'  (참고 PSR: SR*=0, 시행 1회 가정 -> {psr0:.4f})')

    # ---------------- 3) MinTRL vs SPY
    import yfinance as yf
    spy = yf.download('SPY', start='2026-02-01', end='2026-07-04', progress=False, auto_adjust=True)['Close']
    if isinstance(spy, pd.DataFrame): spy = spy.iloc[:, 0]
    spy.index = spy.index.strftime('%Y-%m-%d')
    spy_r = spy.pct_change()
    common = [d for d in dts if d in spy_r.index and not pd.isna(spy_r.loc[d])]
    sp = spy_r.loc[common].values
    sr_spy_d = float(sp.mean() / sp.std(ddof=1))
    strat_common = np.array([rets[dts.index(d)] for d in common])
    sr_strat_c = float(strat_common.mean() / strat_common.std(ddof=1))
    g3c, g4c = skew_kurt(strat_common)
    mtrl = min_trl(sr_strat_c, sr_spy_d, g3c, g4c, conf=0.95)
    print(f'\n=== 3) MinTRL (95%, vs SPY) ===')
    print(f'  SPY 일별SR {sr_spy_d:.4f} (연율 {sr_spy_d*math.sqrt(252):.2f}, 동기간 {len(common)}일)')
    print(f'  전략 일별SR(공통일) {sr_strat_c:.4f} (연율 {sr_strat_c*math.sqrt(252):.2f})')
    print(f'  MinTRL = {mtrl:.1f} 거래일  (현재 T={len(common)}일 -> {"충분" if len(common) >= mtrl else f"{mtrl-len(common):.0f}일 부족"})')
    mtrl0 = min_trl(sr_strat_c, 0.0, g3c, g4c, conf=0.95)
    print(f'  (참고: SR*=0 대비 MinTRL = {mtrl0:.1f}일)')

    # ---------------- 4) 셔플드 시그널 사기탐지 (위상2, 500회, seed=42)
    print(f'\n=== 4) 셔플드 시그널 (rev90 종목간 셔플 -> top{N}, 500회, phase=2) ===')
    rng = random.Random(42)
    NSIM = 500
    sim_tots = []
    for k in range(NSIM):
        def picker(cand, _rng=rng):
            if not cand: return []
            tks = [t for t, _ in cand]; vals = [v for _, v in cand]
            _rng.shuffle(vals)
            pairs = sorted(zip(tks, vals), key=lambda x: -x[1])
            return [t for t, _ in pairs[:N]]
        _, rr = run_daily(PE, GAP, phase=2, N=N, R=R, picker=picker, cand_cache=CAND)
        sim_tots.append(stats_from(rr)[0])
    sim = np.array(sim_tots)
    pctile = float((sim < tot).mean()) * 100
    print(f'  셔플 분포: mean {sim.mean():+.1f}%  std {sim.std(ddof=1):.1f}%  median {np.median(sim):+.1f}%')
    print(f'  p5 {np.percentile(sim,5):+.1f}%  p95 {np.percentile(sim,95):+.1f}%  p99 {np.percentile(sim,99):+.1f}%  max {sim.max():+.1f}%')
    print(f'  실제(위상2) {tot:+.1f}% 의 백분위: {pctile:.1f}%  (셔플 초과 개수 {int((sim>=tot).sum())}/{NSIM})')
    # SPY 동기간 수익 (구조 수익 비교용)
    spy_tot = float((1 + spy_r.loc[common]).prod() - 1) * 100
    print(f'  구조(게이트+유니버스+동일가중 랜덤4) 평균 수익 {sim.mean():+.1f}% vs SPY 동기간 {spy_tot:+.1f}%')

    # ---------------- 저장용 결과 dump
    out = dict(tot=tot, mdd=mdd, sr_d=sr_d, sr_a=sr_a, g3=g3, g4=g4, T=T,
               var_trials=var_trials, dsr=[(nt, s0, dv) for nt, s0, dv in dsr_rows], psr0=psr0,
               sr_spy_d=sr_spy_d, sr_strat_c=sr_strat_c, mtrl=mtrl, mtrl0=mtrl0, n_common=len(common),
               shuffle=dict(mean=float(sim.mean()), std=float(sim.std(ddof=1)), med=float(np.median(sim)),
                            p5=float(np.percentile(sim, 5)), p95=float(np.percentile(sim, 95)),
                            p99=float(np.percentile(sim, 99)), mx=float(sim.max()), pct=pctile,
                            n_ge=int((sim >= tot).sum())),
               spy_tot=spy_tot, avg_tot=float(avg_tot),
               phase_tots={p: stats_from(phase_rets[p][1])[0] for p in range(5)})
    with open(os.path.join(BASE, 'research', 'dsr_shuffle_2026_07_08_results.json'), 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print('\n결과 JSON 저장: research/dsr_shuffle_2026_07_08_results.json')
