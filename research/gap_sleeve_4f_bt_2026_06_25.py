# -*- coding: utf-8 -*-
"""gap 슬리브 — us-4factor broad(1483·8년·PIT EDGAR)서 실제 백테스트 + 방어오버레이 + LOWO + 연도별.
KR 구조: 자격 = forward PER<20 (fwd_PE proxy=trailing_PE/(1+g), g=EPS YoY PIT), 비중 = 기대성장(g)↑.
월간 리밸, top-K 동일/성장가중. 방어오버레이: SPX<MA200(15) | VIX>36(2) | HY-OAS트로프 → 현금(3% note).
⚠️ forward=실현성장 프록시(애널consensus 아님). ⚠️ us-4factor 생존편향=절대수익 과대(상대비교·LOWO·연도가 신뢰).
"""
import sys, os, json
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
U = r'C:\dev\claude-code\us-4factor\research\mf_data'
EW = r'C:\dev\claude-code\eps-momentum-us\research\early_warn'
DC = r'C:\dev\claude-code\eps-momentum-us\data_cache'

px = pd.read_parquet(os.path.join(U, 'prices.parquet')).sort_index(); px.index = pd.to_datetime(px.index)
funda = json.load(open(os.path.join(U, 'funda.json')))
LAG = 120
fy, pa = {}, {}
for tk, f in funda.items():
    if '_err' in f or not f.get('eps') or tk not in px.columns:
        continue
    rows = [(pd.Timestamp(d), f.get('eps', {}).get(d)) for d in sorted(f['eps'].keys())]
    fy[tk] = ([d + pd.Timedelta(days=LAG) for d, _ in rows], [e for _, e in rows])
    s = px[tk].dropna(); pa[tk] = (s.index.values, s.values)
UNIV = list(fy.keys())


def price_at(tk, d):
    a = pa.get(tk)
    if a is None: return None
    i = np.searchsorted(a[0], np.datetime64(d), side='right') - 1
    return float(a[1][i]) if i >= 0 else None


def feat(tk, d):
    avd, eps = fy[tk]; i = -1
    for j in range(len(avd)):
        if avd[j] <= d: i = j
        else: break
    if i < 0 or eps[i] is None or eps[i] <= 0: return None
    p = price_at(tk, d)
    if not p: return None
    tpe = p / eps[i]
    g = (eps[i] / eps[i - 1] - 1) if (i >= 1 and eps[i - 1] and eps[i - 1] > 0) else None
    if g is None or g <= -0.99: return None
    return tpe, g, tpe / (1 + g)   # trailing_PE, growth, fwd_PE_proxy


# 방어 오버레이 (월말 기준): SPX<MA200(15) | VIX>36(2) | HY-OAS 트로프
A = pd.read_parquet(os.path.join(EW, '..', 'regime_assets.parquet')); A.index = pd.to_datetime(A.index)
spx = A['^GSPC'].dropna(); vix = A['^VIX'].reindex(spx.index).ffill()
hy = pd.read_parquet(os.path.join(DC, 'hy_spread.parquet'), engine='fastparquet')['hy_spread']
hy = hy.reindex(spx.index, method='ffill')
def _ser(raw, n):
    st = False; sd = sb = 0; o = []
    for d in raw.values:
        sd = sd + 1 if d else 0; sb = 0 if d else sb + 1
        if not st and sd >= n: st = True
        elif st and sb >= n: st = False
        o.append(st)
    return pd.Series(o, index=raw.index)
def _ser2(raw, ne, nx):
    st = False; sd = sb = 0; o = []
    for d in raw.values:
        sd = sd + 1 if d else 0; sb = 0 if d else sb + 1
        if not st and sd >= ne: st = True
        elif st and sb >= nx: st = False
        o.append(st)
    return pd.Series(o, index=raw.index)
ma_def = _ser((spx < spx.rolling(200).mean()).fillna(False), 15)
vix_def = _ser((vix > 36).fillna(False), 2)
hy_raw = ((hy - hy.rolling(126).min()) >= 1.0) & (hy > hy.shift(20))
hy_def = _ser2(hy_raw.fillna(False), 5, 20)
defense_daily = (ma_def | vix_def | hy_def)

me = [d for d in px.resample('ME').last().index if px.index[0] <= d <= px.index[-1]]
NOTE = (1.03) ** (1 / 12) - 1


def defense_at(d):
    sub = defense_daily.loc[:d]
    return bool(sub.iloc[-1]) if len(sub) else False


def run(K=15, weight='growth', fmax=20, overlay=True, ban=(), hi_trail=False):
    nav = 1.0; peak = 1.0; mdd = 0.0; rets = []
    for i in range(len(me) - 1):
        d, dn = me[i], me[i + 1]
        if overlay and defense_at(d):
            nav *= (1 + NOTE); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1); rets.append(NOTE); continue
        feats = {tk: feat(tk, d) for tk in UNIV if tk not in ban}
        feats = {tk: f for tk, f in feats.items() if f}
        thi = np.quantile([f[0] for f in feats.values()], 0.67) if (hi_trail and feats) else None
        elig = []
        for tk, f in feats.items():
            if f[2] < fmax and (not hi_trail or f[0] >= thi):   # fwd_PE_proxy<fmax (+ 선택: trailing 상위33%)
                elig.append((tk, f[1]))   # (tk, growth)
        elig.sort(key=lambda x: -x[1])
        top = elig[:K]
        if not top:
            rets.append(0); continue
        if weight == 'growth':
            tot = sum(max(g, 0) for _, g in top) or 1
            w = {tk: max(g, 0) / tot for tk, g in top}
            if sum(w.values()) == 0: w = {tk: 1 / len(top) for tk, _ in top}
        else:
            w = {tk: 1 / len(top) for tk, _ in top}
        r = 0.0
        for tk in w:
            p0, p1 = price_at(tk, d), price_at(tk, dn)
            if p0 and p1 and p0 > 0: r += w[tk] * (p1 / p0 - 1)
        nav *= (1 + r); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1); rets.append(r)
    cum = (nav - 1) * 100; yrs = (me[-1] - me[0]).days / 365.25
    cagr = nav ** (1 / yrs) - 1
    sharpe = (np.mean(rets) / np.std(rets) * np.sqrt(12)) if np.std(rets) > 0 else 0
    return cum, cagr * 100, mdd * 100, (cagr * 100 / abs(mdd) if mdd < 0 else 0), sharpe


def uni_ew(overlay=False):
    nav = 1.0
    for i in range(len(me) - 1):
        d, dn = me[i], me[i + 1]
        if overlay and defense_at(d):
            nav *= (1 + NOTE); continue
        rs = [price_at(tk, dn) / price_at(tk, d) - 1 for tk in UNIV
              if price_at(tk, d) and price_at(tk, dn) and price_at(tk, d) > 0]
        if rs: nav *= (1 + np.mean(rs))
    return (nav - 1) * 100


print(f'유니버스 {len(UNIV)}종목, {me[0].date()}~{me[-1].date()} ({len(me)}개월)')
print(f'방어 발동 개월: {sum(defense_at(d) for d in me)}/{len(me)}')
print(f'\n벤치: 유니버스 동일가중 {uni_ew():+.0f}% (방어O {uni_ew(True):+.0f}%) | (README: SPY +154%, 4팩터 +556%/Cal1.16)')
print('\n=== gap 슬리브 (forward<20 자격 + 성장비중), 방어 오버레이 O ===')
print(f'{"구성":<24}{"누적%":>9}{"CAGR%":>8}{"MDD%":>8}{"Cal":>6}{"Sharpe":>8}')
for K in [10, 15, 20, 30]:
    for wt in ['growth', 'equal']:
        r = run(K=K, weight=wt)
        print(f'  K{K} {wt:>6}{"":<10}{r[0]:>+9.0f}{r[1]:>+8.0f}{r[2]:>+8.0f}{r[3]:>6.2f}{r[4]:>8.2f}')
print('  (방어 오버레이 OFF, K15 growth):', '%.0f%% CAGR%.0f MDD%.0f Cal%.2f' % run(15, 'growth', overlay=False)[:4])

print('\n=== LOWO (winner 빼도 분산 유지?) — broad면 버텨야 ===')
base = run(15, 'growth')[0]
# 상위 기여 종목 후보 (대형 winner 가능성)
for w in ['NVDA', 'SMCI', 'LLY', 'VST', 'AVGO', 'CVNA']:
    if w in UNIV:
        r = run(15, 'growth', ban=(w,))[0]
        print(f'   -{w:6}: {r:+.0f}% (Δ{r-base:+.0f}p)')
print('\n참고: 좁은137 슬리브는 -SNDK시 -183p(SNDK단일). broad면 winner 빼도 작은 Δ여야 = 진짜 분산알파.')

print('\n=== ★A 위닝셀 슬리브: trailing PE 상위33% AND forward<15 (성장가중) — 이게 진짜 알파셀? ===')
print(f'{"구성":<28}{"누적%":>9}{"CAGR%":>8}{"MDD%":>8}{"Sharpe":>8}')
for fmax in [15, 20]:
    for K in [15, 30]:
        r = run(K=K, weight='growth', fmax=fmax, hi_trail=True)
        print(f'  hi_trail+fwd<{fmax} K{K}{"":<8}{r[0]:>+9.0f}{r[1]:>+8.0f}{r[2]:>+8.0f}{r[4]:>8.2f}')
print(f'  (벤치 유니버스 방어O +178%, SPY +154%)')
print('판정: A셀(hi_trail×fwd<15)이 유니버스 넘으면 = 진짜 슬리브, 못넘으면 = US broad서도 gap슬리브 불가.')
