# -*- coding: utf-8 -*-
"""'forward PE<15 + trailing PE 20~60' 슬리브 백테스트 + LOWO + 시기분할.
⚠️⚠️ 정직 경고: 임계(fPE<15, tPE20~60)를 *같은 88일 데이터* EDA에서 골랐음 = IN-SAMPLE 과적합.
   + trailing EPS = 현재 스냅샷(look-ahead). 따라서 '좋은 결과'는 당연(증거 아님).
   진짜 시험 = LOWO(winner 빼도 사나) + 시기분할(전반이 후반 예측?) + 종목수(집중도).
"""
import sys, os, json, sqlite3
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import daily_runner as dr
_P = os.path.dirname(os.path.abspath(__file__))

TEPS = json.load(open(os.path.join(_P, '_trailing_eps_cache.json')))
PX = pd.read_parquet(os.path.join(_P, '_eda_px.parquet')); PX.index = pd.to_datetime(PX.index)

conn = sqlite3.connect(dr.DB_PATH); c = conn.cursor()
sdates = [r[0] for r in c.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')]
D = {}
for d in sdates:
    D[d] = {r[0]: {'px': r[1], 'nc': r[2], 'dv': r[3]} for r in c.execute(
        'SELECT ticker,price,ntm_current,dollar_volume_30d FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL', (d,))}
conn.close()
pidx = {d.strftime('%Y-%m-%d'): i for i, d in enumerate(PX.index)}


def qualify(d, fmax=15, tlo=20, thi=60, dv=1000, ban=()):
    out = []
    for tk, v in D.get(d, {}).items():
        if tk in ban or not v['px'] or not v['nc'] or v['nc'] <= 0:
            continue
        te = TEPS.get(tk)
        if not te or te <= 0:
            continue
        fpe = v['px'] / v['nc']; tpe = v['px'] / te
        if fpe < fmax and tlo <= tpe <= thi and (v['dv'] or 0) >= dv:
            out.append(tk)
    return out


def bt(fmax=15, tlo=20, thi=60, rebal='M', kcap=10, ban=()):
    """월간(M)/주간(W) 리밸, 자격종목 동일가중. 가격 parquet로 일수익."""
    pdates = [d for d in PX.index if d.strftime('%Y-%m-%d') >= sdates[0] and d.strftime('%Y-%m-%d') <= sdates[-1]]
    hold = []; nav = 1.0; peak = 1.0; mdd = 0.0; rb = None; rets = []
    sd_set = set(sdates)
    for k in range(1, len(pdates)):
        d, pv = pdates[k], pdates[k - 1]
        # 일수익 (전일 보유 동일가중)
        if hold:
            rs = []
            for tk in hold:
                if tk in PX.columns:
                    p0, p1 = PX[tk].loc[pv], PX[tk].loc[d]
                    if pd.notna(p0) and pd.notna(p1) and p0 > 0:
                        rs.append(p1 / p0 - 1)
            r = np.mean(rs) if rs else 0.0
            nav *= (1 + r); peak = max(peak, nav); mdd = min(mdd, nav / peak - 1); rets.append(r)
        # 리밸 (월 첫날 or 주 첫날), 스크리닝 있는 전일 기준
        ds = pv.strftime('%Y-%m-%d')
        do_rb = (rebal == 'M' and (rb is None or d.month != rb)) or (rebal == 'W' and (rb is None or d.isocalendar()[1] != rb)) or not hold
        if do_rb and ds in sd_set:
            q = qualify(ds, fmax, tlo, thi, ban=ban)
            if q:
                hold = q[:kcap]; rb = d.month if rebal == 'M' else d.isocalendar()[1]
    cum = (nav - 1) * 100
    sharpe = (np.mean(rets) / np.std(rets) * np.sqrt(252)) if rets and np.std(rets) > 0 else 0
    cal = (cum / 100) / abs(mdd) if mdd < 0 else 0
    return cum, mdd * 100, cal, sharpe, hold


def uni_ew():
    pdates = [d for d in PX.index if d.strftime('%Y-%m-%d') >= sdates[0]]
    nav = 1.0
    for k in range(1, len(pdates)):
        d, pv = pdates[k], pdates[k - 1]
        ds = pv.strftime('%Y-%m-%d')
        tks = [t for t in D.get(ds, {})]
        rs = [PX[t].loc[d] / PX[t].loc[pv] - 1 for t in tks if t in PX.columns and pd.notna(PX[t].loc[pv]) and pd.notna(PX[t].loc[d]) and PX[t].loc[pv] > 0]
        if rs:
            nav *= (1 + np.mean(rs))
    return (nav - 1) * 100


print('⚠️ IN-SAMPLE(임계를 같은데서 고름)+look-ahead(trailing EPS 스냅샷). 좋은결과=당연, LOWO/시기가 진짜시험.\n')
print(f'유니버스 동일가중 벤치: {uni_ew():+.1f}%')
print(f'(참고: 2슬롯 +217%, gap K7슬리브 +64%)')
print()
print('=== 슬리브 (forward<15 + trailing 20~60) ===')
for rb in ['M', 'W']:
    cum, m, cal, sh, hold = bt(rebal=rb)
    print(f'  리밸{rb}: {cum:+.1f}% MDD{m:+.1f} Calmar{cal:.2f} Sharpe{sh:.2f} | 최종보유{len(hold)}: {hold}')
print()
print('=== LOWO (winner/주력 하나씩 빼서 — 분산알파면 버텨야) ===')
base = bt(rebal='M')[0]
print(f'  full(월): {base:+.1f}%')
for w in ['SNDK', 'JAZZ', 'NVST', 'CVLT', 'MU', 'STX']:
    r = bt(rebal='M', ban=(w,))[0]
    print(f'   -{w:5}: {r:+.1f}%  (Δ{r-base:+.1f}p)')
print()
print('=== 시기분할 (전반 자격종목이 후반에도 같은가 / 안정성) ===')
mid = sdates[len(sdates)//2]
for lab, lo, hi in [('전반', sdates[0], mid), ('후반', mid, sdates[-1])]:
    qs = set()
    for d in [x for x in sdates if lo <= x < hi]:
        qs.update(qualify(d))
    print(f'  {lab} 자격종목: {sorted(qs)}')
print()
print('=== 임계 인접 민감도 (과적합이면 흔들림) ===')
for fmax in [12, 15, 18]:
    for thi in [50, 60, 80]:
        cum, m, cal, sh, h = bt(fmax=fmax, thi=thi, rebal='M')
        print(f'  fPE<{fmax}, tPE20~{thi}: {cum:+.1f}% MDD{m:+.1f} Cal{cal:.2f} (보유{len(h)})')
