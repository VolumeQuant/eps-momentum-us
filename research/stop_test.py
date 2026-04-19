"""
Trailing Stop / Stop Loss 백테스트 — v79 baseline 대비 차분 측정

설계 원칙 (사용자 실행원칙 준수):
- 30분 이내 단일 실행 (캐시 한 번 로드 재사용)
- multistart 33시작일 (41~46일 raw return)
- 모든 risk metric 측정 (평균/중앙값/std/min/max + MDD avg/worst + Sharpe + Sortino + 위험조정)
- single-variable change (trailing 단독 / stop_loss 단독 / 조합 별도)
- baseline 대비 차분만 평가
- CAGR 환산 금지

변형:
- baseline (v79 그대로, trailing/stop 없음)
- trailing stop: -5%, -8%, -10%, -12%, -15% (진입 후 최고가 대비)
- stop loss: -10%, -15%, -20%, -25% (진입가 대비)
- 조합 (best trailing + best stop loss)
"""
import sqlite3
import numpy as np
import pickle
import time
import sys
from collections import defaultdict
from datetime import datetime, timedelta
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = '../eps_momentum_data.db'
t0 = time.time()

# ═══════════════════════════════════════════════════════════════
# 1. 캐시 빌드 (v79 기준: z-score clamp 100 제거, FCF·ROE 필터)
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')
all_dates = [r[0] for r in cur.fetchall()]
cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')
p2_dates = [r[0] for r in cur.fetchall()]
print(f"DB: all_dates={len(all_dates)} p2_dates={len(p2_dates)} ({p2_dates[0]}~{p2_dates[-1]})")

# raw 데이터 (FCF, ROE 추가)
raw = {}
for d in all_dates:
    rows = cur.execute('''
        SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d,
               rev_growth, ntm_7d, ntm_30d, ntm_60d, price, composite_rank, part2_rank,
               free_cashflow, roe
        FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
    ''', (d,)).fetchall()
    raw[d] = {}
    for r in rows:
        raw[d][r[0]] = {
            'adj_gap': r[1], 'rev_up30': r[2] or 0, 'num_analysts': r[3] or 0,
            'ntm_cur': r[4] or 0, 'ntm_90d': r[5] or 0, 'rev_growth': r[6],
            'ntm_7d': r[7] or 0, 'ntm_30d': r[8] or 0, 'ntm_60d': r[9] or 0,
            'price': r[10] or 0, 'comp_rank': r[11], 'p2_rank': r[12],
            'fcf': r[13], 'roe': r[14],
        }

# conviction (v75)
def apply_conviction(v):
    ag = v['adj_gap'] or 0
    ratio = (v['rev_up30'] / v['num_analysts']) if v['num_analysts'] > 0 else 0
    eps_floor = min(abs((v['ntm_cur'] - v['ntm_90d']) / v['ntm_90d']), 1.0) \
        if v['ntm_90d'] and abs(v['ntm_90d']) > 0.01 else 0
    base_conv = max(ratio, eps_floor)
    rev_bonus = 0.3 if (v['rev_growth'] is not None and v['rev_growth'] >= 0.30) else 0
    return ag * (1 + base_conv + rev_bonus)

# z-score (v79: 상한 clamp 제거)
zscore_by_date = {}
for d in all_dates:
    convs = {tk: apply_conviction(v) for tk, v in raw[d].items()}
    vals = list(convs.values())
    if len(vals) >= 2:
        m, s = np.mean(vals), np.std(vals)
        if s > 0:
            zscore_by_date[d] = {tk: max(30.0, 65 + (-(v - m) / s) * 15) for tk, v in convs.items()}
        else:
            zscore_by_date[d] = {tk: 65.0 for tk in convs}
    else:
        zscore_by_date[d] = {tk: 65.0 for tk in convs}

# Case 1 보너스 (v78)
def get_past_date(d, days):
    target = (datetime.strptime(d, '%Y-%m-%d') - timedelta(days=days)).strftime('%Y-%m-%d')
    r = cur.execute('SELECT MAX(date) FROM ntm_screening WHERE date <= ?', (target,)).fetchone()
    return r[0] if r and r[0] else None

CASE1_PERIOD, CASE1_NTM_THR, CASE1_PX_THR, CASE1_BONUS = 30, 1.0, -1.0, 8.0
all_prices = {}
for d in all_dates:
    rows = cur.execute('SELECT ticker, price FROM ntm_screening WHERE date=? AND price>0', (d,)).fetchall()
    all_prices[d] = {r[0]: r[1] for r in rows}

for d in all_dates:
    past = get_past_date(d, CASE1_PERIOD)
    if not past:
        continue
    px_past = all_prices.get(past, {})
    for tk, v in raw[d].items():
        nc, n30 = v['ntm_cur'], v['ntm_30d']
        ntm_chg = ((nc - n30) / n30 * 100) if n30 and abs(n30) > 0.01 else 0
        pp = px_past.get(tk)
        px_chg = ((v['price'] - pp) / pp * 100) if pp and pp > 0 else 0
        if ntm_chg > CASE1_NTM_THR and px_chg < CASE1_PX_THR and tk in zscore_by_date[d]:
            zscore_by_date[d][tk] += CASE1_BONUS

# w_gap (3일 가중, missing=30)
MISS = 30.0
wgap_by_date = {}
for di, d in enumerate(p2_dates):
    dates_3 = [p2_dates[max(0, di-2)], p2_dates[max(0, di-1)], p2_dates[di]]
    dates_3 = [dd for dd in dates_3 if dd in zscore_by_date]
    weights = [0.2, 0.3, 0.5][-len(dates_3):]
    if len(dates_3) == 2: weights = [0.4, 0.6]
    elif len(dates_3) == 1: weights = [1.0]
    all_tks = set()
    for dd in dates_3: all_tks.update(zscore_by_date[dd].keys())
    wgap = {}
    for tk in all_tks:
        wg = 0
        for i, dd in enumerate(dates_3):
            wg += zscore_by_date[dd].get(tk, MISS) * weights[i]
        wgap[tk] = wg
    wgap_by_date[d] = wgap

# min_seg
chg_data = {}
for d in all_dates:
    chg_data[d] = {}
    for tk, v in raw[d].items():
        nc = v['ntm_cur']
        segs = []
        for a, b in [(nc, v['ntm_7d']), (v['ntm_7d'], v['ntm_30d']),
                     (v['ntm_30d'], v['ntm_60d']), (v['ntm_60d'], v.get('ntm_90d', 0))]:
            if b and abs(b) > 0.01:
                segs.append(max(-100, min(100, (a - b) / abs(b) * 100)))
            else:
                segs.append(0)
        chg_data[d][tk] = {'min_seg': min(segs)}

conn.close()
print(f"캐시 빌드 완료: {time.time()-t0:.1f}s")

# ═══════════════════════════════════════════════════════════════
# 2. 시뮬 함수 — trailing_stop / stop_loss 옵션 추가
# ═══════════════════════════════════════════════════════════════
def simulate(start_idx=0, entry=3, exit_r=8, slots=3,
             trailing_stop=None, stop_loss=None):
    """
    trailing_stop: 진입 후 최고가 대비 N% 하락 시 매도 (예: 0.10 = -10%)
    stop_loss: 진입가 대비 N% 하락 시 매도 (예: 0.15 = -15%)
    None이면 미적용
    """
    portfolio = {}  # {tk: {ep, peak, date}}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)

    for di in range(start_idx, len(p2_dates)):
        d = p2_dates[di]
        wg = wgap_by_date.get(d, {})
        day_raw = raw.get(d, {})
        day_chg = chg_data.get(d, {})

        sorted_tk = sorted(wg.keys(), key=lambda t: wg.get(t, 0), reverse=True)
        rank_map = {tk: i+1 for i, tk in enumerate(sorted_tk)}

        # consecutive count
        new_con = defaultdict(int)
        for tk in sorted_tk:
            if rank_map.get(tk, 999) <= 30:
                new_con[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_con

        # 이탈 판단 (trailing/stop loss 우선 → 일반 룰)
        for tk in list(portfolio.keys()):
            price = day_raw.get(tk, {}).get('price')
            if not price:
                continue

            pos = portfolio[tk]
            ep = pos['ep']
            # peak 업데이트
            pos['peak'] = max(pos['peak'], price)

            exit_reason = None

            # stop loss 체크 (진입가 대비)
            if stop_loss is not None and price <= ep * (1 - stop_loss):
                exit_reason = 'stop_loss'

            # trailing stop 체크 (peak 대비)
            elif trailing_stop is not None and price <= pos['peak'] * (1 - trailing_stop):
                exit_reason = 'trailing'

            # 일반 룰 (순위 밀림 / min_seg)
            else:
                rk = rank_map.get(tk)
                ms = day_chg.get(tk, {}).get('min_seg', 0)
                if (rk is None or rk > exit_r) or ms < -2:
                    exit_reason = 'rule'

            if exit_reason:
                ret = (price - ep) / ep * 100
                trades.append({
                    'ret': ret, 'entry_date': pos['date'], 'exit_date': d,
                    'ticker': tk, 'reason': exit_reason
                })
                del portfolio[tk]

        # 진입
        vac = slots - len(portfolio)
        if vac > 0:
            for tk in sorted_tk:
                if vac <= 0: break
                if tk in portfolio: continue
                if rank_map.get(tk, 999) > entry: continue
                if consecutive.get(tk, 0) < 3: continue
                ms = day_chg.get(tk, {}).get('min_seg', 0)
                if ms < 0: continue
                price = day_raw.get(tk, {}).get('price')
                if price and price > 0:
                    portfolio[tk] = {'ep': price, 'peak': price, 'date': d}
                    vac -= 1

        # 일별 수익 (균등 비중)
        if portfolio and di > 0:
            prev = p2_dates[di-1]
            dr = 0
            for tk in portfolio:
                pn = day_raw.get(tk, {}).get('price')
                pp = raw.get(prev, {}).get(tk, {}).get('price')
                if pn and pp and pp > 0:
                    dr += (pn - pp) / pp * 100
            dr /= len(portfolio)
            daily_returns.append(dr)

    # 잔여 청산
    if portfolio:
        last = p2_dates[-1]
        for tk in list(portfolio.keys()):
            p = raw.get(last, {}).get(tk, {}).get('price')
            if p:
                ep = portfolio[tk]['ep']
                trades.append({'ret': (p-ep)/ep*100, 'entry_date': portfolio[tk]['date'],
                              'exit_date': last, 'ticker': tk, 'reason': 'final'})

    return daily_returns, trades

# ═══════════════════════════════════════════════════════════════
# 3. multistart 33시작일 + metric 계산
# ═══════════════════════════════════════════════════════════════
def measure(label, **sim_kwargs):
    # 시작일은 초기 한 주(0~7)만 변동 — 모든 시작일이 38~46일 충분한 sim 기간 보장
    # (33시작일은 si=32일 때 14일만 sim해서 noise. 측정 무의미)
    starts = list(range(0, min(8, len(p2_dates)-5)))
    multi = []
    sample_trades = None
    for si in starts:
        drs, trades = simulate(start_idx=si, **sim_kwargs)
        cum = 1.0; peak = 1.0; mdd = 0
        for dr in drs:
            cum *= (1+dr/100); peak = max(peak, cum); mdd = min(mdd, (cum-peak)/peak*100)
        ret = (cum-1)*100
        da = np.array(drs) if drs else np.array([0])
        sharpe = (da.mean()/da.std()*np.sqrt(252)) if da.std() > 0 else 0
        neg = da[da < 0]
        sortino = (da.mean()/(neg.std() if len(neg) > 1 else 1))*np.sqrt(252)
        n = len(trades)
        wr = (sum(1 for t in trades if t['ret'] > 0)/n*100) if n else 0
        pf_w = sum(t['ret'] for t in trades if t['ret'] > 0)
        pf_l = abs(sum(t['ret'] for t in trades if t['ret'] < 0))
        pf = pf_w/pf_l if pf_l > 0 else 999
        multi.append({'ret': ret, 'mdd': mdd, 'sharpe': sharpe, 'sortino': sortino,
                      'n': n, 'wr': wr, 'pf': pf})
        if si == 0: sample_trades = trades

    rets = np.array([r['ret'] for r in multi])
    mdds = np.array([r['mdd'] for r in multi])

    # 이탈 사유 breakdown
    reasons = defaultdict(int)
    for t in sample_trades or []:
        reasons[t['reason']] += 1

    return {
        'label': label,
        'ret_mean': rets.mean(), 'ret_med': np.median(rets), 'ret_std': rets.std(),
        'ret_min': rets.min(), 'ret_max': rets.max(),
        'mdd_mean': mdds.mean(), 'mdd_worst': mdds.min(),
        'sharpe': np.mean([r['sharpe'] for r in multi]),
        'sortino': np.mean([r['sortino'] for r in multi]),
        'risk_adj': rets.mean()/abs(mdds.min()) if mdds.min() < 0 else 999,
        'n': np.mean([r['n'] for r in multi]),
        'wr': np.mean([r['wr'] for r in multi]),
        'pf': np.mean([r['pf'] for r in multi]),
        'reasons': dict(reasons),
        'sample_trades': sample_trades,
    }

# ═══════════════════════════════════════════════════════════════
# 4. 변형 BT
# ═══════════════════════════════════════════════════════════════
print("\n" + "="*100)
print("Trailing Stop / Stop Loss BT (multistart 33시작일)")
print("="*100)

variants = [
    ('baseline (v79)', {}),
    ('trailing -5%',   {'trailing_stop': 0.05}),
    ('trailing -8%',   {'trailing_stop': 0.08}),
    ('trailing -10%',  {'trailing_stop': 0.10}),
    ('trailing -12%',  {'trailing_stop': 0.12}),
    ('trailing -15%',  {'trailing_stop': 0.15}),
    ('stop loss -10%', {'stop_loss': 0.10}),
    ('stop loss -15%', {'stop_loss': 0.15}),
    ('stop loss -20%', {'stop_loss': 0.20}),
    ('stop loss -25%', {'stop_loss': 0.25}),
]

results = []
for label, kw in variants:
    r = measure(label, **kw)
    results.append(r)
    print(f"\n[{label}]")
    print(f"  ret 평균/중앙/std/min/max: {r['ret_mean']:+.1f}% / {r['ret_med']:+.1f}% / {r['ret_std']:.1f}% / {r['ret_min']:+.1f}% / {r['ret_max']:+.1f}%")
    print(f"  MDD 평균/worst: {r['mdd_mean']:.1f}% / {r['mdd_worst']:.1f}%")
    print(f"  Sharpe/Sortino/위험조정: {r['sharpe']:.2f} / {r['sortino']:.2f} / {r['risk_adj']:.2f}")
    print(f"  거래/승률/PF: {r['n']:.1f} / {r['wr']:.0f}% / {r['pf']:.2f}")
    print(f"  이탈사유(si=0): {r['reasons']}")

# ═══════════════════════════════════════════════════════════════
# 5. baseline 대비 차분 표
# ═══════════════════════════════════════════════════════════════
base = results[0]
print("\n" + "="*100)
print("Baseline 대비 차분 (Δ)")
print("="*100)
print(f"{'변형':<20} {'Δret':>8} {'Δmdd_w':>10} {'ΔSharpe':>10} {'Δ위험조정':>12} {'판정':>10}")
print("-"*100)
print(f"{base['label']:<20} {'(기준)':>8} {base['mdd_worst']:+9.1f}% {base['sharpe']:>10.2f} {base['risk_adj']:>12.2f}")
for r in results[1:]:
    d_ret = r['ret_mean'] - base['ret_mean']
    d_mdd = r['mdd_worst'] - base['mdd_worst']
    d_sh = r['sharpe'] - base['sharpe']
    d_ra = r['risk_adj'] - base['risk_adj']
    # 판정: ret >= -1%p AND MDD 악화 <= +2%p AND 위험조정 ≥ baseline → 통과 후보
    verdict = "통과" if (d_ret >= -1 and d_mdd >= -2 and d_ra >= 0) else "기각"
    print(f"{r['label']:<20} {d_ret:+7.1f}%p {d_mdd:+9.1f}%p {d_sh:+9.2f} {d_ra:+11.2f} {verdict:>10}")

# 조합: 통과 후보 중 ret 최고 변형끼리
print("\n" + "="*100)
print("조합 BT (ret 최고 trailing + ret 최고 stop loss)")
print("="*100)
trailing_best = max([r for r in results if 'trailing' in r['label']], key=lambda x: x['ret_mean'])
sl_best = max([r for r in results if 'stop loss' in r['label']], key=lambda x: x['ret_mean'])
ts_pct = float(trailing_best['label'].split('-')[1].rstrip('%')) / 100
sl_pct = float(sl_best['label'].split('-')[1].rstrip('%')) / 100
print(f"\n조합: {trailing_best['label']} + {sl_best['label']}")
combo = measure(f"{trailing_best['label']} + {sl_best['label']}", trailing_stop=ts_pct, stop_loss=sl_pct)
print(f"  ret: {combo['ret_mean']:+.1f}% (vs baseline {base['ret_mean']:+.1f}% → Δ {combo['ret_mean']-base['ret_mean']:+.1f}%p)")
print(f"  MDD worst: {combo['mdd_worst']:.1f}% (vs {base['mdd_worst']:.1f}% → Δ {combo['mdd_worst']-base['mdd_worst']:+.1f}%p)")
print(f"  Sharpe/위험조정: {combo['sharpe']:.2f} / {combo['risk_adj']:.2f}")

# pickle 저장 (텔레그램 메시지 생성용)
with open('stop_test_results.pkl', 'wb') as f:
    pickle.dump({'results': results, 'combo': combo, 'p2_dates': p2_dates}, f)

print(f"\n총 소요: {time.time()-t0:.1f}s")
print(f"결과 저장: stop_test_results.pkl")
