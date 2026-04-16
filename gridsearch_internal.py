"""
Case 1 보너스 — 시스템 내부 적용 그리드 서치
_compute_w_gap_map 로직을 내장해서 w_gap 자체를 재계산

적용 위치 4곳:
  P1. adj_gap 단계: conv_gap 계산 전에 adj_gap 자체를 보정
  P2. conviction 단계: conviction에 case1_bonus 추가
  P3. z-score 후: score에 case1_points 가산
  P4. 3일 가중 후: w_gap에 직접 가산

각 위치 × 기간(7d/30d/60d/blend) × 강도 × 진입/이탈/슬롯
"""
import sqlite3
import numpy as np
import sys
from collections import defaultdict
from datetime import datetime, timedelta
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'eps_momentum_data.db'


def load_all_data():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 모든 날짜 (composite_rank 있는)
    cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date')
    all_dates = [r[0] for r in cur.fetchall()]

    # 날짜별 데이터
    raw = {}
    for d in all_dates:
        rows = cur.execute('''
            SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d,
                   rev_growth, ntm_7d, ntm_30d, ntm_60d, price, composite_rank, part2_rank
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        raw[d] = {r[0]: {
            'adj_gap': r[1], 'rev_up30': r[2] or 0, 'num_analysts': r[3] or 0,
            'ntm_cur': r[4] or 0, 'ntm_90d': r[5] or 0, 'rev_growth': r[6],
            'ntm_7d': r[7] or 0, 'ntm_30d': r[8] or 0, 'ntm_60d': r[9] or 0,
            'price': r[10] or 0, 'comp_rank': r[11], 'p2': r[12],
        } for r in rows}

    # 과거 가격 캐시 (기간별)
    all_prices = {}
    for d in all_dates:
        rows = cur.execute('SELECT ticker, price FROM ntm_screening WHERE date=? AND price>0', (d,)).fetchall()
        all_prices[d] = {r[0]: r[1] for r in rows}

    def past_date(d, days):
        t = (datetime.strptime(d, '%Y-%m-%d') - timedelta(days=days)).strftime('%Y-%m-%d')
        r = cur.execute('SELECT MAX(date) FROM ntm_screening WHERE date<=?', (t,)).fetchone()
        return r[0] if r and r[0] else None

    # 각 (date, ticker)의 기간별 NTM/가격 변화율 미리 계산
    chg_data = {}
    for d in all_dates:
        chg_data[d] = {}
        past_d = {p: past_date(d, p) for p in [7, 30, 60, 90]}
        past_px = {p: all_prices.get(pd, {}) if pd else {} for p, pd in past_d.items()}

        for tk, v in raw[d].items():
            nc = v['ntm_cur']
            px_now = v['price']
            chgs = {}
            ntm_map = {'7d': v['ntm_7d'], '30d': v['ntm_30d'], '60d': v['ntm_60d'], '90d': v['ntm_90d']}
            for period, nval in ntm_map.items():
                days = int(period.replace('d', ''))
                chgs[f'ntm_{period}'] = ((nc - nval) / nval * 100) if nval and abs(nval) > 0.01 else 0
                pp = past_px.get(days, {}).get(tk)
                chgs[f'px_{period}'] = ((px_now - pp) / pp * 100) if pp and pp > 0 and px_now else 0
            # blend gap
            bg = sum(w * (chgs.get(f'ntm_{p}', 0) - chgs.get(f'px_{p}', 0))
                     for p, w in [('7d', 0.4), ('30d', 0.3), ('60d', 0.2), ('90d', 0.1)])
            chgs['blend_gap'] = bg
            # min_seg
            segs = []
            for a, b in [(nc, v['ntm_7d']), (v['ntm_7d'], v['ntm_30d']),
                         (v['ntm_30d'], v['ntm_60d']), (v['ntm_60d'], v['ntm_90d'])]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a - b) / abs(b) * 100)))
                else:
                    segs.append(0)
            chgs['min_seg'] = min(segs)
            chg_data[d][tk] = chgs

    conn.close()

    # part2_rank 있는 날짜
    p2_dates = [d for d in all_dates if any(v.get('p2') is not None for v in raw[d].values())]

    return all_dates, p2_dates, raw, chg_data, all_prices


def is_case1(chgs, period, ntm_thr, px_thr):
    if period == 'blend':
        return chgs.get('blend_gap', 0) > ntm_thr
    return chgs.get(f'ntm_{period}', 0) > ntm_thr and chgs.get(f'px_{period}', 0) < px_thr


def compute_w_gap_internal(raw, chg_data, all_dates, target_date, tickers,
                           position='none', period=None, ntm_thr=0, px_thr=0, strength=0):
    """시스템 내부 _compute_w_gap_map 재현 + Case 1 보너스 적용

    position: 'none' / 'P1_adjgap' / 'P2_conviction' / 'P3_zscore' / 'P4_wgap'
    """
    # 최근 3일
    idx = all_dates.index(target_date) if target_date in all_dates else -1
    if idx < 0: return {}
    dates = [all_dates[max(0, idx-2)], all_dates[max(0, idx-1)], all_dates[idx]]
    dates = [d for d in dates if d in raw]
    if not dates: return {}

    weights = [0.2, 0.3, 0.5][-len(dates):]
    if len(dates) == 2: weights = [0.4, 0.6]
    elif len(dates) == 1: weights = [1.0]

    MISS = 30
    score_by_date = {}

    for d in dates:
        day_raw = raw.get(d, {})
        day_chg = chg_data.get(d, {})

        # conviction 적용 → conv_gap
        conv_gaps = {}
        for tk, v in day_raw.items():
            ag = v['adj_gap'] or 0
            ratio = (v['rev_up30'] / v['num_analysts']) if v['num_analysts'] > 0 else 0
            eps_floor = min(abs((v['ntm_cur'] - v['ntm_90d']) / v['ntm_90d']), 1.0) \
                if v['ntm_90d'] and abs(v['ntm_90d']) > 0.01 else 0
            base_conv = max(ratio, eps_floor)
            rev_bonus = 0.3 if (v['rev_growth'] is not None and v['rev_growth'] >= 0.30) else 0
            conviction = base_conv + rev_bonus

            c1 = is_case1(day_chg.get(tk, {}), period, ntm_thr, px_thr) if position != 'none' else False

            # P1: adj_gap 보정
            if position == 'P1_adjgap' and c1:
                ag = ag * (1 + strength)  # adj_gap 증폭

            # P2: conviction 보정
            if position == 'P2_conviction' and c1:
                conviction += strength

            conv_gaps[tk] = ag * (1 + conviction)

        # z-score (30~100)
        vals = list(conv_gaps.values())
        if len(vals) >= 2:
            m, s = np.mean(vals), np.std(vals)
            if s > 0:
                scores = {tk: min(100.0, max(30.0, 65 + (-(v - m) / s) * 15))
                         for tk, v in conv_gaps.items()}
            else:
                scores = {tk: 65 for tk in conv_gaps}
        else:
            scores = {tk: 65 for tk in conv_gaps}

        # P3: z-score 후 가산
        if position == 'P3_zscore':
            for tk in scores:
                if is_case1(day_chg.get(tk, {}), period, ntm_thr, px_thr):
                    scores[tk] += strength  # clip 안 함

        score_by_date[d] = scores

    # 3일 가중
    result = {}
    for tk in tickers:
        wg = 0
        for i, d in enumerate(dates):
            sc = score_by_date.get(d, {}).get(tk, MISS)
            wg += sc * weights[i]

        # P4: w_gap 후 가산
        if position == 'P4_wgap':
            # 오늘 기준 Case 1이면 가산
            if is_case1(chg_data.get(target_date, {}).get(tk, {}), period, ntm_thr, px_thr):
                wg += strength

        result[tk] = wg
    return result


def simulate(p2_dates, raw, chg_data, all_dates,
             entry_top, exit_top, max_slots,
             position='none', period=None, ntm_thr=0, px_thr=0, strength=0,
             start_idx=0):
    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)

    for di in range(start_idx, len(p2_dates)):
        today = p2_dates[di]
        day_raw = raw.get(today, {})
        day_chg = chg_data.get(today, {})
        tickers = [tk for tk in day_raw if day_raw[tk].get('comp_rank') is not None]

        # w_gap 재계산 (보너스 포함)
        wgap = compute_w_gap_internal(raw, chg_data, all_dates, today, tickers,
                                       position, period, ntm_thr, px_thr, strength)
        # w_gap → rank
        sorted_tickers = sorted(tickers, key=lambda tk: wgap.get(tk, 0), reverse=True)
        rank_map = {tk: i+1 for i, tk in enumerate(sorted_tickers)}

        # consecutive
        new_con = defaultdict(int)
        for tk in tickers:
            if rank_map.get(tk, 999) <= 30:
                new_con[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_con

        # 이탈
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            ms = day_chg.get(tk, {}).get('min_seg', 0)
            price = day_raw.get(tk, {}).get('price')
            if (rank is None or rank > exit_top) or ms < -2:
                if price:
                    trades.append((price - portfolio[tk]) / portfolio[tk] * 100)
                del portfolio[tk]

        # 진입
        vac = max_slots - len(portfolio)
        if vac > 0:
            for tk in sorted_tickers:
                if vac <= 0: break
                if tk in portfolio: continue
                if rank_map.get(tk, 999) > entry_top: continue
                if consecutive.get(tk, 0) < 3: continue
                ms = day_chg.get(tk, {}).get('min_seg', 0)
                if ms < 0: continue
                price = day_raw.get(tk, {}).get('price')
                if price and price > 0:
                    portfolio[tk] = price
                    vac -= 1

        # 일별 수익률
        if portfolio and di > 0:
            prev = p2_dates[di-1]
            dr = 0
            for tk in portfolio:
                p_now = day_raw.get(tk, {}).get('price')
                p_prev = raw.get(prev, {}).get(tk, {}).get('price')
                if p_now and p_prev and p_prev > 0:
                    dr += (p_now - p_prev) / p_prev * 100
            dr /= len(portfolio)
            daily_returns.append(dr)

    # 잔여
    if portfolio:
        last = p2_dates[-1]
        for tk in list(portfolio.keys()):
            p = raw.get(last, {}).get(tk, {}).get('price')
            if p: trades.append((p - portfolio[tk]) / portfolio[tk] * 100)

    cum = 1.0; peak = 1.0; mdd = 0
    for dr in daily_returns:
        cum *= (1 + dr/100); peak = max(peak, cum); mdd = min(mdd, (cum-peak)/peak*100)
    n = len(trades)
    wr = (sum(1 for t in trades if t > 0) / n * 100) if n else 0
    da = np.array(daily_returns) if daily_returns else np.array([0])
    sharpe = (da.mean()/da.std()*np.sqrt(252)) if da.std() > 0 else 0
    return {'ret': round((cum-1)*100, 2), 'mdd': round(mdd, 2), 'n': n,
            'wr': round(wr, 1), 'sharpe': round(sharpe, 2)}


def main():
    print("Loading (시스템 내부 재현)...")
    all_dates, p2_dates, raw, chg_data, all_prices = load_all_data()
    print(f"Period: {p2_dates[0]}~{p2_dates[-1]} ({len(p2_dates)} days)")

    # 표본 테스트 (5개)
    print("\n[표본 테스트]")
    bl = simulate(p2_dates, raw, chg_data, all_dates, 5, 12, 3)
    print(f"  Baseline E5/X12/S3: ret {bl['ret']:+.1f}%, Sharpe {bl['sharpe']:.2f}")
    t1 = simulate(p2_dates, raw, chg_data, all_dates, 5, 12, 3,
                  position='P3_zscore', period='7d', ntm_thr=1.5, px_thr=-2.0, strength=10)
    print(f"  P3 zscore +10 7d: ret {t1['ret']:+.1f}%, Sharpe {t1['sharpe']:.2f}")
    t2 = simulate(p2_dates, raw, chg_data, all_dates, 5, 12, 3,
                  position='P2_conviction', period='7d', ntm_thr=1.5, px_thr=-2.0, strength=0.3)
    print(f"  P2 conviction +0.3 7d: ret {t2['ret']:+.1f}%, Sharpe {t2['sharpe']:.2f}")
    print("  → 표본 OK, 본 실행...")

    # 그리드
    ENTRY = [3, 5]
    EXIT = [8, 10, 12]
    SLOTS = [3, 5]

    POSITIONS = {
        'P1_adjgap':    [0.1, 0.2, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0],    # adj_gap 증폭 배율
        'P2_conviction': [0.1, 0.2, 0.3, 0.5, 0.8, 1.0, 1.5],         # conviction 추가
        'P3_zscore':    [3, 5, 8, 10, 15, 20, 25, 30],                 # z-score 가산점
        'P4_wgap':      [3, 5, 8, 10, 15, 20, 25, 30],                 # w_gap 가산점
    }

    PERIODS = {
        '7d':  [(1.0, -1.0), (1.5, -2.0), (2.0, -3.0)],
        '30d': [(3.0, -3.0), (5.0, -5.0)],
        '60d': [(5.0, -5.0), (8.0, -8.0)],
        'blend': [(5.0, 0), (8.0, 0), (10.0, 0)],
    }

    results = []
    total = 0

    # baseline
    for e in ENTRY:
        for x in EXIT:
            if x <= e: continue
            for s in SLOTS:
                r = simulate(p2_dates, raw, chg_data, all_dates, e, x, s)
                results.append({'pos':'none', 'period':'-', 'nt':0, 'pt':0, 'str':0,
                               'e':e, 'x':x, 's':s, **r})
                total += 1

    # 각 위치 × 기간 × 강도 × 파라미터
    for pos, strengths in POSITIONS.items():
        for period, thrs in PERIODS.items():
            for nt, pt in thrs:
                for st in strengths:
                    for e in ENTRY:
                        for x in EXIT:
                            if x <= e: continue
                            for s in SLOTS:
                                r = simulate(p2_dates, raw, chg_data, all_dates, e, x, s,
                                           position=pos, period=period,
                                           ntm_thr=nt, px_thr=pt, strength=st)
                                results.append({
                                    'pos':pos, 'period':period, 'nt':nt, 'pt':pt, 'str':st,
                                    'e':e, 'x':x, 's':s, **r
                                })
                                total += 1
                                if total % 500 == 0:
                                    print(f"  {total} combos...")

    print(f"\n총 {total} 조합 완료")

    # ── 결과 ──
    bl_v72 = [r for r in results if r['pos']=='none' and r['e']==5 and r['x']==12 and r['s']==3]
    bl_best = max([r for r in results if r['pos']=='none'], key=lambda x: x['ret'])

    print(f"\n{'='*115}")
    print("Baseline")
    print(f"{'='*115}")
    if bl_v72:
        c = bl_v72[0]
        print(f"  현행 E5/X12/S3: ret {c['ret']:+.1f}%, MDD {c['mdd']:.1f}%, Sharpe {c['sharpe']:.2f}")
    print(f"  최적: E{bl_best['e']}/X{bl_best['x']}/S{bl_best['s']}: ret {bl_best['ret']:+.1f}%, Sharpe {bl_best['sharpe']:.2f}")

    # 적용 위치별 최적
    print(f"\n{'='*115}")
    print("적용 위치별 최적 (수익률)")
    print(f"{'='*115}")
    for pos in ['none', 'P1_adjgap', 'P2_conviction', 'P3_zscore', 'P4_wgap']:
        sub = [r for r in results if r['pos']==pos]
        if not sub: continue
        best = max(sub, key=lambda x: x['ret'])
        marker = '★' if best['ret'] == max(r['ret'] for r in results) else ' '
        print(f"  {marker} {pos:>14}: E{best['e']}/X{best['x']}/S{best['s']} "
              f"{best['period']} N{best['nt']}/P{best['pt']} str={best['str']} "
              f"→ ret {best['ret']:+.1f}%, MDD {best['mdd']:.1f}%, Sharpe {best['sharpe']:.2f}, n={best['n']}")

    # 기간별 최적 (전 위치 통합)
    print(f"\n{'='*115}")
    print("기간별 최적 (전 위치 통합)")
    print(f"{'='*115}")
    for period in ['7d', '30d', '60d', 'blend']:
        sub = [r for r in results if r['period']==period]
        if not sub: continue
        best = max(sub, key=lambda x: x['ret'])
        print(f"  {period:>5}: {best['pos']:>14} E{best['e']}/X{best['x']}/S{best['s']} "
              f"N{best['nt']}/P{best['pt']} str={best['str']} → ret {best['ret']:+.1f}%, Sharpe {best['sharpe']:.2f}")

    # 전체 Top 20
    print(f"\n{'='*115}")
    print("전체 Top 20 (수익률)")
    print(f"{'='*115}")
    top20 = sorted(results, key=lambda x: -x['ret'])[:20]
    print(f"{'#':<3}{'위치':>14}{'기간':>6}{'E':>3}{'X':>4}{'S':>3}{'NTM':>5}{'PX':>5}{'str':>6}"
          f"{'ret':>8}{'MDD':>8}{'Shp':>7}{'n':>4}{'WR':>6}")
    for i, r in enumerate(top20, 1):
        print(f"{i:<3}{r['pos']:>14}{r['period']:>6}{r['e']:>3}{r['x']:>4}{r['s']:>3}"
              f"{r['nt']:>5}{r['pt']:>5}{r['str']:>6}"
              f"{r['ret']:>+6.1f}%{r['mdd']:>+6.1f}%{r['sharpe']:>6.2f}{r['n']:>4}{r['wr']:>5.0f}%")

    # Sharpe Top 20
    print(f"\n{'='*115}")
    print("전체 Top 20 (Sharpe)")
    print(f"{'='*115}")
    top20s = sorted(results, key=lambda x: -x['sharpe'])[:20]
    for i, r in enumerate(top20s, 1):
        print(f"{i:<3}{r['pos']:>14}{r['period']:>6}{r['e']:>3}{r['x']:>4}{r['s']:>3}"
              f"{r['nt']:>5}{r['pt']:>5}{r['str']:>6}"
              f"{r['ret']:>+6.1f}%{r['mdd']:>+6.1f}%{r['sharpe']:>6.2f}{r['n']:>4}{r['wr']:>5.0f}%")

    # 현행 대비 개선 비율
    bl_ret = bl_v72[0]['ret'] if bl_v72 else 0
    improved = len([r for r in results if r['pos']!='none' and r['ret'] > bl_ret])
    print(f"\n현행({bl_ret:+.1f}%) 대비 개선: {improved}/{total} ({improved/total*100:.1f}%)")

    import pickle
    with open('gridsearch_internal_results.pkl', 'wb') as f:
        pickle.dump(results, f)
    print("[저장] gridsearch_internal_results.pkl")


if __name__ == '__main__':
    main()
