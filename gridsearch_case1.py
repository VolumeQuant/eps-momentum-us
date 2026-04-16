"""
Case 1 보너스 정밀 검증 — gridsearch_v73 기반 (정교한 포트폴리오 시뮬)
- min_seg 필터, 3일 검증(consecutive≥3), 일별 수익률 추적
- 기간별(7d/30d) × 보너스 강도(순위 N칸 올림) × 진입/이탈/슬롯 통합 그리드
- 멀티스타트 (여러 시작일)
"""
import sqlite3
import numpy as np
import sys
from collections import defaultdict
from datetime import datetime, timedelta
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'eps_momentum_data.db'


def load_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    # 각 날짜별 가격 (과거 가격 조회용)
    all_prices = {}  # date → {ticker: price}
    for d in dates:
        rows = cursor.execute(
            'SELECT ticker, price FROM ntm_screening WHERE date=? AND price > 0', (d,)
        ).fetchall()
        all_prices[d] = {r[0]: r[1] for r in rows}

    data = {}
    for d in dates:
        rows = cursor.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        data[d] = {}

        # 7d/30d 전 가격 찾기
        for period_days in [7, 30]:
            target = (datetime.strptime(d, '%Y-%m-%d') - timedelta(days=period_days)).strftime('%Y-%m-%d')
            past_d = cursor.execute('SELECT MAX(date) FROM ntm_screening WHERE date <= ?', (target,)).fetchone()
            if past_d and past_d[0] and past_d[0] in all_prices:
                data[d][f'_px_{period_days}d'] = all_prices[past_d[0]]
            else:
                data[d][f'_px_{period_days}d'] = {}

        for r in rows:
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append((a - b) / abs(b) * 100)
                else:
                    segs.append(0)
            segs = [max(-100, min(100, s)) for s in segs]
            min_seg = min(segs) if segs else 0

            # NTM 변화율
            ntm_7d_chg = ((nc - n7) / n7 * 100) if n7 and abs(n7) > 0.01 else 0
            ntm_30d_chg = ((nc - n30) / n30 * 100) if n30 and abs(n30) > 0.01 else 0

            # 가격 변화율
            price_now = r[2]
            px_7d = data[d].get('_px_7d', {}).get(r[0])
            px_30d = data[d].get('_px_30d', {}).get(r[0])
            px_7d_chg = ((price_now - px_7d) / px_7d * 100) if px_7d and px_7d > 0 and price_now else 0
            px_30d_chg = ((price_now - px_30d) / px_30d * 100) if px_30d and px_30d > 0 and price_now else 0

            data[d][r[0]] = {
                'p2': r[1],
                'price': price_now,
                'comp_rank': r[3],
                'min_seg': min_seg,
                'ntm_7d_chg': ntm_7d_chg,
                'ntm_30d_chg': ntm_30d_chg,
                'px_7d_chg': px_7d_chg,
                'px_30d_chg': px_30d_chg,
            }
    conn.close()
    return dates, data


def simulate(dates, data, entry_top, exit_top, max_slots,
             case1_period=None, case1_ntm_thr=0, case1_px_thr=0,
             case1_entry_bonus=0, case1_exit_defense=0,
             start_idx=0):
    """
    case1_period: '7d' or '30d' or None
    case1_entry_bonus: 순위 N칸 올림
    case1_exit_defense: 이탈 기준 N칸 유예
    """
    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)

    for di in range(start_idx, len(dates)):
        today = dates[di]
        if today not in data:
            continue
        today_data = data[today]
        rank_map = {tk: v['p2'] for tk, v in today_data.items()
                    if isinstance(v, dict) and v.get('p2') is not None}

        # Case 1 판별
        case1_tickers = set()
        if case1_period:
            ntm_key = f'ntm_{case1_period}_chg'
            px_key = f'px_{case1_period}_chg'
            for tk, v in today_data.items():
                if not isinstance(v, dict) or 'p2' not in v:
                    continue
                if v.get(ntm_key, 0) > case1_ntm_thr and v.get(px_key, 0) < case1_px_thr:
                    case1_tickers.add(tk)

        # 보너스 적용된 rank
        bonus_rank_map = {}
        for tk, r in rank_map.items():
            if tk in case1_tickers and case1_entry_bonus > 0:
                bonus_rank_map[tk] = max(1, r - case1_entry_bonus)
            else:
                bonus_rank_map[tk] = r

        today_ranked = set(rank_map.keys())
        new_consecutive = defaultdict(int)
        for tk in today_ranked:
            if tk in rank_map and rank_map[tk] <= 30:
                new_consecutive[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_consecutive

        # 이탈
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            min_seg = today_data.get(tk, {}).get('min_seg', 0) if isinstance(today_data.get(tk), dict) else 0
            price = today_data.get(tk, {}).get('price') if isinstance(today_data.get(tk), dict) else None
            should_exit = False

            # Case 1 이탈 방어
            eff_exit = exit_top
            if tk in case1_tickers and case1_exit_defense > 0:
                eff_exit = exit_top + case1_exit_defense

            if rank is None or rank > eff_exit:
                should_exit = True
            if min_seg < -2:
                should_exit = True
            if should_exit and price:
                entry_price = portfolio[tk]
                ret = (price - entry_price) / entry_price * 100
                trades.append(ret)
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]

        # 진입 (보너스 적용된 순위로)
        vacancies = max_slots - len(portfolio)
        if vacancies > 0:
            candidates = []
            for tk, bonus_rank in sorted(bonus_rank_map.items(), key=lambda x: x[1]):
                if bonus_rank > entry_top:
                    break
                if tk in portfolio:
                    continue
                if consecutive.get(tk, 0) < 3:
                    continue
                min_seg = today_data.get(tk, {}).get('min_seg', 0) if isinstance(today_data.get(tk), dict) else 0
                if min_seg < 0:
                    continue
                price = today_data.get(tk, {}).get('price') if isinstance(today_data.get(tk), dict) else None
                if price and price > 0:
                    candidates.append((tk, price))
            for tk, price in candidates[:vacancies]:
                portfolio[tk] = price

        # 일별 수익률
        if portfolio:
            day_ret = 0
            for tk in portfolio:
                price = today_data.get(tk, {}).get('price') if isinstance(today_data.get(tk), dict) else None
                if price and di > 0:
                    prev_data = data.get(dates[di - 1], {})
                    prev_price = prev_data.get(tk, {}).get('price') if isinstance(prev_data.get(tk), dict) else None
                    if prev_price and prev_price > 0:
                        day_ret += (price - prev_price) / prev_price * 100
            if portfolio:
                day_ret /= len(portfolio)
            daily_returns.append(day_ret)
        else:
            daily_returns.append(0)

    cum_ret = 1.0
    peak = 1.0
    max_dd = 0
    for dr in daily_returns:
        cum_ret *= (1 + dr / 100)
        peak = max(peak, cum_ret)
        dd = (cum_ret - peak) / peak * 100
        max_dd = min(max_dd, dd)

    total_return = (cum_ret - 1) * 100
    n_trades = len(trades)
    win_rate = (sum(1 for t in trades if t > 0) / n_trades * 100) if n_trades > 0 else 0
    sharpe = 0
    if daily_returns:
        dr = np.array(daily_returns)
        if dr.std() > 0:
            sharpe = (dr.mean() / dr.std()) * np.sqrt(252)

    return {
        'total_return': round(total_return, 2),
        'max_dd': round(max_dd, 2),
        'n_trades': n_trades,
        'win_rate': round(win_rate, 1),
        'sharpe': round(sharpe, 2),
    }


def main():
    print("Loading data...")
    dates, data = load_data()
    print(f"Period: {dates[0]} ~ {dates[-1]} ({len(dates)} days)")

    # ── 그리드 ──
    ENTRY = [3, 5]
    EXIT = [8, 10, 12, 15]
    SLOTS = [3, 5]

    # Case 1 설정
    CONFIGS = [
        # (label, period, ntm_thr, px_thr, entry_bonus_list, exit_def_list)
        ('없음', None, 0, 0, [0], [0]),
        ('7d',  '7d',  1.5, -2.0, [0, 2, 5, 10], [0, 3]),
        ('30d', '30d', 3.0, -3.0, [0, 2, 5, 10], [0, 3]),
        ('30d_strict', '30d', 5.0, -5.0, [0, 2, 5, 10], [0, 3]),
        ('30d_loose', '30d', 2.0, -2.0, [0, 2, 5, 10], [0, 3]),
    ]

    # 멀티스타트
    START_OFFSETS = [0, 3, 5, 7, 10]

    results = []
    total = 0
    for label, period, ntm_thr, px_thr, eb_list, ed_list in CONFIGS:
        for e in ENTRY:
            for x in EXIT:
                if x <= e: continue
                for s in SLOTS:
                    for eb in eb_list:
                        for ed in ed_list:
                            multi_rets = []
                            for si in START_OFFSETS:
                                if si >= len(dates) - 10: continue
                                r = simulate(dates, data, e, x, s,
                                           case1_period=period,
                                           case1_ntm_thr=ntm_thr,
                                           case1_px_thr=px_thr,
                                           case1_entry_bonus=eb,
                                           case1_exit_defense=ed,
                                           start_idx=si)
                                multi_rets.append(r['total_return'])
                            avg_ret = np.mean(multi_rets) if multi_rets else 0
                            # 전체 기간 결과
                            full = simulate(dates, data, e, x, s,
                                          case1_period=period,
                                          case1_ntm_thr=ntm_thr,
                                          case1_px_thr=px_thr,
                                          case1_entry_bonus=eb,
                                          case1_exit_defense=ed)
                            results.append({
                                'label': label, 'e': e, 'x': x, 's': s,
                                'eb': eb, 'ed': ed,
                                'ret': full['total_return'],
                                'mdd': full['max_dd'],
                                'trades': full['n_trades'],
                                'wr': full['win_rate'],
                                'sharpe': full['sharpe'],
                                'multi_avg': round(avg_ret, 2),
                            })
                            total += 1
    print(f"\n총 {total} 조합 완료")

    # ── 기존(보너스 없음) 기준선 ──
    baseline = [r for r in results if r['label']=='없음']
    bl_best = max(baseline, key=lambda x: x['ret'])
    bl_v72 = [r for r in baseline if r['e']==5 and r['x']==12 and r['s']==3]

    print(f"\n{'='*100}")
    print(f"기존 (보너스 없음)")
    print(f"{'='*100}")
    print(f"  현행 E5/X12/S3: ret {bl_v72[0]['ret']:+.1f}%, MDD {bl_v72[0]['mdd']:.1f}%, "
          f"Sharpe {bl_v72[0]['sharpe']:.2f}, trades {bl_v72[0]['trades']}, WR {bl_v72[0]['wr']:.0f}%") if bl_v72 else None
    print(f"  최적: E{bl_best['e']}/X{bl_best['x']}/S{bl_best['s']}: ret {bl_best['ret']:+.1f}%, "
          f"MDD {bl_best['mdd']:.1f}%, Sharpe {bl_best['sharpe']:.2f}")

    # ── 기간별 최적 ──
    print(f"\n{'='*100}")
    print(f"기간별 최적 (전체 기간 수익률 기준)")
    print(f"{'='*100}")
    for label in ['없음', '7d', '30d', '30d_strict', '30d_loose']:
        sub = [r for r in results if r['label']==label]
        if not sub: continue
        best = max(sub, key=lambda x: x['ret'])
        marker = '★' if best['ret'] == max(r['ret'] for r in results) else ' '
        print(f"  {marker} {label:>12}: E{best['e']}/X{best['x']}/S{best['s']}/EB{best['eb']}/ED{best['ed']} "
              f"→ ret {best['ret']:+.1f}%, MDD {best['mdd']:.1f}%, Sharpe {best['sharpe']:.2f}, "
              f"trades {best['trades']}, WR {best['wr']:.0f}%, multi_avg {best['multi_avg']:+.1f}%")

    # ── 전체 Top 15 ──
    print(f"\n{'='*100}")
    print(f"전체 Top 15 (수익률 기준)")
    print(f"{'='*100}")
    top15 = sorted(results, key=lambda x: -x['ret'])[:15]
    print(f"{'#':<3}{'기간':>12}{'E':>3}{'X':>4}{'S':>3}{'EB':>4}{'ED':>4}"
          f"{'ret':>8}{'MDD':>8}{'Shp':>7}{'n':>4}{'WR':>6}{'multi':>8}")
    for i, r in enumerate(top15, 1):
        print(f"{i:<3}{r['label']:>12}{r['e']:>3}{r['x']:>4}{r['s']:>3}{r['eb']:>4}{r['ed']:>4}"
              f"{r['ret']:>+6.1f}%{r['mdd']:>+6.1f}%{r['sharpe']:>6.2f}{r['trades']:>4}"
              f"{r['wr']:>5.0f}%{r['multi_avg']:>+6.1f}%")

    # ── 동일 조건 비교 (E5/X12/S3 기준) ──
    print(f"\n{'='*100}")
    print(f"동일 조건 비교 (E5/X12/S3)")
    print(f"{'='*100}")
    for label in ['없음', '7d', '30d', '30d_strict', '30d_loose']:
        sub = [r for r in results if r['label']==label and r['e']==5 and r['x']==12 and r['s']==3]
        if not sub: continue
        print(f"\n  [{label}]")
        for r in sorted(sub, key=lambda x: -x['ret']):
            print(f"    EB{r['eb']}/ED{r['ed']}: ret {r['ret']:+.1f}%, MDD {r['mdd']:.1f}%, "
                  f"Sharpe {r['sharpe']:.2f}, WR {r['wr']:.0f}%")

    # ── Sharpe 기준 Top 10 ──
    print(f"\n{'='*100}")
    print(f"Sharpe 기준 Top 10")
    print(f"{'='*100}")
    top10s = sorted(results, key=lambda x: -x['sharpe'])[:10]
    for i, r in enumerate(top10s, 1):
        print(f"  {i}. {r['label']:>12} E{r['e']}/X{r['x']}/S{r['s']}/EB{r['eb']}/ED{r['ed']}: "
              f"Sharpe {r['sharpe']:.2f}, ret {r['ret']:+.1f}%, MDD {r['mdd']:.1f}%")


if __name__ == '__main__':
    main()
