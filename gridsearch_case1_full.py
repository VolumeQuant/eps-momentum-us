"""
Case 1 보너스 전체 탐색 — gridsearch_v73 정교도
방식 4가지 × 기간 5가지 × 파라미터 넓은 범위

방식:
  D. rank 보정 (가중순위 N칸 올림)
  A. 필터 (Case 1 아닌 종목 진입 제외)
  E. 슬롯 예약 (슬롯 중 1개 반드시 Case 1)
  F. 이탈 방어 (Case 1이면 이탈 기준 N칸 유예)

기간: 7d / 30d / 60d / 90d / blend(시스템식)
임계값: NTM × PX 그리드
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

    # 과거 가격 캐시
    all_prices = {}
    for d in dates:
        rows = cursor.execute('SELECT ticker, price FROM ntm_screening WHERE date=? AND price>0', (d,)).fetchall()
        all_prices[d] = {r[0]: r[1] for r in rows}

    # 각 날짜의 N일 전 날짜 매핑
    def get_past_date(d, days):
        target = (datetime.strptime(d, '%Y-%m-%d') - timedelta(days=days)).strftime('%Y-%m-%d')
        r = cursor.execute('SELECT MAX(date) FROM ntm_screening WHERE date <= ?', (target,)).fetchone()
        return r[0] if r and r[0] else None

    data = {}
    for d in dates:
        rows = cursor.execute('''
            SELECT ticker, part2_rank, price, composite_rank,
                   ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()

        past_dates = {p: get_past_date(d, p) for p in [7, 30, 60, 90]}
        past_prices = {p: all_prices.get(pd, {}) if pd else {} for p, pd in past_dates.items()}

        data[d] = {'_meta': True}
        for r in rows:
            nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[4:9])
            segs = []
            for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
                if b and abs(b) > 0.01:
                    segs.append(max(-100, min(100, (a - b) / abs(b) * 100)))
                else:
                    segs.append(0)

            price_now = r[2] or 0
            ntm_map = {'7d': n7, '30d': n30, '60d': n60, '90d': n90}
            chg = {}
            for period, nval in ntm_map.items():
                days = int(period.replace('d', ''))
                chg[f'ntm_{period}'] = ((nc - nval) / nval * 100) if nval and abs(nval) > 0.01 else 0
                pp = past_prices.get(days, {}).get(r[0])
                chg[f'px_{period}'] = ((price_now - pp) / pp * 100) if pp and pp > 0 and price_now else 0

            # 블렌딩 gap
            blend_gap = 0
            blend_w = 0
            for period, w in [('7d', 0.4), ('30d', 0.3), ('60d', 0.2), ('90d', 0.1)]:
                g = chg.get(f'ntm_{period}', 0) - chg.get(f'px_{period}', 0)
                blend_gap += w * g
                blend_w += w
            if blend_w > 0:
                blend_gap /= blend_w

            data[d][r[0]] = {
                'p2': r[1], 'price': price_now, 'comp_rank': r[3],
                'min_seg': min(segs), **chg, 'blend_gap': blend_gap,
            }
    conn.close()
    return dates, data


def is_case1(ticker_data, period, ntm_thr, px_thr):
    if period == 'blend':
        return ticker_data.get('blend_gap', 0) > ntm_thr  # blend는 gap > thr
    ntm_chg = ticker_data.get(f'ntm_{period}', 0)
    px_chg = ticker_data.get(f'px_{period}', 0)
    return ntm_chg > ntm_thr and px_chg < px_thr


def simulate(dates, data, entry_top, exit_top, max_slots,
             method='none', period=None, ntm_thr=0, px_thr=0,
             rank_bonus=0, exit_defense=0, start_idx=0):
    portfolio = {}
    daily_returns = []
    trades = []
    consecutive = defaultdict(int)

    for di in range(start_idx, len(dates)):
        today = dates[di]
        td = data.get(today, {})
        rank_map = {tk: v['p2'] for tk, v in td.items()
                    if isinstance(v, dict) and v.get('p2') is not None}

        # Case 1 종목
        c1 = set()
        if method != 'none' and period:
            for tk, v in td.items():
                if isinstance(v, dict) and 'p2' in v:
                    if is_case1(v, period, ntm_thr, px_thr):
                        c1.add(tk)

        # consecutive 업데이트
        today_ranked = set(rank_map.keys())
        new_con = defaultdict(int)
        for tk in today_ranked:
            if rank_map.get(tk, 999) <= 30:
                new_con[tk] = consecutive.get(tk, 0) + 1
        consecutive = new_con

        # 이탈
        exited = []
        for tk in list(portfolio.keys()):
            rank = rank_map.get(tk)
            ms = td.get(tk, {}).get('min_seg', 0) if isinstance(td.get(tk), dict) else 0
            price = td.get(tk, {}).get('price') if isinstance(td.get(tk), dict) else None
            eff_exit = exit_top + exit_defense if (method in ('D', 'F', 'combined') and tk in c1) else exit_top
            should_exit = (rank is None or rank > eff_exit) or ms < -2
            if should_exit and price:
                ret = (price - portfolio[tk]) / portfolio[tk] * 100
                trades.append(ret)
                exited.append(tk)
        for tk in exited:
            del portfolio[tk]

        # 진입
        vacancies = max_slots - len(portfolio)

        if method == 'E' and vacancies > 0:
            # 슬롯 예약: 빈 슬롯 1개는 Case 1 최상위
            c1_cands = []
            for tk, r in sorted(rank_map.items(), key=lambda x: x[1]):
                if tk in portfolio or consecutive.get(tk, 0) < 3: continue
                ms = td.get(tk, {}).get('min_seg', 0) if isinstance(td.get(tk), dict) else 0
                if ms < 0: continue
                price = td.get(tk, {}).get('price') if isinstance(td.get(tk), dict) else None
                if not price or price <= 0: continue
                if tk in c1 and r <= 20:  # Case 1이고 Top 20 안
                    c1_cands.append((tk, price, r))
            # Case 1 종목 1개 먼저 넣기
            if c1_cands and vacancies > 0:
                best_c1 = c1_cands[0]
                if best_c1[2] <= entry_top + 10:  # 너무 낮으면 배제
                    portfolio[best_c1[0]] = best_c1[1]
                    vacancies -= 1

        # 진입
        if vacancies > 0:
            if method == 'G':
                # G: Case 1 우선, 남은 슬롯 기존 순위
                c1_cands = []
                other_cands = []
                for tk, r in sorted(rank_map.items(), key=lambda x: x[1]):
                    if tk in portfolio: continue
                    if consecutive.get(tk, 0) < 3: continue
                    ms = td.get(tk, {}).get('min_seg', 0) if isinstance(td.get(tk), dict) else 0
                    if ms < 0: continue
                    price = td.get(tk, {}).get('price') if isinstance(td.get(tk), dict) else None
                    if not price or price <= 0: continue
                    if tk in c1 and r <= entry_top + 10:
                        c1_cands.append((tk, price, r))
                    elif r <= entry_top:
                        other_cands.append((tk, price, r))
                for tk, price, _ in c1_cands[:vacancies]:
                    portfolio[tk] = price
                    vacancies -= 1
                for tk, price, _ in other_cands[:vacancies]:
                    portfolio[tk] = price
                    vacancies -= 1
            else:
                cands = []
                for tk, r in sorted(rank_map.items(), key=lambda x: x[1]):
                    if tk in portfolio: continue
                    if consecutive.get(tk, 0) < 3: continue
                    ms = td.get(tk, {}).get('min_seg', 0) if isinstance(td.get(tk), dict) else 0
                    if ms < 0: continue
                    price = td.get(tk, {}).get('price') if isinstance(td.get(tk), dict) else None
                    if not price or price <= 0: continue

                    if method == 'A':
                        if tk not in c1: continue
                        eff_rank = r
                    elif method == 'D' or method == 'combined':
                        eff_rank = max(1, r - rank_bonus) if tk in c1 else r
                    else:
                        eff_rank = r

                    if eff_rank <= entry_top:
                        cands.append((tk, price, eff_rank))

                cands.sort(key=lambda x: x[2])
                for tk, price, _ in cands[:vacancies]:
                    portfolio[tk] = price

        # 일별 수익률
        if portfolio:
            day_ret = 0
            for tk in portfolio:
                price = td.get(tk, {}).get('price') if isinstance(td.get(tk), dict) else None
                if price and di > 0:
                    prev = data.get(dates[di-1], {})
                    pp = prev.get(tk, {}).get('price') if isinstance(prev.get(tk), dict) else None
                    if pp and pp > 0:
                        day_ret += (price - pp) / pp * 100
            day_ret /= len(portfolio)
            daily_returns.append(day_ret)
        else:
            daily_returns.append(0)

    # 잔여 청산
    if portfolio:
        last = data.get(dates[-1], {})
        for tk in list(portfolio.keys()):
            price = last.get(tk, {}).get('price') if isinstance(last.get(tk), dict) else None
            if price:
                trades.append((price - portfolio[tk]) / portfolio[tk] * 100)

    cum = 1.0; peak = 1.0; mdd = 0
    for dr in daily_returns:
        cum *= (1 + dr/100); peak = max(peak, cum)
        mdd = min(mdd, (cum-peak)/peak*100)
    ret = (cum-1)*100
    n = len(trades)
    wr = (sum(1 for t in trades if t > 0) / n * 100) if n else 0
    dr_arr = np.array(daily_returns) if daily_returns else np.array([0])
    sharpe = (dr_arr.mean()/dr_arr.std()*np.sqrt(252)) if dr_arr.std() > 0 else 0
    return {'ret': round(ret,2), 'mdd': round(mdd,2), 'n': n,
            'wr': round(wr,1), 'sharpe': round(sharpe,2)}


def main():
    print("Loading...")
    dates, data = load_data()
    print(f"Period: {dates[0]}~{dates[-1]} ({len(dates)} days)")

    # 임계값 (기간별 스케일)
    THRESHOLDS = {
        '7d':  [(1.0, -1.0), (1.0, -2.0), (1.5, -2.0), (2.0, -2.0), (2.0, -3.0), (3.0, -3.0)],
        '30d': [(2.0, -2.0), (3.0, -3.0), (5.0, -5.0), (5.0, -3.0), (3.0, -5.0), (8.0, -5.0)],
        '60d': [(3.0, -3.0), (5.0, -5.0), (8.0, -8.0), (5.0, -3.0), (10.0, -5.0), (10.0, -10.0)],
        '90d': [(5.0, -5.0), (8.0, -8.0), (10.0, -10.0), (5.0, -3.0), (15.0, -10.0)],
        'blend': [(3.0, 0), (5.0, 0), (8.0, 0), (10.0, 0), (15.0, 0)],  # blend는 gap > thr만
    }

    ENTRY = [2, 3, 4, 5, 7]
    EXIT = [8, 10, 12, 15]
    SLOTS = [3, 5]
    RANK_BONUS = [0, 1, 2, 3, 5, 7, 10, 15]
    EXIT_DEF = [0, 1, 2, 3, 5, 7, 10]
    STARTS = [0, 3, 5, 7, 10]

    results = []
    total = 0

    # baseline
    for e in ENTRY:
        for x in EXIT:
            if x <= e: continue
            for s in SLOTS:
                multi = [simulate(dates, data, e, x, s, start_idx=si)['ret'] for si in STARTS]
                full = simulate(dates, data, e, x, s)
                results.append({'method':'none', 'period':'-', 'ntm_thr':0, 'px_thr':0,
                               'rb':0, 'ed':0, 'e':e, 'x':x, 's':s,
                               **full, 'multi': round(np.mean(multi),2)})
                total += 1

    # 방식 D (rank 보정) + F (이탈 방어) 통합
    for period, thrs in THRESHOLDS.items():
        for nt, pt in thrs:
            for rb in RANK_BONUS:
                for ed in EXIT_DEF:
                    for e in ENTRY:
                        for x in EXIT:
                            if x <= e: continue
                            for s in SLOTS:
                                full = simulate(dates, data, e, x, s, method='combined',
                                              period=period, ntm_thr=nt, px_thr=pt,
                                              rank_bonus=rb, exit_defense=ed)
                                results.append({
                                    'method':'D+F', 'period':period,
                                    'ntm_thr':nt, 'px_thr':pt, 'rb':rb, 'ed':ed,
                                    'e':e, 'x':x, 's':s, **full, 'multi':0
                                })
                                total += 1
                                if total % 2000 == 0:
                                    print(f"  {total} combos...")

    # 방식 A (필터)
    for period, thrs in THRESHOLDS.items():
        for nt, pt in thrs:
            for e in ENTRY:
                for x in EXIT:
                    if x <= e: continue
                    for s in SLOTS:
                        full = simulate(dates, data, e, x, s, method='A',
                                       period=period, ntm_thr=nt, px_thr=pt)
                        results.append({
                            'method':'A_filter', 'period':period,
                            'ntm_thr':nt, 'px_thr':pt, 'rb':0, 'ed':0,
                            'e':e, 'x':x, 's':s, **full, 'multi':0
                        })
                        total += 1

    # 방식 E (슬롯 예약)
    for period, thrs in THRESHOLDS.items():
        for nt, pt in thrs:
            for e in ENTRY:
                for x in EXIT:
                    if x <= e: continue
                    for s in SLOTS:
                        full = simulate(dates, data, e, x, s, method='E',
                                       period=period, ntm_thr=nt, px_thr=pt)
                        results.append({
                            'method':'E_slot', 'period':period,
                            'ntm_thr':nt, 'px_thr':pt, 'rb':0, 'ed':0,
                            'e':e, 'x':x, 's':s, **full, 'multi':0
                        })
                        total += 1

    # 방식 G (우선순위: Case 1 먼저 진입, 남은 슬롯은 기존)
    for period, thrs in THRESHOLDS.items():
        for nt, pt in thrs:
            for ed in EXIT_DEF:
                for e in ENTRY:
                    for x in EXIT:
                        if x <= e: continue
                        for s in SLOTS:
                            full = simulate(dates, data, e, x, s, method='G',
                                           period=period, ntm_thr=nt, px_thr=pt,
                                           exit_defense=ed)
                            results.append({
                                'method':'G_prio', 'period':period,
                                'ntm_thr':nt, 'px_thr':pt, 'rb':0, 'ed':ed,
                                'e':e, 'x':x, 's':s, **full, 'multi':0
                            })
                            total += 1
                            if total % 5000 == 0:
                                print(f"  {total} combos...")

    # 방식 A+F (필터 + 이탈 방어)
    for period, thrs in THRESHOLDS.items():
        for nt, pt in thrs:
            for ed in [2, 3, 5, 7]:
                for e in ENTRY:
                    for x in EXIT:
                        if x <= e: continue
                        for s in SLOTS:
                            full = simulate(dates, data, e, x, s, method='A',
                                           period=period, ntm_thr=nt, px_thr=pt,
                                           exit_defense=ed)
                            results.append({
                                'method':'A+F', 'period':period,
                                'ntm_thr':nt, 'px_thr':pt, 'rb':0, 'ed':ed,
                                'e':e, 'x':x, 's':s, **full, 'multi':0
                            })
                            total += 1

    print(f"\n총 {total} 조합 완료")

    # ── 결과 ──
    # baseline
    bl = [r for r in results if r['method']=='none']
    bl_best = max(bl, key=lambda x: x['ret'])
    bl_v72 = [r for r in bl if r['e']==5 and r['x']==12 and r['s']==3]

    print(f"\n{'='*115}")
    print("Baseline (보너스 없음)")
    print(f"{'='*115}")
    if bl_v72:
        c = bl_v72[0]
        print(f"  현행 E5/X12/S3: ret {c['ret']:+.1f}%, MDD {c['mdd']:.1f}%, Sharpe {c['sharpe']:.2f}, n={c['n']}, WR {c['wr']:.0f}%")
    print(f"  최적: E{bl_best['e']}/X{bl_best['x']}/S{bl_best['s']}: ret {bl_best['ret']:+.1f}%, Sharpe {bl_best['sharpe']:.2f}")

    # 방식별 최적
    print(f"\n{'='*115}")
    print("방식별 최적 (수익률 기준)")
    print(f"{'='*115}")
    for m in ['none', 'D+F', 'A_filter', 'A+F', 'E_slot', 'G_prio']:
        sub = [r for r in results if r['method']==m]
        if not sub: continue
        best = max(sub, key=lambda x: x['ret'])
        marker = '★' if best['ret'] == max(r['ret'] for r in results) else ' '
        p = f"P{best['period']}" if best['period'] != '-' else ''
        thr = f"N{best['ntm_thr']}/P{best['px_thr']}" if best['ntm_thr'] else ''
        bonus = f"RB{best['rb']}/ED{best['ed']}" if best['rb'] or best['ed'] else ''
        print(f"  {marker} {m:>10}: E{best['e']}/X{best['x']}/S{best['s']} {p} {thr} {bonus}"
              f" → ret {best['ret']:+.1f}%, MDD {best['mdd']:.1f}%, Sharpe {best['sharpe']:.2f}, n={best['n']}, WR {best['wr']:.0f}%")

    # 기간별 최적
    print(f"\n{'='*115}")
    print("기간별 최적 (D+F 방식, 수익률 기준)")
    print(f"{'='*115}")
    for period in ['7d', '30d', '60d', '90d', 'blend']:
        sub = [r for r in results if r['method']=='D+F' and r['period']==period]
        if not sub: continue
        best = max(sub, key=lambda x: x['ret'])
        print(f"  {period:>5}: E{best['e']}/X{best['x']}/S{best['s']} N{best['ntm_thr']}/P{best['px_thr']} RB{best['rb']}/ED{best['ed']}"
              f" → ret {best['ret']:+.1f}%, MDD {best['mdd']:.1f}%, Sharpe {best['sharpe']:.2f}, n={best['n']}")

    # 전체 Top 20
    print(f"\n{'='*115}")
    print("전체 Top 20 (수익률)")
    print(f"{'='*115}")
    top20 = sorted(results, key=lambda x: -x['ret'])[:20]
    print(f"{'#':<3}{'방식':>10}{'기간':>6}{'E':>3}{'X':>4}{'S':>3}{'NTM':>5}{'PX':>5}{'RB':>4}{'ED':>4}"
          f"{'ret':>8}{'MDD':>8}{'Shp':>7}{'n':>4}{'WR':>6}")
    for i, r in enumerate(top20, 1):
        print(f"{i:<3}{r['method']:>10}{r['period']:>6}{r['e']:>3}{r['x']:>4}{r['s']:>3}"
              f"{r['ntm_thr']:>5}{r['px_thr']:>5}{r['rb']:>4}{r['ed']:>4}"
              f"{r['ret']:>+6.1f}%{r['mdd']:>+6.1f}%{r['sharpe']:>6.2f}{r['n']:>4}{r['wr']:>5.0f}%")

    # Sharpe Top 20
    print(f"\n{'='*115}")
    print("전체 Top 20 (Sharpe)")
    print(f"{'='*115}")
    top20s = sorted(results, key=lambda x: -x['sharpe'])[:20]
    for i, r in enumerate(top20s, 1):
        print(f"{i:<3}{r['method']:>10}{r['period']:>6}{r['e']:>3}{r['x']:>4}{r['s']:>3}"
              f"{r['ntm_thr']:>5}{r['px_thr']:>5}{r['rb']:>4}{r['ed']:>4}"
              f"{r['ret']:>+6.1f}%{r['mdd']:>+6.1f}%{r['sharpe']:>6.2f}{r['n']:>4}{r['wr']:>5.0f}%")

    # baseline 대비 개선 통계
    bl_ret = bl_v72[0]['ret'] if bl_v72 else bl_best['ret']
    improved = [r for r in results if r['method']!='none' and r['ret'] > bl_ret]
    print(f"\n현행(E5/X12/S3 {bl_ret:+.1f}%) 대비 개선: {len(improved)}/{total} ({len(improved)/total*100:.1f}%)")

    import pickle
    with open('gridsearch_case1_full_results.pkl', 'wb') as f:
        pickle.dump(results, f)


if __name__ == '__main__':
    main()
