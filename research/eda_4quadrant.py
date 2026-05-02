"""4사분면 EDA — fwd_pe_chg vs direction 부호 조합별 next-day 수익률.

질문:
  C1 (fwd<0 + dir>0, 저평가+가속): 매수 후보로 진입했을 때 알파 있나?
  C2 (fwd<0 + dir<0, 저평가+둔화): 진짜 약화돼야 하나?
  C3 (fwd>0 + dir>0, 고평가+가속): 매수 후보에서 멀리 두는 게 맞나?
  C4 (fwd>0 + dir<0, 고평가+둔화): 사용자 지적 — 매도 강조 맞나?

대상:
  pre-γ baseline DB의 모든 (date, ticker) eligible 데이터.
  cap 발동 종목 제외 (fwd_pe_chg 역산 신뢰성).
  매일 다음 영업일 가격 → next-day return.

분석 단위:
  1. 사분면별 평균 next-day return + 승률 + 표본수
  2. part2_rank Top 30 안에서 사분면 분포
  3. part2_rank Top 3 (매수 후보) 안에서 사분면 분포
  4. 사분면별 평균 part2_rank
"""
import sqlite3
import sys
import statistics
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB = Path('/tmp/db_pre_gamma.db') if Path('/tmp/db_pre_gamma.db').exists() else ROOT / 'eps_momentum_data.db'

SEG_CAP = 100


def classify_4q(fwd_pe_chg, direction):
    """4사분면 분류"""
    if fwd_pe_chg < 0 and direction > 0:
        return 'C1_저평가+가속'
    if fwd_pe_chg < 0 and direction < 0:
        return 'C2_저평가+둔화'
    if fwd_pe_chg > 0 and direction > 0:
        return 'C3_고평가+가속'
    if fwd_pe_chg > 0 and direction < 0:
        return 'C4_고평가+둔화'
    return 'C0_zero'


def fmt_segments(nc, n7, n30, n60, n90):
    if not all(x is not None and x != 0 for x in (n7, n30, n60, n90)):
        return None
    segs_raw = (
        (nc - n7) / abs(n7) * 100,
        (n7 - n30) / abs(n30) * 100,
        (n30 - n60) / abs(n60) * 100,
        (n60 - n90) / abs(n90) * 100,
    )
    segs_capped = tuple(max(-SEG_CAP, min(SEG_CAP, s)) for s in segs_raw)
    return segs_raw, segs_capped


def main():
    print('=' * 100)
    print(f'4사분면 EDA — DB: {DB}')
    print('=' * 100)

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # 1) 모든 영업일 + 가격 (next-day return 계산용)
    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE composite_rank IS NOT NULL ORDER BY date'
    ).fetchall()]
    next_d = {dates[i]: dates[i+1] for i in range(len(dates)-1)}

    # 가격 매핑
    prices = {}
    for r in cur.execute('SELECT date, ticker, price FROM ntm_screening WHERE price IS NOT NULL').fetchall():
        d, tk, p = r
        if p and p > 0:
            prices[(d, tk)] = p

    # 2) 각 (date, ticker) 사분면 분류 + next-day return
    cases = defaultdict(list)  # case -> [{p2, ret, ag, dir}]
    cases_top30 = defaultdict(list)
    cases_top3 = defaultdict(list)

    skipped_cap = 0
    skipped_no_data = 0

    for d in dates:
        if d not in next_d:
            continue
        dn = next_d[d]
        rows = cur.execute('''
            SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d,
                   adj_gap, part2_rank
            FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL
        ''', (d,)).fetchall()
        for tk, nc, n7, n30, n60, n90, ag, p2 in rows:
            seg_pair = fmt_segments(nc, n7, n30, n60, n90)
            if seg_pair is None or ag is None:
                skipped_no_data += 1
                continue
            segs_raw, segs_capped = seg_pair
            cap_hit = any(abs(s) >= SEG_CAP for s in segs_capped)
            if cap_hit:
                skipped_cap += 1
                continue

            # baseline 가정 dir_factor + eps_q로 fwd_pe_chg 역산
            direction = (segs_capped[0] + segs_capped[1]) / 2 - (segs_capped[2] + segs_capped[3]) / 2
            df = max(-0.3, min(0.3, direction / 30))
            min_seg = min(segs_capped)
            eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
            denom = (1 + df) * eps_q
            if abs(denom) < 1e-6:
                continue
            fwd_pe_chg = ag / denom

            case = classify_4q(fwd_pe_chg, direction)
            if case == 'C0_zero':
                continue

            # next-day return
            pt, pn = prices.get((d, tk)), prices.get((dn, tk))
            ret = (pn / pt - 1) * 100 if pt and pn else None

            entry = {'p2': p2, 'ret': ret, 'ag': ag, 'dir': direction, 'fwd': fwd_pe_chg}
            cases[case].append(entry)
            if p2 is not None and p2 <= 30:
                cases_top30[case].append(entry)
            if p2 is not None and p2 <= 3:
                cases_top3[case].append(entry)

    print(f'\n총 (date, ticker) 분류: {sum(len(v) for v in cases.values())}건')
    print(f'  skipped (cap 발동): {skipped_cap}')
    print(f'  skipped (NTM 데이터 부족): {skipped_no_data}')

    # 3) 사분면별 통계
    def stats(name, data):
        rets = [d['ret'] for d in data if d['ret'] is not None]
        ags = [d['ag'] for d in data]
        dirs = [d['dir'] for d in data]
        p2s = [d['p2'] for d in data if d['p2'] is not None]
        n = len(data)
        if not rets:
            return f'{name}: N={n}, 데이터 부족'
        avg = sum(rets) / len(rets)
        med = sorted(rets)[len(rets)//2]
        std = statistics.pstdev(rets) if len(rets) > 1 else 0
        wins = sum(1 for r in rets if r > 0)
        wr = wins/len(rets)*100 if rets else 0
        avg_ag = sum(ags)/len(ags)
        avg_dir = sum(dirs)/len(dirs)
        avg_p2 = sum(p2s)/len(p2s) if p2s else None
        p2_str = f'avg p2 {avg_p2:.1f}' if avg_p2 else 'p2 X'
        return (f'{name}: N={n}, ret avg {avg:+.2f}%, med {med:+.2f}%, std {std:.2f}, '
                f'win {wr:.0f}%, avg_ag {avg_ag:+.2f}, avg_dir {avg_dir:+.1f}, {p2_str}')

    print()
    print('=' * 100)
    print('[전체 eligible 종목] 사분면별 next-day return')
    print('=' * 100)
    for c in sorted(cases.keys()):
        print(f'  {stats(c, cases[c])}')

    print()
    print('=' * 100)
    print('[part2_rank Top 30 (매수 모니터링)] 사분면별 next-day return')
    print('=' * 100)
    for c in sorted(cases_top30.keys()):
        print(f'  {stats(c, cases_top30[c])}')

    print()
    print('=' * 100)
    print('[part2_rank Top 3 (실제 매수 후보)] 사분면별 next-day return')
    print('=' * 100)
    for c in sorted(cases_top3.keys()):
        print(f'  {stats(c, cases_top3[c])}')

    # 4) 30일 holding return
    print()
    print('=' * 100)
    print('[part2_rank Top 30] 5/10/20일 보유 return')
    print('=' * 100)

    # 다시 수집 + 보유 기간 계산
    for hold in [5, 10, 20]:
        cases_hold = defaultdict(list)
        for d in dates:
            d_idx = dates.index(d)
            if d_idx + hold >= len(dates):
                continue
            d_exit = dates[d_idx + hold]
            rows = cur.execute('''
                SELECT ticker, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, adj_gap, part2_rank
                FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL AND part2_rank<=30
            ''', (d,)).fetchall()
            for tk, nc, n7, n30, n60, n90, ag, p2 in rows:
                sp = fmt_segments(nc, n7, n30, n60, n90)
                if sp is None or ag is None:
                    continue
                segs_raw, segs_capped = sp
                if any(abs(s) >= SEG_CAP for s in segs_capped):
                    continue
                direction = (segs_capped[0] + segs_capped[1]) / 2 - (segs_capped[2] + segs_capped[3]) / 2
                df = max(-0.3, min(0.3, direction / 30))
                min_seg = min(segs_capped)
                eps_q = 1.0 + 0.3 * max(-1, min(1, min_seg / 2))
                denom = (1 + df) * eps_q
                if abs(denom) < 1e-6:
                    continue
                fwd_pe_chg = ag / denom
                case = classify_4q(fwd_pe_chg, direction)
                if case == 'C0_zero':
                    continue
                pt, pe = prices.get((d, tk)), prices.get((d_exit, tk))
                if pt and pe:
                    ret = (pe/pt - 1) * 100
                    cases_hold[case].append(ret)

        print(f'\n  {hold}일 보유:')
        for c in sorted(cases_hold.keys()):
            rets = cases_hold[c]
            if rets:
                avg = sum(rets)/len(rets)
                wins = sum(1 for r in rets if r > 0)
                print(f'    {c}: N={len(rets)}, avg {avg:+.2f}%, win {wins/len(rets)*100:.0f}%')

    conn.close()


if __name__ == '__main__':
    main()
