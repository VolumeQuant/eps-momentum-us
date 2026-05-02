"""baseline vs opt2 세부 분석.

질문:
  1. 매일 Top 3 (part2_rank<=3) 매수 후보가 어떻게 다른가?
  2. 차이 종목들의 부호 케이스는? (저평가+가속/고평가+둔화/기타)
  3. 차이 종목들이 실제로 다음날 수익을 냈나?
"""
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'sign_aware_dbs'

SEG_CAP = 100


def fmt_segments(nc, n7, n30, n60, n90):
    if not all(x is not None and x != 0 for x in (n7, n30, n60, n90)):
        return None
    s1 = max(-SEG_CAP, min(SEG_CAP, (nc - n7) / abs(n7) * 100))
    s2 = max(-SEG_CAP, min(SEG_CAP, (n7 - n30) / abs(n30) * 100))
    s3 = max(-SEG_CAP, min(SEG_CAP, (n30 - n60) / abs(n60) * 100))
    s4 = max(-SEG_CAP, min(SEG_CAP, (n60 - n90) / abs(n90) * 100))
    return s1, s2, s3, s4


def get_picks(db_path, top_n=3):
    """매일 part2_rank Top N picks. 부호 정보도 함께."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute(f'''
        SELECT date, ticker, part2_rank, adj_gap, ntm_current, ntm_7d, ntm_30d, ntm_60d, ntm_90d, price
        FROM ntm_screening
        WHERE part2_rank IS NOT NULL AND part2_rank <= {top_n}
        ORDER BY date, part2_rank
    ''').fetchall()
    picks = defaultdict(list)
    info = {}
    for d, tk, p2, ag, nc, n7, n30, n60, n90, pr in rows:
        picks[d].append(tk)
        # adj_gap 부호
        ag_sign = '음(저평가)' if ag and ag < 0 else '양(고평가)' if ag and ag > 0 else '0'
        # direction 부호 (segments 기반)
        segs = fmt_segments(nc, n7, n30, n60, n90)
        dir_sign = '?'
        if segs:
            direction = (segs[0]+segs[1])/2 - (segs[2]+segs[3])/2
            dir_sign = '양(가속)' if direction > 0 else '음(둔화)' if direction < 0 else '0'
        # 4가지 case 분류 (adj_gap에는 fwd_pe_chg × dir × eps_q 인데 부호는 fwd_pe_chg와 거의 같음)
        case = '?'
        if ag and ag < 0 and 'eq(가속)' not in dir_sign:
            if '양' in dir_sign: case = 'C1: 저평가+가속(좋음)'
            elif '음' in dir_sign: case = 'C2: 저평가+둔화(혼합)'
        elif ag and ag > 0:
            if '양' in dir_sign: case = 'C3: 고평가+가속(혼합)'
            elif '음' in dir_sign: case = 'C4: 고평가+둔화(나쁨, buggy 영역)'
        info[(d, tk)] = {'p2': p2, 'ag': ag, 'ag_sign': ag_sign, 'dir_sign': dir_sign,
                         'case': case, 'price': pr}
    # 가격 (전 종목)
    prices = {}
    for r in cur.execute('SELECT date, ticker, price FROM ntm_screening WHERE price IS NOT NULL').fetchall():
        d, tk, p = r
        if p and p > 0:
            prices[(d, tk)] = p
    conn.close()
    return picks, info, prices


def compare_picks(p_a, p_b, info_a, info_b, prices):
    """매일 Top 3 비교 + 차이 종목 트랙."""
    dates = sorted(set(p_a.keys()) | set(p_b.keys()))
    only_a = []  # baseline만
    only_b = []  # opt2만
    common = 0
    total = 0
    next_returns = {'baseline_only': [], 'opt2_only': []}

    for i in range(len(dates) - 1):
        d, dn = dates[i], dates[i+1]
        a_set = set(p_a.get(d, []))
        b_set = set(p_b.get(d, []))
        common += len(a_set & b_set)
        total += len(a_set | b_set)
        for tk in (a_set - b_set):
            inf = info_a.get((d, tk), {})
            pt, pn = prices.get((d, tk)), prices.get((dn, tk))
            ret = (pn / pt - 1) * 100 if pt and pn else None
            only_a.append({'date': d, 'tk': tk, 'p2': inf.get('p2'),
                           'case': inf.get('case'), 'ag_sign': inf.get('ag_sign'),
                           'dir_sign': inf.get('dir_sign'), 'next_ret': ret})
            if ret is not None:
                next_returns['baseline_only'].append(ret)
        for tk in (b_set - a_set):
            inf = info_b.get((d, tk), {})
            pt, pn = prices.get((d, tk)), prices.get((dn, tk))
            ret = (pn / pt - 1) * 100 if pt and pn else None
            only_b.append({'date': d, 'tk': tk, 'p2': inf.get('p2'),
                           'case': inf.get('case'), 'ag_sign': inf.get('ag_sign'),
                           'dir_sign': inf.get('dir_sign'), 'next_ret': ret})
            if ret is not None:
                next_returns['opt2_only'].append(ret)

    return only_a, only_b, common, total, next_returns


def main():
    print('=' * 100)
    print('baseline vs opt2 매수 후보 세부 분석')
    print('=' * 100)

    p_base, info_base, prices = get_picks(GRID / 'baseline.db', top_n=3)
    p_opt2, info_opt2, _ = get_picks(GRID / 'opt2.db', top_n=3)

    only_a, only_b, common, total, next_rets = compare_picks(p_base, p_opt2, info_base, info_opt2, prices)
    print(f'\n공통 매수 후보: {common}/{total} ({common*100/total:.1f}%)')
    print(f'baseline만 매수한 종목-일자: {len(only_a)}건')
    print(f'opt2만 매수한 종목-일자: {len(only_b)}건')

    # baseline-only 차이 종목 — 케이스 분포
    print(f'\n[baseline만 매수 — 차이 종목 ({len(only_a)}건)] 케이스 분포:')
    case_count = defaultdict(int)
    case_returns = defaultdict(list)
    for x in only_a:
        case_count[x['case']] += 1
        if x['next_ret'] is not None:
            case_returns[x['case']].append(x['next_ret'])
    for case in sorted(case_count.keys()):
        rets = case_returns[case]
        avg_r = sum(rets)/len(rets) if rets else 0
        wins = sum(1 for r in rets if r > 0)
        print(f'  {case}: {case_count[case]}건, 평균수익 {avg_r:+.2f}%, 승률 {wins*100/len(rets):.0f}% (n={len(rets)})')

    print(f'\n[opt2만 매수 — 차이 종목 ({len(only_b)}건)] 케이스 분포:')
    case_count = defaultdict(int)
    case_returns = defaultdict(list)
    for x in only_b:
        case_count[x['case']] += 1
        if x['next_ret'] is not None:
            case_returns[x['case']].append(x['next_ret'])
    for case in sorted(case_count.keys()):
        rets = case_returns[case]
        avg_r = sum(rets)/len(rets) if rets else 0
        wins = sum(1 for r in rets if r > 0)
        print(f'  {case}: {case_count[case]}건, 평균수익 {avg_r:+.2f}%, 승률 {wins*100/len(rets):.0f}% (n={len(rets)})')

    # 평균 수익 비교
    print()
    print('=' * 100)
    print('차이 종목 평균 next-day 수익')
    print('=' * 100)
    for label, rets in next_rets.items():
        if rets:
            avg = sum(rets)/len(rets)
            wins = sum(1 for r in rets if r > 0)
            print(f'  {label}: 평균 {avg:+.2f}%, 승률 {wins*100/len(rets):.0f}% (n={len(rets)})')

    # 핵심 — baseline만 매수한 "C4 고평가+둔화 buggy 영역" 종목들 next-day 수익
    print()
    print('=' * 100)
    print('🔍 baseline만 매수한 "C4 고평가+둔화" buggy 종목 (사용자 지적 케이스)')
    print('=' * 100)
    c4 = [x for x in only_a if 'C4' in (x['case'] or '')]
    if c4:
        for x in c4[:20]:
            ret_s = f'{x["next_ret"]:+.2f}%' if x['next_ret'] is not None else 'N/A'
            print(f'  {x["date"]} {x["tk"]:<6} p2={x["p2"]} {x["ag_sign"]} {x["dir_sign"]} → next {ret_s}')
        rets = [x['next_ret'] for x in c4 if x['next_ret'] is not None]
        wins = sum(1 for r in rets if r > 0)
        print(f'\n  C4 케이스 총 {len(c4)}건, 평균 {sum(rets)/len(rets):+.2f}%, 승률 {wins*100/len(rets):.0f}%')
    else:
        print('  baseline만 매수한 C4 종목 없음 — 사용자 가설과 다름')

    # 추가: 매일 Top 3 종목 자체 비교 (처음 10일)
    print()
    print('=' * 100)
    print('일자별 Top 3 비교 (처음 10일)')
    print('=' * 100)
    dates = sorted(set(p_base.keys()) | set(p_opt2.keys()))
    for d in dates[:10]:
        a, b = p_base.get(d, []), p_opt2.get(d, [])
        diff = '✓동일' if a == b else f'⚠diff'
        print(f'  {d}: base={a}, opt2={b} {diff}')


if __name__ == '__main__':
    main()
