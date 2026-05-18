"""MA20 vs production churn 비교 — 사용자 우려 검증

질문: "어제 3등이었는데 다음날 사라지는 경우" 빈도가 얼마인가?
가설: MA20은 단기 평균이라 가격 cross 빈발 → churn 폭증 가능성

측정:
1. 어제 part2_rank 있던 종목이 오늘 NULL이 된 비율 (전체 churn rate)
2. 어제 Top 10 → 오늘 NULL (Watchlist 이탈) 비율
3. 어제 Top 3 → 오늘 NULL (매수 후보 이탈) 비율
4. 일별 Top 30 변동 종목 수
"""
import sys
import sqlite3
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'ma_filter_dbs'

DBS = [
    ('current (MA120+fallback)', GRID / 'ext_current.db'),
    ('v81 (MA20)', GRID / 'ext_ma20.db'),
]


def analyze(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    # 일별 Top 30 set
    top30_by_date = {}
    rank_by_date = {}  # {date: {ticker: rank}}
    for d in dates:
        rows = cur.execute(
            'SELECT ticker, part2_rank FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL',
            (d,)
        ).fetchall()
        top30_by_date[d] = {tk for tk, _ in rows}
        rank_by_date[d] = {tk: r for tk, r in rows}

    # 전일 → 오늘 churn
    total_in_yesterday = 0
    out_today_total = 0
    out_today_top10 = 0  # 어제 Top 10 → 오늘 NULL
    out_today_top3 = 0   # 어제 Top 3 → 오늘 NULL
    yesterday_top10 = 0
    yesterday_top3 = 0
    huge_rank_jump = 0  # 어제 Top 3 → 오늘 Top 20+
    huge_rank_jump_examples = []

    for i in range(1, len(dates)):
        prev = dates[i-1]
        today = dates[i]
        prev_set = top30_by_date[prev]
        today_set = top30_by_date[today]
        prev_ranks = rank_by_date[prev]
        today_ranks = rank_by_date[today]

        # 어제 있던 종목이 오늘 사라진 경우
        out_today = prev_set - today_set
        out_today_total += len(out_today)
        total_in_yesterday += len(prev_set)

        for tk in out_today:
            rk = prev_ranks[tk]
            if rk <= 10:
                out_today_top10 += 1
            if rk <= 3:
                out_today_top3 += 1

        # 어제 Top 3 → 오늘 Top 20+ 또는 NULL (큰 rank 점프)
        for tk in prev_set:
            if prev_ranks[tk] <= 3:
                yesterday_top3 += 1
                today_rk = today_ranks.get(tk)
                if today_rk is None or today_rk >= 20:
                    huge_rank_jump += 1
                    huge_rank_jump_examples.append(
                        (today, tk, prev_ranks[tk], today_rk)
                    )

        for tk in prev_set:
            if prev_ranks[tk] <= 10:
                yesterday_top10 += 1

    conn.close()

    return {
        'n_days': len(dates),
        'total_in_yesterday': total_in_yesterday,
        'out_today_total': out_today_total,
        'out_today_top10': out_today_top10,
        'out_today_top3': out_today_top3,
        'yesterday_top10': yesterday_top10,
        'yesterday_top3': yesterday_top3,
        'huge_rank_jump': huge_rank_jump,
        'huge_rank_jump_examples': huge_rank_jump_examples,
    }


def main():
    print('=' * 100)
    print('MA20 vs production churn 비교 (Top 30 일별 변동)')
    print('=' * 100)

    all_results = {}
    for label, db_path in DBS:
        if not db_path.exists():
            continue
        print(f'\n[{label}]')
        r = analyze(db_path)
        all_results[label] = r
        print(f'  일수: {r["n_days"]}일')
        print(f'  전일 Top 30 → 오늘 NULL: {r["out_today_total"]}/{r["total_in_yesterday"]} = '
              f'{r["out_today_total"]/r["total_in_yesterday"]*100:.1f}%')
        print(f'  전일 Top 10 → 오늘 NULL: {r["out_today_top10"]}/{r["yesterday_top10"]} = '
              f'{r["out_today_top10"]/r["yesterday_top10"]*100:.1f}%')
        print(f'  전일 Top 3 → 오늘 NULL: {r["out_today_top3"]}/{r["yesterday_top3"]} = '
              f'{r["out_today_top3"]/r["yesterday_top3"]*100:.1f}%')
        print(f'  전일 Top 3 → 오늘 Top 20+ 또는 NULL: {r["huge_rank_jump"]}/{r["yesterday_top3"]} = '
              f'{r["huge_rank_jump"]/r["yesterday_top3"]*100:.1f}%')

    # 비교
    print()
    print('=' * 100)
    print('차이 — MA20 적용 시 churn 증가/감소?')
    print('=' * 100)
    cur_r = all_results.get('current (MA120+fallback)')
    new_r = all_results.get('v81 (MA20)')
    if cur_r and new_r:
        for metric, denom in [
            ('out_today_total', 'total_in_yesterday'),
            ('out_today_top10', 'yesterday_top10'),
            ('out_today_top3', 'yesterday_top3'),
            ('huge_rank_jump', 'yesterday_top3'),
        ]:
            cur_pct = cur_r[metric] / cur_r[denom] * 100
            new_pct = new_r[metric] / new_r[denom] * 100
            delta = new_pct - cur_pct
            label = {
                'out_today_total': '전일 Top 30 → NULL',
                'out_today_top10': '전일 Top 10 → NULL',
                'out_today_top3': '전일 Top 3 → NULL (매수 후보 갑자기 사라짐)',
                'huge_rank_jump': '전일 Top 3 → 오늘 Top 20+ / NULL (큰 점프)',
            }[metric]
            print(f'  {label:<55} {cur_pct:6.1f}% → {new_pct:6.1f}% (Δ {delta:+.1f}%p)')

    # 사용자 우려 사례: 어제 Top 3 → 오늘 NULL/Top 20+
    print()
    print('=' * 100)
    print('v81 (MA20)에서 "어제 Top 3 → 오늘 사라짐/큰 점프" 사례')
    print('=' * 100)
    if new_r and new_r['huge_rank_jump_examples']:
        for d, tk, prev_rk, today_rk in new_r['huge_rank_jump_examples'][:20]:
            today_label = 'NULL' if today_rk is None else f'{today_rk}위'
            print(f'  {d}: {tk:<6} 전일 {prev_rk}위 → 오늘 {today_label}')
        if len(new_r['huge_rank_jump_examples']) > 20:
            print(f'  ... 외 {len(new_r["huge_rank_jump_examples"])-20}건')


if __name__ == '__main__':
    main()
