"""MA20 lift가 single-event alpha (3/30 이란 전쟁) 의존인가 검증

가설 (퀀트 제기): MA20 +27.57%p lift가 3/30 ±5일 매크로 충격을 우연히 회피한
부산물일 수 있음. 진짜 알파라면 충격 외 기간에서도 동일 lift 재현돼야 함.

방법:
  시작일 grid를 3구간으로 나눠 paired 비교:
    Early (2/12 ~ 3/13): 충격 포함, 매수 후 충격 직격
    Mid   (3/16 ~ 3/27): 충격 전 진입, 충격 회피 모드 (MA20 알파 가설의 핵심)
    Late  (4/06 ~ 5/05): 충격 후 진입, 충격과 무관

각 구간 시작일별 paired (current vs ma20 vs ma50) 비교.
  - Early/Mid에서 lift 크고 Late에서 lift 작으면 → single-event alpha
  - 모든 구간에서 일관 lift → 진짜 알파
"""
import sys
import statistics
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
sys.path.insert(0, 'research')

import bt_breakout_hold as bth

ROOT = Path(__file__).parent.parent
GRID = ROOT / 'research' / 'ma_filter_dbs'

ENTRY_TOP = 3
EXIT_TOP = 10
MAX_SLOTS = 3
HOLD_DAYS = 0


def run_one(db_path, start_date):
    bth.DB_PATH = db_path
    dates, data, price_series = bth.load_data_ext()
    r = bth.simulate_hold(
        dates, data, price_series, hold_days=HOLD_DAYS,
        entry_top=ENTRY_TOP, exit_top=EXIT_TOP,
        max_slots=MAX_SLOTS, start_date=start_date
    )
    return r['total_return'], r['max_dd']


def main():
    print('=' * 100)
    print('MA20 lift event-robustness — 3/30 이란 전쟁 충격 의존도 검증')
    print('=' * 100)

    db_cur = GRID / 'ext_current.db'
    db_20 = GRID / 'ext_ma20.db'
    db_50 = GRID / 'ext_ma50.db'

    # 시작일 grid (DB에서 실제 거래일만 사용)
    bth.DB_PATH = db_cur
    all_dates, _, _ = bth.load_data_ext()

    # 3구간 정의
    EARLY = [d for d in all_dates if '2026-02-12' <= d <= '2026-03-13']
    MID   = [d for d in all_dates if '2026-03-16' <= d <= '2026-03-27']
    LATE  = [d for d in all_dates if '2026-04-06' <= d <= '2026-05-05']

    print(f'\nEarly (충격 포함): {len(EARLY)}일 — {EARLY[0]} ~ {EARLY[-1]}')
    print(f'Mid (충격 전 진입): {len(MID)}일 — {MID[0]} ~ {MID[-1]}')
    print(f'Late (충격 후): {len(LATE)}일 — {LATE[0]} ~ {LATE[-1]}')

    sections = [
        ('Early (2/12~3/13, 충격 포함)', EARLY),
        ('Mid (3/16~3/27, 충격 직전 진입)', MID),
        ('Late (4/06~5/05, 충격 후)', LATE),
    ]

    summary = []
    for label, starts in sections:
        print()
        print('=' * 100)
        print(f'{label}: {len(starts)}개 시작일')
        print('=' * 100)
        print(f'{"start_date":<14} {"current":>10} {"ma20":>10} {"ma50":>10} '
              f'{"lift_ma20":>11} {"lift_ma50":>11}')
        print('-' * 90)
        lifts_20, lifts_50 = [], []
        for sd in starts:
            rc, _ = run_one(db_cur, sd)
            r20, _ = run_one(db_20, sd)
            r50, _ = run_one(db_50, sd)
            l20 = r20 - rc
            l50 = r50 - rc
            lifts_20.append(l20)
            lifts_50.append(l50)
            print(f'  {sd:<12} {rc:+9.2f}% {r20:+9.2f}% {r50:+9.2f}% '
                  f'{l20:+10.2f}%p {l50:+10.2f}%p')

        if lifts_20:
            avg_20 = sum(lifts_20) / len(lifts_20)
            avg_50 = sum(lifts_50) / len(lifts_50)
            wins_20 = sum(1 for l in lifts_20 if l > 0)
            wins_50 = sum(1 for l in lifts_50 if l > 0)
            print('-' * 90)
            print(f'  avg lift: ma20 {avg_20:+.2f}%p, ma50 {avg_50:+.2f}%p')
            print(f'  wins: ma20 {wins_20}/{len(lifts_20)}, ma50 {wins_50}/{len(lifts_50)}')
            print(f'  min lift: ma20 {min(lifts_20):+.2f}%p, ma50 {min(lifts_50):+.2f}%p')
            print(f'  max lift: ma20 {max(lifts_20):+.2f}%p, ma50 {max(lifts_50):+.2f}%p')
            summary.append((label, avg_20, avg_50, wins_20, len(lifts_20),
                           wins_50, len(lifts_50)))

    print()
    print('=' * 100)
    print('종합 (구간별 avg lift)')
    print('=' * 100)
    print(f'{"구간":<35} {"ma20 avg":>10} {"ma20 wins":>11} {"ma50 avg":>10} {"ma50 wins":>11}')
    for lb, a20, a50, w20, n20, w50, n50 in summary:
        print(f'  {lb:<33} {a20:+9.2f}%p {w20:>4}/{n20:<4} {a50:+9.2f}%p {w50:>4}/{n50:<4}')

    print()
    print('해석:')
    print('  Early/Mid >> Late → single-event alpha (3/30 충격 회피 의존)')
    print('  모든 구간 비슷 → 진짜 robust alpha')


if __name__ == '__main__':
    main()
