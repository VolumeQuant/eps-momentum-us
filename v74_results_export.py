"""v74 채택 전략으로 모든 시작일 시뮬 결과를 CSV로 저장

채택안: E3/X11/S3 + strict hold + base conviction
출력:
  - v74_summary.csv: 시작일별 요약 (수익/MDD/거래수/hold)
  - v74_trades.csv: 모든 거래 내역
"""
import csv
from bt_engine import load_data, simulate

HOLD_STRICT = {'lookback_days': 20, 'price_threshold': 25, 'rev_up_ratio': 0.4,
               'check_ma60': True, 'max_grace': 2}


def main():
    dates, data = load_data()
    print(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    # 모든 시작일 (3일 검증 후 ~ 끝 5일 전)
    start_dates = [d for d in dates if dates.index(d) >= 3 and dates.index(d) < len(dates) - 5]

    # 각 시작일별 시뮬
    summary_rows = []
    all_trades = []
    for sd in start_dates:
        r = simulate(dates, data, 3, 11, 3, start_date=sd, hold_params=HOLD_STRICT)
        summary_rows.append({
            'start_date': sd,
            'total_return': round(r['total_return'], 2),
            'max_dd': round(r['max_dd'], 2),
            'sharpe': round(r['sharpe'], 2),
            'sortino': round(r['sortino'], 2),
            'calmar': round(r['calmar'], 2),
            'n_trades': r['n_trades'],
            'win_rate': round(r['win_rate'], 1),
            'profit_factor': r['profit_factor'],
            'breakout_holds': r['breakout_holds'],
            'n_open': r['n_open'],
            'best_trade': max((t['return'] for t in r['trades']), default=0),
            'worst_trade': min((t['return'] for t in r['trades']), default=0),
        })

        for t in r['trades']:
            all_trades.append({
                'start_date': sd,
                'ticker': t['ticker'],
                'entry_date': t['entry_date'],
                'exit_date': t['exit_date'],
                'entry_price': round(t['entry_price'], 2),
                'exit_price': round(t['exit_price'], 2),
                'return': round(t['return'], 2),
                'reason': t.get('reason', ''),
            })

    # CSV 저장
    with open('v74_summary.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
        writer.writeheader()
        writer.writerows(summary_rows)

    with open('v74_trades.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=all_trades[0].keys())
        writer.writeheader()
        writer.writerows(all_trades)

    # Sanity check
    n = len(summary_rows)
    rets = [r['total_return'] for r in summary_rows]
    avg_ret = sum(rets) / n
    pos_count = sum(1 for r in rets if r > 0)
    total_holds = sum(r['breakout_holds'] for r in summary_rows)

    print(f"\n[CSV 생성 완료]")
    print(f"  v74_summary.csv: {n}개 시작일")
    print(f"  v74_trades.csv: {len(all_trades)}건 거래")
    print(f"\n[Sanity Check]")
    print(f"  평균 수익: {avg_ret:+.2f}% (예상 +31.59%)")
    print(f"  양의 수익률: {pos_count}/{n}")
    print(f"  총 hold 트리거: {total_holds}회")

    if abs(avg_ret - 31.59) < 0.5 and pos_count == n:
        print(f"\n[OK] 검증 통과")
    else:
        print(f"\n[!] 검증 실패")


if __name__ == '__main__':
    main()
