"""정밀 백테스트 metric 계산기

핵심:
  - CAGR (연환산 수익률)
  - 진짜 Sharpe (일간 수익 std 기반)
  - Sortino (하방 위험만)
  - Calmar (CAGR / |MDD|)
  - MDD
  - Profit Factor
  - Win Rate
  - Multistart에서: 평균/최악 MDD/Sharpe 등 분포

기본 가정:
  - 1년 = 252 거래일
  - 무위험 수익률 = 0% (단순화)
"""
import math
from collections import defaultdict


TRADING_DAYS_YEAR = 252


def compute_metrics(daily_returns, trades, n_days):
    """일간 수익률 + 거래 리스트로 모든 metric 계산

    Args:
        daily_returns: list of daily returns in percent (e.g. [1.5, -0.3, ...])
        trades: list of dict with 'return' key (closed trade returns in percent)
        n_days: total trading days

    Returns:
        dict of metrics
    """
    if not daily_returns or n_days == 0:
        return {
            'total_return': 0, 'cagr': 0, 'sharpe': 0, 'sortino': 0,
            'calmar': 0, 'max_dd': 0, 'profit_factor': 0,
            'win_rate': 0, 'n_trades': len(trades),
        }

    # 누적 수익 + MDD
    cum = 1.0
    peak = 1.0
    max_dd = 0
    nav_curve = [1.0]
    for r in daily_returns:
        cum *= (1 + r / 100)
        nav_curve.append(cum)
        peak = max(peak, cum)
        dd = (cum - peak) / peak * 100
        max_dd = min(max_dd, dd)

    total_return_pct = (cum - 1) * 100

    # CAGR
    years = n_days / TRADING_DAYS_YEAR
    if years > 0 and cum > 0:
        cagr = (cum ** (1 / years) - 1) * 100
    else:
        cagr = 0

    # 일간 수익률 통계 (% 단위)
    if len(daily_returns) >= 2:
        mean_daily = sum(daily_returns) / len(daily_returns)
        var_daily = sum((r - mean_daily) ** 2 for r in daily_returns) / len(daily_returns)
        std_daily = math.sqrt(var_daily)
    else:
        mean_daily = daily_returns[0] if daily_returns else 0
        std_daily = 0

    # Sharpe ratio (연환산, 무위험 수익률 0)
    if std_daily > 0:
        sharpe = (mean_daily / std_daily) * math.sqrt(TRADING_DAYS_YEAR)
    else:
        sharpe = 0

    # Sortino (하방 위험만)
    downside_returns = [r for r in daily_returns if r < 0]
    if downside_returns and len(downside_returns) >= 2:
        downside_var = sum(r ** 2 for r in downside_returns) / len(downside_returns)
        downside_std = math.sqrt(downside_var)
        if downside_std > 0:
            sortino = (mean_daily / downside_std) * math.sqrt(TRADING_DAYS_YEAR)
        else:
            sortino = 0
    else:
        sortino = sharpe  # 하방 없음 = sharpe

    # Calmar (CAGR / |MDD|)
    if max_dd < 0:
        calmar = cagr / abs(max_dd)
    else:
        calmar = 0 if cagr == 0 else float('inf')

    # 거래별 통계
    closed_returns = [t['return'] for t in trades]
    n_trades = len(closed_returns)
    if n_trades > 0:
        wins = [r for r in closed_returns if r > 0]
        losses = [r for r in closed_returns if r < 0]
        win_rate = len(wins) / n_trades * 100
        gross_profit = sum(wins) if wins else 0
        gross_loss = abs(sum(losses)) if losses else 0
        if gross_loss > 0:
            profit_factor = gross_profit / gross_loss
        else:
            profit_factor = float('inf') if gross_profit > 0 else 0
        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
    else:
        win_rate = 0
        profit_factor = 0
        avg_win = 0
        avg_loss = 0

    return {
        'total_return': round(total_return_pct, 2),
        'cagr': round(cagr, 2),
        'sharpe': round(sharpe, 2),
        'sortino': round(sortino, 2),
        'calmar': round(calmar, 2),
        'max_dd': round(max_dd, 2),
        'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else 'inf',
        'win_rate': round(win_rate, 1),
        'n_trades': n_trades,
        'avg_win': round(avg_win, 2),
        'avg_loss': round(avg_loss, 2),
    }


def aggregate_multistart(results_list):
    """multistart 결과들을 종합. 각 metric의 평균/중앙값/표준편차/최저/최고

    Args:
        results_list: list of dict (각각 compute_metrics 결과)

    Returns:
        dict of aggregated metrics
    """
    if not results_list:
        return {}

    metrics_to_agg = ['total_return', 'cagr', 'sharpe', 'sortino', 'calmar',
                       'max_dd', 'win_rate', 'n_trades']

    agg = {}
    for m in metrics_to_agg:
        vals = [r[m] for r in results_list if isinstance(r[m], (int, float))]
        if not vals:
            agg[m] = {}
            continue
        n = len(vals)
        avg = sum(vals) / n
        sorted_vals = sorted(vals)
        median = sorted_vals[n // 2]
        std = math.sqrt(sum((v - avg) ** 2 for v in vals) / n)
        agg[m] = {
            'avg': round(avg, 2),
            'median': round(median, 2),
            'std': round(std, 2),
            'min': round(min(vals), 2),
            'max': round(max(vals), 2),
        }
    return agg


def format_summary(label, agg):
    """집계 결과를 한 줄로 포맷"""
    return (
        f"{label:<24s} "
        f"CAGR avg {agg['cagr']['avg']:+6.1f}% (med {agg['cagr']['median']:+5.1f}, "
        f"min {agg['cagr']['min']:+5.1f}, max {agg['cagr']['max']:+5.1f}) | "
        f"MDD avg {agg['max_dd']['avg']:+5.1f}% (worst {agg['max_dd']['min']:+5.1f}) | "
        f"Sharpe {agg['sharpe']['avg']:.2f} | "
        f"Sortino {agg['sortino']['avg']:.2f} | "
        f"Calmar {agg['calmar']['avg']:.2f}"
    )
