"""line 4088 fix 영향 측정 — X8 vs X10 누적 수익률 비교

production DB로 _get_system_performance을 X8 (현재 버그) / X10 (fix 후) 둘 다 실행.
사용자에게 표시되는 헤더 숫자가 어떻게 바뀌는지 정확히 확인.
"""
import sys
import sqlite3
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import daily_runner as dr

ROOT = Path(__file__).parent.parent


def run_perf(exit_top):
    """_get_system_performance을 exit_top 값으로 호출. 4088 라인을 패치."""
    # 원본 함수 코드를 읽어서 exit_top만 바꾼 버전 실행
    import inspect
    src = inspect.getsource(dr._get_system_performance)
    src_patched = src.replace(
        'if (rk is None or rk > 8) or ms < -2:',
        f'if (rk is None or rk > {exit_top}) or ms < -2:'
    )
    # 함수명 변경해서 충돌 방지
    src_patched = src_patched.replace(
        'def _get_system_performance(',
        f'def _get_system_performance_x{exit_top}('
    )
    # 새 함수 실행 (daily_runner 모듈 컨텍스트에서)
    ns = {}
    exec(src_patched, dr.__dict__, ns)
    fn = ns[f'_get_system_performance_x{exit_top}']
    return fn()


def main():
    print('=' * 80)
    print('누적 수익률 표시: X8 (현재 버그) vs X10 (fix 후) 비교')
    print('=' * 80)

    for exit_top in [8, 10]:
        res = run_perf(exit_top)
        if res is None:
            print(f'X{exit_top}: 계산 실패')
            continue
        print(f'\n[X{exit_top}] (rank > {exit_top} → exit)')
        print(f'  시스템 누적: {res["sys_cum"]:+.2f}%')
        print(f'  SPY 누적:    {res["spy_cum"]:+.2f}%')
        print(f'  알파:        {res["alpha"]:+.2f}%p')
        print(f'  N거래일:     {res["n_days"]}')
        print(f'  기간:        {res["start_date"]} ~ {res["end_date"]}')
        print(f'  wins/losses: {res["wins"]}/{res["losses"]}')


if __name__ == '__main__':
    main()
