"""리스크 필터 테스트 — 가짜 데이터로 AI 리스크 필터 + 포트폴리오 메시지 검증
테스트 후 삭제할 것!
"""
import sys, io, os
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import pandas as pd
from datetime import datetime

# daily_runner에서 필요한 함수 import
from daily_runner import (
    load_config, send_telegram_long, log,
    run_ai_analysis, run_portfolio_recommendation,
    create_part2_message, get_last_business_day,
)

config = load_config()
biz_day = get_last_business_day()

# 가짜 종목 데이터 — 다양한 리스크 시나리오
fake_data = [
    # ✅ 종목 (포트폴리오 대상)
    {'ticker': 'AAAA', 'short_name': 'Alpha Growth', 'industry': '기술', 'score': 80, 'adj_score': 75,
     'adj_gap': -35.0, 'price': 150, 'ma60': 140, 'fwd_pe': 25.0, 'eps_change_90d': 45.0,
     'price_chg': 5.0, 'rev_up30': 15, 'rev_down30': 2, 'num_analysts': 20,
     'trend_lights': '🔥☀️🌤️☁️', 'trend_desc': '최근 폭발 성장', 'part2_rank': 1,
     'eps_chg_weighted': 40, 'price_chg_weighted': 5},
    {'ticker': 'BBBB', 'short_name': 'Beta Safe', 'industry': '헬스케어', 'score': 60, 'adj_score': 55,
     'adj_gap': -20.0, 'price': 80, 'ma60': 75, 'fwd_pe': 18.0, 'eps_change_90d': 30.0,
     'price_chg': 3.0, 'rev_up30': 8, 'rev_down30': 1, 'num_analysts': 12,
     'trend_lights': '☀️☀️🌤️☁️', 'trend_desc': '꾸준한 상승', 'part2_rank': 2,
     'eps_chg_weighted': 25, 'price_chg_weighted': 3},
    # ✅ but 하향 과반 → 포트폴리오 제외되어야 함
    {'ticker': 'CCCC', 'short_name': 'Charlie Risky', 'industry': '에너지', 'score': 50, 'adj_score': 45,
     'adj_gap': -15.0, 'price': 60, 'ma60': 55, 'fwd_pe': 12.0, 'eps_change_90d': 20.0,
     'price_chg': -8.0, 'rev_up30': 2, 'rev_down30': 5, 'num_analysts': 10,
     'trend_lights': '☁️🌧️☁️🌧️', 'trend_desc': '혼조 하강', 'part2_rank': 3,
     'eps_chg_weighted': 15, 'price_chg_weighted': -8},
    # ✅ but 저커버리지 → 포트폴리오 제외되어야 함
    {'ticker': 'DDDD', 'short_name': 'Delta Thin', 'industry': 'REIT', 'score': 55, 'adj_score': 50,
     'adj_gap': -40.0, 'price': 110, 'ma60': 100, 'fwd_pe': 55.0, 'eps_change_90d': 50.0,
     'price_chg': 2.0, 'rev_up30': 0, 'rev_down30': 0, 'num_analysts': 2,
     'trend_lights': '🔥☀️☁️☁️', 'trend_desc': '초반 급등 후 정체', 'part2_rank': 4,
     'eps_chg_weighted': 45, 'price_chg_weighted': 2},
    # ✅ 클린 종목
    {'ticker': 'EEEE', 'short_name': 'Echo Strong', 'industry': '소비재', 'score': 65, 'adj_score': 60,
     'adj_gap': -25.0, 'price': 200, 'ma60': 190, 'fwd_pe': 30.0, 'eps_change_90d': 35.0,
     'price_chg': 8.0, 'rev_up30': 12, 'rev_down30': 1, 'num_analysts': 18,
     'trend_lights': '☀️🌤️🌤️☁️', 'trend_desc': '안정적 상승', 'part2_rank': 5,
     'eps_chg_weighted': 30, 'price_chg_weighted': 8},
    # ✅ 고PE 종목 (PE>100이지만 필터 제거했으니 통과해야 함)
    {'ticker': 'FFFF', 'short_name': 'Foxtrot HiPE', 'industry': 'AI/소프트웨어', 'score': 70, 'adj_score': 65,
     'adj_gap': -30.0, 'price': 300, 'ma60': 280, 'fwd_pe': 150.0, 'eps_change_90d': 40.0,
     'price_chg': 15.0, 'rev_up30': 20, 'rev_down30': 0, 'num_analysts': 25,
     'trend_lights': '🔥🔥☀️🌤️', 'trend_desc': '폭발적 가속', 'part2_rank': 6,
     'eps_chg_weighted': 38, 'price_chg_weighted': 15},
    # ⏳ 종목 (포트폴리오 미대상)
    {'ticker': 'GGGG', 'short_name': 'Golf Watch', 'industry': '금융', 'score': 45, 'adj_score': 40,
     'adj_gap': -10.0, 'price': 50, 'ma60': 48, 'fwd_pe': 15.0, 'eps_change_90d': 15.0,
     'price_chg': 1.0, 'rev_up30': 5, 'rev_down30': 3, 'num_analysts': 8,
     'trend_lights': '🌤️☁️☁️🌧️', 'trend_desc': '초반만 상승', 'part2_rank': 7,
     'eps_chg_weighted': 12, 'price_chg_weighted': 1},
    # 🆕 종목
    {'ticker': 'HHHH', 'short_name': 'Hotel New', 'industry': '통신', 'score': 40, 'adj_score': 35,
     'adj_gap': -8.0, 'price': 35, 'ma60': 33, 'fwd_pe': 20.0, 'eps_change_90d': 12.0,
     'price_chg': -2.0, 'rev_up30': 3, 'rev_down30': 0, 'num_analysts': 5,
     'trend_lights': '🌤️☁️☁️☁️', 'trend_desc': '미약한 상승', 'part2_rank': 8,
     'eps_chg_weighted': 10, 'price_chg_weighted': -2},
]

df = pd.DataFrame(fake_data)

# status_map: ✅ 6개, ⏳ 1개, 🆕 1개
status_map = {
    'AAAA': '✅', 'BBBB': '✅', 'CCCC': '✅', 'DDDD': '✅',
    'EEEE': '✅', 'FFFF': '✅', 'GGGG': '⏳', 'HHHH': '🆕',
}

private_id = os.environ.get('TELEGRAM_PRIVATE_ID')

print("=" * 60)
print("  리스크 필터 테스트 시작")
print("=" * 60)
print(f"  종목: {len(fake_data)}개 (✅6, ⏳1, 🆕1)")
print(f"  예상 제외: CCCC(하향과반), DDDD(저커버리지)")
print(f"  예상 통과: AAAA, BBBB, EEEE, FFFF(PE>100이지만 통과)")
print()

# [1/3] 매수 후보 메시지
exited = ['ZZZZ']  # 이탈 종목
market_lines = [
    '📈 SPY $502.30 (+0.8%) MA60↑ · QQQ $430.10 (+1.2%) MA60↑',
]
msg_part2 = create_part2_message(
    df, status_map=status_map, exited_tickers=exited, market_lines=market_lines,
)
if msg_part2 and private_id:
    send_telegram_long(msg_part2, config, chat_id=private_id)
    print("[1/3] 매수 후보 전송 완료")

# [2/3] AI 리스크 필터
msg_ai = run_ai_analysis(config, results_df=df, status_map=status_map, biz_day=biz_day)
if msg_ai and private_id:
    send_telegram_long(msg_ai, config, chat_id=private_id)
    print("[2/3] AI 리스크 필터 전송 완료")
elif not msg_ai:
    print("[2/3] AI 리스크 필터 생성 실패")

# [3/3] 최종 추천
msg_port = run_portfolio_recommendation(config, df, status_map, biz_day=biz_day)
if msg_port and private_id:
    send_telegram_long(msg_port, config, chat_id=private_id)
    print("[3/3] 최종 추천 전송 완료")
elif not msg_port:
    print("[3/3] 포트폴리오 생성 실패 (✅ 안전 종목 부족?)")

print("\n테스트 완료!")
