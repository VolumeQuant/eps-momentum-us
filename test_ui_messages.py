"""
UI 테스트용 스크립트 — 가짜 데이터로 모든 메시지 UI 확인
실행: py -3 test_ui_messages.py
"""
import sys
import io
import json
import os
from pathlib import Path

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

PROJECT_ROOT = Path(__file__).parent

# config 로드
config_path = PROJECT_ROOT / 'config.json'
if config_path.exists():
    with open(config_path) as f:
        config = json.load(f)
else:
    config = {}

# 환경변수 오버라이드
config['telegram_enabled'] = True
config['telegram_bot_token'] = os.environ.get('TELEGRAM_BOT_TOKEN', config.get('telegram_bot_token', ''))
config['telegram_private_id'] = os.environ.get('TELEGRAM_PRIVATE_ID', config.get('telegram_private_id', ''))

private_id = config.get('telegram_private_id', '')
if not private_id or not config['telegram_bot_token']:
    print("ERROR: telegram_bot_token / telegram_private_id 설정 필요")
    sys.exit(1)

# daily_runner에서 함수 import
from daily_runner import (
    create_guide_message,
    create_part2_message,
    create_system_log_message,
    send_telegram_long,
    get_part2_candidates,
)

import pandas as pd
import numpy as np

# ============================================================
# 1. 가짜 데이터 생성 (30종목 — 모든 UI 요소 커버)
# ============================================================
fake_stocks = [
    # ✅ 3일 검증 — 다양한 추세 패턴
    {'ticker': 'NVDA', 'short_name': 'NVIDIA', 'industry': '반도체', 'adj_score': 85.2, 'adj_gap': -44.1, 'eps_change_90d': 32.5, 'fwd_pe': 38.2, 'price': 890.0, 'ma60': 820.0, 'trend_lights': '🔥☀️☀️🌤️', 'trend_desc': '폭발적 급상향', 'price_chg': 18.3, 'rev_up30': 12, 'rev_down30': 0, 'eps_chg_weighted': 15.0, 'price_chg_weighted': 10.0, 'num_analysts': 35},
    {'ticker': 'AAPL', 'short_name': 'Apple', 'industry': '소비자 전자', 'adj_score': 72.1, 'adj_gap': -38.5, 'eps_change_90d': 18.7, 'fwd_pe': 28.5, 'price': 235.0, 'ma60': 225.0, 'trend_lights': '☀️☀️🌤️🌤️', 'trend_desc': '꾸준한 상승', 'price_chg': 8.2, 'rev_up30': 8, 'rev_down30': 1, 'eps_chg_weighted': 8.0, 'price_chg_weighted': 5.0, 'num_analysts': 40},
    {'ticker': 'MSFT', 'short_name': 'Microsoft', 'industry': '소프트웨어', 'adj_score': 65.0, 'adj_gap': -33.9, 'eps_change_90d': 15.2, 'fwd_pe': 32.1, 'price': 420.0, 'ma60': 405.0, 'trend_lights': '🌤️☀️☀️🌤️', 'trend_desc': '중반 강세', 'price_chg': 5.1, 'rev_up30': 10, 'rev_down30': 2, 'eps_chg_weighted': 6.0, 'price_chg_weighted': 3.0, 'num_analysts': 38},
    {'ticker': 'LUV', 'short_name': 'Southwest Airlines', 'industry': '항공', 'adj_score': 58.3, 'adj_gap': -29.7, 'eps_change_90d': 42.1, 'fwd_pe': 18.5, 'price': 35.0, 'ma60': 32.0, 'trend_lights': '🌧️🌤️☀️🔥', 'trend_desc': '반등', 'price_chg': 12.5, 'rev_up30': 5, 'rev_down30': 0, 'eps_chg_weighted': 20.0, 'price_chg_weighted': 8.0, 'num_analysts': 12},
    {'ticker': 'AA', 'short_name': 'Alcoa', 'industry': '알루미늄', 'adj_score': 52.7, 'adj_gap': -26.3, 'eps_change_90d': 65.3, 'fwd_pe': 12.8, 'price': 42.0, 'ma60': 38.0, 'trend_lights': '☁️🌤️☀️☀️', 'trend_desc': '상향 가속', 'price_chg': 22.1, 'rev_up30': 6, 'rev_down30': 1, 'eps_chg_weighted': 25.0, 'price_chg_weighted': 12.0, 'num_analysts': 15},
    {'ticker': 'FCX', 'short_name': 'Freeport-McMoRan', 'industry': '구리 채굴', 'adj_score': 48.5, 'adj_gap': -23.1, 'eps_change_90d': 28.9, 'fwd_pe': 22.3, 'price': 48.5, 'ma60': 44.0, 'trend_lights': '🌤️🌤️☀️🌤️', 'trend_desc': '중반 강세', 'price_chg': 10.2, 'rev_up30': 7, 'rev_down30': 0, 'eps_chg_weighted': 12.0, 'price_chg_weighted': 6.0, 'num_analysts': 18},
    {'ticker': 'FIVE', 'short_name': 'Five Below', 'industry': '할인 소매', 'adj_score': 44.2, 'adj_gap': -21.5, 'eps_change_90d': 35.8, 'fwd_pe': 25.1, 'price': 125.0, 'ma60': 115.0, 'trend_lights': '☀️🌤️☀️🌤️', 'trend_desc': '상승 등락', 'price_chg': 7.3, 'rev_up30': 4, 'rev_down30': 1, 'eps_chg_weighted': 14.0, 'price_chg_weighted': 4.0, 'num_analysts': 20},
    {'ticker': 'SNDK', 'short_name': 'SanDisk', 'industry': '반도체', 'adj_score': 41.8, 'adj_gap': -19.8, 'eps_change_90d': 55.2, 'fwd_pe': 15.3, 'price': 52.0, 'ma60': 48.0, 'trend_lights': '🌤️🌤️🔥🌤️', 'trend_desc': '중반 급등', 'price_chg': 15.8, 'rev_up30': 3, 'rev_down30': 0, 'eps_chg_weighted': 22.0, 'price_chg_weighted': 8.0, 'num_analysts': 10},
    {'ticker': 'CPRI', 'short_name': 'Capri Holdings', 'industry': '명품 패션', 'adj_score': 38.1, 'adj_gap': -17.2, 'eps_change_90d': 22.4, 'fwd_pe': 14.2, 'price': 28.0, 'ma60': 25.0, 'trend_lights': '🌧️☁️🌤️☀️', 'trend_desc': '추세 전환', 'price_chg': -2.1, 'rev_up30': 2, 'rev_down30': 0, 'eps_chg_weighted': 8.0, 'price_chg_weighted': -1.0, 'num_analysts': 14},
    {'ticker': 'APH', 'short_name': 'Amphenol', 'industry': '전자 부품', 'adj_score': 35.5, 'adj_gap': -15.8, 'eps_change_90d': 19.1, 'fwd_pe': 35.8, 'price': 72.0, 'ma60': 68.0, 'trend_lights': '☀️☀️🌤️🌤️', 'trend_desc': '상향 둔화', 'price_chg': 3.5, 'rev_up30': 9, 'rev_down30': 2, 'eps_chg_weighted': 7.0, 'price_chg_weighted': 2.0, 'num_analysts': 22},

    # ⏳ 2일 연속 — 내일 검증 가능
    {'ticker': 'META', 'short_name': 'Meta Platforms', 'industry': '소셜 미디어', 'adj_score': 33.0, 'adj_gap': -14.3, 'eps_change_90d': 12.8, 'fwd_pe': 24.5, 'price': 580.0, 'ma60': 560.0, 'trend_lights': '🌤️☀️🌤️🌤️', 'trend_desc': '중반 강세', 'price_chg': 6.8, 'rev_up30': 7, 'rev_down30': 1, 'eps_chg_weighted': 5.0, 'price_chg_weighted': 3.0, 'num_analysts': 42},
    {'ticker': 'AMZN', 'short_name': 'Amazon', 'industry': '이커머스', 'adj_score': 30.5, 'adj_gap': -12.9, 'eps_change_90d': 25.3, 'fwd_pe': 42.1, 'price': 225.0, 'ma60': 210.0, 'trend_lights': '☁️🌤️☀️🌤️', 'trend_desc': '상향 가속', 'price_chg': 11.2, 'rev_up30': 11, 'rev_down30': 0, 'eps_chg_weighted': 10.0, 'price_chg_weighted': 6.0, 'num_analysts': 45},
    {'ticker': 'GOOGL', 'short_name': 'Alphabet', 'industry': '인터넷', 'adj_score': 28.2, 'adj_gap': -11.5, 'eps_change_90d': 14.1, 'fwd_pe': 21.8, 'price': 185.0, 'ma60': 178.0, 'trend_lights': '🌤️🌤️🌤️☀️', 'trend_desc': '꾸준한 상승', 'price_chg': 4.5, 'rev_up30': 6, 'rev_down30': 2, 'eps_chg_weighted': 6.0, 'price_chg_weighted': 2.5, 'num_analysts': 40},
    {'ticker': 'CRM', 'short_name': 'Salesforce', 'industry': '클라우드', 'adj_score': 25.8, 'adj_gap': -10.2, 'eps_change_90d': 18.5, 'fwd_pe': 30.2, 'price': 295.0, 'ma60': 280.0, 'trend_lights': '☀️🌤️🌤️☁️', 'trend_desc': '상향 둔화', 'price_chg': 2.1, 'rev_up30': 5, 'rev_down30': 3, 'eps_chg_weighted': 7.0, 'price_chg_weighted': 1.0, 'num_analysts': 30},
    {'ticker': 'AMD', 'short_name': 'AMD', 'industry': '반도체', 'adj_score': 23.4, 'adj_gap': -9.1, 'eps_change_90d': 30.5, 'fwd_pe': 45.2, 'price': 165.0, 'ma60': 155.0, 'trend_lights': '🌤️🔥🌤️☁️', 'trend_desc': '중반 급등', 'price_chg': 8.8, 'rev_up30': 4, 'rev_down30': 1, 'eps_chg_weighted': 12.0, 'price_chg_weighted': 5.0, 'num_analysts': 35},

    # 🆕 오늘 첫 진입
    {'ticker': 'TSLA', 'short_name': 'Tesla', 'industry': '전기차', 'adj_score': 21.0, 'adj_gap': -8.5, 'eps_change_90d': 48.2, 'fwd_pe': 68.5, 'price': 280.0, 'ma60': 260.0, 'trend_lights': '🌧️🌧️☀️🔥', 'trend_desc': '반등', 'price_chg': 25.3, 'rev_up30': 3, 'rev_down30': 2, 'eps_chg_weighted': 18.0, 'price_chg_weighted': 15.0, 'num_analysts': 38},
    {'ticker': 'NFLX', 'short_name': 'Netflix', 'industry': '스트리밍', 'adj_score': 19.5, 'adj_gap': -7.8, 'eps_change_90d': 15.8, 'fwd_pe': 35.2, 'price': 920.0, 'ma60': 880.0, 'trend_lights': '🌤️🌤️☀️☁️', 'trend_desc': '중반 강세', 'price_chg': 5.2, 'rev_up30': 6, 'rev_down30': 0, 'eps_chg_weighted': 6.0, 'price_chg_weighted': 3.0, 'num_analysts': 32},
    {'ticker': 'V', 'short_name': 'Visa', 'industry': '결제', 'adj_score': 17.8, 'adj_gap': -6.5, 'eps_change_90d': 8.5, 'fwd_pe': 26.8, 'price': 315.0, 'ma60': 305.0, 'trend_lights': '🌤️🌤️🌤️🌤️', 'trend_desc': '전구간 상승', 'price_chg': 3.2, 'rev_up30': 4, 'rev_down30': 0, 'eps_chg_weighted': 3.0, 'price_chg_weighted': 1.5, 'num_analysts': 28},
    {'ticker': 'JPM', 'short_name': 'JPMorgan Chase', 'industry': '은행', 'adj_score': 16.2, 'adj_gap': -5.8, 'eps_change_90d': 12.3, 'fwd_pe': 12.5, 'price': 245.0, 'ma60': 235.0, 'trend_lights': '☁️☁️🌤️☀️', 'trend_desc': '상향 가속', 'price_chg': 4.8, 'rev_up30': 3, 'rev_down30': 1, 'eps_chg_weighted': 5.0, 'price_chg_weighted': 2.0, 'num_analysts': 25},
    # ⚠️ 경고 표시 종목 (EPS↑인데 주가↓ 비율 > 5)
    {'ticker': 'PLTR', 'short_name': 'Palantir', 'industry': 'AI 분석', 'adj_score': 14.5, 'adj_gap': -5.2, 'eps_change_90d': 22.1, 'fwd_pe': 85.0, 'price': 28.0, 'ma60': 26.0, 'trend_lights': '☀️🌤️☁️🌧️', 'trend_desc': '상향 둔화', 'price_chg': -15.5, 'rev_up30': 2, 'rev_down30': 4, 'eps_chg_weighted': 3.0, 'price_chg_weighted': -18.0, 'num_analysts': 15},

    # 추가 🆕 (21~25)
    {'ticker': 'UNH', 'short_name': 'UnitedHealth', 'industry': '건강보험', 'adj_score': 13.8, 'adj_gap': -4.8, 'eps_change_90d': 6.5, 'fwd_pe': 18.9, 'price': 520.0, 'ma60': 510.0, 'trend_lights': '🌤️🌤️☁️🌤️', 'trend_desc': '상승 등락', 'price_chg': 2.1, 'rev_up30': 5, 'rev_down30': 1, 'eps_chg_weighted': 2.5, 'price_chg_weighted': 1.0, 'num_analysts': 22},
    {'ticker': 'HD', 'short_name': 'Home Depot', 'industry': '홈인테리어', 'adj_score': 12.5, 'adj_gap': -4.2, 'eps_change_90d': 5.8, 'fwd_pe': 24.1, 'price': 385.0, 'ma60': 375.0, 'trend_lights': '☁️🌤️🌤️☁️', 'trend_desc': '등락 반복', 'price_chg': 1.5, 'rev_up30': 3, 'rev_down30': 2, 'eps_chg_weighted': 2.0, 'price_chg_weighted': 0.5, 'num_analysts': 28},
    {'ticker': 'CAT', 'short_name': 'Caterpillar', 'industry': '중장비', 'adj_score': 11.2, 'adj_gap': -3.5, 'eps_change_90d': 8.2, 'fwd_pe': 16.5, 'price': 355.0, 'ma60': 345.0, 'trend_lights': '🌤️☁️🌤️☀️', 'trend_desc': '추세 전환', 'price_chg': 3.8, 'rev_up30': 4, 'rev_down30': 0, 'eps_chg_weighted': 3.5, 'price_chg_weighted': 2.0, 'num_analysts': 20},
    {'ticker': 'GE', 'short_name': 'GE Aerospace', 'industry': '항공 엔진', 'adj_score': 10.8, 'adj_gap': -3.0, 'eps_change_90d': 12.5, 'fwd_pe': 35.2, 'price': 195.0, 'ma60': 188.0, 'trend_lights': '☁️☁️☀️🌤️', 'trend_desc': '상향 가속', 'price_chg': 5.5, 'rev_up30': 6, 'rev_down30': 1, 'eps_chg_weighted': 5.0, 'price_chg_weighted': 3.0, 'num_analysts': 18},
    {'ticker': 'DE', 'short_name': 'Deere & Co', 'industry': '농기계', 'adj_score': 10.1, 'adj_gap': -2.5, 'eps_change_90d': 4.2, 'fwd_pe': 20.8, 'price': 420.0, 'ma60': 410.0, 'trend_lights': '🌧️☁️🌤️🌤️', 'trend_desc': '반등', 'price_chg': 1.8, 'rev_up30': 2, 'rev_down30': 1, 'eps_chg_weighted': 1.5, 'price_chg_weighted': 0.8, 'num_analysts': 16},
]

# DataFrame 생성
df = pd.DataFrame(fake_stocks)

# ============================================================
# 2. 상태 맵 (✅/⏳/🆕)
# ============================================================
status_map = {}
# 1~10: ✅
for s in fake_stocks[:10]:
    status_map[s['ticker']] = '✅'
# 11~15: ⏳
for s in fake_stocks[10:15]:
    status_map[s['ticker']] = '⏳'
# 16~25: 🆕
for s in fake_stocks[15:]:
    status_map[s['ticker']] = '🆕'

# ============================================================
# 3. 이탈 종목 (어제 대비)
# ============================================================
exited_tickers = ['ASML', 'COST', 'LRCX']
# 이탈 종목도 df에 넣어야 이름 변환됨
for t, name in [('ASML', 'ASML Holding'), ('COST', 'Costco'), ('LRCX', 'Lam Research')]:
    df = pd.concat([df, pd.DataFrame([{
        'ticker': t, 'short_name': name, 'industry': '', 'adj_score': 5, 'adj_gap': 1,
        'eps_change_90d': -1, 'fwd_pe': 20, 'price': 100, 'ma60': 120,
    }])], ignore_index=True)

# ============================================================
# 4. 시장 컨텍스트
# ============================================================
market_lines = [
    'S&P 500  6,025.31 (+0.67%)',
    'NASDAQ  19,432.18 (+1.12%)',
]

# ============================================================
# 5. 가짜 AI 점검 메시지
# ============================================================
fake_ai_msg = '\n'.join([
    '━━━━━━━━━━━━━━━━━━━',
    '    🛡️ AI 점검',
    '━━━━━━━━━━━━━━━━━━━',
    '',
    '후보 종목 중 주의할 점을 AI가 점검했어요.',
    '',
    '📰 <b>시장 동향</b>',
    'S&P 500과 나스닥이 동반 상승하며 기술주 중심 강세 흐름이에요.',
    '연준 의장 발언 이후 금리 인하 기대감이 커지고 있어요.',
    '',
    '⚠️ <b>매수 주의</b>',
    '',
    '<b>Palantir(PLTR)</b>',
    '애널리스트 의견이 ↑2 ↓4로 하향 우세예요.',
    'EPS는 오르고 있지만 주가가 -15.5% 급락해서 괴리가 커요.',
    '',
    '<b>Capri Holdings(CPRI)</b>',
    '주가가 최근 하락 전환(-2.1%)했어요.',
    '추세가 🌧️에서 시작해서 아직 안정적이지 않아요.',
    '',
    '📅 <b>어닝 주의</b>',
    'NVDA 2/26 · FIVE 3/12',
    '',
    '👉 다음: 최종 추천 포트폴리오',
])

# ============================================================
# 6. 가짜 포트폴리오 메시지
# ============================================================
fake_portfolio_msg = '\n'.join([
    '━━━━━━━━━━━━━━━━━━━',
    '    🎯 최종 추천',
    '━━━━━━━━━━━━━━━━━━━',
    '📅 2026년 02월 10일 (미국장 기준)',
    '',
    '916종목 → Top 30 → ✅ 검증 → <b>최종 5종목</b>',
    '',
    '📊 <b>비중 한눈에 보기</b>',
    'NVIDIA(NVDA) 30% · Apple(AAPL) 25% · Southwest Airlines(LUV) 20% · Alcoa(AA) 15% · Freeport-McMoRan(FCX) 10%',
    '',
    '─────────────────',
    '<b>1. NVIDIA(NVDA) · 비중 30%</b>',
    '🔥 EPS 전망이 32.5% 급등하며 반도체 AI 수요를 이끌고 있어요.',
    '──────────────────',
    '<b>2. Apple(AAPL) · 비중 25%</b>',
    '☀️ 꾸준한 EPS 상승과 안정적인 주가 흐름이 매력적이에요.',
    '──────────────────',
    '<b>3. Southwest Airlines(LUV) · 비중 20%</b>',
    '🔥 EPS가 42% 급등하며 반등 추세에 있어요. 저평가 매력이 커요.',
    '──────────────────',
    '<b>4. Alcoa(AA) · 비중 15%</b>',
    '☀️ 알루미늄 가격 상승과 함께 EPS가 65% 폭등 중이에요.',
    '──────────────────',
    '<b>5. Freeport-McMoRan(FCX) · 비중 10%</b>',
    '🌤️ 구리 수요 증가로 EPS가 안정적으로 올라가고 있어요.',
    '',
    '💡 <b>활용법</b>',
    '· 비중대로 분산 투자를 권장해요',
    '· 목록에서 빠지면 매도 검토',
    '· 최소 2주 보유, 매일 후보 갱신 확인',
    '⚠️ 참고용이며, 투자 판단은 본인 책임이에요.',
])

# ============================================================
# 7. 시스템 로그
# ============================================================
fake_stats = {
    'main_count': 864,
    'turnaround_count': 49,
    'error_count': 3,
    'universe': 916,
    'error_tickers': ['BRK-B', 'BF-B', 'LUMN'],
    'score_gt0': 412,
    'score_gt3': 185,
    'aligned_count': 52,
}

# ============================================================
# 메시지 생성 & 전송
# ============================================================
print("=" * 50)
print("  UI 테스트 — 가짜 데이터로 전체 메시지 전송")
print("=" * 50)

# 1) 📖 투자 가이드
msg_guide = create_guide_message()
print(f"\n[1] 📖 투자 가이드 ({len(msg_guide)}자)")

# 2) [1/2] 매수 후보 (가짜 데이터로 직접 생성)
msg_part2 = create_part2_message(df, status_map, exited_tickers, market_lines, top_n=25)
print(f"[2] [1/2] 매수 후보 ({len(msg_part2)}자)")

# 3) [2/2] AI 점검 + 최종 추천 (통합)
msg_combined = fake_ai_msg + '\n\n' + fake_portfolio_msg
print(f"[3] [2/2] AI+포트폴리오 ({len(msg_combined)}자)")

# 4) 시스템 로그
msg_log = create_system_log_message(fake_stats, 142.5, config)
print(f"[4] 시스템 로그 ({len(msg_log)}자)")

# 전송
messages = [
    ('📖 투자 가이드', msg_guide),
    ('[1/2] 매수 후보', msg_part2),
    ('[2/2] AI+포트폴리오', msg_combined),
    ('시스템 로그', msg_log),
]

import time
for label, msg in messages:
    ok = send_telegram_long(msg, config, chat_id=private_id)
    status = '✅' if ok else '❌'
    print(f"  {status} {label} 전송 완료")
    time.sleep(0.5)

print(f"\n전송 완료! 텔레그램에서 확인하세요.")
