"""stop_test 결과를 텔레그램 개인봇으로 전송"""
import sys, os, pickle, json
sys.path.insert(0, '..')
sys.stdout.reconfigure(encoding='utf-8')

# config 로드
with open('../config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

# 결과 로드
with open('stop_test_results.pkl', 'rb') as f:
    data = pickle.load(f)

results = data['results']
combo = data['combo']
base = results[0]

# 메시지 작성 (중학생 이해 가능, 핵심 정보 포함)
msg = f"""🧪 <b>Trailing Stop / Stop Loss 백테스트 결과</b>

<b>한 줄 요약</b>: 어떤 손절매를 추가해도 <b>수익·MDD가 그대로</b>거나 <b>오히려 나빠짐</b>. 현재 시스템 그대로 유지.

━━━━━━━━━━━━━━━
<b>📊 검증 방법</b>
━━━━━━━━━━━━━━━
• 데이터: 46거래일 (2/10~4/16)
• 시뮬: 8개 시작일 평균 (각 38~46일 충분 sim)
• 시작일은 초기 한 주 안에서만 변동, 이후 끝까지 연속

<b>실험 변형 11개</b>:
• Trailing Stop: -5/-8/-10/-12/-15% (5종)
• Stop Loss: -10/-15/-20/-25% (4종)
• 조합 1종

━━━━━━━━━━━━━━━
<b>🎯 결과 (vs 현재 시스템)</b>
━━━━━━━━━━━━━━━

현재 v79: 수익 <b>+{base['ret_mean']:.1f}%</b>, MDD <b>{base['mdd_worst']:.1f}%</b>, Sharpe <b>{base['sharpe']:.2f}</b>

| 변형 | 수익 차이 | MDD 차이 |
|------|-----------|----------|
"""

for r in results[1:]:
    d_ret = r['ret_mean'] - base['ret_mean']
    d_mdd = r['mdd_worst'] - base['mdd_worst']
    label = r['label'].replace('trailing', 'TS').replace('stop loss', 'SL')
    msg += f"| {label} | {d_ret:+.1f}%p | {d_mdd:+.1f}%p |\n"

d_combo_ret = combo['ret_mean'] - base['ret_mean']
d_combo_mdd = combo['mdd_worst'] - base['mdd_worst']
msg += f"| 조합 (TS-8% + SL-10%) | {d_combo_ret:+.1f}%p | {d_combo_mdd:+.1f}%p |\n"

msg += """
━━━━━━━━━━━━━━━
<b>🤔 왜 효과가 없나?</b>
━━━━━━━━━━━━━━━

<b>1. Trailing -5%만 손실 (-11.3%p)인 이유</b>
너무 빠듯한 기준. 좋은 종목도 정상적으로 5% 정도는 출렁이는데, 그 변동성에 걸려서 <b>수익 안 났는데 미리 매도</b>됨. 그 후 같은 종목이 다시 Top 3에 들어오면 <b>더 비싼 가격에 재진입</b>. 손실만 누적.

<b>2. 나머지가 모두 효과 0인 이유</b>
Trailing/Stop이 발동되긴 함. 그런데 <b>매도 다음날 같은 종목이 또 Top 3에 들어오면 즉시 재진입</b>. "1~2일 비웠다가 다시 사기" 패턴 → 순수익 변동 거의 0.

비유하자면, 식당에서 음식이 살짝 식었다고 주방에 돌려보냈는데 5분 뒤 같은 음식 그대로 다시 나오는 격. 시간만 낭비하고 결과는 같음.

<b>3. 현재 시스템의 이탈 룰이 이미 빠름</b>
• 순위 8위 밖 → 즉시 매도
• EPS 4구간 중 하나라도 -2% 악화 → 매도
• MA120 이탈, 매출 둔화, FCF·ROE 음수 등

이 룰들이 <b>Trailing/Stop이 발동하기 전에 먼저 작동</b>해서 추가 안전장치가 불필요.

━━━━━━━━━━━━━━━
<b>⚠️ 실거래에선 더 나쁨</b>
━━━━━━━━━━━━━━━

백테스트엔 반영 안 된 비용:
• 매도/재매수 = 수수료 2번
• 슬리피지 (호가 차이)
• 단기 양도세

Trailing/Stop이 발동될수록 거래 횟수가 늘어나는데 이 비용 모두 손실. <b>백테스트 +0%로 나온 변형도 실거래에선 마이너스</b>.

━━━━━━━━━━━━━━━
<b>✅ 최종 결정</b>
━━━━━━━━━━━━━━━

<b>production 변경 0건</b> — 기존 이탈 룰 그대로 유지.

근거 3가지:
1. 시스템 이탈 룰이 이미 빠르고 효과적
2. Trailing/Stop 발동해도 재진입으로 net 효과 0
3. 빠듯한 Trailing(-5%)은 정상 변동성에 걸려 손실

이전 검증(MEMORY.md)에서도 동일 결론 확인됨:
"역변동성 비중 채택, trailing stop / VIX regime / correlation / portfolio DD 모두 기각"

━━━━━━━━━━━━━━━
<b>📁 산출물</b>
━━━━━━━━━━━━━━━
• research/stop_test.py (BT 스크립트)
• research/stop_loss_results.md (상세 분석)
• 소요: 0.3초 (캐시 재사용)
"""

# 전송
import urllib.request, urllib.parse
bot = config['telegram_bot_token']
chat = config['telegram_chat_id']  # 개인봇 ID

# 4000자 분할
chunks = []
remaining = msg.strip()
while remaining:
    if len(remaining) <= 4000:
        chunks.append(remaining); break
    sp = remaining[:4000].rfind('\n')
    if sp <= 0: sp = 4000
    chunks.append(remaining[:sp])
    remaining = remaining[sp:].strip()

print(f"메시지 {len(chunks)}개 청크로 분할")

for i, chunk in enumerate(chunks):
    url = f"https://api.telegram.org/bot{bot}/sendMessage"
    data = urllib.parse.urlencode({
        'chat_id': chat, 'text': chunk, 'parse_mode': 'HTML'
    }).encode()
    req = urllib.request.Request(url, data=data)
    resp = urllib.request.urlopen(req, timeout=10)
    print(f"  청크 {i+1}/{len(chunks)} 전송 완료 (status {resp.status})")

print("✅ 텔레그램 전송 완료")
