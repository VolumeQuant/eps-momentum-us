import requests
import json
import os

os.chdir(r'C:\dev\claude code\eps-momentum-us')

BOT_TOKEN = '7948087946:AAGVHj7FdBxr0LRJzQTzfEp0HadzAtoXs-8'
CHAT_ID = '7580571403'

# Load data
with open('claude_top10_comprehensive.json', 'r', encoding='utf-8') as f:
    top10 = json.load(f)

# Rank emojis
rank_emoji = {1: '🥇', 2: '🥈', 3: '🥉', 4: '4️⃣', 5: '5️⃣', 6: '6️⃣', 7: '7️⃣', 8: '8️⃣', 9: '9️⃣', 10: '🔟'}

# Action mapping based on RSI and 52w high
def get_action(rsi, from_52w):
    if rsi >= 60 or from_52w > -5:
        return '🚀돌파매수'
    else:
        return '🛡️분할매수'

def get_action_reason(rsi, from_52w):
    if rsi >= 60:
        return f'RSI{rsi:.0f}≥60'
    else:
        return f'RSI{rsi:.0f}, 고점{from_52w:.0f}%'

msg = """🇺🇸 EPS Momentum v7.0.6
━━━━━━━━━━━━━━━━━━━━━━
📅 2026-02-05 | 🔴 RED 시장
📊 917개 중 37개 통과 (4.0%)
━━━━━━━━━━━━━━━━━━━━━━

📋 점수 계산 기준

💎 밸류 (펀더멘털 100점)
• EPS 모멘텀 30점: 애널리스트 상향
• ROE 25점: 자기자본이익률
• EPS 성장률 20점: 이익 증가율
• 추세 15점: MA200 위 여부
• 거래량 10점: 수급 강도

💵 가격 (타이밍 100점)
• PEG 35점: 성장 대비 주가
• Forward PER 25점: 미래 이익 대비
• 52주고점 25점: 조정폭 (싼지 비싼지)
• RSI 15점: 과매도/과매수

🎯 액션 구분
• 🛡️분할매수: RSI<60 + 고점에서 -5%↓ 조정
  → 아직 덜 올랐음, 3번 나눠서 진입
• 🚀돌파매수: RSI≥60 OR 고점 근처(-5%이내)
  → 이미 상승중, 돌파 확인 후 진입
━━━━━━━━━━━━━━━━━━━━━━

🏆 TOP 10

"""

# Data from screening CSV (manually extracted key metrics)
stock_data = [
    {'ticker': 'AVGO', 'price': 308.05, 'chg': -3.8, 'score': 73.3, 'quality': 77, 'value': 70, 'rsi': 30.8, 'high': -25.6, 'eps_chg': 13.3, 'roe': 31.1, 'vol': 1.6, 'news': '메모리 업체 주가, AI 붐에 1000% 상승'},
    {'ticker': 'NEM', 'price': 116.85, 'chg': -0.2, 'score': 71.5, 'quality': 63, 'value': 80, 'rsi': 53.2, 'high': -13.4, 'eps_chg': 19.6, 'roe': 22.9, 'vol': 1.0, 'news': 'Zacks 분석가: Agnico Eagle, Kinross Gold, Newmont 강조'},
    {'ticker': 'LITE', 'price': 465.54, 'chg': 7.0, 'score': 71.0, 'quality': 77, 'value': 52, 'rsi': 78.0, 'high': -7.7, 'eps_chg': 60.6, 'roe': 13.4, 'vol': 2.3, 'news': '투자자들이 AI 거래의 확대되는 에너지 수요를 활용하는 방법'},
    {'ticker': 'CMC', 'price': 82.70, 'chg': 1.0, 'score': 69.7, 'quality': 67, 'value': 60, 'rsi': 71.7, 'high': -1.7, 'eps_chg': 38.2, 'roe': 10.5, 'vol': 1.0, 'news': 'Carpenter Technology 2분기 실적, 예상치 상회'},
    {'ticker': 'LRCX', 'price': 209.78, 'chg': -8.8, 'score': 66.7, 'quality': 73, 'value': 60, 'rsi': 50.5, 'high': -16.7, 'eps_chg': 21.6, 'roe': 65.6, 'vol': 1.4, 'news': 'Jim Cramer: Lam Research 4분의 1 포지션 진입 권장'},
    {'ticker': 'STX', 'price': 418.63, 'chg': -5.8, 'score': 63.3, 'quality': 55, 'value': 60, 'rsi': 72.4, 'high': -8.9, 'eps_chg': 30.2, 'roe': 0, 'vol': 1.2, 'news': 'MACOM, Seagate, Semtech 주가 급락 분석'},
    {'ticker': 'KLAC', 'price': 1307.22, 'chg': -3.6, 'score': 56.4, 'quality': 71, 'value': 42, 'rsi': 42.3, 'high': -22.8, 'eps_chg': 8.9, 'roe': 100.7, 'vol': 1.2, 'news': 'KLA 국제 수익 동향 주목'},
    {'ticker': 'MU', 'price': 379.40, 'chg': -9.5, 'score': 56.0, 'quality': 85, 'value': 75, 'rsi': 60.2, 'high': -16.7, 'eps_chg': 117.8, 'roe': 22.6, 'vol': 1.5, 'news': '메모리 업체 주가, AI 붐에 1000% 상승'},
    {'ticker': 'RMD', 'price': 263.03, 'chg': 4.6, 'score': 49.6, 'quality': 59, 'value': 40, 'rsi': 53.8, 'high': -10.3, 'eps_chg': 1.4, 'roe': 25.7, 'vol': 1.6, 'news': '실버 경제 붐: 헬스케어 노령화 순풍 투자'},
    {'ticker': 'AMGN', 'price': 366.20, 'chg': 8.2, 'score': 46.4, 'quality': 64, 'value': 20, 'rsi': 75.0, 'high': -0.7, 'eps_chg': 1.8, 'roe': 81.7, 'vol': 2.0, 'news': '다우존스 선물 상승; Google AI 지출 보고'},
]

for i, s in enumerate(stock_data, 1):
    action = get_action(s['rsi'], s['high'])
    action_reason = get_action_reason(s['rsi'], s['high'])

    # Build insight line
    insights = []
    insights.append(f"EPS정배열+{s['eps_chg']:.0f}%")
    if s['roe'] >= 25:
        insights.append(f"ROE{s['roe']:.0f}%↑")
    if s['rsi'] <= 35:
        insights.append(f"RSI{s['rsi']:.0f}과매도")
    elif s['rsi'] >= 70:
        insights.append(f"RSI{s['rsi']:.0f}과열")
    if s['high'] <= -15:
        insights.append(f"고점{s['high']:.0f}%할인")
    if s['vol'] >= 1.5:
        insights.append(f"거래량{s['vol']:.1f}배")
    if s['chg'] >= 5:
        insights.append(f"오늘+{s['chg']:.0f}%")
    elif s['chg'] <= -5:
        insights.append(f"오늘{s['chg']:.0f}%")

    insight_str = ' | '.join(insights[:4])  # Max 4 insights

    msg += f"""{rank_emoji[i]} {s['ticker']} ${s['price']:.0f} ({s['chg']:+.1f}%)
   [{action}] {s['score']:.1f}점
   • 💎밸류: {s['quality']}점 | 💵가격: {s['value']}점
   • 액션근거: {action_reason}
   💡 {insight_str}
   📰 {s['news'][:45]}...

"""

msg += """━━━━━━━━━━━━━━━━━━━━━━
🤖 Claude 추천 (RED시장)
🥇 NEM - 금광=안전자산, RSI53 적정
🥈 AVGO - RSI31 과매도 급락매수
🥉 RMD - 헬스케어 방어주, 유일상승

⚠️ 주의: LITE/CMC/STX/AMGN (RSI70+)
━━━━━━━━━━━━━━━━━━━━━━
🤖 EPS Momentum v7.0.6"""

url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
resp = requests.post(url, json={'chat_id': CHAT_ID, 'text': msg})
print(f'Status: {resp.status_code}')
if resp.status_code != 200:
    print(resp.json())
else:
    print('Message sent successfully!')
