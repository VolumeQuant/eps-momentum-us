import requests
import json
import os

os.chdir(r'C:\dev\claude code\eps-momentum-us')

BOT_TOKEN = '7948087946:AAGVHj7FdBxr0LRJzQTzfEp0HadzAtoXs-8'
CHAT_ID = '7580571403'

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

# 11-20위 데이터
stocks_11_20 = [
    {'rank': 11, 'ticker': 'FTI', 'sector': '에너지장비', 'price': 56.54, 'chg': 0.0, 'score': 43.6, 'quality': 59, 'value': 20, 'rsi': 77.4, 'high': -1.9, 'eps_chg': 1.7, 'roe': 29.3},
    {'rank': 12, 'ticker': 'CACI', 'sector': 'IT서비스', 'price': 605.08, 'chg': 0.0, 'score': 42.1, 'quality': 44, 'value': 40, 'rsi': 47.7, 'high': -11.5, 'eps_chg': 1.2, 'roe': 13.2},
    {'rank': 13, 'ticker': 'RGLD', 'sector': '금로열티', 'price': 265.89, 'chg': 0.0, 'score': 42.0, 'quality': 57, 'value': 63, 'rsi': 55.8, 'high': -13.2, 'eps_chg': 14.6, 'roe': 11.0},
    {'rank': 14, 'ticker': 'CRS', 'sector': '특수금속', 'price': 334.19, 'chg': 0.0, 'score': 41.5, 'quality': 65, 'value': 27, 'rsi': 54.4, 'high': -5.9, 'eps_chg': 6.4, 'roe': 23.5},
    {'rank': 15, 'ticker': 'FDX', 'sector': '물류', 'price': 362.54, 'chg': 0.0, 'score': 41.0, 'quality': 55, 'value': 20, 'rsi': 78.7, 'high': -0.8, 'eps_chg': 2.7, 'roe': 15.9},
    {'rank': 16, 'ticker': 'HII', 'sector': '방산', 'price': 413.14, 'chg': 0.0, 'score': 40.2, 'quality': 54, 'value': 35, 'rsi': 48.7, 'high': -5.3, 'eps_chg': 1.8, 'roe': 12.4},
    {'rank': 17, 'ticker': 'F', 'sector': '자동차', 'price': 13.82, 'chg': 0.0, 'score': 39.7, 'quality': 51, 'value': 63, 'rsi': 49.6, 'high': -4.7, 'eps_chg': 7.6, 'roe': 10.3},
    {'rank': 18, 'ticker': 'ATI', 'sector': '특수금속', 'price': 127.50, 'chg': 0.0, 'score': 39.3, 'quality': 65, 'value': 22, 'rsi': 60.0, 'high': -6.9, 'eps_chg': 6.6, 'roe': 21.6},
    {'rank': 19, 'ticker': 'CTSH', 'sector': 'IT서비스', 'price': 76.71, 'chg': 0.0, 'score': 35.4, 'quality': 54, 'value': 47, 'rsi': 29.0, 'high': -14.2, 'eps_chg': 0.7, 'roe': 14.5},
    {'rank': 20, 'ticker': 'INCY', 'sector': '바이오', 'price': 102.60, 'chg': 0.0, 'score': 34.8, 'quality': 54, 'value': 45, 'rsi': 42.3, 'high': -8.6, 'eps_chg': 2.0, 'roe': 30.4},
]

# 21-30위 데이터
stocks_21_30 = [
    {'rank': 21, 'ticker': 'FIVE', 'sector': '소매', 'price': 192.93, 'chg': 0.0, 'score': 34.4, 'quality': 56, 'value': 42, 'rsi': 44.3, 'high': -6.0, 'eps_chg': 11.5, 'roe': 17.3},
    {'rank': 22, 'ticker': 'JBL', 'sector': '전자부품', 'price': 236.06, 'chg': 0.0, 'score': 33.3, 'quality': 60, 'value': 35, 'rsi': 45.8, 'high': -8.5, 'eps_chg': 5.1, 'roe': 47.8},
    {'rank': 23, 'ticker': 'TPR', 'sector': '명품패션', 'price': 129.92, 'chg': 0.0, 'score': 31.4, 'quality': 55, 'value': 35, 'rsi': 44.0, 'high': -4.5, 'eps_chg': 2.8, 'roe': 16.1},
    {'rank': 24, 'ticker': 'GMED', 'sector': '의료기기', 'price': 87.06, 'chg': 0.0, 'score': 30.3, 'quality': 45, 'value': 42, 'rsi': 30.0, 'high': -14.1, 'eps_chg': 3.3, 'roe': 10.0},
    {'rank': 25, 'ticker': 'CVNA', 'sector': '중고차', 'price': 393.04, 'chg': 0.0, 'score': 29.5, 'quality': 54, 'value': 30, 'rsi': 34.0, 'high': -19.3, 'eps_chg': 1.1, 'roe': 68.2},
    {'rank': 26, 'ticker': 'DRI', 'sector': '외식', 'price': 212.22, 'chg': 0.0, 'score': 29.4, 'quality': 54, 'value': 30, 'rsi': 55.0, 'high': -4.9, 'eps_chg': 0.1, 'roe': 54.1},
    {'rank': 27, 'ticker': 'LLY', 'sector': '제약', 'price': 1107.12, 'chg': 0.0, 'score': 28.7, 'quality': 65, 'value': 17, 'rsi': 55.2, 'high': -2.4, 'eps_chg': 4.6, 'roe': 96.5},
    {'rank': 28, 'ticker': 'CBOE', 'sector': '거래소', 'price': 271.20, 'chg': 0.0, 'score': 27.9, 'quality': 50, 'value': 30, 'rsi': 53.5, 'high': -2.9, 'eps_chg': 3.9, 'roe': 21.6},
    {'rank': 29, 'ticker': 'CCL', 'sector': '크루즈', 'price': 32.09, 'chg': 0.0, 'score': 27.8, 'quality': 49, 'value': 30, 'rsi': 60.0, 'high': -3.2, 'eps_chg': 2.3, 'roe': 25.6},
    {'rank': 30, 'ticker': 'ROK', 'sector': '산업자동화', 'price': 429.84, 'chg': 0.0, 'score': 26.6, 'quality': 59, 'value': 17, 'rsi': 57.5, 'high': -2.0, 'eps_chg': 0.2, 'roe': 20.3},
]

# 31-37위 데이터
stocks_31_37 = [
    {'rank': 31, 'ticker': 'CAH', 'sector': '의약품유통', 'price': 206.85, 'chg': 0.0, 'score': 26.0, 'quality': 34, 'value': 40, 'rsi': 41.2, 'high': -6.1, 'eps_chg': 2.0, 'roe': 0},
    {'rank': 32, 'ticker': 'DGX', 'sector': '진단검사', 'price': 189.23, 'chg': 0.0, 'score': 25.9, 'quality': 44, 'value': 30, 'rsi': 54.3, 'high': -3.8, 'eps_chg': 0.4, 'roe': 14.3},
    {'rank': 33, 'ticker': 'PH', 'sector': '산업장비', 'price': 967.99, 'chg': 0.0, 'score': 23.3, 'quality': 50, 'value': 17, 'rsi': 60.2, 'high': -1.2, 'eps_chg': 2.8, 'roe': 25.8},
    {'rank': 34, 'ticker': 'ROL', 'sector': '해충방제', 'price': 63.51, 'chg': 0.0, 'score': 22.4, 'quality': 54, 'value': 10, 'rsi': 64.8, 'high': -0.5, 'eps_chg': 0.5, 'roe': 36.2},
    {'rank': 35, 'ticker': 'CASY', 'sector': '편의점', 'price': 647.76, 'chg': 0.0, 'score': 21.7, 'quality': 45, 'value': 17, 'rsi': 64.9, 'high': -1.6, 'eps_chg': 4.3, 'roe': 17.0},
    {'rank': 36, 'ticker': 'WDC', 'sector': '저장장치', 'price': 269.41, 'chg': 0.0, 'score': 20.4, 'quality': 76, 'value': 60, 'rsi': 66.0, 'high': -9.1, 'eps_chg': 35.6, 'roe': 41.1},
    {'rank': 37, 'ticker': 'WTS', 'sector': '산업장비', 'price': 306.91, 'chg': 0.0, 'score': 19.7, 'quality': 44, 'value': 12, 'rsi': 65.0, 'high': -1.5, 'eps_chg': 1.0, 'roe': 17.8},
]

def build_stock_line(s):
    action = get_action(s['rsi'], s['high'])

    insights = []
    insights.append(f"EPS+{s['eps_chg']:.0f}%")
    if s['roe'] >= 25:
        insights.append(f"ROE{s['roe']:.0f}%↑")
    if s['rsi'] <= 35:
        insights.append(f"RSI{s['rsi']:.0f}과매도")
    elif s['rsi'] >= 70:
        insights.append(f"RSI{s['rsi']:.0f}과열")
    if s['high'] <= -15:
        insights.append(f"고점{s['high']:.0f}%")

    insight_str = ' | '.join(insights[:3])

    return f"""{s['rank']}. {s['ticker']} ({s['sector']}) ${s['price']:.0f}
   [{action}] {s['score']:.1f}점 | 💎{s['quality']} 💵{s['value']}
   💡 {insight_str}
"""

# Message 1: 11-20위
msg1 = """📊 11-20위 종목
━━━━━━━━━━━━━━━━━━━━━━

"""
for s in stocks_11_20:
    msg1 += build_stock_line(s)
    msg1 += "\n"

msg1 += """━━━━━━━━━━━━━━━━━━━━━━
💡 주목: CTSH RSI29 과매도
🤖 EPS Momentum v7.0.6"""

# Message 2: 21-30위
msg2 = """📊 21-30위 종목
━━━━━━━━━━━━━━━━━━━━━━

"""
for s in stocks_21_30:
    msg2 += build_stock_line(s)
    msg2 += "\n"

msg2 += """━━━━━━━━━━━━━━━━━━━━━━
💡 주목: LLY ROE97% | GMED RSI30
🤖 EPS Momentum v7.0.6"""

# Message 3: 31-37위
msg3 = """📊 31-37위 종목
━━━━━━━━━━━━━━━━━━━━━━

"""
for s in stocks_31_37:
    msg3 += build_stock_line(s)
    msg3 += "\n"

msg3 += """━━━━━━━━━━━━━━━━━━━━━━
💡 주목: WDC 밸류76점 A급
🤖 EPS Momentum v7.0.6"""

url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'

resp1 = requests.post(url, json={'chat_id': CHAT_ID, 'text': msg1})
resp2 = requests.post(url, json={'chat_id': CHAT_ID, 'text': msg2})
resp3 = requests.post(url, json={'chat_id': CHAT_ID, 'text': msg3})

print(f'11-20: {resp1.status_code}')
print(f'21-30: {resp2.status_code}')
print(f'31-37: {resp3.status_code}')
