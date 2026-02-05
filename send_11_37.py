import requests
import json
import os

os.chdir(r'C:\dev\claude code\eps-momentum-us')

# Load 11-37 analysis
with open('claude_rank11_37_analysis.json', 'r', encoding='utf-8') as f:
    stocks = json.load(f)

BOT_TOKEN = '7948087946:AAGVHj7FdBxr0LRJzQTzfEp0HadzAtoXs-8'
CHAT_ID = '7580571403'

action_map = {
    '대기': 'HOLD',
    '적극매수': 'BUY',
    '매수적기': 'BUY',
    '신고가추격': 'BREAKOUT',
    '진입금지': 'AVOID'
}

# Message for 11-24
msg1 = '[11-24위 종목 분석]\n\n'
for s in stocks[:14]:
    act = action_map.get(s['action'], s['action'])
    msg1 += f"{s['rank']}. {s['ticker']} ({s['sector']}) [{act}]\n"
    msg1 += f"   ${s['price']:.2f} | RSI {s['rsi']:.0f} | 52w {s['from_52w_high']:.1f}%\n"
    msg1 += f"   {s['선정이유'][:50]}...\n\n"

# Message for 25-37
msg2 = '[25-37위 종목 분석]\n\n'
for s in stocks[14:]:
    act = action_map.get(s['action'], s['action'])
    msg2 += f"{s['rank']}. {s['ticker']} ({s['sector']}) [{act}]\n"
    msg2 += f"   ${s['price']:.2f} | RSI {s['rsi']:.0f} | 52w {s['from_52w_high']:.1f}%\n"
    msg2 += f"   {s['선정이유'][:50]}...\n\n"

url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
resp1 = requests.post(url, json={'chat_id': CHAT_ID, 'text': msg1})
resp2 = requests.post(url, json={'chat_id': CHAT_ID, 'text': msg2})
print(f'11-24: {resp1.status_code}, 25-37: {resp2.status_code}')
