"""공지사항 전송 — 채널 고정용 시스템 안내 메시지"""
import os
import urllib.request
import urllib.parse
import json

BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

if not BOT_TOKEN or not CHAT_ID:
    print('TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 환경변수 필요')
    exit(1)

MSG = """📡 <b>AI 종목 브리핑 US</b> — 서비스 안내

월가 애널리스트의 이익 전망 변화를 추적해
유망 미국 주식을 매일 선별해 드리는 서비스예요.

━━━━━━━━━━━━━━━
<b>📌 작동 방식</b>

매일 아침 3개 메시지가 발송돼요.

<b>1️⃣ Signal</b> — 오늘 뭘 살까
미국 916개 종목의 EPS 전망 변화를 추적해요.
애널리스트가 실적을 올려잡는데
주가가 아직 덜 반영된 종목을 찾아내요.
3일 연속 상위권 유지 종목만 최종 추천해요.

<b>2️⃣ AI Risk</b> — 시장은 어떤가
S&P 500, 나스닥, 신용시장(HY), VIX를
종합 분석하고 AI가 시장 동향을 요약해요.
매수 주의 종목도 알려드려요.

<b>3️⃣ Watchlist</b> — 내 종목은 괜찮은가
상위 30개 종목의 순위 변화를 추적해요.
✅ 3일 검증 = 매수 대상
⏳ 2일째 관찰 중
🆕 오늘 첫 진입
빠진 종목은 매도 검토 알림을 드려요.

━━━━━━━━━━━━━━━
<b>💡 한 줄 원칙</b>
"목록에 있으면 보유, 빠지면 매도 검토"

━━━━━━━━━━━━━━━
⚠️ 본 서비스는 투자 참고용이며,
최종 투자 판단과 책임은 본인에게 있어요."""

url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
data = urllib.parse.urlencode({
    'chat_id': CHAT_ID,
    'text': MSG,
    'parse_mode': 'HTML',
}).encode()

req = urllib.request.Request(url, data=data)
resp = urllib.request.urlopen(req, timeout=10)
result = json.loads(resp.read())
msg_id = result['result']['message_id']
print(f'전송 완료: message_id={msg_id}')

# 메시지 고정
pin_url = f'https://api.telegram.org/bot{BOT_TOKEN}/pinChatMessage'
pin_data = urllib.parse.urlencode({
    'chat_id': CHAT_ID,
    'message_id': msg_id,
}).encode()
pin_req = urllib.request.Request(pin_url, data=pin_data)
pin_resp = urllib.request.urlopen(pin_req, timeout=10)
print(f'고정 완료')
