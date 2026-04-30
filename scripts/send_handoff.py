"""핸드오프 메시지를 텔레그램 개인봇으로 전송.

GitHub Actions에서 환경변수 TELEGRAM_BOT_TOKEN + TELEGRAM_PRIVATE_ID 사용.
"""
import os
import sys
import urllib.request
import urllib.parse
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')


def send_telegram_chunks(bot_token, chat_id, message):
    """4000자씩 분할 전송."""
    chunks = []
    remaining = message.strip()
    while remaining:
        if len(remaining) <= 4000:
            chunks.append(remaining)
            break
        split = remaining[:4000].rfind('\n')
        if split <= 0:
            split = 4000
        chunks.append(remaining[:split])
        remaining = remaining[split:].strip()

    for i, chunk in enumerate(chunks):
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': chunk,
            'parse_mode': 'HTML',
            'disable_web_page_preview': 'true',
        }).encode()
        req = urllib.request.Request(url, data=data)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                code = resp.getcode()
                print(f'  chunk {i+1}/{len(chunks)} ({len(chunk)} chars) → HTTP {code}')
        except Exception as e:
            print(f'  chunk {i+1} 실패: {e}')
            return False
    return True


def main():
    token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    chat = os.environ.get('TELEGRAM_PRIVATE_ID', '')
    if not token or not chat:
        print('❌ TELEGRAM_BOT_TOKEN or TELEGRAM_PRIVATE_ID 미설정')
        sys.exit(1)

    md_path = Path(__file__).parent.parent / 'research' / 'handoff_message.md'
    if not md_path.exists():
        print(f'❌ {md_path} not found')
        sys.exit(1)

    message = md_path.read_text(encoding='utf-8')
    print(f'메시지 길이: {len(message)} chars')
    ok = send_telegram_chunks(token, chat, message)
    if ok:
        print('✅ 전송 완료')
    else:
        print('❌ 전송 실패')
        sys.exit(1)


if __name__ == '__main__':
    main()
