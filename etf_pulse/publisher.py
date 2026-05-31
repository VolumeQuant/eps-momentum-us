"""ETF Pulse Publisher — Markdown → 텔레그램/이메일/Substack

지원 채널:
  - 텔레그램 (즉시, 무료, retail 친화)
  - 파일 저장 (Substack 수동 복사)
  - 이메일 (선택, SendGrid 등)
"""
import sys
import json
import urllib.request
import urllib.parse
from pathlib import Path
import re

sys.stdout.reconfigure(encoding='utf-8')


def md_to_telegram_html(md):
    """Markdown → 텔레그램 HTML (간단 변환)"""
    text = md
    # H1 - H3
    text = re.sub(r'^#{1,3}\s*(.+)$', r'<b>\1</b>', text, flags=re.MULTILINE)
    # **bold**
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    # `code`
    text = re.sub(r'`(.+?)`', r'<code>\1</code>', text)
    # _italic_
    text = re.sub(r'(?<!\w)_([^_\n]+)_(?!\w)', r'<i>\1</i>', text)
    # 수평선
    text = text.replace('---', '━━━━━━━━━━━━━━━')
    # 빈 줄 정리 (3개 이상 → 2개)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text


def send_telegram(text, bot_token, chat_id, parse_mode='HTML'):
    """텔레그램 메시지 전송 (긴 메시지는 분할)"""
    if not bot_token or not chat_id:
        return False, 'token/chat_id 없음'

    MAX_LEN = 4000
    chunks = []
    if len(text) <= MAX_LEN:
        chunks = [text]
    else:
        # 단락 단위로 자르기
        paras = text.split('\n\n')
        cur = ''
        for p in paras:
            if len(cur) + len(p) + 2 > MAX_LEN:
                if cur: chunks.append(cur)
                cur = p
            else:
                cur = cur + '\n\n' + p if cur else p
        if cur:
            chunks.append(cur)

    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    for i, chunk in enumerate(chunks):
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': chunk,
            'parse_mode': parse_mode,
            'disable_web_page_preview': 'true',
        }).encode('utf-8')
        try:
            req = urllib.request.Request(url, data=data, method='POST')
            with urllib.request.urlopen(req, timeout=10) as r:
                resp = json.loads(r.read())
                if not resp.get('ok'):
                    return False, f'chunk {i+1}/{len(chunks)}: {resp}'
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode('utf-8')
            except: err_body = ''
            return False, f'HTTP {e.code}: {err_body[:200]}'
        except Exception as e:
            return False, f'{type(e).__name__}: {e}'
    return True, f'{len(chunks)} chunks 전송'


def save_for_substack(md_content, date_str):
    """Substack 수동 발행용 — Markdown + meta 정보"""
    out_dir = Path(__file__).parent / 'content'
    out_dir.mkdir(exist_ok=True)

    # 원본 .md
    md_file = out_dir / f'pulse_{date_str}.md'
    md_file.write_text(md_content, encoding='utf-8')

    # Substack-friendly 형식 (제목 + 본문 분리)
    lines = md_content.split('\n')
    title = lines[0].replace('# ', '').strip() if lines and lines[0].startswith('# ') else f'ETF Pulse {date_str}'
    body = '\n'.join(lines[1:]).strip()

    meta = {
        'title': title,
        'date': date_str,
        'body_md_path': str(md_file),
        'word_count': len(md_content.split()),
        'char_count': len(md_content),
    }
    meta_file = out_dir / f'pulse_{date_str}.meta.json'
    meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')

    return md_file, meta_file


def load_config():
    """텔레그램 봇 토큰 등 config 로드"""
    # 1) etf_pulse/config.json 시도
    cfg_path = Path(__file__).parent / 'config.json'
    if cfg_path.exists():
        return json.loads(cfg_path.read_text(encoding='utf-8'))
    # 2) 부모 디렉토리 config.json (eps-momentum-us의 config)
    parent_cfg = Path(__file__).parent.parent / 'config.json'
    if parent_cfg.exists():
        cfg = json.loads(parent_cfg.read_text(encoding='utf-8'))
        return {
            'telegram_bot_token': cfg.get('telegram_bot_token', ''),
            'telegram_chat_id': cfg.get('telegram_chat_id', ''),
        }
    return {}


if __name__ == '__main__':
    import sys
    # 최신 콘텐츠 파일 찾기
    out_dir = Path(__file__).parent / 'content'
    if not out_dir.exists():
        print('content/ 없음 — narrative.py 먼저 실행')
        sys.exit(1)

    md_files = sorted(out_dir.glob('pulse_*.md'), reverse=True)
    if not md_files:
        print('pulse_*.md 없음')
        sys.exit(1)

    latest = md_files[0]
    md = latest.read_text(encoding='utf-8')
    print(f'발행 대상: {latest.name} ({len(md)} chars)')

    # 1. Substack용 파일 저장
    date_match = re.search(r'pulse_(\d{4}-\d{2}-\d{2})', latest.name)
    date_str = date_match.group(1) if date_match else 'unknown'
    md_file, meta_file = save_for_substack(md, date_str)
    print(f'\n[Substack 발행용 저장]')
    print(f'  md:   {md_file}')
    print(f'  meta: {meta_file}')

    # 2. 텔레그램 발행 시도
    print(f'\n[텔레그램 발행 시도]')
    cfg = load_config()
    if cfg.get('telegram_bot_token'):
        html = md_to_telegram_html(md)
        ok, msg = send_telegram(html, cfg['telegram_bot_token'], cfg.get('telegram_chat_id'))
        print(f'  {"✓ 성공" if ok else "✗ 실패"}: {msg}')
    else:
        print('  config 없음 — 수동 발행 필요')

    # 3. Substack 발행 안내
    print(f'\n[Substack 발행 방법]')
    print(f'  1. https://substack.com 에서 새 글 작성')
    print(f'  2. {md_file} 내용 복사 → 붙여넣기')
    print(f'  3. "Subscribers" 또는 "Free" 발행')
    print(f'  (자동 발행은 Substack API 키 필요 — phase 2)')
