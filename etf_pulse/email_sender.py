"""ETF Pulse Email Sender — Gmail SMTP 발행

사용자 본인 Gmail로 본인/구독자에게 자동 발행.
config.json:
  {
    "gmail_user": "your.email@gmail.com",
    "gmail_app_password": "16-digit app password",  # Gmail "앱 비밀번호"
    "to_emails": ["self@email.com", "subscriber@email.com"]
  }

Gmail 앱 비밀번호 생성:
  myaccount.google.com → 보안 → 2단계 인증 → 앱 비밀번호
"""
import sys
import json
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
import re

sys.stdout.reconfigure(encoding='utf-8')


def md_to_html(md):
    """Markdown → 간단 HTML (Gmail 친화)"""
    html = md
    # H1
    html = re.sub(r'^# (.+)$', r'<h1 style="color:#1a1a1a;border-bottom:2px solid #333;padding-bottom:8px;">\1</h1>', html, flags=re.MULTILINE)
    # H2
    html = re.sub(r'^## (.+)$', r'<h2 style="color:#2a2a2a;margin-top:24px;">\1</h2>', html, flags=re.MULTILINE)
    # H3
    html = re.sub(r'^### (.+)$', r'<h3 style="color:#3a3a3a;">\1</h3>', html, flags=re.MULTILINE)
    # **bold**
    html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html)
    # _italic_
    html = re.sub(r'(?<!\w)_([^_\n]+)_(?!\w)', r'<em>\1</em>', html)
    # 리스트
    lines = html.split('\n')
    out = []
    in_list = False
    for line in lines:
        if re.match(r'^[\s]*[-*]\s+', line):
            if not in_list:
                out.append('<ul style="line-height:1.6;">')
                in_list = True
            content = re.sub(r'^[\s]*[-*]\s+', '', line)
            out.append(f'  <li>{content}</li>')
        else:
            if in_list:
                out.append('</ul>')
                in_list = False
            if line.strip() == '---':
                out.append('<hr style="border:none;border-top:1px solid #ccc;margin:20px 0;">')
            elif line.strip():
                out.append(f'<p style="line-height:1.6;">{line}</p>')
            else:
                out.append('<br>')
    if in_list:
        out.append('</ul>')
    body = '\n'.join(out)
    return f'''<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; color: #1a1a1a;">
{body}
</body></html>'''


def send_email(subject, md_content, cfg, dry_run=False):
    user = cfg.get('gmail_user')
    pw = cfg.get('gmail_app_password')
    to_emails = cfg.get('to_emails', [user])
    if not user or not pw:
        return False, 'gmail_user/gmail_app_password 없음 (config.json 확인)'

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f'ETF Pulse <{user}>'
    msg['To'] = ', '.join(to_emails)

    text_part = MIMEText(md_content, 'plain', 'utf-8')
    html_part = MIMEText(md_to_html(md_content), 'html', 'utf-8')
    msg.attach(text_part)
    msg.attach(html_part)

    if dry_run:
        print(f'[DRY RUN] to={to_emails}, subject={subject}, len={len(md_content)}')
        return True, 'dry run OK'

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=ctx) as server:
            server.login(user, pw)
            server.send_message(msg)
        return True, f'{len(to_emails)} 명에게 전송'
    except Exception as e:
        return False, f'{type(e).__name__}: {e}'


def load_email_config():
    """이메일 config 로드"""
    cfg_path = Path(__file__).parent / 'config.json'
    if cfg_path.exists():
        return json.loads(cfg_path.read_text(encoding='utf-8'))
    # 부모 디렉토리 fallback
    parent_cfg = Path(__file__).parent.parent / 'config.json'
    if parent_cfg.exists():
        cfg = json.loads(parent_cfg.read_text(encoding='utf-8'))
        return {
            'gmail_user': cfg.get('gmail_user', ''),
            'gmail_app_password': cfg.get('gmail_app_password', ''),
            'to_emails': cfg.get('to_emails', []),
        }
    return {}


if __name__ == '__main__':
    # 최신 콘텐츠 파일 찾기
    out_dir = Path(__file__).parent / 'content'
    md_files = sorted(out_dir.glob('pulse_*.md'), reverse=True)
    if not md_files:
        print('content/pulse_*.md 없음')
        sys.exit(1)

    md = md_files[0].read_text(encoding='utf-8')
    date_str = md_files[0].stem.replace('pulse_', '')
    subject = f'🌅 ETF Pulse — {date_str}'

    cfg = load_email_config()
    print(f'config: gmail_user={cfg.get("gmail_user", "<none>")}')
    print(f'to_emails: {cfg.get("to_emails", [])}')

    # 1. 기본은 dry run (config 검증)
    ok, msg = send_email(subject, md, cfg, dry_run=True)
    print(f'\nDRY RUN: {"✓" if ok else "✗"} {msg}')

    # 2. 실제 전송 (config 있으면)
    if cfg.get('gmail_app_password'):
        print('\n실제 전송 시도...')
        ok, msg = send_email(subject, md, cfg, dry_run=False)
        print(f'{"✓ 성공" if ok else "✗ 실패"}: {msg}')
    else:
        print('\n실제 전송 skip — gmail_app_password 없음')
        print('  config.json에 gmail_user + gmail_app_password 추가하면 자동 전송')
