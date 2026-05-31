"""ETF Pulse Substack 자동 발행 — 이메일 to Substack publishing

Substack은 공식 API 없지만, "send by email to publish" 기능 제공:
1. Substack settings → "Post by email" 활성화
2. unique email 주소 받음 (publish-XXXX@substack.com)
3. 그 주소로 이메일 보내면 자동으로 draft 생성 또는 발행

Subject = post title
Body (HTML) = post content
"""
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))
from email_sender import send_email, load_email_config, md_to_html
import re


def publish_to_substack(md_content, date_str, substack_email):
    """Substack 이메일로 발송 (자동 draft 생성)"""
    cfg = load_email_config()
    if not cfg.get('gmail_app_password'):
        return False, 'Gmail config 없음'

    # 제목 추출 (첫 H1)
    first_line = md_content.split('\n')[0]
    title = re.sub(r'^#\s*', '', first_line) if first_line.startswith('#') else f'ETF Pulse {date_str}'

    # body는 H1 제거한 나머지
    body_md = '\n'.join(md_content.split('\n')[1:]).strip()

    cfg_to = dict(cfg)
    cfg_to['to_emails'] = [substack_email]

    return send_email(title, body_md, cfg_to)


def gen_substack_workflow_guide():
    """Substack 발행 워크플로 가이드"""
    return """# Substack 자동 발행 워크플로

## 1단계: Substack 설정 (1회)

1. https://substack.com 본인 publication 생성
2. **Settings → Publishing → Post by email 활성화**
3. unique 이메일 주소 받음 (예: publish-abc123@substack.com)

## 2단계: ETF Pulse config에 추가

```json
{
  "gmail_user": "your.email@gmail.com",
  "gmail_app_password": "16-digit",
  "substack_post_email": "publish-abc123@substack.com"
}
```

## 3단계: 매일 자동 발행

```bash
python etf_pulse/substack_email.py
```

또는 cron에 등록:
```bash
0 6 * * * python /path/to/etf_pulse/run_daily.py && python /path/to/etf_pulse/substack_email.py
```

## 4단계: Substack에서 final review

- Email로 도착한 글이 Substack에 **draft**로 저장됨
- 본인 검토 후 "Publish" 클릭
- 자동 발행 원하면 Substack 설정에서 "auto-publish" 옵션 (있을 경우)

## 워크플로 정리

```
매일 06:00 KST
  ↓
[1] run_daily.py
  ↓ Markdown 콘텐츠 생성 (etf_pulse/content/pulse_YYYY-MM-DD.md)
  ↓
[2] substack_email.py
  ↓ Gmail → Substack publish email
  ↓
Substack draft 생성
  ↓
[3] (수동) 검토 + Publish 클릭
  ↓ 또는 auto-publish 활성화 시 자동
  ↓
구독자에게 이메일 발송
```

## 대안: Buttondown / ConvertKit

Substack 외에 자동 발행 더 잘 지원:
- **Buttondown**: API 있음, $9/월부터 (500 구독자 free)
- **ConvertKit**: API 있음, free tier 1000 구독자
- **Beehiiv**: API 있음, free tier 2500 구독자

API 사용 시 진짜 자동 발행 가능 (사용자 검토 단계 skip).
"""


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--guide', action='store_true', help='Show workflow guide')
    parser.add_argument('--publish', action='store_true', help='Actually publish')
    parser.add_argument('--md', help='Markdown file path')
    parser.add_argument('--email', help='Substack publish email')
    args = parser.parse_args()

    if args.guide or not (args.publish or args.md):
        print(gen_substack_workflow_guide())
        sys.exit(0)

    if args.publish:
        if not args.md:
            # 최신 콘텐츠
            out_dir = Path(__file__).parent / 'content'
            md_files = sorted(out_dir.glob('pulse_2*.md'), reverse=True)
            md_files = [f for f in md_files if '_en' not in f.name]
            if not md_files:
                print('content 없음')
                sys.exit(1)
            args.md = str(md_files[0])

        md = Path(args.md).read_text(encoding='utf-8')
        date_str = re.search(r'(\d{4}-\d{2}-\d{2})', args.md)
        date_str = date_str.group(1) if date_str else 'unknown'

        substack_email = args.email
        if not substack_email:
            cfg = load_email_config()
            substack_email = cfg.get('substack_post_email')
        if not substack_email:
            print('substack_post_email 없음 (config.json 또는 --email)')
            sys.exit(1)

        ok, msg = publish_to_substack(md, date_str, substack_email)
        print(f'{"✓" if ok else "✗"} {msg}')
