# -*- coding: utf-8 -*-
"""forward_PER 전수 테스트 결과 + gap게이트 ON 알림 → 개인봇. 중학생 눈높이 + 예시."""
import os, sys, urllib.request, urllib.parse
sys.stdout.reconfigure(encoding='utf-8')

msg = """🧾 <b>[forward_PER 전수 테스트 — 쉽게 + 예시]</b>
2026-06-26

<b>forward_PER이 뭐냐</b>: 주가 ÷ 내년 예상이익. 낮으면 "내년 이익 기준 싸다", 높으면 "비싸다".
🔸 예(오늘): NVDA 18.7(싼편) / LITE 47.8(비쌈) / AVGO 22.7.

<b>이걸 어디에 쓸까 — 두 군데 후보를 다 테스트했어요:</b>
① 살 때(진입) "쌀 때만 사자"
② 팔 때(이탈) "비싸지면 팔자"

<b>━━ 테스트① 살 때 forward_PER 캡 (≤15·20·25·30·40 다 해봄) ━━</b>
<b>전부 손해.</b> base +257% 인데 캡 씌우면 +220~252%로 떨어짐.
<b>왜?</b> 우리가 잡는 대박주는 <b>비싼 성장주</b>예요. 오늘 1위 LITE가 forward_PER 47.8 — "싸야 산다"고 하면 이런 애들을 다 놓쳐요. 🔸 SNDK·MU 같은 옛 대박주도 비슷.
➡️ <b>결론: 살 때는 forward_PER 안 씀.</b>

<b>━━ 테스트② 팔 때 forward_PER 임계 (15~50) — ★함정 주의 ━━</b>
숫자만 보면 <b>"15로 낮추면 +322%!"</b> 라고 유혹해요(현행 30은 +257%). 솔깃하죠?
<b>근데 가짜예요.</b> 그 +322%는 <b>SNDK·MU(원래 엄청 싼 종목) 딱 둘 덕분</b>이고, 그 둘을 빼고 다시 계산하면 <b>오히려 30이 더 좋아져요</b>(20은 +83%, 30은 +111% — 순서 뒤집힘). = 한두 종목 운이 만든 신기루.
🔸 형이 늘 경계하던 "한 종목 빼면 무너지는 착시" 바로 그거예요.
<b>진짜 단단한 구간은 25~50(다 +257%)</b>, 30이 그 안에 있어요. 그리고 <b>아예 안 팔면(파는 규칙 없음) +149%로 폭락</b> → "비싸지면 판다"는 규칙 자체는 가치 있음.
➡️ <b>결론: 팔 때 forward_PER 30 유지(현행 그대로). 15/20 쫓는 건 과적합이라 안 함.</b>

<b>━━ 한 줄 ━━</b>
<b>forward_PER은 "살 때 말고, 팔 때만, 30에서" 쓰는 게 검증된 정답.</b> 형 직감(이탈 전용 30) 정확했어요.

<b>━━ 추신: 오늘 gap 게이트 켰어요 ★ ━━</b>
형 결정대로 <b>gap≥3.0 진입게이트 ON</b> 했습니다(production+테스트 둘 다). 오늘 빈손으로 시작하면 <b>LITE만 매수</b>(NVDA·AVGO는 기대성장 gap이 3.0 미만이라 컷). 테스트워크플로우로 실제 메시지 확인 중. 끄고 싶으면 한 줄로 되돌릴 수 있어요. 단 게이트는 marginal(91일·강세장)이라 단일종목 집중은 감수하는 거예요."""

tok = os.environ.get('TELEGRAM_BOT_TOKEN'); pid = os.environ.get('TELEGRAM_PRIVATE_ID')
if not tok or not pid:
    print('시크릿 없음 — 미리보기:\n'); print(msg); sys.exit(0)
parts, buf = [], ''
for line in msg.split('\n'):
    if len(buf) + len(line) + 1 > 3900:
        parts.append(buf); buf = ''
    buf += line + '\n'
if buf:
    parts.append(buf)
for i, p in enumerate(parts):
    data = urllib.parse.urlencode({'chat_id': pid, 'text': p, 'parse_mode': 'HTML'}).encode()
    r = urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{tok}/sendMessage', data=data))
    print(f'발송 {i+1}/{len(parts)}:', r.status)
