# -*- coding: utf-8 -*-
"""$1B 검토 최종 결론을 개인봇에 발송 (쉬운 설명 + 예시). production 무변경."""
import os, sys, urllib.request, urllib.parse
sys.stdout.reconfigure(encoding='utf-8')
msg = """🧪 <b>[$1B 순위 버전 검토 — 최종 결론]</b>

<b>❓ 무슨 고민이었나</b>
순위판에 KEYS처럼 "거래량 적어 못 사는 종목"이 1위로 떠서 헷갈림.
→ "살 수 있는 종목만으로 순위 매기는 $1B 버전"을 검토함.

━━━━━━━━━━━━━━━
<b>✅ 결론: 시스템은 그대로, 화면만 정리</b>
━━━━━━━━━━━━━━━

<b>1) "성과 똑같다"는 맞지만…</b>
지난 4개월(88일) 두 버전 다 +270%로 동일.
근데 그건 <b>둘 다 똑같이 MU·SNDK(대박종목)를 들고 있어서</b>임. 운 좋게 차이 안 난 것.

<b>2) "$1B가 더 안전"은 가짜였음</b>
$1B가 위기에 더 잘 버틴다고 나왔는데, 파보니 <b>딱 한 종목(SNDK·VRT) 운</b>이었음.
🔸 예: "A팀이 점수 더 냈다"는데 알고보니 그 추가점이 전부 운 좋은 한 방. 빼는 종목 바꾸면 결과가 뒤집힘 = 실력 아니라 운.

<b>3) $1B엔 진짜 위험이 숨어있음</b>
$1B는 살 수 있는 종목만 순위 매기니 풀이 너무 작음(6~10개).
매도 규칙이 "10위 밖이면 팔아라"인데, 종목이 6개면 10위 밖이 없어 → <b>아무것도 안 팔림 → 매도 규칙이 죽음.</b>
🔸 예: "성적 하위 10명 자르기"인데 반에 6명뿐이면 아무도 못 자름.
👉 실제로 88일 중 절반은 종목 ≤12개, 매도 규칙은 88일간 <b>딱 1번</b> 발동. 폭락장이면 패자를 못 털어내 위험.

<b>4) 원한 건 간단히 됨</b>
"못 사는 종목 화면에 안 보이게" = <b>화면만 가리면 됨.</b> 매매는 안 건드림. 위험 0.

━━━━━━━━━━━━━━━
<b>🎯 최종 결정</b>
━━━━━━━━━━━━━━━
• 시스템(순위·매수·매도) = <b>현행 그대로</b> (검증됨)
• 화면(워치리스트) = <b>거래량 미달주만 숨김</b> (단, 보유 종목은 계속 표시)

독립 전문가 2명 모두 같은 결론.
참고: BE도 현행 이탈선(12위)이면 이미 잡혔음 — 06-17 매도는 옛 룰(10위) 타이밍 탓."""
tok=os.environ.get('TELEGRAM_BOT_TOKEN');pid=os.environ.get('TELEGRAM_PRIVATE_ID')
if not tok or not pid:
    print('시크릿 없음 — 미리보기:\n'); print(msg); sys.exit(0)
data=urllib.parse.urlencode({'chat_id':pid,'text':msg,'parse_mode':'HTML'}).encode()
r=urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{tok}/sendMessage',data=data))
print('발송 결과:',r.status)
