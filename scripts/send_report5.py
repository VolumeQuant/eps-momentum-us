# -*- coding: utf-8 -*-
"""종합보고 5번(맹점) 재발송 — '<' 제거 수정본. 채널 X."""
import os, json, urllib.request, urllib.parse
MSG="""📊 <b>[5/5] 우리 시스템의 맹점 (솔직하게)</b>

1. <b>시뮬 ≠ 실제</b>: +229%는 백테스트. 실제 track record는 며칠치뿐. (4번 메시지 참고)
2. <b>소수 종목 의존</b>: 성과 거의 전부가 SNDK·MU 2개. 빼면 붕괴(검증됨). 다음 대박주 못 잡으면 평범해짐.
3. <b>약세장 미경험</b>: 4개월·상승장만. "비싸지면 매도"·약세장 방어가 실전 발동된 적 없음 = 미검증.
4. <b>높은 변동성·낙폭</b>: 집중의 대가로 변동성 87%/년, 하루 −12% 겪음. 멘탈·자금 관리 필요.
5. <b>통계적 유의성 부족</b>: 수십~백 번 실험을 거쳐 고른 거라(Deflated Sharpe 보정 시 "진짜일 확률" 62~72%로 95% 문턱 미달), +229%엔 운·거품이 섞여 있을 수 있음.
6. <b>데이터 취약</b>: yfinance 글리치·차단 가끔 발생(방어장치는 넣음).

<b>결론</b>: 강력하지만 "검증 짧고·집중되고·상승장만 본" 시스템. 그래서 <b>실투자는 5~20%·80:20 버퍼</b>로 작게, 규칙은 칼같이, 그리고 <b>앞으로의 실제 성과로 검증</b>하는 게 정답이에요."""
t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
d=urllib.parse.urlencode({'chat_id':c,'text':MSG,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
    print('ok=',json.loads(r.read().decode()).get('ok'))
