# -*- coding: utf-8 -*-
import os, json, time, urllib.request, urllib.parse
MSGS=[
"""🔍 <b>[PER 15 검증 1/2] 사장님 지적이 맞았어요 — 너무 가혹</b>

"PER 15는 가혹하다, M7도 15 넘는다"를 데이터로 확인했어요.

<b>오늘 상위 20종목 PER</b>:
• NVDA 19.9 · AVGO 23.3 · AMZN 25.9 · LLY 28.4 · KEYS 31 · BE 83...
• <b>20개 중 18개가 PER 15 이상!</b> PER 15 미만은 단 2개(HGV, VIRT).

즉 지금 우리 시스템의 "PER 15 미만이면 순위 밀려도 보유" 규칙은 <b>SNDK·MU(PER 9~11) 같은 옛날 싼 종목에 맞춘 것</b>이라, 지금의 <b>고PER 성장 리더(NVDA류)엔 거의 작동 안 해요.</b> 이들은 순위 밀리면 바로 매도됩니다.""",

"""🔍 <b>[2/2] 그래서 어떻게? — PER 기준 풀면 더 좋아짐</b>

PER 기준을 15→20→40→순위만 으로 검증(SNDK/MU 빼고 = 진짜 실력):
• PER 15(현행): +39.5%
• PER 20: +39.5% <b>(똑같음 = 풀어도 손해 0)</b>
• <b>PER 40: +50.2% (★ +10.7%p 더 벎)</b>
• 순위만(PER무시): +48.5%
• <b>최대손실(MDD)은 PER 기준과 무관하게 −19%로 동일</b> → "거품방어" 걱정은 데이터상 근거 약함.

<b>결론</b>:
• PER 15는 싼 종목(SNDK/MU)엔 최적이나, <b>고PER 성장주(M7/NVDA류)를 너무 일찍 잘라 손해.</b>
• 권고: <b>PER 기준 25~30으로 완화</b>(또는 순위만) → 우량 메가를 순위 밀려도 더 잡고, 실력수익 ↑, 손실위험 그대로.
• PER 20은 "공짜 중간값"(현행과 0% 차이, 부담 없이 즉시 가능).
• ⚠️ 트레이드오프: 미래 winner가 다시 SNDK처럼 <b>싼 시클리컬이면 PER 15가 유리</b>(메가포함 전체선 PER 40이 −12%p), <b>고PER 성장시대면 완화가 유리.</b>

→ 지금 시장 리더십(고PER 성장) 보면 <b>PER 25~30 완화 권고.</b> 적용할지 결정해주세요(매매로직 한 줄).""",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
