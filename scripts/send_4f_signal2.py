# -*- coding: utf-8 -*-
import os, json, time, urllib.request, urllib.parse
MSGS=[
"📡 <b>[US 4팩터 연구시스템] 일일 신호</b> · 2026-06-12(금)\n<i>※ 실전 미적용 — 종목 확인용. KR 4팩터 시스템 충실 이식.</i>\n\n📈 <b>시스템 누적 +238% (2.8년)</b> / SPY +65%\n    CAGR +53% · Calmar 1.93 · MDD -28% (SPY CAGR +19%)\n    <i>⚠️ 유효 2.8년(yf 재무 5년 한계)·생존편향 — 상대비교용</i>\n\n📊 <b>국면: 📈 공격</b> (S&P500 단기선이 장기선 위) — 주식 보유\n    <i>(방어 전환 시 전량 현금)</i>\n\n━━━━━━━━━━━━━━━\n🛒 <b>매수/보유</b> (4팩터 결합순위 ✅검증 상위 3 = 3슬롯)\n━━━━━━━━━━━━━━━\n1. <b>SEI</b> · Consumer Discretionary · wr 1.6\n    가치 -0.7σ · 품질 -1.2σ · 성장 +2.4σ · 모멘텀 +1.9σ\n2. <b>TSM</b> · Technology · wr 2.5\n    가치 +2.3σ · 품질 +1.4σ · 성장 +1.8σ · 모멘텀 +0.2σ\n3. <b>ARGX</b> · Health Care · wr 3.7\n    가치 -1.3σ · 품질 +0.3σ · 성장 +2.6σ · 모멘텀 +0.7σ\n\n👀 <b>다음 후보</b> (wr 6 이내): INCY(3.8) · AROC(5.4)",
"🧪 <b>[#연구시스템] 규칙 + 한계</b>\n\n📋 <b>규칙 (KR 공격모드 충실 이식)</b>\n• 결합: <b>가치15 + 성장55 + 모멘텀30</b> + 단기모멘텀·저변동·과열캡 (품질0)\n• 매수: 결합순위 ✅3일검증 상위 3 (3슬롯 균등)\n• 매도: 3일가중순위(wr) 6위 밖 이탈 — <b>손절·트레일링 없음</b>\n• 방어국면(S&P 단기선&lt;장기선 5일): <b>전량 현금</b>, 전환 시 전량청산\n• 제외: 금융·부동산·에너지·원자재(금/은/철강/석탄 등) 섹터\n\n⚠️ <b>아직 실전 미적용</b>: ① 생존편향(사라진 종목 누락) ② yfinance 재무 5년뿐→유효 BT 2.8년(KR은 7.4년). SEC EDGAR 쓰면 장기 연장 가능. 신뢰=상대순위·구조·견고성(원자재/은행 자동제외 확인), 절대수익 아님.",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
