# -*- coding: utf-8 -*-
import os, json, time, urllib.request, urllib.parse
MSGS=[
"📡 <b>[US 4팩터 연구시스템] 일일 신호 (정정판)</b> · 2026-06-12(금)\n<i>※ 실전 미적용 — SEC EDGAR 장기재무로 재계산. KR 4팩터 충실 이식.</i>\n\n📈 <b>시스템 누적 +368% (7.0년)</b> / SPY +154%\n    CAGR +25% · Calmar 0.91 · MDD -27% (SPY CAGR +14%)\n    <i>⚠️ SEC EDGAR 장기재무(7년)·생존편향(상장폐지 누락) — 상대비교용</i>\n\n📊 <b>국면: 📈 공격</b> (S&P500 200일선 위 + VIX 안정) — 주식 보유\n    <i>(200일선 15일 이탈 또는 VIX&gt;36 시 전량 현금)</i>\n\n━━━━━━━━━━━━━━━\n🛒 <b>현재 보유</b> (4팩터 결합순위 ✅검증 상위 = 3슬롯)\n━━━━━━━━━━━━━━━\n1. <b>SEI</b> · Consumer Discretionary · wr 1.0\n    가치 -1.4σ · 품질 -0.9σ · 성장 +2.5σ · 모멘텀 +1.9σ\n2. <b>LGND</b> · Health Care · wr 2.8\n    가치 -1.2σ · 품질 -1.2σ · 성장 +2.1σ · 모멘텀 +1.4σ\n3. <b>ECG</b> · Consumer Discretionary · wr 6.0\n    가치 -0.5σ · 품질 +0.3σ · 성장 +1.6σ · 모멘텀 +1.8σ\n\n👀 <b>다음 후보</b> (wr 6 이내): INCY(3.0) · AROC(4.4) · KRYS(5.8)",
"🧪 <b>[#연구시스템] 규칙 + 한계 (중요)</b>\n\n📋 <b>규칙 (KR 공격모드 충실 이식)</b>\n• 결합: <b>가치15 + 성장55 + 모멘텀30</b> + 단기모멘텀·저변동·과열캡 (품질0)\n• 매수: 결합순위 ✅3일검증 상위 3 (3슬롯 균등)\n• 매도: 3일가중순위(wr) 6위 밖 이탈 — <b>손절·트레일링 없음</b>\n• 방어국면(S&P 200일선 15일 이탈 또는 VIX&gt;36): <b>전량 현금</b>, 전환 시 전량청산\n• 제외: 금융·부동산·에너지·원자재(금/은/철강/석탄 등) 섹터\n\n⚠️ <b>정직한 7년 성적</b>: <b>Calmar 0.91</b> — SPY(누적+154%·CAGR+14%)는 이기지만(누적+368%·CAGR+25%·MDD−27%) KR(Calmar 3.86)엔 한참 못 미침. 앞서 \"2.8년 Calmar 1.93\"은 유리한 최근구간 착시였고, SEC EDGAR 장기데이터로 보니 2019~22 부진. 국면은 US 맞춤(S&P200DMA+VIX; KR MA20/80은 US 휩쏘로 부적합). 미적용 사유: 생존편향+모멘텀 의존(약세장 취약). 신뢰=상대우위·구조, 절대수익 아님.",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
