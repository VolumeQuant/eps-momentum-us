# -*- coding: utf-8 -*-
import os, json, time, urllib.request, urllib.parse
MSGS=[
"🛠️ <b>[정정] 앞 메시지 SEI·AROC는 버그였습니다</b>\n\n앞서 보낸 SEI(=Solaris <b>석유·가스</b> 장비)·AROC(=Archrock <b>천연가스</b>)는 <b>에너지 종목이라 원래 제외</b>돼야 했는데, NASDAQ 데이터가 이들 섹터를 엉뚱하게(소비재/유틸리티) 분류해 필터를 빠져나갔습니다. <b>제 검증 부족</b>입니다. 업종 키워드로 다시 걸러 에너지 34종목 제거 후 재계산했습니다. 아래가 정정된 신호입니다. ↓",
"📡 <b>[US 4팩터 연구시스템] 일일 신호 (정정)</b> · 2026-06-12(금)\n<i>※ 실전 미적용 — 종목 확인용. KR 4팩터 충실 이식.</i>\n\n📈 <b>시스템 누적 +398% (7.0년)</b> / SPY +154%\n    CAGR +26% · Calmar 0.96 · MDD -27% (SPY CAGR +14%)\n    <i>⚠️ SEC EDGAR 장기재무(7년)·생존편향 — 상대비교용</i>\n\n📊 <b>국면: 📈 공격</b> (S&P500 200일선 위 + VIX 안정) — 주식 보유\n\n━━━━━━━━━━━━━━━\n🛒 <b>현재 보유</b> (4팩터 결합순위 ✅검증 상위 3)\n━━━━━━━━━━━━━━━\n1. <b>LGND</b> (Ligand Pharmaceuticals, 바이오) · wr 1.4\n    가치 -1.2σ · 품질 -1.2σ · 성장 +2.2σ · 모멘텀 +1.4σ\n2. <b>ECG</b> (Everus Construction, 건설) · wr 3.6\n    가치 -0.5σ · 품질 +0.3σ · 성장 +1.7σ · 모멘텀 +1.9σ\n3. <b>FIX</b> (Comfort Systems USA, HVAC) · wr 5.8\n    가치 -1.4σ · 품질 +1.1σ · 성장 +2.0σ · 모멘텀 +1.8σ\n\n👀 <b>다음 후보</b>: INCY(Incyte, 2.4) · KRYS(Krystal Biotech, 4.6)",
"🧪 <b>[#연구시스템] 규칙 + 한계</b>\n\n📋 <b>규칙 (KR 공격모드 충실 이식)</b>\n• 결합: <b>가치15 + 성장55 + 모멘텀30</b> + 단기모멘텀·저변동·과열캡 (품질0)\n• 매수: 결합순위 ✅3일검증 상위 3 (3슬롯 균등)\n• 매도: 3일가중순위(wr) 6위 밖 이탈 — 손절·트레일링 없음\n• 방어국면(S&P 200일선 15일 이탈 또는 VIX&gt;36): 전량 현금\n• 제외: 금융·부동산·<b>에너지(석유/가스)</b>·원자재(금/은/철강/석탄) 섹터\n\n⚠️ <b>정직한 7년 성적</b>: Calmar 0.96 — SPY(CAGR+14%)는 이기지만(CAGR+26%·MDD−27%) KR(3.86)엔 한참 못 미침. 앞서 \"2.8년 Calmar 1.93\"은 최근구간 착시. 모멘텀 의존+약세장 취약. 신뢰=상대우위·구조, 절대수익·미적용.",
]
def send(t,c,x):
    d=urllib.parse.urlencode({'chat_id':c,'text':x,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
    with urllib.request.urlopen(urllib.request.Request(f'https://api.telegram.org/bot{t}/sendMessage',data=d),timeout=30) as r:
        return json.loads(r.read().decode()).get('ok')
if __name__=='__main__':
    t=os.environ['TELEGRAM_BOT_TOKEN'];c=os.environ['TELEGRAM_PRIVATE_ID']
    for i,m in enumerate(MSGS): print(f'msg {i+1}/{len(MSGS)} ok={send(t,c,m)}');time.sleep(1.5)
    print('done')
