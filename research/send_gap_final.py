# -*- coding: utf-8 -*-
"""gap(fwd PER<<trailing PER) US 적용 전체 검증 종합 → 개인봇. 중학생도 이해 쉽게."""
import os, sys, urllib.request, urllib.parse
sys.stdout.reconfigure(encoding='utf-8')

msg = """📊 <b>[fwd 12m PER이 압도적으로 낮으면? — US 적용 전체 검증 종합]</b>

<b>━━ 형 질문 ━━</b>
"fwd(미래) 12개월 PER이 지금(trailing) PER보다 <b>압도적으로 낮으면</b> = 성장 기대 큰 강력 신호 아냐?
US 시스템에 더 적극 적용할 방법 없어?"

<b>━━ 한 줄 답 ━━</b>
<b>신호는 진짜 강력함(맞음). 근데 US 시스템엔 이미 녹아 있고, 더 넣는 방법은 4가지 다 함정이었음.</b>
그대로 <b>2슬롯 50:50 유지</b>가 답.

<b>━━ 먼저 쉬운 용어 ━━</b>
• <b>gap</b> = trailing PER ÷ forward PER = "시장이 기대하는 이익 성장률". fwd PER이 압도적 낮을수록 gap 큼.
• <b>상관</b> = 두 신호가 닮은 정도(1이면 똑같음, 0이면 무관).
• <b>LOWO</b> = "대박종목 하나 빼고 다시 계산". 빼도 좋으면 실력, 빼니까 무너지면 그 종목 운(사후약방문).
• <b>MDD</b> = 최대낙폭(고점 대비 제일 많이 깨진 폭). <b>레버리지</b> = 빚내서 더 큰 베팅.

<b>━━ gap은 진짜 강력한 신호 (형 직감 맞음) ━━</b>
• 정보계수 IC +0.23, gap 상위 분위 향후20일 <b>+13.9%</b> vs 하위 +3.5% (압도적일수록 좋음).
✅ 형 직감은 데이터로 입증됨.

<b>━━ 그런데 "더 적용하기" 4가지 다 함정 (전부 직접 검증) ━━</b>

<b>① 1등에 2배·3배 비중(집중)</b>
100% 몰면 +217%→+294%로 예뻐 보임. 근데 <b>LOWO로 SNDK 빼면 −13%p로 뒤집힘</b> = 대박주 사후몰빵.

<b>② 빚내서 레버리지(2~3배)</b>
3배면 +869%! 근데 이 88일에 <b>폭락장이 0번</b>(강세장만 봄). 강세장서도 MDD −22%→−37%,
폭락 초기(−20~36%) 3배면 <b>−60~100% 계좌 깡통</b>. 착시.

<b>③ 점수 산식에 녹이기</b>
gap을 점수에 가산/곱셈 주입 → <b>−25~50%p</b>. 검증된 리비전 winner를 떨구고 트랩(CIEN −18%)을 잡음.

<b>④ 매수 후보 'gap 압도적인지' 체크 (형의 최신 아이디어)</b> ← 오늘 fresh 데이터로 검증
• 부드럽게 체크(후보 중 gap 높은 것 우선) → <b>효과 0.0%p</b>. (후보가 이미 고gap이라 중복)
• <b>"압도적(gap≥2.5)만 사기" → −50%p 손해!</b> 오늘 예: <b>KEYS(1위)·NVDA(3위)</b>는 좋은 종목인데
  gap이 "압도적"까진 아니라 <b>탈락</b> → 좋은 winner를 버려서 수익 붕괴.

<b>━━ 왜 다 안 되나 (근본 이유) ━━</b>
gap은 <b>분산형 신호</b>(5~7종목 펼쳐야 평균으로 실현). 2슬롯은 <b>집중형</b>(2종목 대박 몰빵).
<b>모양이 정반대라 한 산식에 못 합침.</b> 5~7종목 신호를 2종목에 압축하면 죽음.

<b>━━ ★사실 이미 녹아 있음 (두 군데) ━━</b>
gap·모멘텀 상관이 <b>0.83</b>이라 — 시스템이 이미 gap을 다 챙기고 있음:
• <b>진입</b>: gap 큰 종목은 모멘텀 순위가 자연히 올라옴 → 지금 <b>LITE 2위(gap 3.19)·NVDA 3위</b>로 대기 중.
• <b>보유</b>: 순위 빠져도 fwd PER&lt;30이면 보유하는 <b>PE_HOLD 룰</b> = 바로 "forward PER 낮으면 잡아둔다".
  → <b>SNDK(보유, gap 6.19, fwd PER 10.6)</b>가 정확히 그 예. 순위 밖인데 싸서 안 팖.
= 형이 원한 "fwd PER 낮으면 우대"가 진입은 상관으로, 보유는 PE_HOLD로 <b>이미 둘 다 적용돼 있음.</b>

<b>━━ 결론 ━━</b>
• <b>gap은 강력하지만 US 2슬롯엔 이미 반영됨.</b> 명시적으로 더 넣으면 중복(0)이거나 winner 탈락(−).
• 더 짜내고 싶으면 <b>별도 5~7종목 gap 슬리브 소액(10~20%)</b> 하나뿐 — 수익 일부 내주고 분산 얻는 트레이드.
• <b>production 변경 없음.</b> 검증코드·문서 전부 커밋(conviction_bt / gap_entry_bt / CONVICTION_FINDINGS).

한 줄: <b>"올려야 할 종목은 이미 순위가 올라가 있고(LITE·NVDA), 안 올라간 싼 winner는 PE_HOLD가 잡고
있다(SNDK). 그래서 gap은 이미 적용돼 있고, 억지로 더 넣으면 손해다."</b>"""

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
