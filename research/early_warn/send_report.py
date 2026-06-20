# -*- coding: utf-8 -*-
"""조기위험탐지 12시간 자율주행 최종보고 → 개인봇(채널 X). production 무변경."""
import os, sys, urllib.request, urllib.parse
sys.stdout.reconfigure(encoding='utf-8')

msg = """🛡️ <b>[US 조기위험탐지 — 12시간 자율주행 최종보고]</b>

<b>━━ 쉬운 버전 (결론부터) ━━</b>

<b>❓ 뭘 풀려고 했나</b>
지금 우리 "위험하면 방어로 전환" 스위치는 <b>이미 한참 떨어진 뒤에야</b> 켜진다.
26년 데이터로 재보니 방어 켜질 때 이미 <b>평균 −14% 빠진 상태</b>. 특히 2000년 닷컴 때는
스위치 켜지기도 전에 8주 만에 −36%가 다 빠졌다(스위치가 그 구간 0% 작동).
→ "더 일찍 피하되, 괜히 출렁일 때 헛방어로 수익 까먹지 않는" 신호를 찾아라.

<b>🔬 어떻게 찾았나</b>
AI 에이전트 7팀을 풀어 논문·깃허브·웹을 싹 뒤졌다(레짐모델·브레드스·금리·변동성·CTA·EPS·환율 등
후보 30여개). 그 다음 후보를 우리 26년 백테스트에 <b>직접</b> 넣어 정직하게 검증.

<b>✅ 딱 하나 통과 — "시장 참여 폭(브레드스)"</b>
의미: <b>지수는 멀쩡한데 속을 보면 11개 업종 중 몇 개나 건강한가</b>를 본다.
천장 직전엔 <b>소수 대장주만 버티고 나머지 업종은 먼저 무너진다</b>(2000·2007·2018·2022 다 그랬음).
이 "속병"을 지수가 깨지기 전에 잡는다.
🔸 비유: 반 평균점수(지수)는 그대로인데 <b>상위 2명 빼고 다 성적 추락</b> → 곧 평균도 무너진다.

<b>📊 효과 (26년 검증)</b>
• 최대낙폭(MDD): <b>−36.5% → −27.4%</b> (−9%p 개선, 손실 덜 본다)
• 위험대비수익(Calmar): <b>0.36 → 0.44</b> (오히려 좋아짐 = 헛방어로 수익 안 까먹음)
• 2000 닷컴: 스위치가 0%였던 그 구간을 <b>고점 −3.4%에서</b> 미리 방어 진입
• 2018·2011처럼 옛 스위치가 통째로 놓친 급락도 절반으로 줄임

<b>⚠️ 정직한 한계 3가지</b>
1) 실제 우리 종목의 약세장 데이터는 없어서 <b>지수 대용(QQQ)으로만 검증</b>(기존 국면연구 공통 한계).
2) <b>2023년처럼 소수 대장주만 끌고 가는 장</b>에선 헛방어가 좀 생긴다(그 해 +79일 헛방어). 단 전체
   성적은 그래도 더 좋았고, 2024·2026엔 멀쩡(고착 아님).
3) 진짜 "느린 천장" 표본이 4번뿐(소표본).

<b>🚦 그래서 어떻게?</b>
<b>아직 안 켰다(승인 대기).</b> 코드는 다 준비(기본 OFF). 권장: 1차는 <b>화면에 "브레드스 NN%"만
표시</b>해서 1~2번 약세장 지켜본 뒤 매매에 연결. (지금 브레드스 82% = 정상, 켜도 당장 변화 0.)

<b>━━ 기술 상세 ━━</b>
• baseline: SPX&lt;MA200(15d) OR VIX&gt;36(2d), 발행어음 방어. 26y QQQ: CAGR+13.0/MDD−36.5/Cal0.36/
  WFmin0.34/LOWO0.37/늦음−14.1.
• 진단: MDD 상위 드로다운 대부분 "방어 0%" 초기레그 갭(2000·2018Q4·2025·2022 첫레그).
• 채택: sector_breadth = (11 SPDR 중 자기 200DMA 위 비율) &lt; 0.45, conf(3d진입/15d퇴출), 게이트에 OR.
  → CAGR+12.2/MDD<b>−27.4</b>/Cal<b>0.44</b>/WFmin<b>0.40</b>/LOWO<b>0.46</b>/늦음−11.4/전환38.
• robust: 인접CV 0.070(0.35~0.55 plateau), OOS(슬로우탑반기 0.30→0.37·벤치반기 0.66→0.66 무해),
  leave-one-bear-out 7/7 MDD개선, 확인일 9셀 robust, 섹터MA200 최적(150/100 휩쏘), 룩어헤드 無.
• 기각: 수익률곡선 dis-inversion(2000보다 늦음·Cal0.29), 방어/경기로테이션(MDD−43), 앙상블K4(MDD불변),
  GTT(MDD−65), SKEW·slope·copper/gold·absmom 전부 미달. FRED계열(HY-OAS ROC·NFCI·EBP·NTFS)·
  VVIX·EPS리비전 = 이 환경 데이터차단/소표본으로 검증불가(차기 후보로 메모).
• 스택 옵션: 브레드스+추세앙상블K4 = WFmin 0.45로 추가robust(복잡도↑, 선택).
• 산출: research/early_warn/ (harness·baseline·validate·candidates·deep·stress·FINDINGS·deploy패치).
  연구파일 커밋/푸시 완료. <b>프로덕션 daily_runner.py 무수정.</b>

전체 리포트: research/early_warn/FINDINGS_2026_06_20.md"""

tok = os.environ.get('TELEGRAM_BOT_TOKEN'); pid = os.environ.get('TELEGRAM_PRIVATE_ID')
if not tok or not pid:
    print('시크릿 없음 — 미리보기:\n'); print(msg); sys.exit(0)
# 텔레그램 4096자 제한 → 분할
parts = []
buf = ''
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
