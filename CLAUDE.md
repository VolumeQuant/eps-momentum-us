# 🇺🇸 US 전략 — eps-momentum-us (v119b fwd_PER<30, 국면=검증된 binary 200DMA+VIX36, 메인 cron 활성, 2026-06-14)

> **★ v121→v121c: VIX 텀구조 진입가속 시도 → 비robust로 기각·OFF (2026-06-14)**: 전문가자문으로 VIX/VIX3M 백워데이션 조기경보를 시도하고 적용했으나(v121), **사용자 과적합 의심 → 임계값 스윕 재검증서 비robust 확정 → 기본 OFF로 복귀(v121c)**. 이유: 개선이 임계 1.05에서만 나오고(이득+헛방어 한몸: 2026-3월 1.06·2014 1.11 헛방어 vs 진짜폭락 1.25+) 1.08+엔 baseline과 동일, 구현방식 따라 결과 흔들림(phase13 ex-V 0.80 vs 재검증 0.67)=과적합 신호. **라이브는 검증된 binary(S&P200DMA 15일확인 OR VIX36 2일확인) 유지.** 코드·검증 보존(research/regime_eda_phase11~14, daily_runner _confirm_regime_ts, REGIME_TS_DISABLE 기본'1'). 켜려면 REGIME_TS_DISABLE=0. ⚠️교훈: QQQ프록시·N=3 빠른약세장 신호는 임계 스윕으로 robust확인 필수, 단일임계 호조는 착시. **(미적용 메타옵션) 조기50%축소(phase14): MA100이탈시 시스템비중 50%→MA200전량 = binary보다 Cal0.37→0.42, 부분포지션 필요라 메타배분 레벨 — 단 이것도 표본부족 유의.**


> **★★ 현금/국면 정책 확정 (2026-06-14, 26년 QQQ프록시 검증 — 인접·WF·약세장별 통과)**: research/regime_eda_phase7~10. **국면 전환기준=이미 robust 최적**(MA200·진입15일·재진입15일+VIX36; 인접 CV0.023·WF 3블록 안정·2000/08/20/22 4대약세장 포착·2018Q4놓침·2025휩쏘는 추세신호 환원불가 한계). **재진입=15일 확인 후 전액 1회 투입(분할X, 빠를수록 손해).** **현금버퍼(공격적)=주식85~90%/안전 10~15%**(매끈 다이얼, 버퍼가 발행어음3~5%면 100→85%로 줄여도 CAGR −1%p·MDD 대폭↓). **방어자산=약세장 종류별**(패닉형 2008/2020=IEF채권 +4~5% 승 / 금리인상형 2022=발행어음·현금 승[IEF −9.8%]); 단순·확정수익이면 **달러 수시형 발행어음(3~5%, 듀레이션0)**, 양쪽헤지면 IEF+발행어음 반반. **production 코드 변경필요=방어자산 추천 IEF→발행어음/단기 한 줄(매매로직 무변경).** HY스프레드 추가=중복·과적합으로 기각. 2슬롯비중=동적(v84 2step_t15) 유지(순수1:1 아님). **★KR vs US 시스템 비중=KR65/US35 시작→US검증되며 50:50**(코스피·나스닥 상관0.64·분산9%, US는 미검증·2슬롯집중이라 KR틸트, US35%는 달러분산). ⚠️ 전부 QQQ/시장프록시 검증(EPS 실약세장 데이터無)·구조적판단. 봇발송: send_policy_guide(결론4+사용법5)·send_kr_us_alloc.
> **★ v119b: PE_HOLD 15→30 완화 — 적용완료 (2026-06-14, commit/push 됨)**: fwd_PER = price/ntm_current (forward 12M, 후행 아님). top20 중 **18/20이 fwd_PE 15+**(NVDA19.9·AVGO23.3·AMZN26·LLY28). PE<15는 SNDK/MU(PE9~11) 시클리컬 과적합→우량메가 carryover 미발동. 스윕(research/opt_exwinner.py SNDK/MU제외 robust): **PE15=PE30 무비용**(robust 39.5%≈38.2%·메가포함 126.5% 동일), **MDD PE무관 −19% 동일**. PE30=NVDA/AVGO/AMZN/LLY 잡고 froth(BE83/VRT40) 배제. **PE40은 비채택**(robust +50.2% 보이나 비단조 노이즈+메가포함 −12%p). 적용=PE_HOLD 상수 1줄(15→30, 모든 로직 전파)+메시지 3곳(PER15↑→30↑)+주석. **매도 트리거 2개**: ①rank>10 AND fwd_PER≥30 ②EPS꺾임(min_seg<-2) 즉시. 롤백=PE_HOLD 30→15. ⚠️ 미래 winner가 싼 시클리컬 회귀면 PE15 유리(시장관 베팅). 봇 send_per_check.
> **★ US 4팩터 별도연구(2026-06-13, 미배포)**: `C:\dev\claude-code\us-4factor`(GitHub VolumeQuant/us-4factor private). KR형 V/Q/G/M, SEC EDGAR 장기재무, 7년BT Calmar1.16(SPY상회·KR3.86 못미침·강세장의존·생존편향). EPS와 융합=신호불가(거의 직교)·2슬리브 병렬만. 메인 EPS와 별개·연구용.

> **공통 작업 원칙**(사용자 지시 준수 / 표본 먼저 / EDA→인사이트→계획 / 한 번에 하나 / 재사용 우선 등)은 상위 폴더 `C:\dev\CLAUDE.md`에 있고 Claude Code가 자동 로드한다. 이 파일은 **US 전략 운영 기준만** 담는다.
> 변경 이력 상세는 `C:\dev\CHANGELOG.md`.

> **★ 현재 상태 (2026-06-11)**:
> - **V119 제3방안(fwd_PE<15 저평가 보유) 배포 — 메인 cron 재개(2026-06-11)**
> - **메인 cron 활성화** (daily-screening.yml schedule 복구, 평일 미국장 마감 후 발송) — 실투자 5~20% 권장(N=1)
> - 테스트 워크플로우 (`test-private-only.yml`)로 검증 완료(BE+VRT 진입, 전송 정상)
> - 시스템 시뮬 누적 **+193.3%**(전기간 BT) / SPY +7.9%, 시뮬 보유 **[MU, SNDK]** (BT와 정확 일치)
> - **후속 수정(06-11)**: ① 메시지 정리(아이콘 최소·매매규칙 3줄 볼드라벨·PER 표기·보유줄 제거 B안·SNDK 백테스트 자랑 제거=성과 epoch 적용) ② **high30 결정적 계산**(매 실행 fetch→DB 누적가격: dd_30_25가 VRT 같은 경계종목을 실행마다 비결정적 부당제외하던 버그 fix, BT정합) ③ **진입 Top3→Top5**($1B 필터로 Top3 다 막히는 날 우량주(BE 4위) 0종목 방지; BT 수익 동일+LOWO +12.7p, Top5 sweet spot/Top8~20은 LOWO 하락). dd_30_25 자체는 상승장 무가치(0/300)나 약세장 안전장치라 유지.
> - **06-12 작업(메시지·수집·검증)**:
>   · **메시지**: "저평가"→"매력도" 라벨 분리(매수=상대적 매력 / 보유·매도=절대 PER<15, 의미충돌 제거, 폭 동일=줄바꿈0). 약세장 안내문 옛 "가격<12일선"→"PER 15↑" 정합. 테스트워크플로우 발송 확인 OK(BE+NVDA, VRT는 dd_30_25 −25% 경계 탈락). commit f522d66.
>   · **수집 안정화**: 6/11 테스트 n=30 사고=야후 YFRateLimitError(벌크 yf.download threads=True burst). fix: `threads=2`(b4a8120) + yfinance>=0.2.65 버전고정+`YF_PROXY` 후크(e20b8c7, 휴면). 근본원인=GH Actions IP rate-limit(테스트 과다실행시), 가드+30분재시도로 자가복구 확인(n=1202). yfinance 0.2.65+는 이미 curl_cffi impersonate=chrome라 세션주입=no-op.
>   · **검증 4건(research/auto_bt_entry_pe_cap.py, auto_bt_pe_soft.py, auto_regime_validate.py)**: ①진입PE캡=전부 기각(캡 50~15 수익·LOWO↓, winner 배제) ②보유PE 15~25 사실상 동일(+8.6%p는 STX 1종목 2일=노이즈, 미세그리드가 잡음)→현행 15 유지 ③**국면오버레이 26년 검증 견고**: 4대약세장 포착(커버76~89%), MDD −56.8%→−24.7%, Calmar 0.12→0.33, plateau MA확인10~15·VIX36~40, 현행15/36 최적(10일은 얕은dip 휘프소). 단 전략이득은 지수프록시 추정(약세장 종목데이터 無). ④목표가팩터=기각(후행: SNDK/TER 현재가>목표가, IC~0, EPS가 동일 분석가낙관 이미 포착). 목표가 *수집*은 미구현(원하면 추후 daily targetMeanPrice 저장→직접IC 재검증).
> - **결론: 진입PE/보유PE/국면기준 모두 현행이 최적 근처 — 바꿀 것 없음.** 약세장 판단=S&P<MA200(15일확인) OR VIX>36(2일확인)→IEF. 현재 boost(정상).
> - 회사 PC 이어가려면: `git pull` → 목표가 수집 구현 or 보유PE 20 적용 검토(둘 다 선택, 급하지 않음)

> **V119 채택 — 제3방안 fwd_PE<15 저평가 보유 (2026-06-11, 테스트워크플로우 검증 중)**: V118 메가 carryover(PEG<0.18+매출25%)/MA12 전면 교체.
> 계기: 사용자 "SNDK 놓침 충격. 제3의 방안? SNDK가 실제 고평가된 적 있나(EPS 그대론데 가격만 오름)?" → 검증: SNDK는 EPS 86→176 2배 + **PE 9~11 박스(전 기간 저평가)** = 순위만 밀렸지 팔 이유 없었음. v117 순위10밖 매도가 비합리.
> 산식 정밀검증(research/auto_bt_v117_recheck.py): 매출PEG vs fwd_PE 단독 = **완전 동일**(SNDK PE9·PEG0.037 둘 다 저평가). **fwd_PE 단독 채택** — 매출성장 추정 불필요(일회성 매출 착시 無)+단순. PE<15(plateau 12~20, PE<20과 수익 동일 +193%, 더 보수적=거품방어 강).
> BT 매도규칙 비교(전기간 single-path): 단순 +166%/SNDK놓침, MA12 +168%/SNDK놓침, **제3방안 +193%/SNDK끝까지/calmar8.81/회전6**(수익·calmar·회전 최고). **MDD는 4방안 다 -21.9%(매도규칙 무관, 집중도가 결정)** → MDD는 메타배분(80:20=계좌-17.5%)으로 별도관리. slot3 분산 = -90p 수익붕괴+MDD악화(알파=소수winner집중, 분산이 죽임) 기각.
> 매도: EPS꺾임(min_seg<-2) 즉시 → 10위 안 보유 → 10위 밖이면 **fwd_PE<15만 보유**(비싸지면 매도). 진입: slot1·2 모두 part2 Top(메가슬롯 제거). 휩쏘보험 제거(PE는 가격급락시 더 싸져 보유강화).
> 변경 8곳: PE_HOLD=15+_below_pe_live/_live_pe / _replay_holdings / _get_system_performance(+순위 **part2_rank 통일** — 기존 _w_gap 재계산이 BT와 어긋나 보유 BE/NVDA로 튀던 버그 fix) / select_display_top5 / new_buy_top2 / classify_exit_reasons / 메시지(매도사유·🌟저평가보유 PE표시·Watchlist "저평가 유지(PE X)→순위밀려도 보유"·footer).
> **BT==production 정합 확인**: _replay {MU,SNDK}, perf +193.3%/{MU,SNDK} = BT 전기간 정확 일치.
> ⚠️ 4개월 N=1 단일regime(시장은 3월-9%조정·6월하락 있는 변동성장, 본격약세장 아님), "비싸지면 매도"는 이 기간 미발동(SNDK 끝까지 쌈). 약세장은 국면오버레이(S&P200DMA+VIX→IEF)가 별도 처리. dead: check_mega_hold/calc_mega_score/get_mega_hold_tickers/_above_ma12/_today_gap/_w_gap(호출0, 회귀용 잔존).
> 롤백: `git revert <v119커밋>` (또는 v118 3f40de4로).

> **V118 채택 (2026-06-11, commit 3f40de4) — V119로 대체됨**: MA12 추세홀드 제거 + 메가 carryover + 메가 entry (V110 G style 복원).
> 사용자 명령: "SNDK 같은 메가 수익 중간에 놓치는게 충격적. MA12 없애고 전부 BT". 자율주행 9 variants 결과 G best (calmar 7.39, Full +351%, +68p vs v117c).
> 변경: ① `_replay_holdings` 매도: EPS꺾임 / 비메가 + rank>10 / **메가는 무한 carryover (PEG<0.18 + 매출≥25%)** ② `_replay_holdings` 진입: slot 1 part2 Top 1 + slot 2 mega Top 1 ③ `_get_system_performance` 시뮬 동일 logic (BT==production 정합) ④ `select_display_top5` 매수: slot 1 part2 + slot 2 mega_score Top 1 ⑤ `new_buy_top2` 신규: 동일 ⑥ 메시지: header "저평가 1위 + 메가 1위 50/50, 메가는 carryover", footer "메가는 순위 무관 carryover (winner 안 놓침)".
> Caveat: LOWO -SNDK/MU +47.9% (다중 메가 부재기 약점), MDD -24.1% (vs v117c -17.9%, +6p 증가), 75일 BT N=1.
> 전문가 권고 (sub-agent 분석): V118 유지 OK + 4개 daily monitoring trigger 구현 권장 (메가 매도 SNDK PEG>0.20 / MDD -15% 도달 / 메가 부재기 5일 / 비메가 winner 놓침 5일+20%). 사용자 실투자 5-20% 이내.
> Patch BT (commit 2357475): 재진입 우선 logic = 효과 0 (V118 carryover로 매도 자체 없음), 슬롯 3 = -23.1p 손해. **V118 base 그대로 sweet spot**.
> 롤백: `git revert 7c7cb78 3f40de4` (또는 v117c a01fd75로).

> **v117 거래량 $1B+ 필터 + Top 3 한정 (2026-06-09, master 배포됨)**: 사용자 비판 "AEIS/KEYS/HWM 비주도주 거래량 미미". yfinance EDA: 거래량 82배 차이만 유일 시그널 (어닝/모멘텀/revision 모두 무효). v114 + $1B+: calmar 5.70 → 5.93 (+14.5p). v117b: candidates iter Top 3 한정 (BT vs production 불일치 fix). v117c: candidates 정렬을 DB.part2_rank로 통일 (score_100_map 불일치 fix — 6/09 VRT p2=3 누락 사고). V118에서 거래량 필터 유지.

> **배포 완료 (2026-06-09, commit eeeffb7)**: v115 휩쏘 보험밸브 + v116 fresh-start/Signal 단순화 master 병합. 다음 cron부터 적용. 롤백: `git revert eeeffb7 a795a64` (또는 v114 b2ed205로).
> **배포 (2026-06-05, commit b2ed205)**: v111 MA12 추세홀드 + v112 점수 고정스케일 + v114(보유표시 제거·EPS꺾임 매도 유지·문구 정리). 롤백: v86e++(PEG 메가홀드)로 revert.

> 경로: `C:\dev\claude code\eps-momentum-us`

- **v116 fresh-start + Signal 결론 단순화 (2026-06-09, master 배포됨)**: ① **HOLDINGS_EPOCH='2026-06-09'** — 전략 교체로 재생성 DB 보유(MU 등)는 라이브 허구(실제 고객은 옛 신호로 진작 매도) → 보유/매도 표시는 **배포일 이후 실제 진입만** 집계(`_replay_holdings(apply_epoch=True)`, dts를 epoch 이후 필터). 적용: select_display_top5/create_signal_message/classify_exit_reasons. **_get_system_performance는 epoch 미적용** = 백테스트 누적수익률 track record 그대로(사용자 결정). 지금=빈손(fresh start), 06-09 데이터부터 실제 보유 누적. ② **Signal 결론 단순화**: "🛒 오늘의 매수 후보(50/50)" + 보유/매도는 실제 포지션 있을 때만 🌟/🔴 한 줄(사유 명시). 매도 사유: 가격>MA12 "실적 전망 꺾임" / 당일 양봉 "반등했지만 12일선 회복 실패"(가격↑ 매도 혼란 차단) / else "추세 이탈". 1차 시도(시스템포트폴리오+신규/기존 분기)는 혼란으로 폐기. **교훈: 전략교체 시 재구성보유≠라이브보유(허구 표시 금지), 신규자/보유자 이중 audience를 분기로 넣으면 혼란→단순 단일뷰 우월.** 테스트워크플로우 검증(KEYS·HWM fresh 진입, MU carryover 0).

- **v115 휩쏘 보험밸브 (2026-06-09, master 배포됨)**: 보유종목이 **하루 −10%+ 패닉 급락(WHIPSAW_GUARD_GAP=-0.10)으로 MA12 깨진 첫날은 1일 매도 유예** (EPS꺾임 min_seg<-2 즉시매도는 무관). 계기: 06-05 MU(−13%)/SNDK(−11%) 하루딥 휩쏘(MA12 깨짐→매도→직후 회복). BT(`research/_bt_whipsaw_fix.py`): confirm-days·buffer 전부 LOWO 붕괴·수익손실로 기각, **gap-10%만 생존**(측정효과 0=무비용 보험, gap≥8% plateau, LOWO 양수). EPS꺾임 룰과 동일 보험 논리. 구현: `_replay_holdings`(grace set+진입가추적) + `select_display_top5` 인라인 today-결정(`_today_gap` 헬퍼) **양쪽**(매도결정 2곳: replay=어제까지, select=오늘). **기각된 대안**(research/): 목표가 팩터(IC~0 후행지표), 가격모멘텀/staleness 진입필터(winner와 깔짝이 진입 시그니처 공유→분리불가, SNDK winner도 차단). 롤백: WHIPSAW_GUARD_GAP 블록 제거+revert.

- **v114 보유 표시 제거 (2026-06-04, master 배포됨 — v116에서 fresh-start로 재구성)**: **추세 보유(MU/SNDK) 메시지 표시 제거 — v110 지시 복원**. v111에서 다시 넣었던 🌟 추세 보유 박스 제거(신규 진입자 "지나간 종목 약올림"). 보유 로직(perf/replay)은 그대로, 메시지엔 매수 후보만. footer "매도: 순위 이탈 또는 실적 꺾임 / 상승추세(>MA12)면 보유". **EPS꺾임 매도 제거(Alt-A)는 검토 후 기각**: BT상 현행과 +0.0p 동일(75일 상승장에선 미발동)이나, min_seg<-2 매도는 **v55(2026-03-13)~ 핵심 퇴출 규칙**이고 v86 문서상 "사이클천장 디레이팅 시 유일보호"로 명시됨 → 무비용≠무가치(상승장이라 보험금 탈 일이 없었을 뿐). 사용자 결정으로 **EPS꺾임 매도 유지/복원**. research: `research/auto_bt_eps_override.py`(altA=현행 동일 +0.0p, altB 순수MA12 -42.2p 기각).

- **v112 점수 표시 고정 스케일 (2026-06-04, master 배포됨)**: 표시점수 `ws/그날최댓값×100` → **고정 앵커 `(ws-30)/70×100` clip 0~100**. 문제: 분모가 "그날 1등"이라 같은 종목도 그날 누가 1등이냐에 따라 출렁(EDA: 일별 최댓값 15일간 83~112 변동 std7.7, +1.2σ 종목이 74~100점 왔다갔다 = "어제 95점이 왜 오늘 100점"). 해결: ws30(하한/missing)→0, ws100(+2.3σ)→100. **날짜 안정**(같은 펀더멘털=같은 점수) + **강도 보존**(괴물주 MU급→100, 밋밋한 날 1등→72). 검증 KEYS72.3/ITT54.4/중앙값31.5. **매매·DB 영향 0**(순수 표시, 매매는 ws 순위 불변), `score_display_map` 한 곳 변경→Signal·Watchlist 자동반영. 사용자 승인(B안, AskUserQuestion 4안 비교).

- **v111 MA12 추세홀드 (2026-06-04, master 배포됨)**: "일찍 안 팔기" — 보유 종목이 **가격 > 12일 이동평균(MA12, 상승추세)**이면 part2_rank>10이어도 보유, **추세 깨지면(가격<MA12) 또는 EPS 꺾임(min_seg<-2)** 매도. **PEG 메가홀드(v86~v110) 전면 교체**. 계기: 시스템이 winner를 순위>10 조기매도 → MU(+156%)/SNDK(+191%)/STX(+118%) 이탈 후 상승 놓침. PEG홀드는 규칙 2개+고객설득력 약함("47위 SNDK 사라고?"), MA12는 규칙 1개+직관적. **검증(paired 100×3+LOWO+walk-forward+인접안정성)**: baseline 대비 +33p(100/100), MA10~15 plateau(MA12=중심), WF 5/5 블록 양수, **LOWO(-MU-SNDK-STX) +2.1p(broad, 단일종목 착시 아님)**, STX(비메가 winner) 포착(PEG홀드 못잡던 것). 재점검 확정 config: 슬롯2/진입 part2≤3/이탈 rank>10/**50-50**. **BT==production 정합**: `_replay_holdings`=perf-sim holdings={MU,SNDK}, sys_cum +204.6% vs SPY +10.4%(74일). **변경**: `_replay_holdings`/`_get_system_performance`/`select_display_top5`/`classify_exit_reasons` 모두 MA12로 통일(단일규칙), `_above_ma12()` 헬퍼(데이터<6 carryover True), mega_score진입/composite게이트 제거→part2 Top 진입, 메시지 PEG/메가→"추세 보유" 언어. dead: `check_mega_hold`/`calc_mega_score`(호출처 0). ⚠️ 75일 in-sample 단일 bull regime, 약세장은 국면 오버레이(S&P 200DMA+VIX→채권)가 portfolio레벨 처리, EPS trend 과거데이터 없어 약세장 BT 불가. **롤백**: v86e++(PEG 메가홀드)로 git revert. research: `research/auto_bt_hold_winners.py`/`auto_bt_ma10_validate.py`/`auto_bt_ma12_reexam.py`/`auto_bt_strategy_compare.py`

- **v86e++ 데이터 정합성·신뢰성·UX 수정 (2026-06-03, v111로 대체됨)**: ① **carryover 버그**: 메가홀드 carryover가 `_get_prev_portfolio`(동결 portfolio_log, log_portfolio_trades 미호출로 2026-03-05 멈춤)를 읽어 라이브 메가홀드가 한번도 작동 안 했음(perf-sim만 동작). `select_display_top5(today_str)` 파라미터 추가 + **`_replay_holdings`(성능sim 동일 forward replay)로 보유 재구성** → BT==production 입증({KEYS,SNDK} 완전일치). ② **수집 건강성 가드(KR <150 이식)**: `_validate_collection_health`(수집<900 OR 에러율>30%) → 미달 시 30분 후 재수집→그래도 미달이면 랭킹 미기록+채널 차단(개인봇 알림). 2026-05-28~29 yfinance 대량실패(에러53%, 수집600/315 vs 정상1240)인데 가드 없어 망가진 신호 발송된 사고 재발 방지. ③ **핵심성장주 표시(고객 친화)**: "메가/PEG<0.22" 전문용어 전면 제거 → **"🌟핵심 성장주(성장 대비 저평가)"**. 매수후보 점수순 정렬(고확신 먼저). **메가 표시는 현재가치 기준**(PEG<0.22+성장≥25%+EPS안꺾임+최근상위권) — 보유이력 무관 → SNDK·MU 같은 메가는 같게 표시(데이터갭으로 보유끊긴 MU 부당누락 모순 해소). 저평가-조건부 보유임을 명시("저평가 해소되거나 실적 꺾이면 매도" — 무작정 홀드 아님). research: `research/auto_bt_unranked_mega.py`(옵션B 순위무관홀드 = MU 단일착시 +3.6p, 기각)

- **메가홀드 오버라이드 (v86, 브랜치 `v86-mega-hold` — master 미병합, 집PC 메시지확인 후 병합)**: 보유 종목이 메가 시그니처(**NTM EPS 추정치 상향 ntm_current/ntm_90d-1 ≥60% AND PEG<0.2**) 유지 시 part2_rank>10이어도 홀드(매도 스킵). min_seg<-2(EPS꺾임) 매도는 유지=펀더멘털 기반 홀드. 계기: MU(NTM+139%/PEG0.06)·SNDK(+147%/0.04)가 초저평가+EPS폭발 유지인데 fwd_pe_chg 식어 순위밀려 회전매도→큰상승 놓침(사용자 "MU 많이 놓침"). **구현(정석, hold_entries)**: `select_display_top5`에서 어제보유(portfolio_log) 메가를 selected에 우선 캐리오버→슬롯점유→신규는 남은슬롯만→성능/슬롯/이탈 자동정합. 메가홀드 포함 시 50/50 균등(저순위 메가가 2step gap으로 0% 되는 것 방지). `check_mega_hold`/`get_mega_hold_tickers` + watchlist 🔒섹션. **BT 100×3**: +81.5p(100/100), Calmar 8.9→10.3, 부분기간(전반+최근) 둘다 100/100(슬롯3 죽인 테스트 통과), LOWO 무해(MU/SNDK제외 시 0 음수아님), 인접성 평탄. 트레일링스탑은 휘프소로 edge파괴(-28p)→가격스탑 없음. **재최적화(메가홀드 ON에서 slots×exit 그리드)**: slot3 실패(-21p), exit12 +6p지만 LOWO -14/-15(MU/SNDK착시) → **v84 파라미터(슬롯2/진입≤3/이탈>10)가 메가홀드 버전에서도 최적, 추가변경 전부 과적합.** ⚠️ N=2 상승장 in-sample, 사이클천장 디레이팅 시 min_seg가 유일보호. 랭킹불변→DB재계산 불필요. research: `research/auto_bt_mega_hold*.py`, `auto_mega_signature.py`, `auto_reoptimize_mega.py`

- **비성장 소비/미디어 업종 제외 (v85, 2026-06-02)**: `OFF_STRATEGY_INDUSTRIES = {엔터, 전문소매}` 블록리스트 (daily_runner.py, COMMODITY_INDUSTRIES와 동일 메커니즘 — eligible 필터 단계서 제거). 계기: WMG(음반, 6/1 첫 진입) 같은 catalyst형 소비재가 "압도적 성장기업만" 사용자 목적에 안 맞음. **숫자 필터 전부 실패 확인**: rev_growth≥25%(FORM 14%/TTMI 19% winner도 차단), PEG/fwd_PE(MU/SNDK 제외 시 -25~-67p 착시), MA20 가격모멘텀(MU/SNDK 착시), revision 집중도(WMG 77% < FORM 88% 분리불가). **WMG와 진짜 winner FORM이 숫자상 거의 동일** → 유일한 robust 분리축 = 업종(반도체 vs 음반사). BT: 300회 paired에서 winning trade 0개 차단, lift +0.00p (비용 0, WMG/FIVE만 제거). 본질은 통계 edge가 아니라 **가치판단**(원자재 제외와 동일 성격). ⚠️ 미래 폭발성장 미디어/소매 나와도 차단됨 (사용자 선호 명시적). 다음 GA 실행부터 적용. 롤백: 상수+필터 4줄 제거. research: `research/auto_bt_sector_exclude.py`, `auto_bt_value_growth.py`, `auto_diag_eda.py`

- **국면 오버레이 (v84, 2026-05-27)**: S&P 500 < 200일선(15일 확인) OR VIX > 36(2일 확인) → **defense(방어)**. defense 시 주식 매수 중단 + 채권ETF(**IEF 기본 / BIL 안전**) 권장, 200일선 회복(15일) 시 자동 재개. KR v80.16(defense=현금) 참고. **26년(2000~) 시장데이터 EDA(QQQ 프록시)**: 4대 약세장 포착(dotcom 92/GFC 90/COVID 71(VIX)/2022 77%), MDD -83%→-29%(IEF), Cal 0.11→0.50. 인버스ETF는 탈락(52% 오발일+감쇠, Cal 0.20~0.25). 확인 15일 = 2026-04 얕은 dip 휘프소(-105%p) 거르며 진짜 약세장만 포착(버퍼/데드크로스보다 우월, 깊이 아닌 지속기간 판별). **현재 regime=boost → 배포 즉시 영향 0, 미래 약세장에만 발동.** ⚠️ 신호는 26년 검증, 전략 이득은 프록시 추정(약세장 종목데이터 없음). 구현: `get_market_regime`/`_detect_regime_transition`(regime_state.json)/`_get_system_performance` defense 시 IEF 반영. 킬스위치 `REGIME_OVERLAY_DISABLE=1`, 테스트 `REGIME_FORCE`. research: `research/regime_eda_*.py`

- EPS Revision Momentum, conviction z-score 기반, **2슬롯 + dynamic weight (2step_t15) + dd_30_25 진입필터** (v84)
- conviction: adj_gap × (1 + max(up30/N, min(|eps_chg|/100, 3)) + min(min(rg,0.5)×0.6, 0.3))  ← v80.9 X2: cap 3.0, rev_bonus smooth
- adj_gap = fwd_pe_chg × (1 + dir_factor) × eps_quality
- **fwd_pe_chg 가중치 (v80.10)**: **7d 0.30 / 30d 0.10 / 60d 0.10 / 90d 0.50** (90일 누적 PE 압축 강조, long-tail)
- 점수: 일별 z-score(**하한30, 상한 무제한**) → 3일 가중(T0×0.5+T1×0.3+T2×0.2), 빈 날=30점
- **v79**: z-score 상한 100 clamp 제거 → outlier 변별력 보존
- **Case 1 보너스 폐기 (v80.5)**: cr/score_100/part2_rank 정렬 일관성 회복 위해 제거
- 진입: 3일 가중 Top **2** + ✅(3일 검증) + min_seg ≥ 0%, 슬롯 **2** (v82: 3→2)
- **비중 (v84, 2026-05-30)**: **2step_t15 dynamic** — 1·2위 score gap ≥ 15 → 1위 100%/2위 0%, gap < 15 → 50:50. v83.3 (90/10) 폐기 이유: 채택 BT가 매일 rebalance simulator(production entry_fixed 불일치)로 +21.45%p 부풀려진 결과였음. entry_fixed 재BT 시 +1.38%p 불과. v84 BT 검증 (entry_fixed 100x3 paired): incl +5.63%p (98/100), excl(MU+SNDK 제외) +17.27%p (99/100), 평균 +11.45%p robust 우월. **메타 자산 배분 80:20 (시스템:BIL) 사용자 영역 별도** — 시스템은 "투자금" = 80% 부분에서만 운영, 본인이 매년 1월 1회 80:20 리밸런싱 (project_cash_buffer_rule)
- **진입 필터 (v84, 2026-05-30)**: **dd_30_25** — 30거래일 high 대비 -25%+ drawdown 종목 매수 후보 제외. 단기 폭락 종목 자동 차단. DB high30 컬럼 추가 (113,746 entries). BT lift: incl +8.73%p (94/100), excl +7.16%p (77/100)
- **비중 이력**: v82 70/30 → v83 80/20 → v83.3 90/10 (폐기, simulator 결함) → **v84 dynamic (2step_t15)**. 슬롯 idx 기반 배정 (진입 시 cash로 매수, 매도 시 cash 환원)
- **C2 boost 제거 (v83.2, 2026-05-27)**: v83 C2 rank+3 boost를 완전 제거. `_apply_c2_boost_rerank`/`_is_c2_for_v83` 헬퍼 + 호출 3곳 삭제. part2_rank = 순수 w_gap 순위로 복귀 (DB 71일 재마이그레이션, `research/apply_no_boost.py`). **이유**: leave-one-superwinner-out 검증서 C2 boost edge가 전부 MU 한 종목 — MU 제외 시 gate vs no_boost 동전던지기(239/500), M24 음수. binary는 no_boost보다 -8.64%p로 더 나빴음. C1 boost(과거 미적용)도 SNDK 제외 시 -4.68%p(186/500) = 동일 single-stock 착시. **부수 효과**: BWXT(약한 EPS+dip)가 FIX보다 높게 표시되던 점수 왜곡 + 궤적(cr, boost無) vs 픽(p2, boost有) 불일치 동시 해소
- **퇴출 (v80.10b)**: part2_rank > **10** OR min_seg < -2%  ← 8→10 변경 (회전 정책 재최적화)
- **품질 필터 (v79.1)**: FCF < 0 AND ROE < 0 동시 → eligible 제외
- **rev_up30 ≥ 3 필터 (v80.8)**: 단일 분석가 의존 종목 차단 (WELL 사례)
- **Signal 진입 (v80.2)**: ✅ but min_seg<0/하향과반/저커버리지 탈락 시 다음 ✅ 후보로 슬라이드
- **⏸️ 매도 유예 제거 (v80.10c)**: v80.10 장기 가중치 전환으로 ⏸️ 알파(단기 가중 노이즈 완충재) 소멸. BT N=0이 모든 N>0보다 paired 100/100 우월. `check_breakout_hold` 함수는 유지(약세장 재토글용)
- **v81 롤백**: MA120→MA20 단기 모멘텀 필터 시도 → bt_breakout_hold simulator의 pool-exit price masking 버그 발견 후 롤백 (MA120 유지)
- **HISTORICAL MODE (v83.1)**: yfinance eps_trend `7daysAgo/30d/60d/90daysAgo`가 호출 시점 기준 → MARKET_DATE 과거 재실행 시 window(사용자 날짜)와 EPS 값(yf 시점) misalign → adj_gap drift. `is_historical_mode()` 감지 시 fetch SKIP + DB part2_rank 그대로 사용(write 0). **production cron 영향 없음** (매일 새 날짜 = real_today 정합), test workflow + 과거 날짜만 영향
- composite_rank=당일 conviction 순위(추이 표시), part2_rank=3일 가중 순위(매매)
- RETURN_MATRIX: S&P500 기반 (26년 6,593일), VIX는 yfinance 최신 보완
- 시장 공포 기반 비중 조절 안 함 (portfolio_mode normal 하드코딩 — 알파가 공포 구간에서 발생). 종목간 dynamic weight는 별개
- 상관관계: 🔗 유사도% + BFS 그룹핑 + 택1/택1~2 권장
- **leave-one-superwinner-out 교훈 (v83.2)**: 71일 단일 표본 + 2슬롯 80/20에서 boost/집중 메커니즘은 MU/SNDK 한 종목만 빼도 edge가 무너짐(동전던지기 or 음수) = single-stock 착시. **boost·집중 평가 시 반드시 dominant winner 제외 robustness 확인.**
- **롤백 트리거 (v84)**: 5거래일 SPY 대비 알파 -5%p / MDD -10% 초과. v84 롤백 시 `daily_runner.py`의 dd_30_25 필터 + 2step_t15 dynamic weight 로직 환원 (v83.2 기준 80/20 정적) + git revert. 단 v83.3 자체가 simulator 결함 결과였으므로 v83.2 (80/20)로 환원이 안전 기준
