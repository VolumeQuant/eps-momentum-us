# -*- coding: utf-8 -*-
"""자율 보고서 → 개인봇 발송 (자격증명 있는 환경에서만 실제 발송, 로컬은 graceful skip).
실행: python research/_send_autoreport.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MSG = """📋 <b>자율 보고 — 가치 vs 모멘텀 끝장 연구 (6/26)</b>

🎯 <b>결론: 모멘텀 2슬롯 유지 확정.</b>

<b>1. 가치 신호는 진짜였다</b> (내 이전 us-4factor 기각은 버그—후행gap 썼었음). forward gap으로 정정→8년 Calmar 0.7→2.0, 2022 약세장 +38% 방어. 애널 ±40% 오차 줘도 Calmar 1.77 = robust.

<b>2. 근데 모멘텀이 근소하게 낫다</b>: Calmar 2.7 vs 2.0, 승률 93% vs 84%. 단일이면 모멘텀.

<b>3. US 시너지 0 이유</b>: gap↔revision corr +0.83(같은 책). KR은 +0.015(직교)라 KR엔 별도 가치 sleeve 시너지 가능 → KR 핸드오프 작성함.

<b>4. 통합 9가지 전부 손해</b>: 진입에 가치 얹으면 모멘텀 승자(NVDA 저gap·SNDK 데이터공백)를 veto. revision=궤적(강건), gap레벨=스냅샷(노이즈).

⭐ <b>5. 진짜 lead — gap≥2.5 진입게이트(데이터없으면 통과)</b>
· 91일 faithful: +217→+221%, STX 빼도 +36p(단일종목운 아님)
· <b>★네 보정("약세장=현금 빼라") 적용 후 오히려 강해짐</b>: 국면 오버레이로 방어13개월 현금처리 → K=2 Calmar 0.77→<b>1.09(+0.32)</b>. 게이트 이득은 약세장(현금이라 무의미) 아니라 boost기간 종목선택이었음. 내 사전추측 틀림.
· ⚠️caveat: 8년 proxy·연도노이즈·K=10은 패
· <b>판정=라이브 자동배포 보류</b>(proxy검증, 배포철칙) but 한단계 격상=진짜 후보. ENTRY_GAP_GATE 플래그로 페이퍼관찰→faithful 재확인 후 네 판정.

📁 상세: research/AUTO_REPORT_2026_06_26.md (커밋됨)
오늘 작업 메모리 저장 + 전체 커밋푸쉬 완료."""

def main():
    try:
        from daily_runner import load_config, send_telegram_long
        config = load_config()
        if not config.get('telegram_enabled'):
            print("[autoreport] telegram 비활성(로컬) — 발송 skip. 보고서는 research/AUTO_REPORT_2026_06_26.md 참조.")
            return
        pid = config.get('telegram_private_id') or config.get('telegram_chat_id')
        if not pid:
            print("[autoreport] 개인봇 ID 없음 — 발송 skip.")
            return
        send_telegram_long(MSG, config, chat_id=pid)
        print("[autoreport] 개인봇 발송 완료.")
    except Exception as e:
        print(f"[autoreport] 발송 실패(무시): {e}")

if __name__ == '__main__':
    main()
