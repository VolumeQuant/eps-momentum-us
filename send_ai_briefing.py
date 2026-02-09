"""AI 브리핑만 재발송하는 1회용 스크립트"""
import sys
# Note: daily_runner.py import 시 stdout UTF-8 래핑이 자동 적용됨

from daily_runner import (
    load_config, log, run_ntm_collection,
    run_ai_analysis, send_telegram_long
)
from datetime import datetime

def main():
    log("AI 브리핑 재발송 스크립트")
    config = load_config()

    # Gemini API 키 확인
    if not config.get('gemini_api_key'):
        log("GEMINI_API_KEY 없음 — config.json 또는 환경변수 확인", "ERROR")
        return 1

    # 데이터 수집 (오늘자 데이터로 results_df 재구성)
    log("데이터 수집 중 (results_df 재구성)...")
    results_df, turnaround_df, stats = run_ntm_collection(config)

    if results_df.empty:
        log("results_df 비어있음", "ERROR")
        return 1

    log(f"수집 완료: {len(results_df)}종목")

    # AI 브리핑 생성
    log("AI 브리핑 생성 중...")
    msg_ai = run_ai_analysis(None, None, None, config, results_df=results_df)

    if not msg_ai:
        log("AI 브리핑 생성 실패", "ERROR")
        return 1

    log(f"AI 브리핑 생성 완료 ({len(msg_ai)}자)")

    # 텔레그램 발송 — 개인봇에만 (테스트)
    if config.get('telegram_enabled', False):
        private_id = config.get('telegram_private_id') or config.get('telegram_chat_id')

        if private_id:
            send_telegram_long(msg_ai, config, chat_id=private_id)
            log("AI 브리핑 전송 완료 → 개인봇")
    else:
        log("텔레그램 비활성화 — 메시지 출력만")
        print(msg_ai)

    log("완료!")
    return 0

if __name__ == '__main__':
    sys.exit(main())
