"""전체 메시지 테스트 발송 스크립트 (개인봇 전용)"""
import sys
import time
# Note: daily_runner.py import 시 stdout UTF-8 래핑이 자동 적용됨

from daily_runner import (
    load_config, log, run_ntm_collection,
    create_part1_message, create_part2_message, create_system_log_message,
    run_ai_analysis, run_portfolio_recommendation, send_telegram_long
)

def main():
    start = time.time()
    log("전체 메시지 테스트 발송")
    config = load_config()

    # 데이터 수집
    log("데이터 수집 중...")
    results_df, turnaround_df, stats = run_ntm_collection(config)

    if results_df.empty:
        log("results_df 비어있음", "ERROR")
        return 1

    elapsed = time.time() - start
    log(f"수집 완료: {len(results_df)}종목")

    # 메시지 생성
    msg_part1 = create_part1_message(results_df)
    msg_part2 = create_part2_message(results_df)
    msg_ai = run_ai_analysis(msg_part1, msg_part2, None, config, results_df=results_df)
    msg_portfolio = run_portfolio_recommendation(config, results_df)
    msg_log = create_system_log_message(stats, elapsed, config)

    # 텔레그램 발송 — 개인봇에만
    if config.get('telegram_enabled', False):
        private_id = config.get('telegram_private_id') or config.get('telegram_chat_id')

        if private_id:
            send_telegram_long(msg_part1, config, chat_id=private_id)
            log("Part 1 전송 완료 → 개인봇")

            send_telegram_long(msg_part2, config, chat_id=private_id)
            log("Part 2 전송 완료 → 개인봇")

            if msg_ai:
                send_telegram_long(msg_ai, config, chat_id=private_id)
                log("AI 브리핑 전송 완료 → 개인봇")

            if msg_portfolio:
                send_telegram_long(msg_portfolio, config, chat_id=private_id)
                log("포트폴리오 추천 전송 완료 → 개인봇")

            send_telegram_long(msg_log, config, chat_id=private_id)
            log("시스템 로그 전송 완료 → 개인봇")
    else:
        log("텔레그램 비활성화 — 메시지 출력만")
        print(msg_part1)
        print(msg_part2)
        if msg_ai:
            print(msg_ai)
        if msg_portfolio:
            print(msg_portfolio)
        print(msg_log)

    log("완료!")
    return 0

if __name__ == '__main__':
    sys.exit(main())
