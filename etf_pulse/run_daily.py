"""ETF Pulse Daily Pipeline — 통합 cron 진입점

매일 한국 시간 06:00 실행:
  1. daily_fetch: yfinance 데이터 수집
  2. signals: 신호 추출
  3. narrative + narrative_en: 한국어/영어 콘텐츠
  4. category_best: 카테고리별 best 추천
  5. intent_best: 의도별 best
  6. advanced_signals: 추적오차 등
  7. ranking_changes: 주간 변동
  8. charts: 시각화
  9. publisher: 텔레그램 + 이메일
"""
import sys
import time
import traceback
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))


def run_step(name, fn, *args, **kwargs):
    """단계별 안전 실행 (에러 시 다음 단계 진행)"""
    print(f'\n[{name}] 시작')
    try:
        t0 = time.time()
        result = fn(*args, **kwargs)
        print(f'[{name}] ✓ 완료 ({time.time()-t0:.0f}s)')
        return True, result
    except Exception as e:
        print(f'[{name}] ✗ 실패: {e}')
        traceback.print_exc()
        return False, None


def main():
    t0 = time.time()
    print('=' * 80)
    print(f'★ ETF Pulse Daily — {time.strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 80)

    # Step 1: 데이터 수집
    import daily_fetch
    run_step('1. daily_fetch', daily_fetch.main)

    # Step 2: 신호
    from signals import get_signals, print_signals
    ok, signals = run_step('2. signals', get_signals)
    if not ok: return

    # Step 3: 한국어 콘텐츠
    from narrative import gen_narrative_kr, save_content
    def gen_kr():
        content = gen_narrative_kr(signals)
        save_content(content, signals['date'])
        return content
    ok, content_kr = run_step('3. narrative_kr', gen_kr)

    # Step 4: 영어 콘텐츠
    from narrative_en import gen_narrative_en
    def gen_en():
        md_en = gen_narrative_en(signals)
        out_en = Path(__file__).parent / 'content' / f'pulse_{signals["date"]}_en.md'
        out_en.write_text(md_en, encoding='utf-8')
        return md_en
    run_step('4. narrative_en', gen_en)

    # Step 5: 카테고리 best
    from category_best import gen_weekly_best_md
    def gen_cat():
        md = gen_weekly_best_md(signals['date'])
        out = Path(__file__).parent / 'content' / 'category_best_weekly.md'
        out.write_text(md, encoding='utf-8')
    run_step('5. category_best', gen_cat)

    # Step 6: 의도별 best
    from intent_best import gen_intent_best_md
    def gen_int():
        md = gen_intent_best_md(signals['date'])
        out = Path(__file__).parent / 'content' / 'intent_best.md'
        out.write_text(md, encoding='utf-8')
    run_step('6. intent_best', gen_int)

    # Step 7: 추적오차
    from advanced_signals import gen_tracking_error_md
    def gen_track():
        md = gen_tracking_error_md()
        out = Path(__file__).parent / 'content' / 'tracking_error.md'
        out.write_text(md, encoding='utf-8')
    run_step('7. tracking_error', gen_track)

    # Step 8: 주간 변동
    from ranking_changes import gen_changes_md
    def gen_chg():
        md = gen_changes_md(signals['date'])
        out = Path(__file__).parent / 'content' / 'weekly_changes.md'
        out.write_text(md, encoding='utf-8')
    run_step('8. weekly_changes', gen_chg)

    # Step 9: 차트
    from charts import chart_all
    run_step('9. charts', chart_all, signals['date'])

    # Step 10: 발행 (텔레그램 + 이메일)
    from publisher import md_to_telegram_html, send_telegram, save_for_substack, load_config
    from email_sender import send_email, load_email_config
    if content_kr:
        # Substack 저장
        date_str = signals['date']
        save_for_substack(content_kr, date_str)

        # 텔레그램
        cfg = load_config()
        if cfg.get('telegram_bot_token'):
            html = md_to_telegram_html(content_kr)
            run_step('10a. telegram', send_telegram, html,
                     cfg['telegram_bot_token'], cfg.get('telegram_chat_id'))

        # 이메일
        ecfg = load_email_config()
        if ecfg.get('gmail_app_password'):
            run_step('10b. email', send_email,
                     f'🌅 ETF Pulse — {date_str}', content_kr, ecfg)

    print(f'\n총 소요: {time.time()-t0:.0f}s')
    print('=' * 80)


if __name__ == '__main__':
    main()
