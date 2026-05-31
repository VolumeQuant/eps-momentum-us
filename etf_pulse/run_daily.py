"""ETF Pulse Daily Pipeline — 통합 cron 진입점

매일 한국 시간 06:00 (미국 시장 마감 후 5시간) 실행:
  1. daily_fetch.py: yfinance 데이터 수집 + DB 저장
  2. signals.py: 신호 추출
  3. narrative.py: 콘텐츠 자동 생성
  4. publisher.py: 텔레그램 + Substack 발행
"""
import sys
import time
import traceback
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent))


def main():
    t0 = time.time()
    print('=' * 80)
    print(f'★ ETF Pulse Daily — {time.strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 80)

    # Step 1: 데이터 수집
    print('\n[Step 1] 데이터 수집 (yfinance fetch)')
    try:
        import daily_fetch
        daily_fetch.main()
    except Exception as e:
        print(f'  실패: {e}')
        traceback.print_exc()
        return False

    # Step 2: 신호 추출
    print(f'\n[Step 2] 신호 추출 ({time.time()-t0:.0f}s)')
    try:
        from signals import get_signals, print_signals
        signals = get_signals()
        print_signals(signals)
    except Exception as e:
        print(f'  실패: {e}')
        traceback.print_exc()
        return False

    # Step 3: 콘텐츠 생성
    print(f'\n[Step 3] 콘텐츠 생성 ({time.time()-t0:.0f}s)')
    try:
        from narrative import gen_narrative_kr, save_content
        content = gen_narrative_kr(signals)
        out_file = save_content(content, signals['date'])
        print(f'  저장: {out_file}')
    except Exception as e:
        print(f'  실패: {e}')
        traceback.print_exc()
        return False

    # Step 4: 발행
    print(f'\n[Step 4] 발행 ({time.time()-t0:.0f}s)')
    try:
        from publisher import md_to_telegram_html, send_telegram, save_for_substack, load_config
        date_str = signals['date']
        save_for_substack(content, date_str)
        cfg = load_config()
        if cfg.get('telegram_bot_token'):
            html = md_to_telegram_html(content)
            ok, msg = send_telegram(html, cfg['telegram_bot_token'], cfg.get('telegram_chat_id'))
            print(f'  텔레그램: {"✓" if ok else "✗"} {msg}')
        else:
            print('  텔레그램 config 없음 — 수동 발행')
    except Exception as e:
        print(f'  발행 실패: {e}')
        traceback.print_exc()

    print(f'\n총 소요: {time.time()-t0:.0f}s')
    print('=' * 80)
    return True


if __name__ == '__main__':
    main()
