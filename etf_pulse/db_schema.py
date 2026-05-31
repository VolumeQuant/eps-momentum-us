"""ETF Pulse DB schema 초기화"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / 'etf_pulse.db'


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ━━━ 일별 ETF 스냅샷 (가격, 거래량, AUM, 수익률) ━━━
    cur.execute('''
        CREATE TABLE IF NOT EXISTS etf_daily (
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            category TEXT,
            price REAL,
            volume INTEGER,
            avg_volume_30d INTEGER,
            volume_spike REAL,      -- volume / avg_volume_30d
            aum REAL,                -- total assets
            day_return REAL,         -- (price - prev_close) / prev_close
            estimated_flow REAL,     -- AUM diff - 가격수익률 효과
            expense_ratio REAL,
            dividend_yield REAL,
            beta REAL,
            PRIMARY KEY (date, ticker)
        )
    ''')

    # ━━━ ETF 보유종목 일별 스냅샷 (top 10 + 전체 종목 수) ━━━
    cur.execute('''
        CREATE TABLE IF NOT EXISTS etf_holdings_daily (
            date TEXT NOT NULL,
            etf_ticker TEXT NOT NULL,
            holding_ticker TEXT NOT NULL,
            holding_name TEXT,
            weight REAL,             -- 비중 (0~1)
            rank INTEGER,            -- 1, 2, 3, ...
            PRIMARY KEY (date, etf_ticker, holding_ticker)
        )
    ''')

    # ━━━ 보유종목 변동 history (diff 결과) ━━━
    cur.execute('''
        CREATE TABLE IF NOT EXISTS etf_holdings_changes (
            date TEXT NOT NULL,
            etf_ticker TEXT NOT NULL,
            holding_ticker TEXT NOT NULL,
            change_type TEXT,        -- 'NEW', 'EXIT', 'INCREASE', 'DECREASE'
            old_weight REAL,
            new_weight REAL,
            weight_delta REAL,
            PRIMARY KEY (date, etf_ticker, holding_ticker)
        )
    ''')

    # ━━━ 일별 뉴스 (ETF별) ━━━
    cur.execute('''
        CREATE TABLE IF NOT EXISTS etf_news (
            etf_ticker TEXT NOT NULL,
            news_id TEXT NOT NULL,
            title TEXT,
            link TEXT,
            publisher TEXT,
            published_at TEXT,
            fetched_at TEXT,
            PRIMARY KEY (etf_ticker, news_id)
        )
    ''')

    # ━━━ 일별 발행 콘텐츠 (Newsletter 자동 생성) ━━━
    cur.execute('''
        CREATE TABLE IF NOT EXISTS daily_content (
            date TEXT PRIMARY KEY,
            top_flow_etfs TEXT,      -- JSON
            top_volume_spikes TEXT,  -- JSON
            holdings_changes TEXT,   -- JSON
            narrative TEXT,          -- AI 생성
            published_at TEXT,
            channel TEXT             -- 'substack', 'telegram', etc
        )
    ''')

    conn.commit()
    conn.close()
    print(f'DB 초기화 완료: {DB_PATH}')

    # 확인
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for table in ['etf_daily', 'etf_holdings_daily', 'etf_holdings_changes', 'etf_news', 'daily_content']:
        cols = cur.execute(f'PRAGMA table_info({table})').fetchall()
        print(f'  {table}: {len(cols)} cols')
    conn.close()


if __name__ == '__main__':
    init_db()
