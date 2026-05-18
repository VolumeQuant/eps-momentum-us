"""yfinance로 1451 종목의 14개월 가격 이력 fetch — MA20~MA200 BT용

저장: research/price_history_for_ma_bt.parquet (date × ticker, Close)
"""
import sys
import sqlite3
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

import yfinance as yf
import pandas as pd

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / 'eps_momentum_data.db'
OUT_PATH = Path(__file__).parent / 'price_history_for_ma_bt.parquet'

# BT 가장 빠른 일자: 2026-02-06 → MA200 위해 200거래일 + 여유 = 약 14개월 전부터
START = '2025-04-01'
END = '2026-05-16'


def main():
    conn = sqlite3.connect(DB_PATH)
    tickers = sorted({r[0] for r in conn.execute(
        'SELECT DISTINCT ticker FROM ntm_screening'
    ).fetchall()})
    conn.close()
    print(f'Tickers: {len(tickers)}')

    # 100개씩 batch fetch (yfinance 안정성)
    BATCH = 100
    frames = []
    t0 = time.time()
    for i in range(0, len(tickers), BATCH):
        chunk = tickers[i:i + BATCH]
        elapsed = time.time() - t0
        print(f'  [{elapsed:>6.1f}s] batch {i//BATCH + 1}/{(len(tickers)+BATCH-1)//BATCH}: {len(chunk)} tickers ({chunk[0]}~{chunk[-1]})')
        try:
            df = yf.download(
                chunk, start=START, end=END, auto_adjust=True,
                progress=False, threads=True, group_by='ticker',
            )
        except Exception as e:
            print(f'    Error: {e}, skip batch')
            continue
        if df is None or len(df) == 0:
            print('    empty result, skip')
            continue
        # group_by='ticker' → MultiIndex columns (ticker, field)
        # we want only Close → unstack to (date × ticker)
        close_dict = {}
        if isinstance(df.columns, pd.MultiIndex):
            for tk in chunk:
                if (tk, 'Close') in df.columns:
                    close_dict[tk] = df[(tk, 'Close')]
        else:
            # single ticker
            if 'Close' in df.columns:
                close_dict[chunk[0]] = df['Close']
        if close_dict:
            frames.append(pd.DataFrame(close_dict))

    if not frames:
        print('No data fetched!')
        return

    full = pd.concat(frames, axis=1)
    full.index = pd.to_datetime(full.index).date.astype(str)
    print(f'\nFinal shape: {full.shape}')
    print(f'Date range: {full.index.min()} ~ {full.index.max()}')
    print(f'Coverage: {full.notna().sum().sum() / (full.shape[0] * full.shape[1]) * 100:.1f}%')

    full.to_parquet(OUT_PATH)
    print(f'Saved: {OUT_PATH}')


if __name__ == '__main__':
    main()
