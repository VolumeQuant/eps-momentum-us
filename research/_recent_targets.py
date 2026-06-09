# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
for t in ['MU','SNDK','KEYS','HWM','STX']:
    print('='*60, t)
    tk = yf.Ticker(t)
    try:
        ud = tk.upgrades_downgrades
        if ud is None or not len(ud):
            print('  (empty)'); continue
        ud = ud.sort_index(ascending=False)  # newest first
        recent = ud[ud.index >= '2026-04-01']
        if not len(recent):
            print('  2026-04 이후 액션 없음. 최신 5건:')
            print(ud.head(5)[['Firm','Action','priceTargetAction','currentPriceTarget','priorPriceTarget']].to_string())
        else:
            print(recent[['Firm','Action','priceTargetAction','currentPriceTarget','priorPriceTarget']].to_string())
    except Exception as e:
        print('  ERR', e)
