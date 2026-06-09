# -*- coding: utf-8 -*-
import yfinance as yf
import json

for t in ['MU','SNDK','KEYS','HWM']:
    print('='*60)
    print(t)
    print('='*60)
    tk = yf.Ticker(t)
    info = tk.info
    # target price related fields in .info
    keys = ['targetMeanPrice','targetHighPrice','targetLowPrice','targetMedianPrice',
            'numberOfAnalystOpinions','recommendationMean','recommendationKey',
            'currentPrice']
    print('--- .info target fields ---')
    for k in keys:
        print(f'  {k}: {info.get(k)}')
    # analyst_price_targets (newer yfinance)
    try:
        apt = tk.analyst_price_targets
        print('--- analyst_price_targets ---')
        print(' ', apt)
    except Exception as e:
        print('  analyst_price_targets ERR:', e)
    # upgrades_downgrades (dated actions)
    try:
        ud = tk.upgrades_downgrades
        print('--- upgrades_downgrades (tail 8) ---')
        if ud is not None and len(ud):
            print(ud.tail(8).to_string())
        else:
            print('  (empty)')
    except Exception as e:
        print('  upgrades_downgrades ERR:', e)
    # recommendations_summary / trend
    try:
        rt = tk.recommendations
        print('--- recommendations (head) ---')
        print(rt.head(5).to_string() if rt is not None else None)
    except Exception as e:
        print('  recommendations ERR:', e)
    print()
