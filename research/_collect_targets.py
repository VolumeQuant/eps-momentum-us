# -*- coding: utf-8 -*-
import yfinance as yf
import json, time, sys
tickers = json.load(open(r'C:\dev\claude code\eps-momentum-us\research\_window_tickers.json'))
out = {}
fail = []
for i, t in enumerate(tickers):
    try:
        ud = yf.Ticker(t).upgrades_downgrades
        if ud is None or not len(ud):
            out[t] = []
        else:
            recs = []
            for idx, row in ud.iterrows():
                recs.append({
                    'date': str(idx)[:10],
                    'firm': row.get('Firm'),
                    'pta': row.get('priceTargetAction'),
                    'cur': float(row.get('currentPriceTarget') or 0),
                    'prior': float(row.get('priorPriceTarget') or 0),
                    'action': row.get('Action'),
                })
            out[t] = recs
    except Exception as e:
        fail.append((t, str(e)[:60])); out[t] = []
    if (i+1) % 10 == 0:
        print(f'  {i+1}/{len(tickers)} done', flush=True)
    time.sleep(0.25)
json.dump(out, open(r'C:\dev\claude code\eps-momentum-us\research\_targets_cache.json','w'))
print('수집완료. 종목:', len(out), '| 빈 데이터:', sum(1 for v in out.values() if not v), '| 실패:', len(fail))
if fail: print('실패목록:', fail[:20])
