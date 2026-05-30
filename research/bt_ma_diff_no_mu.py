"""MU 제외 후 current vs ma60_only trade-level diff
DB: research/ma_filter_dbs/no_mu_current.db, no_mu_ma60_only.db (이미 regenerate 완료)
"""
import sys
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'research')
import bt_ma_diff_current_vs_ma60 as diff

ROOT = Path(__file__).parent.parent

# override DB paths
diff.DB_CUR = ROOT / 'research' / 'ma_filter_dbs' / 'no_mu_current.db'
diff.DB_M60 = ROOT / 'research' / 'ma_filter_dbs' / 'no_mu_ma60_only.db'

if __name__ == '__main__':
    diff.main()
