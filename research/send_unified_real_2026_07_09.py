# -*- coding: utf-8 -*-
"""실데이터(US 실시간 + KR 스냅샷 DB) 통합 신호 풀버전 발송 — Actions용 (일회성 샘플)."""
import os, sys, runpy
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ['KR_DB_PATH'] = os.path.join(BASE, 'research', 'kr_db_snapshot_2026_07_09.db')
os.chdir(BASE)
sys.argv = ['unified_vm_track.py', '--run']
runpy.run_path(os.path.join(BASE, 'unified_vm_track.py'), run_name='__main__')
