# -*- coding: utf-8 -*-
"""V92 자율주행 — 4 후보 검증 + "v84 이후 흔들림" 진단

사용자 지적: "v84 이후로 자꾸 흔들리네" — patch on patch 의심.
v84 → v85 (업종) → v86 (carryover) → V86e (PEG only) → V86e+ (PEG 0.22)

각 단계 진짜 진보인지 BT로 재검증 + 4 후보 검증:
  A. eps_quality 재설계 (min_seg → mean_seg / weighted_seg) — 가속도 보존
  B. rev_growth cutoff 정밀 (0.10/0.15/0.20/0.25/0.30)
  C. dd_30 메가 면제 (메가는 -25% drawdown 진입필터 X)
  D. 결합 best (가장 robust한 conf)

bonus: v84 → V86e+ 각 단계 단독 효과 분리 검증
"""
import sys, sqlite3, random, statistics, time
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).parent.parent
DB = ROOT / 'eps_momentum_data.db'
N_SEEDS = 100
SAMPLES = 3
MIN_HOLD = 10

t0 = time.time()
con = sqlite3.connect(DB); cur = con.cursor()
dates = [r[0] for r in cur.execute('SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date')]
data = {}
for d in dates:
    data[d] = {}
    for r in cur.execute(
        '''SELECT ticker,part2_rank,composite_rank,price,score,ntm_current,ntm_7d,ntm_30d,ntm_60d,ntm_90d,high30,rev_growth FROM ntm_screening WHERE date=?''',
        (d,)):
        tk = r[0]
        nc, n7, n30, n60, n90 = (float(x) if x else 0 for x in r[5:10])
        segs = []
        for a, b in [(nc, n7), (n7, n30), (n30, n60), (n60, n90)]:
            segs.append(max(-100, min(100, (a - b) / abs(b) * 100)) if b and abs(b) > 0.01 else 0)
        rg = r[11]
        fpe = (r[3] / nc) if (r[3] and nc > 0) else None
        peg = (fpe / (rg * 100)) if (fpe and rg and rg > 0) else None
        data[d][tk] = dict(p2=r[1], cr=r[2], price=r[3], score=r[4] or 0,
                           segs=segs,
                           min_seg=min(segs) if segs else 0,
                           mean_seg=sum(segs)/len(segs) if segs else 0,
                           # weighted: 최근 가중치 더 큼 (seg1=가장 최근)
                           w_seg=(segs[0]*0.4 + segs[1]*0.3 + segs[2]*0.2 + segs[3]*0.1) if len(segs)>=4 else 0,
                           high30=r[10], peg=peg, rev_growth=rg)

pf = defaultdict(dict)
for tk, d, p in cur.execute('SELECT ticker,date,price FROM ntm_screening WHERE price IS NOT NULL'):
    pf[d][tk] = p


def verified(t, i):
    for j in (i, i - 1, i - 2):
        if j < 0:
            return False
        x = data[dates[j]].get(t)
        if not x or x.get('cr') is None or x['cr'] > 30:
            return False
    return True


def is_mega(info, peg_thr=0.22):
    return info is not None and info.get('peg') is not None and info['peg'] < peg_thr


def sim(config, exclude=(), start=0):
    """config dict: {
        'mega_carry': bool (V86e+ carryover),
        'peg_thr': float (메가 PEG 임계값),
        'rev_exit': float (rev_growth 매도 cutoff),
        'seg_metric': 'min' | 'mean' | 'weighted' (entry filter, exit trigger의 seg),
        'dd_mega_exempt': bool (메가는 dd_30 filter 면제),
        'dd_thr': float (dd_30 임계값, default 0.25)
    }
    """
    mega_carry = config.get('mega_carry', True)
    peg_thr = config.get('peg_thr', 0.22)
    rev_exit = config.get('rev_exit', 0.25)
    seg_metric = config.get('seg_metric', 'min')
    dd_mega_exempt = config.get('dd_mega_exempt', False)
    dd_thr = config.get('dd_thr', 0.25)

    def get_seg(info):
        if seg_metric == 'mean':
            return info.get('mean_seg', 0)
        elif seg_metric == 'weighted':
            return info.get('w_seg', 0)
        else:
            return info.get('min_seg', 0)

    held = {}
    prev = None
    val = 1.0
    peak = 1.0
    mdd = 0.0
    for i in range(start, len(dates)):
        d = dates[i]
        if prev and i > start:
            dp = dates[i - 1]
            ret = 0
            for tk, (ed, ep, w) in prev.items():
                pp = pf[dp].get(tk)
                pn = pf[d].get(tk, pp)
                if pp and pn:
                    ret += w * (pn / pp - 1)
            val *= (1 + ret)
            peak = max(peak, val)
            mdd = max(mdd, (peak - val) / peak)
        dd = data[d]

        # 매도
        for tk in list(held):
            info = dd.get(tk)
            if info is not None:
                if get_seg(info) < -2:
                    del held[tk]
                    continue
                if mega_carry and info.get('rev_growth') is not None and info['rev_growth'] < rev_exit:
                    del held[tk]
                    continue
            p2 = info.get('p2') if info else None
            if info is None or p2 is None or p2 > 10:
                if mega_carry and is_mega(info, peg_thr):
                    continue
                del held[tk]

        # 매수
        if len(held) < 2:
            cands = []
            for tk, info in dd.items():
                if info['p2'] is None or info['p2'] > 3:
                    continue
                if tk in held or tk in exclude:
                    continue
                if get_seg(info) < 0:
                    continue
                if not info['price']:
                    continue
                if not verified(tk, i):
                    continue
                # dd_30 filter
                if info.get('high30') and info['price']:
                    dd_ratio = info['price'] / info['high30'] - 1
                    if dd_ratio < -dd_thr:
                        # 메가 면제 옵션
                        if dd_mega_exempt and is_mega(info, peg_thr):
                            pass  # 면제
                        else:
                            continue
                cands.append((info['p2'], info['score'], tk))
            cands.sort(key=lambda x: x[0])
            pick = cands[:2 - len(held)]
            if len(held) == 0 and len(pick) >= 2:
                s1, s2 = pick[0][1], pick[1][1]
                w = [1.0, 0.0] if (s1 - s2) >= 15 else [0.5, 0.5]
                for si, (_, _, tk) in enumerate(pick[:2]):
                    if w[si] > 0:
                        held[tk] = (d, dd[tk]['price'], w[si])
            else:
                for _, _, tk in pick:
                    held[tk] = (d, dd[tk]['price'], 0.5 if len(held) >= 1 else 1.0)
        prev = dict(held)
    return dict(cum=(val - 1) * 100, mdd=mdd * 100)


elig = dates[:-MIN_HOLD]
seeds = []
for s in range(N_SEEDS):
    random.seed(s)
    seeds.append(random.sample(range(len(elig)), SAMPLES))


def run(config, exclude=()):
    cums = []
    savg = []
    for ch in seeds:
        sr = []
        for s in ch:
            r = sim(config, exclude=exclude, start=s)
            cums.append(r['cum'])
            sr.append(r['cum'])
        savg.append(sum(sr) / len(sr))
    return cums, savg


# ============================================================================
print('=' * 100)
print('V92 자율주행: 4 후보 검증 + "v84 이후 흔들림" 진단')
print('=' * 100)

# baseline: 메가홀드 없음 (= v84 직후 상태)
base_config = dict(mega_carry=False, peg_thr=0.22, rev_exit=0, seg_metric='min',
                   dd_mega_exempt=False, dd_thr=0.25)
_, base_savg = run(base_config)
print(f'\nbaseline (v84) avg: {sum(base_savg)/len(base_savg):+.1f}%')

# V86e+ current production
v86e_config = dict(mega_carry=True, peg_thr=0.22, rev_exit=0.25, seg_metric='min',
                   dd_mega_exempt=False, dd_thr=0.25)
_, v86e_savg = run(v86e_config)
print(f'V86e+ (current) avg: {sum(v86e_savg)/len(v86e_savg):+.1f}%')

# ────────────────────────────────────────────────────────────────────────
print('\n' + '=' * 80)
print('Phase A: eps_quality 재설계 (seg_metric)')
print('=' * 80)
print(f'{"variant":<25}{"avg lift vs base":>20}{"wins":>10}{"LOWO -MU-SNDK":>20}')
print('-' * 75)
for sm in ['min', 'mean', 'weighted']:
    cfg = dict(v86e_config); cfg['seg_metric'] = sm
    _, savg = run(cfg)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    _, b_ex = run(base_config, exclude=('MU', 'SNDK'))
    _, n_ex = run(cfg, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(b_ex, n_ex)]
    al_lowo = sum(lifts_lowo)/len(lifts_lowo); w_lowo = sum(1 for l in lifts_lowo if l > 0)
    print(f'{f"seg_metric={sm}":<25}{avg:>+18.1f}p{wins:>7}/100{al_lowo:>+10.1f}p({w_lowo:>3}/100)')
    sys.stdout.flush()

# ────────────────────────────────────────────────────────────────────────
print('\n' + '=' * 80)
print('Phase B: rev_growth cutoff stress (V86e+ 환경)')
print('=' * 80)
print(f'{"rev_exit":<15}{"avg lift":>15}{"wins":>10}{"LOWO -MU-SNDK":>20}')
print('-' * 65)
for re in [0.10, 0.15, 0.20, 0.25, 0.30]:
    cfg = dict(v86e_config); cfg['rev_exit'] = re
    _, savg = run(cfg)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    _, n_ex = run(cfg, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(b_ex, n_ex)]
    al_lowo = sum(lifts_lowo)/len(lifts_lowo); w_lowo = sum(1 for l in lifts_lowo if l > 0)
    mk = ' ★' if re == 0.25 else '  '
    print(f'{mk}{re:<13.2f}{avg:>+13.1f}p{wins:>7}/100{al_lowo:>+10.1f}p({w_lowo:>3}/100)')
    sys.stdout.flush()

# ────────────────────────────────────────────────────────────────────────
print('\n' + '=' * 80)
print('Phase C: dd_30 메가 면제 (메가는 -25% drawdown 진입필터 무시)')
print('=' * 80)
print(f'{"variant":<30}{"avg lift":>15}{"wins":>10}{"LOWO -MU-SNDK":>20}')
print('-' * 80)

for exempt in [False, True]:
    for dd in [0.20, 0.25, 0.30]:
        cfg = dict(v86e_config); cfg['dd_mega_exempt'] = exempt; cfg['dd_thr'] = dd
        _, savg = run(cfg)
        lifts = [b - a for a, b in zip(base_savg, savg)]
        avg = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
        _, n_ex = run(cfg, exclude=('MU', 'SNDK'))
        lifts_lowo = [y - x for x, y in zip(b_ex, n_ex)]
        al_lowo = sum(lifts_lowo)/len(lifts_lowo); w_lowo = sum(1 for l in lifts_lowo if l > 0)
        ex_s = 'EXEMPT' if exempt else 'NONE'
        print(f'{f"dd={dd} mega_exempt={ex_s}":<30}{avg:>+13.1f}p{wins:>7}/100{al_lowo:>+10.1f}p({w_lowo:>3}/100)')
        sys.stdout.flush()

# ────────────────────────────────────────────────────────────────────────
print('\n' + '=' * 80)
print('Phase D: "v84 이후 흔들림" 진단 — 각 patch 단독 효과')
print('=' * 80)

# v84 자체 = baseline (메가 없음)
# v85 효과: 업종 차단 — 이건 BT simulator 안 함 (업종 데이터 미반영)
# v86 carryover (PEG<0.20 + min_seg<-2 only) vs V86e+ (PEG<0.22 + rev_growth exit)

phases = [
    ('v84 (baseline)', dict(mega_carry=False, peg_thr=0.22, rev_exit=0, seg_metric='min', dd_mega_exempt=False, dd_thr=0.25)),
    ('v86 (NTM≥60 AND PEG<0.20, rev_exit X)', dict(mega_carry=True, peg_thr=0.20, rev_exit=0, seg_metric='min', dd_mega_exempt=False, dd_thr=0.25)),
    ('V86e (PEG<0.20, rev_exit X)', dict(mega_carry=True, peg_thr=0.20, rev_exit=0, seg_metric='min', dd_mega_exempt=False, dd_thr=0.25)),
    ('V86e (PEG<0.20, rev_exit 0.25)', dict(mega_carry=True, peg_thr=0.20, rev_exit=0.25, seg_metric='min', dd_mega_exempt=False, dd_thr=0.25)),
    ('V86e+ (PEG<0.22, rev_exit 0.25)', dict(mega_carry=True, peg_thr=0.22, rev_exit=0.25, seg_metric='min', dd_mega_exempt=False, dd_thr=0.25)),
]
print(f'{"phase":<45}{"avg":>10}{"vs base":>12}{"wins":>10}{"LOWO":>12}')
print('-' * 90)
for name, cfg in phases:
    cums, savg = run(cfg)
    avg = sum(cums) / len(cums)
    lifts = [b - a for a, b in zip(base_savg, savg)]
    al = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
    _, n_ex = run(cfg, exclude=('MU', 'SNDK'))
    lifts_lowo = [y - x for x, y in zip(b_ex, n_ex)]
    al_lowo = sum(lifts_lowo)/len(lifts_lowo)
    print(f'{name:<45}{avg:>+9.1f}%{al:>+10.1f}p{wins:>7}/100{al_lowo:>+10.1f}p')
    sys.stdout.flush()

# ────────────────────────────────────────────────────────────────────────
print('\n' + '=' * 80)
print('종합 — 가장 robust한 conf 탐색')
print('=' * 80)
candidates = []
# eps_quality × rev_growth × dd_mega 결합 탐색
for sm in ['min', 'mean', 'weighted']:
    for re in [0.15, 0.25]:
        for exempt in [False, True]:
            cfg = dict(v86e_config); cfg['seg_metric'] = sm; cfg['rev_exit'] = re; cfg['dd_mega_exempt'] = exempt
            _, savg = run(cfg)
            lifts = [b - a for a, b in zip(base_savg, savg)]
            al = sum(lifts)/len(lifts); wins = sum(1 for l in lifts if l > 0)
            _, n_ex = run(cfg, exclude=('MU', 'SNDK'))
            lifts_lowo = [y - x for x, y in zip(b_ex, n_ex)]
            al_lowo = sum(lifts_lowo)/len(lifts_lowo); w_lowo = sum(1 for l in lifts_lowo if l > 0)
            candidates.append((sm, re, exempt, al, wins, al_lowo, w_lowo))

# best: lift 가장 큼 + LOWO ≥ 0 + wins ≥ 95
print(f'{"seg":<10}{"rev":<8}{"dd_exempt":<12}{"lift":>10}{"wins":>10}{"LOWO":>10}{"LOWO wins":>12}')
print('-' * 75)
candidates.sort(key=lambda x: -x[3])
for sm, re, exempt, al, wins, al_lowo, w_lowo in candidates:
    ex_s = 'YES' if exempt else 'NO'
    mark = ' ★' if al == candidates[0][3] else '  '
    print(f'{mark}{sm:<8}{re:<8.2f}{ex_s:<12}{al:>+9.1f}p{wins:>7}/100{al_lowo:>+9.1f}p{w_lowo:>9}/100')

print(f'\n총 소요 {time.time() - t0:.0f}초')
con.close()
