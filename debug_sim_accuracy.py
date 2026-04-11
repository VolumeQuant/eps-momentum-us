"""B vs B2 차이 디버깅
DB part2_rank vs recompute(같은 conviction) 결과 비교
어디서 차이가 나는지 식별
"""
import sqlite3
import numpy as np
from collections import defaultdict

DB_PATH = 'eps_momentum_data.db'


def conv_base(adj_gap, rev_up, num_analysts, ntm_current, ntm_90d):
    if adj_gap is None:
        return None
    ratio = (rev_up / num_analysts) if num_analysts and num_analysts > 0 else 0
    eps_floor = 0
    if ntm_current is not None and ntm_90d and abs(ntm_90d) > 0.01:
        eps_floor = min(abs((ntm_current - ntm_90d) / ntm_90d), 1.0)
    conviction = max(ratio, eps_floor)
    return adj_gap * (1 + conviction)


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    dates = [r[0] for r in cursor.execute(
        'SELECT DISTINCT date FROM ntm_screening WHERE part2_rank IS NOT NULL ORDER BY date'
    ).fetchall()]

    print(f"Total dates: {len(dates)}")

    # 1) 각 날짜의 DB part2_rank Top 5 vs recompute Top 5 비교
    diff_count = 0
    full_matches = 0

    print(f"\n{'date':<12s} {'DB Top5':<35s} {'Recompute Top5':<35s} match")
    print('-' * 100)

    # composite_rank 계산
    for di, today in enumerate(dates):
        # DB Top 5 (part2_rank 1~5)
        rows = cursor.execute(
            'SELECT ticker, part2_rank FROM ntm_screening '
            'WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank LIMIT 5',
            (today,)
        ).fetchall()
        db_top5 = [r[0] for r in rows]

        # Recompute: 최근 3일 가져오기
        recent = []
        for j in range(di, max(di - 3, -1), -1):
            recent.insert(0, dates[j])
            if len(recent) >= 3:
                break

        weights = [0.2, 0.3, 0.5]
        if len(recent) == 2:
            weights = [0.4, 0.6]
        elif len(recent) == 1:
            weights = [1.0]

        # 각 날짜의 conv_gap 계산
        score_by_date = {}
        for d in recent:
            rs = cursor.execute(
                'SELECT ticker, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d '
                'FROM ntm_screening WHERE date=? AND composite_rank IS NOT NULL',
                (d,)
            ).fetchall()
            cgs = {}
            for r in rs:
                cg = conv_base(r[1], r[2], r[3], r[4], r[5])
                if cg is not None:
                    cgs[r[0]] = cg
            vals = list(cgs.values())
            if len(vals) >= 2:
                m = np.mean(vals)
                s = np.std(vals)
                if s > 0:
                    score_by_date[d] = {
                        tk: min(100.0, max(30.0, 65 + (-(v - m) / s) * 15))
                        for tk, v in cgs.items()
                    }
                else:
                    score_by_date[d] = {tk: 65 for tk in cgs}

        # 오늘의 eligible 종목
        today_eligible = list(score_by_date.get(today, {}).keys())

        def carry(tk, idx):
            for j in range(idx - 1, -1, -1):
                prev = score_by_date.get(recent[j], {}).get(tk)
                if prev is not None:
                    return prev
            return 30

        wgap = {}
        for tk in today_eligible:
            ws = 0
            for i, d in enumerate(recent):
                score = score_by_date.get(d, {}).get(tk)
                if score is None:
                    score = carry(tk, i)
                ws += score * weights[i]
            wgap[tk] = ws

        sorted_tks = sorted(today_eligible, key=lambda tk: -wgap.get(tk, -999))
        recompute_top5 = sorted_tks[:5]

        match = db_top5 == recompute_top5
        if not match:
            diff_count += 1
        else:
            full_matches += 1

        # 출력 (다른 날짜만 강조)
        marker = "OK" if match else "DIFF"
        if not match or di < 5 or di > len(dates) - 3:
            print(f"{today:<12s} {','.join(db_top5):<35s} {','.join(recompute_top5):<35s} {marker}")

    print(f"\n총 {len(dates)}일 중 {full_matches}일 동일, {diff_count}일 차이")

    # 2) 가장 다른 한 날짜 깊이 분석
    print("\n=== 차이 나는 날짜 상세 분석 ===")
    for today in dates[:5]:
        rows = cursor.execute(
            'SELECT ticker, part2_rank, adj_gap, rev_up30, num_analysts, ntm_current, ntm_90d '
            'FROM ntm_screening WHERE date=? AND part2_rank IS NOT NULL ORDER BY part2_rank LIMIT 10',
            (today,)
        ).fetchall()
        print(f"\n{today} - DB Top 10:")
        for r in rows:
            cg = conv_base(r[2], r[3], r[4], r[5], r[6])
            cg_str = f"{cg:+7.2f}" if cg is not None else "    nan"
            print(f"  rank {r[1]:2d}: {r[0]:6s} adj_gap={r[2]:+7.2f} conv_gap={cg_str} "
                  f"rev_up={r[3]} N={r[4]}")

    conn.close()


if __name__ == '__main__':
    main()
