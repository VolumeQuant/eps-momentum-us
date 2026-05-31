"""ETF Pulse Health Check — cron 실패/데이터 이상 자동 감지

매일 cron 마지막에 실행. 문제 발견 시 텔레그램/이메일 alert.

체크 항목:
1. DB 최근 갱신 (오늘 데이터 들어왔나)
2. ETF 수 (예상 vs 실제)
3. 거래량 spike 비정상 (모두 0 or 모두 큰 값 등)
4. holdings 누락 (이전 대비 큰 변동)
5. 컨텐츠 생성 (오늘자 파일 존재)
"""
import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = Path(__file__).parent / 'etf_pulse.db'
CONTENT_DIR = Path(__file__).parent / 'content'


class HealthIssue:
    def __init__(self, severity, msg):
        self.severity = severity  # 'critical', 'warning', 'info'
        self.msg = msg
    def __repr__(self):
        emoji = {'critical': '🚨', 'warning': '⚠️', 'info': 'ℹ️'}.get(self.severity, '')
        return f'{emoji} [{self.severity.upper()}] {self.msg}'


def check_all():
    issues = []

    # 1. DB 존재
    if not DB_PATH.exists():
        issues.append(HealthIssue('critical', 'DB 파일 없음 — daily_fetch 실패'))
        return issues

    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()

    # 2. 최근 데이터
    latest = cur.execute('SELECT MAX(date) FROM etf_daily').fetchone()[0]
    if not latest:
        issues.append(HealthIssue('critical', 'etf_daily 비어있음'))
    else:
        # 최신 데이터가 3일 이상 오래됨 = 문제
        try:
            latest_dt = datetime.strptime(latest, '%Y-%m-%d')
            days_old = (datetime.now() - latest_dt).days
            if days_old > 4:  # 주말 고려해도 4일 이상은 이상
                issues.append(HealthIssue('critical',
                    f'최신 데이터 {days_old}일 오래됨 ({latest}) — cron 실패 가능성'))
            elif days_old > 2:
                issues.append(HealthIssue('warning', f'최신 데이터 {days_old}일 전 ({latest})'))
        except: pass

    # 3. ETF 수
    n_tickers = cur.execute('SELECT COUNT(DISTINCT ticker) FROM etf_daily WHERE date=?',
                            (latest,)).fetchone()[0] if latest else 0
    if n_tickers < 150:
        issues.append(HealthIssue('critical', f'{latest} ETF 수 {n_tickers} (예상 200+)'))
    elif n_tickers < 200:
        issues.append(HealthIssue('warning', f'{latest} ETF 수 {n_tickers} (보통 220+)'))

    # 4. 거래량 spike 정상 분포
    if latest:
        avg_spike = cur.execute('SELECT AVG(volume_spike) FROM etf_daily WHERE date=? AND volume_spike > 0',
                                (latest,)).fetchone()[0]
        if avg_spike is None or avg_spike == 0:
            issues.append(HealthIssue('warning', 'volume_spike 모두 0 — 계산 오류 가능성'))
        elif avg_spike > 10:
            issues.append(HealthIssue('warning', f'volume_spike 평균 {avg_spike:.2f}x 비정상적 높음'))

    # 5. holdings 데이터
    if latest:
        n_holdings = cur.execute('SELECT COUNT(DISTINCT etf_ticker) FROM etf_holdings_daily WHERE date=?',
                                 (latest,)).fetchone()[0]
        if n_holdings < 100:
            issues.append(HealthIssue('warning', f'holdings 확보 {n_holdings} ETF (예상 150+)'))

    # 6. 오늘자 콘텐츠 파일
    if latest and CONTENT_DIR.exists():
        kr_file = CONTENT_DIR / f'pulse_{latest}.md'
        if not kr_file.exists():
            issues.append(HealthIssue('warning', f'오늘 콘텐츠 파일 없음: {kr_file.name}'))

    # 7. 가격 데이터 0 (이상)
    if latest:
        n_zero = cur.execute('SELECT COUNT(*) FROM etf_daily WHERE date=? AND (price IS NULL OR price = 0)',
                             (latest,)).fetchone()[0]
        if n_zero > 5:
            issues.append(HealthIssue('warning', f'{latest} 가격 0/null ETF {n_zero}개'))

    # 8. 정상 신호
    if not issues:
        issues.append(HealthIssue('info', f'모든 항목 정상 ({latest}, {n_tickers} ETF)'))

    conn.close()
    return issues


def gen_health_report(issues):
    """건강 검사 리포트"""
    lines = [f'# 🏥 ETF Pulse Health Check — {datetime.now().strftime("%Y-%m-%d %H:%M")}', '']
    critical = [i for i in issues if i.severity == 'critical']
    warning = [i for i in issues if i.severity == 'warning']
    info = [i for i in issues if i.severity == 'info']

    if critical:
        lines.append('## 🚨 Critical')
        for i in critical:
            lines.append(f'- {i.msg}')
        lines.append('')
    if warning:
        lines.append('## ⚠️ Warning')
        for i in warning:
            lines.append(f'- {i.msg}')
        lines.append('')
    if info:
        lines.append('## ℹ️ Info')
        for i in info:
            lines.append(f'- {i.msg}')
        lines.append('')

    lines.append('---')
    lines.append(f'_Total: {len(critical)} critical / {len(warning)} warning / {len(info)} info_')
    return '\n'.join(lines), bool(critical)


if __name__ == '__main__':
    issues = check_all()
    report, has_critical = gen_health_report(issues)
    print(report)

    # 결과 저장
    out = CONTENT_DIR / 'health_check.md'
    CONTENT_DIR.mkdir(exist_ok=True)
    out.write_text(report, encoding='utf-8')

    sys.exit(1 if has_critical else 0)
