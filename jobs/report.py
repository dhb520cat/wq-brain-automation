"""每日进度报告: 汇总 Neon 数据 + Genius 目标进度,输出 markdown 到 stdout。

Actions 会把它写入 reports/ 目录并提交,同时更新置顶 issue。
"""

from __future__ import annotations

import datetime as dt
import sys

sys.path.insert(0, ".")

from wq import db

TARGET = {"signals": 20, "pyramids": 10, "performance": 0.5}  # Expert 门槛
QUARTER_END = dt.date(2026, 9, 30)


def q(c, sql, args=()):
    return c.execute(sql, args).fetchall()


def main() -> int:
    today = dt.date.today()
    days_left = (QUARTER_END - today).days
    out = [f"# WQ 自动化日报 {today}", ""]
    out.append(f"距 Q3 结算还有 **{days_left} 天**;Expert 需: "
               f"{TARGET['signals']} signals + {TARGET['pyramids']} pyramids "
               f"+ performance ≥{TARGET['performance']}")
    out.append("")

    with db.conn() as c:
        # 库存概况
        rows = q(c, """
            SELECT status, count(*), avg(sharpe), count(*) FILTER (WHERE sharpe >= 1.25)
            FROM alphas GROUP BY status ORDER BY status""")
        out.append("## 库存")
        out.append("")
        out.append("| status | 条数 | 平均 Sharpe | Sharpe≥1.25 |")
        out.append("|---|---|---|---|")
        for r in rows:
            avg = f"{r[2]:.2f}" if r[2] is not None else "-"
            out.append(f"| {r[0]} | {r[1]} | {avg} | {r[3]} |")
        out.append("")

        # 高潜力候选分布(可过门槛线的)
        rows = q(c, """
            SELECT region, delay, count(*)
            FROM alphas
            WHERE status='UNSUBMITTED' AND sharpe>=1.25 AND turnover BETWEEN 0.01 AND 0.7
            GROUP BY region, delay ORDER BY count(*) DESC LIMIT 12""")
        if rows:
            out.append("## 达标候选分布 (Sharpe≥1.25, turnover 1%-70%)")
            out.append("")
            out.append("| region | delay | 候选数 |")
            out.append("|---|---|---|")
            for r in rows:
                out.append(f"| {r[0]} | {r[1]} | {r[2]} |")
            out.append("")

        # 本季提交进度
        rows = q(c, """
            SELECT count(*) FILTER (WHERE submitted), count(*)
            FROM submissions WHERE submitted_at >= '2026-07-01'""")
        sub_ok, sub_all = rows[0] if rows else (0, 0)
        out.append("## 升级进度")
        out.append("")
        out.append(f"- Signals(本季已提交): **{sub_ok}** / {TARGET['signals']}")
        rows = q(c, """
            SELECT count(*) FROM pyramid_progress
            WHERE quarter='2026-Q3' AND completed""")
        pyr = rows[0][0] if rows else 0
        out.append(f"- Pyramids completed: **{pyr}** / {TARGET['pyramids']}")
        out.append(f"- 提交尝试总数: {sub_all}")
        out.append("")

        # 最近任务运行
        rows = q(c, """
            SELECT job, ok, ended_at FROM runs ORDER BY ended_at DESC LIMIT 8""")
        out.append("## 最近任务")
        out.append("")
        for r in rows:
            flag = "✅" if r[1] else "❌"
            out.append(f"- {flag} {r[0]} @ {r[2]:%m-%d %H:%M}")

    print("\n".join(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
