"""把 data/*.jsonl 模拟结果统一入 Neon sim_results 表(本地/云端通用)。"""

from __future__ import annotations

import glob
import json
import os
import sys

sys.path.insert(0, ".")
import psycopg

from wq import db


def main() -> int:
    db.init_schema()
    n = 0
    with db.conn() as c:
        for f in glob.glob("data/*.jsonl"):
            batch = os.path.basename(f).replace(".jsonl", "")
            if batch.startswith(("wq_mail", "mail_insights")):
                continue
            for line in open(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if "expr" not in r:
                    continue
                c.execute(
                    """INSERT INTO sim_results (batch, alpha_id, expr, region,
                         sharpe, fitness, turnover, margin, fails, extra)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (batch, expr) DO UPDATE SET
                         alpha_id=EXCLUDED.alpha_id, sharpe=EXCLUDED.sharpe,
                         fitness=EXCLUDED.fitness, turnover=EXCLUDED.turnover,
                         margin=EXCLUDED.margin, fails=EXCLUDED.fails""",
                    (batch, r.get("alpha_id"), r["expr"], r.get("region"),
                     r.get("sharpe"), r.get("fitness"), r.get("turnover"),
                     r.get("margin"), psycopg.types.json.Jsonb(r.get("fails") or []),
                     psycopg.types.json.Jsonb({k: v for k, v in r.items()
                                               if k not in ("alpha_id", "expr", "region",
                                                            "sharpe", "fitness", "turnover",
                                                            "margin", "fails")})))
                n += 1
        c.commit()
    print(f"入库 {n} 条")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
