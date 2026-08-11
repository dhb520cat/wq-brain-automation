"""链路验证: Neon 连通 + 建表 + WQ 登录。首个 Actions 运行用。"""

from __future__ import annotations

import datetime as dt
import logging
import sys

sys.path.insert(0, ".")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("healthcheck")


def main() -> int:
    started = dt.datetime.now(dt.timezone.utc)
    detail: dict = {}

    # 1) Neon
    from wq import db
    db.init_schema()
    with db.conn() as c:
        row = c.execute("SELECT version()").fetchone()
    detail["postgres"] = row[0][:60]
    log.info("Neon OK: %s", detail["postgres"])

    # 2) WQ 登录
    from wq.client import BrainClient
    cl = BrainClient()
    try:
        cl.login()
        detail["wq_login"] = "ok"
        ops = cl.operators()
        detail["operators_visible"] = len(ops)
        log.info("WQ OK: %d 个算子可用", len(ops))
        ok = True
    except Exception as e:  # 登录失败不阻塞 DB 部分的验证
        detail["wq_login"] = str(e)[:300]
        log.error("WQ 登录失败: %s", e)
        ok = False

    db.log_run("healthcheck", ok, detail, started)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
