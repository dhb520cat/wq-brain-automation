"""P0 盘点: 拉取库存 alpha 写入 Neon。

API 限制: offset 上限约 1000、count 显示上限 10000。
用 dateCreated 游标分片绕开: 每片内 offset<=900,片尾时间戳作为下一片上界。
"""

from __future__ import annotations

import datetime as dt
import logging
import sys

sys.path.insert(0, ".")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("inventory")

import psycopg

from wq import db
from wq.client import BrainClient

PAGE = 100
OFFSET_CAP = 900  # 单片内最大 offset,低于 API 限制


def upsert_alpha(c, a: dict) -> None:
    s = a.get("settings", {})
    is_ = a.get("is", {}) or {}
    c.execute(
        """
        INSERT INTO alphas (alpha_id, expression, region, delay, universe,
                            sharpe, fitness, turnover, status, date_created, payload)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (alpha_id) DO UPDATE SET
            sharpe = EXCLUDED.sharpe, fitness = EXCLUDED.fitness,
            turnover = EXCLUDED.turnover, status = EXCLUDED.status,
            payload = EXCLUDED.payload, synced_at = now()
        """,
        (
            a["id"], (a.get("regular") or {}).get("code"),
            s.get("region"), s.get("delay"), s.get("universe"),
            is_.get("sharpe"), is_.get("fitness"), is_.get("turnover"),
            a.get("status"), a.get("dateCreated"),
            psycopg.types.json.Jsonb(a),
        ),
    )


def sync_status(c, cl: BrainClient, status: str) -> int:
    total, cutoff = 0, None
    while True:
        filters = {"dateCreated<": cutoff} if cutoff else {}
        offset, last_date, page_count = 0, None, None
        while offset <= OFFSET_CAP:
            j = cl.my_alphas(status=status, limit=PAGE, offset=offset, **filters)
            results = j.get("results", [])
            page_count = j.get("count", 0)
            if not results:
                return total
            for a in results:
                upsert_alpha(c, a)
            total += len(results)
            last_date = results[-1].get("dateCreated")
            offset += PAGE
            if total % 1000 == 0:
                log.info("%s: 已同步 %d (片内 count=%s, cutoff=%s)",
                         status, total, page_count, cutoff)
            if offset >= page_count:
                c.commit()
                return total
        c.commit()
        if last_date is None or last_date == cutoff:
            log.warning("%s: 游标未推进,终止 (cutoff=%s)", status, cutoff)
            return total
        cutoff = last_date


def main() -> int:
    started = dt.datetime.now(dt.timezone.utc)
    db.init_schema()
    cl = BrainClient()
    cl.login()

    detail = {}
    with db.conn() as c:
        for status in ("ACTIVE", "UNSUBMITTED"):
            n = sync_status(c, cl, status)
            detail[status] = n
            log.info("%s 完成: %d 条", status, n)

    db.log_run("inventory", True, detail, started)
    log.info("全部完成: %s", detail)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
