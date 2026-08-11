"""multi-simulation 引擎: 10 表达式/请求 × 10 并发槽 = 100 并行。

用法:
    from wq.msim import run_msim
    run_msim(items, out_path)                  # items: list[(expr, settings)]
    run_msim(items, out_path, sentinel=True)   # 零成本语法校验(不消耗回测次数)

断点续跑:
    指纹 = sha256(json.dumps(settings, sort_keys=True) + 去空白表达式)。
    结果行(含失败行)都写完整 settings,续跑时按指纹比对跳过。
    ⚠️ 失败行(post failed / poll timeout)同样算"已完成",要重跑请先删掉对应行。
    旧版结果行没有 settings 字段,按 (expr, region, universe, neut, decay) 兼容判重。

轮询节流(手册 3.7): progress <= 0.35 睡 60s,否则 5s —— 单批 GET 从 ~93 次降到个位数。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from wq import http
from wq.client import BrainClient

log = logging.getLogger("msim")
API = "https://api.worldquantbrain.com"

POOL = 10                     # 表达式/请求
SLOTS = 10                    # 并发 multi-sim 槽位
SENTINEL_EXPR = "abc123xyz("  # 非法表达式: 逼整批 ERROR,借此零成本校验语法
POLL_SLOW = 60.0              # progress <= 0.35 的轮询间隔
POLL_FAST = 5.0               # progress > 0.35 的轮询间隔
POLL_MAX_ERR = 5              # 轮询连续出错上限,超过判定该组失败
PROGRESS_TH = 0.35


# ---------- 指纹与判重 ----------

def _norm(expr: str | None) -> str:
    return re.sub(r"\s+", "", expr or "")


def _fp(expr: str, settings: dict) -> str:
    """去重指纹: sha256(规范化 settings JSON + 去空白表达式)。"""
    raw = json.dumps(settings or {}, sort_keys=True, ensure_ascii=False) + _norm(expr)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _legacy_key(expr, region, universe, neut, decay) -> str:
    """旧结果行(无 settings)的兼容键,避免历史产出被重跑白烧额度。"""
    return f"{_norm(expr)}|{region}|{universe}|{neut}|{decay}"


# ---------- 请求 ----------

def _req(cl: BrainClient, method: str, url: str, **kw):
    """统一请求层封装,异常收敛成 None。"""
    try:
        return http.request(cl.s, method, url, on_401=cl.relogin, **kw)
    except Exception as ex:
        log.warning("%s %s 失败: %s", method, url[-70:], str(ex)[:100])
        return None


def _post_group(cl: BrainClient, group: list[tuple[str, dict]]) -> str | None:
    payload = [{"type": "REGULAR", "settings": st, "regular": e} for e, st in group]
    for _ in range(3):
        r = _req(cl, "POST", f"{API}/simulations", json=payload, timeout=60)
        if r is None:
            time.sleep(15); continue
        if r.status_code == 201:
            return r.headers.get("Location")
        log.warning("POST %s: %s", r.status_code, r.text[:150])
        if r.status_code == 403:
            try:
                cl.login()
            except Exception as ex:
                log.warning("重登失败: %s", str(ex)[:100])
                return None
            continue
        return None
    return None


def _progress(resp) -> tuple[float, dict]:
    try:
        j = resp.json() or {}
    except Exception:
        return 0.0, {}
    try:
        return float(j.get("progress") or 0), j
    except (TypeError, ValueError):
        return 0.0, j


def _poll_multi(cl: BrainClient, loc: str, timeout: float = 3600) -> list[str] | None:
    """轮询 multi 进度。

    返回 children 列表;**超时/连续出错返回 None**(与"跑完但没有 children"区分开,
    否则超时批次既不落行也进不了 done 集,会被无限重跑)。
    """
    t0 = time.time()
    errs = 0
    while time.time() - t0 < timeout:
        r = _req(cl, "GET", loc, timeout=60)
        if r is None or r.status_code >= 400:
            errs += 1
            if r is not None:
                log.warning("poll %s: %s", r.status_code, r.text[:120])
            if errs >= POLL_MAX_ERR:      # 别为一个坏 Location 死轮一小时
                return None
            time.sleep(15)
            continue
        errs = 0
        retry = http.retry_after(r)
        prog, j = _progress(r)
        if retry:
            # progress 只用来决定睡多久;是否结束看有没有 Retry-After(手册 3.7.2)
            time.sleep(max(retry, POLL_SLOW if prog <= PROGRESS_TH else POLL_FAST))
            continue
        return j.get("children") or []
    return None


def _child_alpha(cl: BrainClient, child: str) -> dict | None:
    for _ in range(3):
        r = _req(cl, "GET", f"{API}/simulations/{child}", timeout=60)
        if r is None or r.status_code != 200:
            time.sleep(10); continue
        try:
            j = r.json()
        except Exception:
            time.sleep(10); continue
        aid = j.get("alpha")
        if not aid:
            # 语法错误 / 被取消的子模拟: status=ERROR 带 message, 正确项则 CANCELED
            msg = j.get("message")
            return {"expr": j.get("regular") or "", "status": j.get("status"),
                    "error": str(msg)[:200] if msg else None}
        ar = _req(cl, "GET", f"{API}/alphas/{aid}", timeout=60)
        if ar is None or ar.status_code != 200:
            time.sleep(10); continue
        a = ar.json()
        is_ = a.get("is") or {}
        st = a.get("settings") or {}
        return {"alpha_id": aid, "expr": (a.get("regular") or {}).get("code"),
                "region": st.get("region"), "universe": st.get("universe"),
                "neut": st.get("neutralization"), "decay": st.get("decay"),
                "sharpe": is_.get("sharpe"), "fitness": is_.get("fitness"),
                "turnover": is_.get("turnover"), "margin": is_.get("margin"),
                "fails": [c["name"] for c in (is_.get("checks") or [])
                          if c.get("result") == "FAIL"]}
    return None


def _failrec(expr: str, settings: dict, err: str, sentinel: bool) -> dict:
    """失败行也带完整 settings,才能进 done 集参与续跑比对。"""
    if sentinel:
        return {"expr": expr, "error": err, "mode": "sentinel"}
    return {"expr": expr, "settings": settings, "error": err}


def _load_done(out_path: str) -> tuple[set, set]:
    done, legacy = set(), set()
    if not os.path.exists(out_path):
        return done, legacy
    with open(out_path, encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            e = r.get("expr")
            if not e or r.get("mode") == "sentinel":
                continue          # 哨兵行没真回测,不算完成
            if isinstance(r.get("settings"), dict):
                done.add(_fp(e, r["settings"]))
            else:
                legacy.add(_legacy_key(e, r.get("region"), r.get("universe"),
                                       r.get("neut"), r.get("decay")))
    return done, legacy


def run_msim(items: list[tuple[str, dict]], out_path: str,
             sentinel: bool = False) -> dict:
    """items -> 分组打包并发跑完,追加写 out_path。返回统计。

    sentinel=True: 每组附加一个非法表达式逼整批 ERROR,只做语法校验,
    不消耗回测次数;错误项 status=ERROR 带 message,其余 CANCELED。
    """
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    if sentinel:
        todo = list(items)        # 哨兵模式不判重(不产生真结果)
    else:
        done, legacy = _load_done(out_path)
        todo = [(e, s) for e, s in items
                if _fp(e, s) not in done
                and _legacy_key(e, s.get("region"), s.get("universe"),
                                s.get("neutralization"), s.get("decay")) not in legacy]
    pool_sz = POOL - 1 if sentinel else POOL   # 哨兵占一个位,每批最多 9 条
    log.info("msim%s: 总 %d, 待跑 %d (池=%d×%d)", " [哨兵校验]" if sentinel else "",
             len(items), len(todo), pool_sz, SLOTS)
    if not todo:
        return {"total": 0, "ok": 0}

    cl = BrainClient(); cl.login()
    groups = [todo[i:i + pool_sz] for i in range(0, len(todo), pool_sz)]
    lock = threading.Lock()
    stats = {"total": len(todo), "ok": 0, "hit": 0, "err": 0, "fail": 0}
    fout = open(out_path, "a", encoding="utf-8")

    def write(rec: dict) -> None:
        with lock:
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fout.flush()

    def bail(group, why):
        log.warning("组失败(%s): %d 条", why, len(group))
        for e, s in group:
            write(_failrec(e, s, why, sentinel))
        with lock:
            stats["fail"] += len(group)

    def work(group):
        if http.daily_limit_reached.is_set():
            log.warning("日额度刹车已触发,跳过 %d 条(留待下次续跑)", len(group))
            return
        posted = (group + [(SENTINEL_EXPR, group[0][1])]) if sentinel else group
        loc = _post_group(cl, posted)
        if not loc:
            bail(group, "post failed")
            return
        children = _poll_multi(cl, loc)
        if children is None:
            bail(group, "poll timeout")     # 超时也要落行,否则永远进不了 done 集
            return
        if not children:
            log.warning("无 children: %s", loc)
            return
        smap = {_norm(e): s for e, s in group}
        for child in children:
            rec = _child_alpha(cl, child)
            if not rec:
                continue
            key = _norm(rec.get("expr"))
            if sentinel:
                if key == _norm(SENTINEL_EXPR):
                    continue                # 哨兵自身不落行
                rec["mode"] = "sentinel"
            else:
                st = smap.get(key)
                if st is not None:          # 写回提交时的 settings 供指纹比对
                    rec["settings"] = st
            write(rec)
            with lock:
                if rec.get("error"):
                    stats["err"] += 1
                    log.info("%s %s -> %s", "✗" if sentinel else "..",
                             (rec.get("expr") or "")[:55], (rec.get("error") or "")[:80])
                elif rec.get("sharpe") is not None:
                    stats["ok"] += 1
                    good = ((rec["sharpe"] or 0) >= 1.58
                            and (rec["fitness"] or 0) >= 1 and not rec["fails"])
                    if good:
                        stats["hit"] += 1
                    log.info("%s sh=%.2f fit=%s to=%.3f %s",
                             "🎯" if good else "..", rec["sharpe"] or 0, rec["fitness"],
                             rec["turnover"] or 0, (rec.get("expr") or "")[:55])

    try:
        with ThreadPoolExecutor(max_workers=SLOTS) as pool:
            futs = [pool.submit(work, g) for g in groups]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as ex:
                    log.warning("组异常: %s", str(ex)[:150])
    finally:
        fout.close()
    log.info("msim 完成: %s", stats)
    return stats
