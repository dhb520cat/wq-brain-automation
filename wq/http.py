"""统一请求层: 全局超时 / Retry-After 优先 / 401 重登 / 429 退避 / 断路器 / 限流刹车。

手册 3.3.1 的四层结构:
    第 1 层  响应头优先    —— Retry-After / x-ratelimit-remaining
    第 2 层  错误码分派    —— 401 重登(不计次) / 429 退避 / 其他 4xx 不重试 / 5xx 重试
    第 3 层  指数退避+抖动 —— 5s ×2 封顶 120s, ±30% jitter
    第 4 层  断路器        —— 连续 5 次 429 → 全局暂停 60s → 重新认证

⚠️ 设计约束(不破坏调用方的轮询语义):
    只在"需要退避"时(429 / 5xx / 网络异常)才自己 sleep 重发;
    2xx 响应即使带 Retry-After 也**原样返回** —— 那是"任务未完成,请稍后再查"的业务
    信号,由调用方的轮询循环自行决定睡多久(见 msim._poll_multi 的进度节流)。

用法:
    from wq import http
    r = http.request(session, "GET", url, on_401=cl.relogin, timeout=60)
"""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any, Callable

import requests

log = logging.getLogger("wq.http")

TIMEOUT = 30            # 全局默认超时(秒)
MAX_ATTEMPTS = 6
BACKOFF_BASE = 5.0      # 指数退避基数
BACKOFF_MAX = 120.0     # 退避封顶
JITTER = 0.3            # ±30% 抖动,避免雷鸣群体效应
BREAKER_429 = 5         # 连续 5 次 429 触发断路
BREAKER_PAUSE = 60.0    # 断路期间全局零请求
RL_THRESHOLD = 1000     # x-ratelimit-remaining 刹车线(给手动回测留 1000 次)
RL_SLEEP = 5.0          # 触线后每请求降速
RL_WARN_GAP = 60.0      # 告警日志最小间隔,防刷屏

# 日额度触线标志: 调用方可在投放新任务前 if http.daily_limit_reached.is_set(): return
daily_limit_reached = threading.Event()

_lock = threading.Lock()
_streak_429 = 0
_pause_until = 0.0
_last_rl_warn = 0.0


# ---------- 工具 ----------

def retry_after(resp: requests.Response) -> float:
    """读 Retry-After 头,无/非法则 0。"""
    try:
        return float(resp.headers.get("Retry-After", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def _backoff(att: int) -> float:
    d = min(BACKOFF_MAX, BACKOFF_BASE * (2 ** att))
    return d * random.uniform(1 - JITTER, 1 + JITTER)


# ---------- 断路器 ----------

def _wait_breaker() -> None:
    while True:
        with _lock:
            left = _pause_until - time.time()
        if left <= 0:
            return
        time.sleep(min(left, 5.0))


def _hit_429() -> bool:
    """记一次 429,返回是否触发断路。"""
    global _streak_429, _pause_until
    with _lock:
        _streak_429 += 1
        if _streak_429 < BREAKER_429:
            return False
        _streak_429 = 0
        _pause_until = max(_pause_until, time.time() + BREAKER_PAUSE)
    return True


def _reset_429() -> None:
    global _streak_429
    if _streak_429:
        with _lock:
            _streak_429 = 0


# ---------- 限流刹车 ----------

def brake(resp: requests.Response) -> None:
    """x-ratelimit 刹车。

    只认小写 x- 前缀的 Simulator 日额度池(手册 5.1.3);相关性池用的是
    RateLimit-Remaining(每分钟 60),不能套用 1000 这条阈值。
    """
    global _last_rl_warn
    raw = resp.headers.get("x-ratelimit-remaining")
    if raw is None:
        return
    try:
        remaining = int(float(raw))
    except (TypeError, ValueError):
        return
    if remaining > RL_THRESHOLD:
        return
    daily_limit_reached.set()
    now = time.time()
    if now - _last_rl_warn > RL_WARN_GAP:
        _last_rl_warn = now
        log.warning("日额度告警: x-ratelimit-remaining=%d (<=%d), 每请求降速 %.0fs, reset=%s",
                    remaining, RL_THRESHOLD, RL_SLEEP,
                    resp.headers.get("x-ratelimit-reset"))
    time.sleep(RL_SLEEP)


# ---------- 主入口 ----------

def request(session: requests.Session, method: str, url: str, *,
            on_401: Callable[[], Any] | None = None,
            max_attempts: int = MAX_ATTEMPTS, **kw: Any) -> requests.Response:
    """发一次请求(含重试),返回 Response;重试用尽仍是网络异常才抛。"""
    kw.setdefault("timeout", TIMEOUT)
    method = method.upper()
    att = 0
    relogged = False
    last_exc: Exception | None = None

    while att < max_attempts:
        _wait_breaker()
        try:
            r = session.request(method, url, **kw)
        except requests.RequestException as ex:
            last_exc = ex
            att += 1
            log.warning("%s %s 网络异常(%d/%d): %s", method, url[:80], att,
                        max_attempts, str(ex)[:120])
            if att >= max_attempts:
                break
            time.sleep(_backoff(att - 1))
            continue

        brake(r)

        if r.status_code == 429:
            tripped = _hit_429()
            ra = retry_after(r)
            delay = ra if ra > 0 else _backoff(att)
            att += 1
            if att >= max_attempts:
                log.warning("429 重试用尽: %s %s", method, url[:80])
                return r
            if tripped:
                log.warning("断路器触发: 连续 %d 次 429, 全局暂停 %.0fs 并重新认证",
                            BREAKER_429, BREAKER_PAUSE)
            time.sleep(delay)
            if tripped and on_401 is not None:
                try:
                    on_401()          # 恢复动作: 换新 token
                except Exception as ex:
                    log.warning("断路后重登失败: %s", str(ex)[:120])
            continue

        _reset_429()

        if r.status_code == 401 and on_401 is not None and not relogged:
            relogged = True           # 重登只做一次,且不计入重试次数
            try:
                on_401()
            except Exception as ex:
                log.warning("401 重登失败: %s", str(ex)[:120])
                return r
            continue

        if r.status_code >= 500:
            att += 1
            if att >= max_attempts:
                return r
            ra = retry_after(r)
            log.warning("%s %s -> %d, 退避重试", method, url[:80], r.status_code)
            time.sleep(ra if ra > 0 else _backoff(att - 1))
            continue

        return r                      # 2xx/3xx 与除 401/429 外的 4xx: 直接返回不重试

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{method} {url} 重试用尽")
