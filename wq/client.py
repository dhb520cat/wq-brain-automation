"""WorldQuant BRAIN API 客户端。

认证: Basic auth -> session cookie。凭据从环境变量 WQ_EMAIL / WQ_PASSWORD 读取。
API 根: https://api.worldquantbrain.com
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

import requests

from wq import http

API = "https://api.worldquantbrain.com"
log = logging.getLogger(__name__)

RELOGIN_GAP = 30.0        # 多线程下合并重登请求的最小间隔(秒)


class BrainClient:
    def __init__(self, email: str | None = None, password: str | None = None):
        self.email = email or os.environ["WQ_EMAIL"]
        self.password = password or os.environ["WQ_PASSWORD"]
        self.s = requests.Session()
        # 本机代理(Clash)不稳定且 WQ API 可直连;禁用环境代理变量
        self.s.trust_env = False
        self._auth_lock = threading.Lock()
        self._last_login = 0.0

    # ---------- 统一请求层 ----------

    def _req(self, method: str, url: str, **kw: Any) -> requests.Response:
        """所有业务请求都走这里: 超时/退避/断路/限流刹车/401 自动重登。"""
        return http.request(self.s, method, url, on_401=self.relogin, **kw)

    # ---------- 认证 ----------

    def login(self) -> None:
        # 认证请求本身不带 on_401,避免递归
        r = http.request(self.s, "POST", f"{API}/authentication",
                         auth=(self.email, self.password))
        if r.status_code == 201:
            self._last_login = time.time()
            log.info("登录成功")
            return
        # 部分账号需要 persona 二次验证,直接报错交给人工处理
        raise RuntimeError(f"登录失败 HTTP {r.status_code}: {r.text[:300]}")

    def relogin(self) -> None:
        """401/断路恢复回调: 并发下只让一个线程真正重登,其余直接复用。"""
        with self._auth_lock:
            if time.time() - self._last_login < RELOGIN_GAP:
                return
            log.warning("会话失效 -> 重新登录")
            self.login()

    # ---------- 元数据 ----------

    def operators(self) -> list[dict[str, Any]]:
        r = self._req("GET", f"{API}/operators")
        r.raise_for_status()
        return r.json()

    def datasets(self, region: str, delay: int, universe: str,
                 instrument_type: str = "EQUITY") -> list[dict[str, Any]]:
        out, offset = [], 0
        while True:
            r = self._req("GET", f"{API}/data-sets", params={
                "instrumentType": instrument_type, "region": region,
                "delay": delay, "universe": universe,
                "limit": 50, "offset": offset,
            })
            r.raise_for_status()
            j = r.json()
            out += j["results"]
            offset += 50
            if offset >= j["count"]:
                return out

    # ---------- Alpha 库存 ----------

    def my_alphas(self, status: str = "UNSUBMITTED", limit: int = 100,
                  offset: int = 0, **filters: Any) -> dict[str, Any]:
        params = {"limit": limit, "offset": offset, "status": status,
                  "order": "-dateCreated", **filters}
        r = self._req("GET", f"{API}/users/self/alphas", params=params)
        r.raise_for_status()
        return r.json()

    def alpha(self, alpha_id: str) -> dict[str, Any]:
        r = self._req("GET", f"{API}/alphas/{alpha_id}")
        r.raise_for_status()
        return r.json()

    # ---------- 模拟 ----------

    def simulate(self, expression: str, settings: dict[str, Any],
                 poll_secs: float = 5.0, timeout: float = 900.0) -> dict[str, Any]:
        """跑一次模拟,阻塞直到完成,返回 alpha 详情。"""
        payload = {"type": "REGULAR", "settings": settings, "regular": expression}
        r = self._req("POST", f"{API}/simulations", json=payload)
        if r.status_code != 201:
            raise RuntimeError(f"simulate 提交失败 {r.status_code}: {r.text[:300]}")
        progress_url = r.headers["Location"]
        t0 = time.time()
        while True:
            pr = self._req("GET", progress_url)
            retry = http.retry_after(pr)
            if retry:
                if time.time() - t0 > timeout:
                    raise TimeoutError(f"模拟超时: {progress_url}")
                # 轮询节流: progress<=0.35 段耗时最长,睡 60s;之后 5s
                try:
                    prog = float((pr.json() or {}).get("progress") or 0)
                except Exception:
                    prog = 0.0
                time.sleep(max(retry, 60.0 if prog <= 0.35 else poll_secs))
                continue
            j = pr.json()
            if j.get("status") == "ERROR":
                raise RuntimeError(f"模拟失败: {j.get('message')}")
            return self.alpha(j["alpha"])

    # ---------- 提交 ----------

    def check_submission(self, alpha_id: str) -> dict[str, Any]:
        r = self._req("GET", f"{API}/alphas/{alpha_id}/check")
        r.raise_for_status()
        return r.json()

    def submit(self, alpha_id: str, poll_secs: float = 5.0,
               timeout: float = 600.0) -> bool:
        r = self._req("POST", f"{API}/alphas/{alpha_id}/submit")
        if r.status_code not in (200, 201):
            log.warning("submit %s -> %s %s", alpha_id, r.status_code, r.text[:200])
            return False
        t0 = time.time()
        while True:
            pr = self._req("GET", f"{API}/alphas/{alpha_id}/submit")
            retry = http.retry_after(pr)
            if retry:
                if time.time() - t0 > timeout:
                    raise TimeoutError(f"submit 轮询超时: {alpha_id}")
                time.sleep(max(retry, poll_secs))
                continue
            return pr.status_code == 200

    # ---------- Genius ----------

    def genius_status(self) -> dict[str, Any]:
        r = self._req("GET", f"{API}/users/self/activities/genius")
        if r.status_code != 200:
            return {}
        return r.json()
