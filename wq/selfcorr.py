"""本地 self-correlation 预筛。

把平台的 SELF_CORRELATION 检查搬到本地做:缓存账号全部 ACTIVE(OS) alpha 的日 PnL,
新 alpha 提交前先在本地算一遍相关性,超线的直接毙掉,不浪费平台 check 配额
(平台 corr check 约 10 次/小时,且顾问号 alpha 越多排队越久)。

相关性算法与平台口径对齐:
    1. 日收益 = pnl - pnl.ffill().shift(1)   —— 是差分不是 pct_change
       (PnL 是累计值且可能过零,pct_change 会炸成天文数字)
    2. 窗口 = 最新日期往前 4 年 (DateOffset(years=4))
    3. 收益里的 0 视为"当天没交易",replace(0, NaN) 后按 pairwise 丢弃
    4. Pearson corr,取最大值

用法:
    python -m wq.selfcorr <alpha_id> [region]        # 单个候选打分
    python -m wq.selfcorr --refresh                  # 只刷新缓存
    python -m wq.selfcorr --dry-run                  # 不联网,合成数据自检数据流

    from wq.selfcorr import SelfCorrCache, max_self_corr
    corr, partner = max_self_corr("aBcD123", region="USA")
"""

from __future__ import annotations

import argparse
import logging
import os
import pickle
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterable

import numpy as np
import pandas as pd

from wq.client import BrainClient

API = "https://api.worldquantbrain.com"
log = logging.getLogger("selfcorr")

# 项目根/data/pnl_cache.pkl(与 cwd 无关)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_PATH = os.path.join(ROOT, "data", "pnl_cache.pkl")

THREADS = 10          # PnL 拉取并发
YEARS = 4             # 相关性窗口(年)
MIN_PERIODS = 60      # 重叠交易日不足这么多天的对子不计,防小样本假高相关
CACHE_VERSION = 1

_login_lock = threading.Lock()   # 401 时只让一个线程去重登


# ---------- PnL 拉取 ----------

def _records_to_series(payload: dict[str, Any], alpha_id: str) -> pd.Series | None:
    """recordsets/pnl 响应 -> 以日期为索引的累计 PnL Series。"""
    recs = payload.get("records") or []
    if not recs:
        return None
    props = [p.get("name") for p in ((payload.get("schema") or {}).get("properties") or [])]
    if len(props) != len(recs[0]):
        props = [f"c{i}" for i in range(len(recs[0]))]   # schema 缺失时兜底
    df = pd.DataFrame(recs, columns=props)
    date_col = next((c for c in props if "date" in str(c).lower()), props[0])
    val_col = next((c for c in props if str(c).lower() == "pnl"), props[-1])
    s = pd.Series(
        pd.to_numeric(df[val_col], errors="coerce").values,
        index=pd.to_datetime(df[date_col], errors="coerce"),
        name=alpha_id, dtype="float64",
    )
    s = s[s.index.notna()].sort_index()
    s = s[~s.index.duplicated(keep="last")].dropna()
    return s if len(s) else None


def fetch_pnl(cl: BrainClient, alpha_id: str, tries: int = 8) -> pd.Series | None:
    """拉单个 alpha 的日 PnL。处理 Retry-After(记录集异步生成)/429/掉登录。"""
    url = f"{API}/alphas/{alpha_id}/recordsets/pnl"
    for att in range(tries):
        try:
            r = cl.s.get(url, timeout=60)
        except Exception as ex:
            log.debug("pnl %s 网络重试: %s", alpha_id, str(ex)[:80])
            time.sleep(10 + att * 5)
            continue
        retry = float(r.headers.get("Retry-After", 0) or 0)
        if retry or r.status_code == 429:
            time.sleep(max(retry, 5 + att * 5))
            continue
        if r.status_code in (401, 403):
            with _login_lock:
                try:
                    cl.login()
                except Exception as ex:
                    log.warning("重登失败: %s", str(ex)[:120])
            time.sleep(2)
            continue
        if r.status_code != 200:
            log.warning("pnl %s HTTP %s %s", alpha_id, r.status_code, r.text[:120])
            return None
        try:
            return _records_to_series(r.json(), alpha_id)
        except Exception as ex:
            log.warning("pnl %s 解析失败: %s", alpha_id, str(ex)[:120])
            return None
    log.warning("pnl %s 重试耗尽", alpha_id)
    return None


# ---------- 缓存 ----------

class SelfCorrCache:
    """账号 ACTIVE(OS) alpha 的日 PnL 本地缓存,增量更新。

    结构: {"version": int, "pnl": {alpha_id: Series}, "meta": {alpha_id: {...}}, "updated_at": float}
    path=None 表示纯内存(给 --dry-run / 单测用)。
    """

    def __init__(self, path: str | None = CACHE_PATH, client: BrainClient | None = None):
        self.path = path
        self._cl = client
        self.pnl: dict[str, pd.Series] = {}
        self.meta: dict[str, dict[str, Any]] = {}
        self.updated_at: float = 0.0
        self.load()

    # -- 客户端惰性登录,dry-run 下永不触发 --
    @property
    def client(self) -> BrainClient:
        if self._cl is None:
            self._cl = BrainClient()
            self._cl.login()
        return self._cl

    def __len__(self) -> int:
        return len(self.pnl)

    def load(self) -> None:
        if not self.path or not os.path.exists(self.path):
            return
        try:
            with open(self.path, "rb") as f:
                d = pickle.load(f)
        except Exception as ex:
            log.warning("缓存读取失败(当空缓存处理): %s", str(ex)[:120])
            return
        if d.get("version") != CACHE_VERSION:
            log.warning("缓存版本 %s != %s,弃用重建", d.get("version"), CACHE_VERSION)
            return
        self.pnl = d.get("pnl") or {}
        self.meta = d.get("meta") or {}
        self.updated_at = d.get("updated_at") or 0.0
        log.info("缓存载入 %d 个 alpha (更新于 %s)", len(self.pnl),
                 time.strftime("%Y-%m-%d %H:%M", time.localtime(self.updated_at)) if self.updated_at else "?")

    def save(self) -> None:
        if not self.path:
            return
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.tmp"
        with open(tmp, "wb") as f:
            pickle.dump({"version": CACHE_VERSION, "pnl": self.pnl,
                         "meta": self.meta, "updated_at": time.time()},
                        f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, self.path)   # 原子替换,防写一半损坏
        log.info("缓存已存 %s (%d 个 alpha)", self.path, len(self.pnl))

    # ---------- 库存 ----------

    def list_active(self) -> list[dict[str, Any]]:
        """分页拉 /users/self/alphas?status=ACTIVE(即已提交在跑的 OS alpha)。"""
        out: list[dict[str, Any]] = []
        offset, limit = 0, 100
        while True:
            j = self.client.my_alphas(status="ACTIVE", limit=limit, offset=offset)
            res = j.get("results") or []
            out += res
            offset += limit
            if not res or offset >= (j.get("count") or 0):
                break
        log.info("账号 ACTIVE alpha: %d 个", len(out))
        return out

    @staticmethod
    def _meta_of(a: dict[str, Any]) -> dict[str, Any]:
        st = a.get("settings") or {}
        return {"region": st.get("region"), "delay": st.get("delay"),
                "universe": st.get("universe"), "neut": st.get("neutralization"),
                "date_created": a.get("dateCreated"),
                "sharpe": (a.get("is") or {}).get("sharpe")}

    def refresh(self, force: bool = False, prune: bool = True,
                threads: int = THREADS) -> dict[str, int]:
        """增量更新:只拉缓存里没有的 alpha id。force=True 则全量重拉。"""
        alphas = self.list_active()
        active_ids = [a["id"] for a in alphas if a.get("id")]
        for a in alphas:
            self.meta[a["id"]] = self._meta_of(a)

        if prune:
            stale = set(self.pnl) - set(active_ids)
            for aid in stale:
                self.pnl.pop(aid, None)
                self.meta.pop(aid, None)
            if stale:
                log.info("剔除已不在 ACTIVE 的 %d 个 alpha", len(stale))
        else:
            stale = set()

        todo = active_ids if force else [i for i in active_ids if i not in self.pnl]
        log.info("PnL 待拉取 %d / %d (已缓存 %d)", len(todo), len(active_ids), len(self.pnl))
        stats = {"total": len(active_ids), "fetched": 0, "failed": 0,
                 "cached": len(self.pnl), "pruned": len(stale)}
        if not todo:
            self.save()
            return stats

        cl = self.client
        lock = threading.Lock()

        def work(aid: str) -> None:
            s = fetch_pnl(cl, aid)
            with lock:
                if s is None or s.empty:
                    stats["failed"] += 1
                    log.warning("PnL 为空: %s", aid)
                    return
                self.pnl[aid] = s
                stats["fetched"] += 1
                if stats["fetched"] % 20 == 0:
                    log.info("... 已拉 %d/%d", stats["fetched"], len(todo))

        with ThreadPoolExecutor(max_workers=threads) as ex:
            list(ex.map(work, todo))

        self.save()
        log.info("刷新完成: %s", stats)
        return stats

    # ---------- 取用 ----------

    def frame(self, region: str | None = None,
              exclude: Iterable[str] = ()) -> pd.DataFrame:
        """拼成宽表: index=日期, columns=alpha_id, 值=累计 PnL。"""
        ex = set(exclude)
        cols = [aid for aid, s in self.pnl.items()
                if aid not in ex and s is not None and len(s)
                and (region is None or (self.meta.get(aid) or {}).get("region") == region)]
        if not cols:
            return pd.DataFrame()
        return pd.concat([self.pnl[c] for c in cols], axis=1, join="outer").sort_index()

    def get_pnl(self, alpha_id: str, use_cache: bool = True) -> pd.Series | None:
        """取候选 alpha 的 PnL(缓存优先,没有就联网拉,不写回缓存)。"""
        if use_cache and alpha_id in self.pnl:
            return self.pnl[alpha_id]
        return fetch_pnl(self.client, alpha_id)


# ---------- 相关性核心 ----------

def daily_returns(pnl: pd.DataFrame | pd.Series, years: int = YEARS) -> pd.DataFrame | pd.Series:
    """累计 PnL -> 日收益,裁到最近 years 年,0 值置 NaN。

    注意三个必须点(错一个结果就跟平台对不上):
      - 差分而非 pct_change
      - shift 前先 ffill(补上停牌/缺失日,避免把缺口算成一天的收益)
      - 收益 0 = 当天没有交易 -> NaN,不参与 corr
    """
    ret = pnl - pnl.ffill().shift(1)
    if len(ret.index):
        start = ret.index.max() - pd.DateOffset(years=years)
        ret = ret[ret.index > start]
    return ret.replace(0, np.nan)


def corr_against_pool(cand_pnl: pd.Series, pool: pd.DataFrame,
                      years: int = YEARS,
                      min_periods: int = MIN_PERIODS) -> pd.Series:
    """候选 vs 池内每个 alpha 的相关性,降序返回(index=alpha_id)。"""
    if cand_pnl is None or cand_pnl.empty or pool is None or pool.empty:
        return pd.Series(dtype="float64")
    tag = "__candidate__"
    wide = pool.join(cand_pnl.rename(tag), how="outer").sort_index()
    ret = daily_returns(wide, years)
    cand = ret.pop(tag)
    if ret.empty or cand.notna().sum() < min_periods:
        return pd.Series(dtype="float64")
    # pairwise 重叠交易日,不足 min_periods 的对子直接丢(3 个点也能算出 0.99)
    overlap = ret.notna().mul(cand.notna(), axis=0).sum()
    corr = ret.corrwith(cand)
    corr = corr[(overlap >= min_periods) & corr.notna()]
    return corr.sort_values(ascending=False)


def max_self_corr(candidate_alpha_id: str, region: str | None = None,
                  cache: SelfCorrCache | None = None,
                  client: BrainClient | None = None,
                  years: int = YEARS, min_periods: int = MIN_PERIODS,
                  ) -> tuple[float, str | None]:
    """候选 alpha 与账号 OS 池的最大自相关。

    返回 (max_corr, worst_partner_id);池为空或数据不足时返回 (nan, None)。
    region 传了就只跟同区域的比(平台 self-corr 也是同区域口径)。
    """
    cache = cache if cache is not None else SelfCorrCache(client=client)
    cand = cache.get_pnl(candidate_alpha_id)
    if cand is None or cand.empty:
        log.warning("候选 %s 拿不到 PnL", candidate_alpha_id)
        return float("nan"), None
    pool = cache.frame(region=region, exclude=[candidate_alpha_id])
    if pool.empty:
        log.warning("池为空(region=%s),先跑 refresh", region)
        return float("nan"), None
    corr = corr_against_pool(cand, pool, years=years, min_periods=min_periods)
    if corr.empty:
        return float("nan"), None
    return float(corr.iloc[0]), str(corr.index[0])


def self_corr_top(candidate_alpha_id: str, region: str | None = None,
                  top: int = 5, cache: SelfCorrCache | None = None,
                  client: BrainClient | None = None,
                  years: int = YEARS, min_periods: int = MIN_PERIODS) -> pd.Series:
    """同上,但返回 top N 相关伙伴(便于排查是哪一族因子撞了)。"""
    cache = cache if cache is not None else SelfCorrCache(client=client)
    cand = cache.get_pnl(candidate_alpha_id)
    if cand is None or cand.empty:
        return pd.Series(dtype="float64")
    pool = cache.frame(region=region, exclude=[candidate_alpha_id])
    return corr_against_pool(cand, pool, years=years, min_periods=min_periods).head(top)


# ---------- dry-run: 不联网,合成数据跑通整条数据流 ----------

def _demo_cache(n: int = 30, days: int = 1400, seed: int = 7) -> tuple[SelfCorrCache, str]:
    """造一个内存缓存: n 个随机游走 alpha + 1 个与候选强相关的"孪生" alpha。"""
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=days)
    cache = SelfCorrCache(path=None, client=None)

    cand_ret = rng.normal(0, 1.0, days)
    cand_id = "CANDIDATE"
    cache.pnl[cand_id] = pd.Series(cand_ret.cumsum(), index=idx, name=cand_id)
    cache.meta[cand_id] = {"region": "USA"}

    # 孪生: 90% 同源 -> 相关性应该 ~0.9
    twin = cand_ret * 0.9 + rng.normal(0, 1.0, days) * 0.1
    cache.pnl["TWIN"] = pd.Series(twin.cumsum(), index=idx, name="TWIN")
    cache.meta["TWIN"] = {"region": "USA"}

    for i in range(n):
        aid = f"RAND{i:03d}"
        r = rng.normal(0, 1.0, days)
        # 一半 alpha 制造缺失段+零值段,检验 ffill/replace(0,nan) 分支
        if i % 2 == 0:
            r[: days // 3] = 0.0
        s = pd.Series(r.cumsum(), index=idx, name=aid)
        if i % 5 == 0:
            s = s.drop(s.index[100:130])          # 缺失日
        cache.pnl[aid] = s
        cache.meta[aid] = {"region": "USA" if i % 3 else "EUR"}
    return cache, cand_id


def _dry_run() -> int:
    print("=== dry-run: 合成数据,不走网络 ===")
    cache, cand_id = _demo_cache()
    print(f"合成池: {len(cache)} 个 alpha, 候选={cand_id}")

    pool = cache.frame(exclude=[cand_id])
    print(f"宽表: {pool.shape[0]} 天 × {pool.shape[1]} 列, "
          f"{pool.index.min().date()} ~ {pool.index.max().date()}")

    ret = daily_returns(pool)
    cut = pool.index.max() - pd.DateOffset(years=YEARS)
    print(f"日收益: {ret.shape[0]} 天(窗口 > {cut.date()}), NaN 占比 {ret.isna().mean().mean():.1%}")
    assert ret.index.min() > cut, "窗口裁剪没生效"
    assert (ret.fillna(1) != 0).all().all(), "0 值没有被置 NaN"

    corr_all = corr_against_pool(cache.pnl[cand_id], pool)
    print(f"全区域: 有效对子 {len(corr_all)} 个")
    for aid, v in corr_all.head(3).items():
        print(f"    {aid:<10} {v:+.4f}")

    mx, who = max_self_corr(cand_id, cache=cache)
    print(f"max_self_corr(全区域) = {mx:.4f}  伙伴={who}")
    assert who == "TWIN" and mx > 0.8, "孪生 alpha 没被识别出来"

    mx_eur, who_eur = max_self_corr(cand_id, region="EUR", cache=cache)
    print(f"max_self_corr(region=EUR) = {mx_eur:.4f}  伙伴={who_eur}  (region 过滤生效)")
    assert who_eur != "TWIN", "region 过滤没生效"

    empty, none_id = max_self_corr(cand_id, region="不存在的区域", cache=cache)
    print(f"空池: corr={empty} 伙伴={none_id}")
    assert none_id is None and np.isnan(empty)

    # 缓存读写往返
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "pnl_cache.pkl")
        cache.path = p
        cache.save()
        again = SelfCorrCache(path=p)
        assert len(again) == len(cache), "缓存往返数量对不上"
        pd.testing.assert_series_equal(again.pnl["TWIN"], cache.pnl["TWIN"])
        print(f"缓存往返 OK: {len(again)} 个 alpha, {os.path.getsize(p) / 1024:.0f} KB")

    # 记录集解析
    payload = {"schema": {"properties": [{"name": "date"}, {"name": "pnl"}]},
               "records": [["2024-01-02", 0.0], ["2024-01-03", 12.5], ["2024-01-03", 13.0]]}
    s = _records_to_series(payload, "X1")
    assert len(s) == 2 and s.iloc[-1] == 13.0, "记录集解析/去重有问题"
    assert _records_to_series({"records": []}, "X2") is None
    print("记录集解析 OK(重复日期取最后一条,空记录返回 None)")

    print("=== dry-run 全部通过 ===")
    return 0


# ---------- CLI ----------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m wq.selfcorr",
        description="本地 self-correlation 预筛(不消耗平台 check 配额)")
    ap.add_argument("alpha_id", nargs="?", help="候选 alpha id")
    ap.add_argument("region", nargs="?", default=None,
                    help="只跟该区域的 OS alpha 比(USA/EUR/ASI/CHN/...)")
    ap.add_argument("--refresh", action="store_true", help="先增量刷新 PnL 缓存")
    ap.add_argument("--force", action="store_true", help="配合 --refresh 全量重拉")
    ap.add_argument("--no-prune", action="store_true", help="刷新时保留已退役 alpha")
    ap.add_argument("--dry-run", action="store_true", help="不联网,用合成数据自检数据流")
    ap.add_argument("--top", type=int, default=5, help="展示 top N 相关伙伴(默认 5)")
    ap.add_argument("--threshold", type=float, default=0.7, help="拦截线(默认 0.7)")
    ap.add_argument("--years", type=int, default=YEARS, help="相关窗口年数(默认 4)")
    ap.add_argument("--min-periods", type=int, default=MIN_PERIODS,
                    help="重叠交易日下限(默认 60)")
    ap.add_argument("--cache", default=CACHE_PATH, help=f"缓存路径(默认 {CACHE_PATH})")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")

    if a.dry_run:
        return _dry_run()

    if not a.alpha_id and not a.refresh:
        ap.error("要么给 alpha_id,要么用 --refresh / --dry-run")

    cache = SelfCorrCache(path=a.cache)
    if a.refresh:
        cache.refresh(force=a.force, prune=not a.no_prune)
    if not a.alpha_id:
        return 0
    if len(cache) == 0:
        print("缓存为空,自动刷新一次...")
        cache.refresh()

    corr = self_corr_top(a.alpha_id, region=a.region, top=a.top, cache=cache,
                         years=a.years, min_periods=a.min_periods)
    scope = a.region or "全区域"
    print(f"\n候选 {a.alpha_id}  vs  OS 池({scope}, {cache.frame(region=a.region).shape[1]} 个 alpha)")
    if corr.empty:
        print("  无可比对象(池为空 / 重叠交易日不足 / PnL 拉取失败)")
        return 2
    mx, who = float(corr.iloc[0]), str(corr.index[0])
    print(f"  max self-corr = {mx:.4f}   最相关伙伴 = {who}")
    print(f"  判定: {'❌ 超线,别交' if mx >= a.threshold else '✅ 低于阈值,可交'} (阈值 {a.threshold})")
    print(f"  top{len(corr)}:")
    for aid, v in corr.items():
        m = cache.meta.get(aid) or {}
        print(f"    {aid:<12} {v:+.4f}  {m.get('region') or '?':<4} "
              f"d{m.get('delay')} {m.get('universe') or ''} sh={m.get('sharpe')}")
    return 1 if mx >= a.threshold else 0


if __name__ == "__main__":
    sys.exit(main())
