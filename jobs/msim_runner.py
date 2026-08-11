"""msim 通用跑批入口。

用法(任务文件): python jobs/msim_runner.py tasks.jsonl out_name
  tasks.jsonl 每行 {"expr": "...", "settings": {...}}

表达式怎么生成由你自己决定 —— 本仓库只提供执行引擎(wq/msim.py),
不含任何模板库或因子构造逻辑。写一个生成 tasks.jsonl 的脚本即可接入。
"""

from __future__ import annotations

import json
import logging
import sys

sys.path.insert(0, ".")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from wq.msim import run_msim


def from_file(path: str, out_name: str):
    items = []
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        items.append((r["expr"], r["settings"]))
    run_msim(items, f"data/{out_name}.jsonl")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit("用法: python jobs/msim_runner.py <tasks.jsonl> <out_name>")
    from_file(sys.argv[1], sys.argv[2])
