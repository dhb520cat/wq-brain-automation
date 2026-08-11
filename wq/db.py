"""Neon Postgres 状态存储。连接串从环境变量 DATABASE_URL 读取。"""

from __future__ import annotations

import os
from contextlib import contextmanager

import psycopg

DDL = """
CREATE TABLE IF NOT EXISTS alphas (
    alpha_id      text PRIMARY KEY,
    expression    text,
    region        text,
    delay         int,
    universe      text,
    category      text,          -- 数据集类别(pyramid 维度)
    dataset_id    text,
    sharpe        double precision,
    fitness       double precision,
    turnover      double precision,
    status        text,          -- UNSUBMITTED / ACTIVE / REJECTED ...
    date_created  timestamptz,
    payload       jsonb,
    synced_at     timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS submissions (
    id            bigserial PRIMARY KEY,
    alpha_id      text,
    check_result  jsonb,
    submitted     boolean,
    submitted_at  timestamptz DEFAULT now(),
    note          text
);

CREATE TABLE IF NOT EXISTS pyramid_progress (
    quarter       text,
    region        text,
    delay         int,
    category      text,
    alpha_count   int DEFAULT 0,
    completed     boolean GENERATED ALWAYS AS (alpha_count >= 3) STORED,
    updated_at    timestamptz DEFAULT now(),
    PRIMARY KEY (quarter, region, delay, category)
);

CREATE TABLE IF NOT EXISTS sim_results (
    id         bigserial PRIMARY KEY,
    batch      text,               -- 批次名(jsonl 文件名)
    alpha_id   text,
    expr       text,
    region     text,
    sharpe     double precision,
    fitness    double precision,
    turnover   double precision,
    margin     double precision,
    fails      jsonb,
    extra      jsonb,
    created_at timestamptz DEFAULT now(),
    UNIQUE (batch, expr)
);

CREATE TABLE IF NOT EXISTS runs (
    id         bigserial PRIMARY KEY,
    job        text,
    ok         boolean,
    detail     jsonb,
    started_at timestamptz,
    ended_at   timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS batch_queue (
    id         bigserial PRIMARY KEY,
    script     text NOT NULL,      -- jobs/ 下的脚本名(不带 .py)
    args       text DEFAULT '',    -- 空格分隔的参数
    field_pat  text,               -- 可选: 传给脚本的 FIELD_PAT
    priority   int DEFAULT 100,    -- 数字小的先跑
    status     text DEFAULT 'pending',   -- pending / running / done / failed
    started_at timestamptz,
    ended_at   timestamptz,
    created_at timestamptz DEFAULT now()
);
"""


@contextmanager
def conn():
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        yield c


def init_schema() -> None:
    with conn() as c:
        c.execute(DDL)


def log_run(job: str, ok: bool, detail: dict, started_at) -> None:
    with conn() as c:
        c.execute(
            "INSERT INTO runs (job, ok, detail, started_at) VALUES (%s,%s,%s,%s)",
            (job, ok, psycopg.types.json.Jsonb(detail), started_at),
        )
