# wq-automation

WorldQuant BRAIN 自动化的**工程骨架**：批量模拟引擎、库存同步、队列调度。
不含任何因子模板、表达式或策略逻辑 —— 那部分请自己实现。

## 架构

- **GitHub Actions**(cron + dispatch) 执行任务
- **Postgres**(Neon 等) 存状态
- **BRAIN API** 操作平台

Secrets: `DATABASE_URL` / `WQ_EMAIL` / `WQ_PASSWORD`

## 模块

| 模块 | 说明 |
|---|---|
| `wq/client.py` | BRAIN API 客户端，Basic auth → session cookie，401 自动重登 |
| `wq/http.py` | 统一请求层：超时 / 指数退避 / 断路器 / 限流刹车 |
| `wq/msim.py` | multi-simulation 引擎，10 表达式/请求 × 10 并发槽；断点续跑；哨兵模式零成本语法校验 |
| `wq/selfcorr.py` | 本地 self-correlation 预筛，省平台 check 配额 |
| `wq/db.py` | Postgres schema 与连接管理 |

## 任务

| job | 说明 |
|---|---|
| `healthcheck` | 验证 DB 连通 + 建表 + WQ 登录 |
| `inventory` | 同步 alpha 库存(ACTIVE + UNSUBMITTED)到 DB |
| `report` | 生成日报 |
| `msim_runner` | 从 `tasks.jsonl` 跑批模拟 |
| `queue_worker` | 从 `batch_queue` 表消费一个待跑任务（云端 cron 每 20 分钟触发） |
| `persist_results` | 把 jsonl 结果落库到 `sim_results` |
| `mail_sync` | IMAP 拉取 WQ 相关邮件存 JSONL（可选） |

## 用法

```bash
pip install -r requirements.txt
export DATABASE_URL=... WQ_EMAIL=... WQ_PASSWORD=...
python jobs/healthcheck.py          # 建表 + 连通性自检

# 自己生成 tasks.jsonl，每行 {"expr": "...", "settings": {...}}
python jobs/msim_runner.py tasks.jsonl my_batch
```

队列模式：往 `batch_queue` 插一行即可，cron 会自动消费。

```sql
INSERT INTO batch_queue (script, args, priority) VALUES ('msim_runner', 'tasks.jsonl my_batch', 10);
```

## 注意

- ⚠️ **`automation.yml` 的 `report` 任务会把日报 commit 进仓库并发到 issue**。在公开仓库里给它配了
  `DATABASE_URL` 就等于把自己的 alpha 库存、Sharpe 分布、提交进度全部公开。要在公开 fork 上跑，
  先删掉那个 `Commit report & update issue` 步骤，或者只在私有仓库启用 `report`。
- msim 引擎的轮询节流（progress ≤ 0.35 睡 60s，否则 5s）是为了尊重平台限流，别改小。
- `concurrency.group` 限制同时只跑一个模拟批，同样是为了不打爆平台并发配额。

## License

MIT
