"""队列消费者: 从 Neon batch_queue 取最高优先级 pending 任务执行一个。
云端 cron 每20分钟调用;本地也可手动跑。用户可随时 UPDATE 表替换 pending 任务。"""
import os, sys, subprocess, datetime
sys.path.insert(0, ".")
import psycopg

url = os.environ["DATABASE_URL"]
with psycopg.connect(url) as c:
    # 取任务(跳过 running 中的——并发保护:仅当无 running 时取)
    running = c.execute("SELECT count(*) FROM batch_queue WHERE status='running' AND started_at > now() - interval '2 hours'").fetchone()[0]
    if running:
        print(f"已有 {running} 个任务在跑,本轮跳过"); sys.exit(0)
    row = c.execute("""SELECT id, script, args, field_pat FROM batch_queue
                       WHERE status='pending' ORDER BY priority, id LIMIT 1""").fetchone()
    if not row:
        print("队列为空"); sys.exit(0)
    qid, script, args, fpat = row
    c.execute("UPDATE batch_queue SET status='running', started_at=now() WHERE id=%s", (qid,))
    c.commit()
print(f"执行 #{qid}: {script} {args}")
env = dict(os.environ)
if fpat: env["FIELD_PAT"] = fpat
ret = subprocess.run([sys.executable, "-u", f"jobs/{script}.py"] + args.split(), env=env).returncode
with psycopg.connect(url) as c:
    c.execute("UPDATE batch_queue SET status=%s, ended_at=now() WHERE id=%s",
              ("done" if ret == 0 else "failed", qid))
    c.commit()
print(f"#{qid} -> {'done' if ret==0 else 'failed'}")
sys.exit(0)
