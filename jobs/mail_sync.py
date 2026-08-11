"""163 IMAP 拉取 WQ 相关邮件全文,存 JSONL。

用法: MAIL_USER=your-account@163.com MAIL_PASS=<授权码> python jobs/mail_sync.py [输出目录]
163 要求登录后发 ID 命令,否则 SELECT 报 Unsafe Login。
"""

from __future__ import annotations

import email
import email.policy
import imaplib
import json
import os
import re
import sys

WQ_PAT = re.compile(r"worldquant|wqbrain|brain", re.I)
BATCH = 200


def decode(h) -> str:
    if h is None:
        return ""
    out = []
    for part, enc in email.header.decode_header(str(h)):
        if isinstance(part, bytes):
            out.append(part.decode(enc or "utf-8", "replace"))
        else:
            out.append(part)
    return "".join(out)


def body_text(msg) -> str:
    """优先 text/plain,退回 html 去标签。"""
    def _get(m):
        try:
            return m.get_content()
        except Exception:
            p = m.get_payload(decode=True)
            return p.decode(m.get_content_charset() or "utf-8", "replace") if p else ""
    plain = html = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain" and not plain:
                plain = _get(part)
            elif ct == "text/html" and not html:
                html = _get(part)
    else:
        if msg.get_content_type() == "text/html":
            html = _get(msg)
        else:
            plain = _get(msg)
    if plain.strip():
        return plain
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"[ \t]+", " ", text)


def main() -> int:
    outdir = sys.argv[1] if len(sys.argv) > 1 else "data"
    os.makedirs(outdir, exist_ok=True)
    out_path = os.path.join(outdir, "wq_mail.jsonl")
    seen = set()
    if os.path.exists(out_path):
        with open(out_path) as f:
            for line in f:
                try:
                    seen.add(json.loads(line)["uid"])
                except Exception:
                    pass

    M = imaplib.IMAP4_SSL("imap.163.com", 993)
    M.login(os.environ["MAIL_USER"], os.environ["MAIL_PASS"])
    M.xatom('ID', '("name" "wqsync" "version" "1.0" "vendor" "claude")')
    M.select("INBOX", readonly=True)

    typ, dat = M.uid("search", None, "ALL")
    uids = [u.decode() for u in dat[0].split()]
    print(f"INBOX 共 {len(uids)} 封, 已同步 {len(seen)}")

    todo = [u for u in uids if u not in seen]
    todo.reverse()  # 最新优先
    n_wq = 0
    with open(out_path, "a", encoding="utf-8") as fout:
        for i in range(0, len(todo), BATCH):
            chunk = todo[i:i + BATCH]
            typ, dat = M.uid("fetch", ",".join(chunk),
                             "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)])")
            # dat: [(b'x (UID y ...', b'headers'), b')', ...]
            headers = {}
            cur_uid = None
            for item in dat:
                if isinstance(item, tuple):
                    m = re.search(rb"UID (\d+)", item[0])
                    if m:
                        cur_uid = m.group(1).decode()
                        headers[cur_uid] = item[1].decode("utf-8", "replace")
            wq_uids = []
            for uid, hdr in headers.items():
                msg = email.message_from_string(hdr)
                frm = decode(msg.get("From"))
                subj = decode(msg.get("Subject"))
                if WQ_PAT.search(frm) or WQ_PAT.search(subj):
                    wq_uids.append(uid)
            # 拉全文
            for uid in wq_uids:
                typ, fdat = M.uid("fetch", uid, "(BODY.PEEK[])")
                raw = next((it[1] for it in fdat if isinstance(it, tuple)), None)
                if not raw:
                    continue
                msg = email.message_from_bytes(raw, policy=email.policy.default)
                rec = {
                    "uid": uid,
                    "from": decode(msg.get("From")),
                    "subject": decode(msg.get("Subject")),
                    "date": str(msg.get("Date")),
                    "body": body_text(msg)[:20000],
                }
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n_wq += 1
            print(f"进度 {min(i+BATCH,len(todo))}/{len(todo)}, 本批 WQ {len(wq_uids)}, 累计 {n_wq}", flush=True)
    M.logout()
    print(f"完成: 新增 {n_wq} 封 -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
