
import os, re, hashlib, json, sqlite3, datetime as dt, time
import feedparser
from dateutil import parser as dp
from bs4 import BeautifulSoup
import polars as pl
from pathlib import Path
from feedgen.feed import FeedGenerator
import yaml

BASE = Path(__file__).parent
DATA = BASE / "data"; DATA.mkdir(exist_ok=True)
DB = DATA / "archive.sqlite3"
CFG = yaml.safe_load((BASE/"config.yml").read_text(encoding="utf-8"))

def init_db():
    con = sqlite3.connect(DB); cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS items(
        id TEXT PRIMARY KEY,
        source TEXT, title TEXT, url TEXT, published TEXT,
        summary TEXT, tags TEXT, raw TEXT, fetched_at TEXT
    )""")
    con.commit(); con.close()

def norm_date(s):
    # 失敗しても現在時刻に
    try:
        return dp.parse(s).astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
    except Exception:
        return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S%z")

def sha(s): return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def passes_filters(title, summary):
    text = f"{title} {summary}".lower()
    inc = [k.lower() for k in CFG.get("keywords",{}).get("include",[])]
    exc = [k.lower() for k in CFG.get("keywords",{}).get("exclude",[])]
    if inc and not any(k in text for k in inc): return False
    if any(k in text for k in exc): return False
    return True

def fetch_rss(name, url, tags):
    d = feedparser.parse(url)
    rows = []
    for e in d.entries:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        summary_html = e.get("summary") or e.get("description") or ""
        summary = BeautifulSoup(summary_html, "html.parser").get_text(" ", strip=True)
        if not title and not link: 
            continue
        if not passes_filters(title, summary): 
            continue
        uid = sha(link or title or (name+summary))
        pub = e.get("published") or e.get("updated") or ""
        rows.append(dict(
            id=uid,
            source=name,
            title=title or "(no title)",
            url=link,
            published=norm_date(pub),
            summary=summary[:2000],
            tags=",".join(tags or []),
            raw=json.dumps({k:str(v)[:10000] for k,v in e.items()}, ensure_ascii=False)
        ))
    return rows

def upsert(rows):
    con = sqlite3.connect(DB); cur = con.cursor()
    new=0
    for r in rows:
        try:
            cur.execute("INSERT OR IGNORE INTO items VALUES(?,?,?,?,?,?,?,?,datetime('now'))",
                (r["id"], r["source"], r["title"], r["url"], r["published"], r["summary"], r["tags"], r["raw"]))
            if cur.rowcount: new += 1
        except Exception as e:
            print("[WARN] upsert:", e)
    con.commit(); con.close(); 
    return new

def tidy_and_export():
    import json, csv
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT source, title, url, published, summary FROM items ORDER BY published DESC")
    rows = cur.fetchall()
    con.close()

    # 最新 1000 件だけを対象
    rows_recent = rows[:1000]

    # index.md（最新200件）
    lines = ["# AI / 生成AI クリッピング（最新200件）\n"]
    for source, title, url, published, summary in rows_recent[:200]:
        date = (published or "")[:16].replace("T"," ")
        lines.append(f"- **{date}** · **[{title}]({url})** — _{source}_\n  - { (summary or '')[:160] }")
    (BASE / "index.md").write_text("\n".join(lines), encoding="utf-8")

    # JSON / CSV スナップショット
    today = dt.datetime.now().strftime("%Y%m%d")
    dicts = [
        {"source": s, "title": t, "url": u, "published": p, "summary": (sn or "")}
        for (s, t, u, p, sn) in rows_recent
    ]
    (DATA / f"snapshot_{today}.json").write_text(
        json.dumps(dicts, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    with open(DATA / f"snapshot_{today}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source","title","url","published","summary"])
        w.writeheader()
        w.writerows(dicts)

    # RSS 出力（最新150件）
    fg = FeedGenerator()
    fg.title("AI/生成AI クリッピング（Free Stack）")
    fg.link(href="https://example", rel="alternate")
    fg.description("Multiple sources → daily curated feed")
    fg.language("ja")
    max_items = CFG["output"].get("rss_max_items", 150)
    for item in dicts[:max_items]:
        fe = fg.add_entry()
        fe.title(item["title"] or "(no title)")
        fe.link(href=item["url"] or "")
        fe.description(item["summary"] or "")
        try:
            fe.pubDate(dp.parse(item["published"]))
        except Exception:
            pass
    fg.rss_file(DATA / "feed.xml")


def main():
    init_db()
    total_new = 0

    # 通常 RSS
    for feed in CFG.get("feeds", []):
        name, url, tags = feed.get("name"), feed.get("url"), feed.get("tags",[])
        if not url: 
            continue
        print("[INFO] fetch", name, url)
        rows = fetch_rss(name, url, tags)
        total_new += upsert(rows)
        time.sleep(1)

    # 追加 RSS（Nitter / RSSHub など）
    for feed in CFG.get("extra_rss", []):
        name, url, tags = feed.get("name"), feed.get("url"), feed.get("tags",[])
        if not url: 
            continue
        print("[INFO] extra", name, url)
        rows = fetch_rss(name, url, tags)
        total_new += upsert(rows)
        time.sleep(1)

    tidy_and_export()
    print("new items:", total_new)

if __name__ == "__main__":
    main()
