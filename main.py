
import os, re, hashlib, json, sqlite3, datetime as dt, time
import feedparser
import requests
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

def passes_local_filters(title, summary, feed_cfg):
    """サイト個別の include/exclude を優先。無ければ全体のkeywordsを使う。"""
    text = f"{title} {summary}".lower()
    inc = [k.lower() for k in (feed_cfg.get("include") or CFG.get("keywords",{}).get("include",[]))]
    exc = [k.lower() for k in (feed_cfg.get("exclude") or CFG.get("keywords",{}).get("exclude",[]))]
    if inc and not any(k in text for k in inc): 
        return False
    if any(k in text for k in exc): 
        return False
    return True

def fetch_site_list(feed_cfg):
    """ニュース一覧ページを1枚だけ取得して、aタグの見出しをキーワードで拾う軽量クロール"""
    url = feed_cfg["url"]
    name = feed_cfg["name"]
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent":"ai-curator/1.0"})
        r.raise_for_status()
    except Exception as e:
        print(f"[WARN] fetch_site_list fail {name}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    rows = []
    # 一覧ページの a タグを片っ端から見る（過剰取得しないよう top 200 件に制限）
    for a in soup.find_all("a")[:200]:
        t = (a.get_text(" ", strip=True) or "").strip()
        href = a.get("href") or ""
        if not t or not href:
            continue
        # 絶対URL化（相対パス対応）
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            from urllib.parse import urljoin
            href = urljoin(url, href)

        # タイトルに対してキーワードフィルタ
        if not passes_local_filters(t, "", feed_cfg):
            continue

        rows.append(dict(
            id=sha(href or t),
            source=name,
            title=t,
            url=href,
            published=dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S%z"),
            summary="",
            tags="company,site",
            raw=""
        ))
        # 取りすぎ防止（多くても50本）
        if len(rows) >= 50:
            break
    return rows


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
    extra = CFG.get("extra_rss") or []
    for feed in extra:
        name, url, tags = feed.get("name"), feed.get("url"), feed.get("tags",[])
        if not url: 
            continue
        print("[INFO] extra", name, url)
        rows = fetch_rss(name, url, tags)
        total_new += upsert(rows)
        time.sleep(1)

        # 会社サイトの一覧ページからキーワード拾い上げ
    for s in CFG.get("site_feeds", []) or []:
        print("[INFO] site", s.get("name"), s.get("url"))
        rows = fetch_site_list(s)
        total_new += upsert(rows)

    tidy_and_export()
    print("new items:", total_new)

if __name__ == "__main__":
    main()
