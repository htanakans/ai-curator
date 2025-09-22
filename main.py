import os, re, hashlib, json, sqlite3, datetime as dt, time
import feedparser
import requests
import urllib.parse as up  # ← モジュールとして up を使う（up.urlparse / up.urljoin）
from dateutil import parser as dp
from bs4 import BeautifulSoup
import polars as pl  # 使わないなら消してOK
from pathlib import Path
from feedgen.feed import FeedGenerator
import yaml
import unicodedata


BASE = Path(__file__).parent
DATA = BASE / "data"; DATA.mkdir(exist_ok=True)
DB = DATA / "archive.sqlite3"
CFG = yaml.safe_load((BASE/"config.yml").read_text(encoding="utf-8"))
# --- リンク生存チェック（404/リダイレクト等を軽量判定） ---
def is_alive(url: str, timeout=7) -> bool:
    if not url or not url.startswith(("http://", "https://")):
        return False
    headers = {"User-Agent": "ai-curator/1.0 (+github actions)"}
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        if r.status_code < 400:
            return True
        # HEAD拒否サイト向けフォールバック
        r = requests.get(url, allow_redirects=True, timeout=timeout, headers=headers, stream=True)
        return r.status_code < 400
    except Exception:
        return False


# ---- URL正規化・フィルタ・HTMLエスケープ・記事判定 ----
def normalize_url(href: str) -> str:
    try:
        u = up.urlparse(href)
        path = (u.path or "/").rstrip("/") or "/"
        # クエリ・フラグメントは除去（?utm=... 等を落として重複抑制）
        return up.urlunparse((u.scheme, u.netloc, path, "", "", ""))
    except Exception:
        return href or ""

def is_blocked(url: str, title: str) -> bool:
    """採用/IR/ポリシー/サイトマップ/検索/問い合わせ等のノイズ除外 + 短文見出し除外"""
    t = (title or "").lower()
    u = (url or "").lower()

    defaults_words = [
        "採用","求人","リクルート","募集","インターン","説明会",
        "ir","決算","株主","投資家","disclosure",
        "ポリシー","プライバシー","個人情報","利用規約","約款","terms","policy","privacy",
        "サイトマップ","sitemap","サイト マップ",
        "検索","search",
        "お問い合わせ","問い合わせ","contact","inquiry","faq","guideline","ガイドライン","ヘルプ","help",
        "会社概要","outline","会社案内","アバウト","about"
    ]
    defaults_paths = [
        "/recruit","/career","/careers","/jobs",
        "/ir","/investor","/shareholder",
        "/privacy","/policy","/terms","/agreement","/security",
        "/sitemap","/site-map","/search","/contact","/inquiry","/help","/faq","/guideline",
        "/company/outline","/about"
    ]

    cfg_filters = CFG.get("filters", {}) if isinstance(CFG, dict) else {}
    block_words = cfg_filters.get("block_words", defaults_words)
    block_paths = cfg_filters.get("block_path_patterns", defaults_paths)
    min_len     = int(cfg_filters.get("min_title_len", 6))

    if len((title or "").strip()) < min_len:
        return True
    if any(w.lower() in t or w.lower() in u for w in block_words):
        return True
    if any(p in u for p in block_paths):
        return True
    return False

def looks_like_article(url: str, base_url: str = "") -> bool:
    """
    記事（詳細）ページっぽいURLだけを通す軽量判定。
    - 例: /news/2025/..., /press/..., /solution/issue/..., /blog/..., /topics/...
    - カテゴリや一覧: /news/, /category/, /tag/, /top-category/, /topics/（末尾/のみ）等は除外
    """
    try:
        p = up.urlparse(url)
        path = (p.path or "/").rstrip("/")
        if path == "/":
            return False
        # 典型的な一覧パスは除外
        bad_segments = {"news","press","topics","blog","category","tag","tags","top-category","archive","archives","list"}
        segs = [s for s in path.split("/") if s]
        if len(segs) == 1 and segs[0] in bad_segments:
            return False
        if any(s in {"category","tag","tags","top-category","archive","archives","list","search"} for s in segs):
            return False
        # baseドメイン外は別で弾く（fetch_site_list内で実施）
        # “記事っぽい”ヒューリスティクス
        has_year = any(re.fullmatch(r"(19|20)\d{2}", s) for s in segs)
        has_slug = any(len(s) >= 6 and "-" in s for s in segs)
        has_numeric_id = any(re.fullmatch(r"\d{6,}", s) for s in segs)
        # よくある記事ルート
        good_prefixes = ["/news/","/press/","/solution/","/blog/","/topics/","/information/","/docs/"]
        if any(path.startswith(pref.rstrip("/")) for pref in good_prefixes):
            return True
        # 年・slug・数字IDのいずれかがあるなら記事可能性高
        return has_year or has_slug or has_numeric_id
    except Exception:
        return False

def html_escape(s: str) -> str:
    s = s or ""
    return (s.replace("&","&amp;")
             .replace("<","&lt;")
             .replace(">","&gt;")
             .replace('"',"&quot;"))

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


def sha(s): 
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def passes_filters(title, summary):
    text = f"{title} {summary}".lower()
    inc = [k.lower() for k in CFG.get("keywords",{}).get("include",[])]
    exc = [k.lower() for k in CFG.get("keywords",{}).get("exclude",[])]
    if inc and not any(k in text for k in inc): 
        return False
    if any(k in text for k in exc): 
        return False
    return True


def fetch_rss(name, url, tags):
    d = feedparser.parse(url)
    rows = []
    for e in d.entries:
        title = (e.get("title") or "").strip()
        link = normalize_url((e.get("link") or "").strip())

        # リンク切れ・ノイズ記事を除外
        if link and not is_alive(link):
            continue
        if is_blocked(link, title):
            continue

        summary_html = e.get("summary") or e.get("description") or ""
        summary = BeautifulSoup(summary_html, "html.parser").get_text(" ", strip=True)

        if not title and not link:
            continue
        if not passes_filters(title, summary):
            continue

        uid = sha(link or title or (name + summary))
        pub = e.get("published") or e.get("updated") or ""
        rows.append(dict(
            id=uid,
            source=name,
            title=title or "(no title)",
            url=link,
            published=norm_date(pub),
            summary=summary[:2000],
            tags=",".join(tags or []),
            raw=json.dumps({k: str(v)[:10000] for k, v in e.items()}, ensure_ascii=False)
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

    # 同一URLは最新publishedのみ（重複排除）
    cur.execute("""
        SELECT i.source, i.title, i.url, MAX(i.published) AS published, i.summary
        FROM items i
        GROUP BY i.url
        ORDER BY published DESC
    """)
    rows = cur.fetchall()
    con.close()

    # 最新1000件をスナップショット対象、ページは最新200件
    rows_recent = rows[:1000]
    rows_page = rows[:200]

    # index.md（空サマリーは出さない）
    lines = ["# AI / 生成AI クリッピング（最新200件）\n"]
    for source, title, url, published, summary in rows_page:
        date = (published or "")[:16].replace("T", " ")
        lines.append(f'- **{date}** · <a href="{url}" target="_blank"><strong>{title}</strong></a> — <em>{source}</em>')
        s = (summary or "").strip()
        if s:
            lines.append(f"  - {s[:160]}")
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
        w = csv.DictWriter(f, fieldnames=["source", "title", "url", "published", "summary"])
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


def _decode_best(content: bytes, candidates: list[str]) -> str:
    """複数エンコードでデコードし、日本語スコアが最良のものを返す"""
    best_text, best_score = "", -1
    jp_re = re.compile(r"[ぁ-んァ-ヶ一-龥々〆ヵヶ]")
    for enc in candidates:
        try:
            text = content.decode(enc, errors="replace")
            # 日本語文字の量と文字化け数（�）からスコア算出
            jp = len(jp_re.findall(text))
            bad = text.count("�")
            score = jp - bad * 5
            if score > best_score:
                best_score, best_text = score, text
        except Exception:
            continue
    return best_text or content.decode("utf-8", errors="replace")


def fetch_site_list(feed_cfg):
    """ニュース一覧ページを1枚だけ取得して、aタグの見出しをキーワードで拾う軽量クロール"""
    url = feed_cfg["url"]
    name = feed_cfg["name"]
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "ai-curator/1.0"})
        r.raise_for_status()
    except Exception as e:
        print(f"[WARN] fetch_site_list fail {name}: {e}")
        return []

    # --- エンコーディング補正付きでHTMLデコード ---
    hinted = (r.encoding or "").lower()
    cand = []
    if hinted and hinted not in ("iso-8859-1", "ascii"):
        cand.append(hinted)
    cand += ["utf-8", "cp932", "shift_jis", (r.apparent_encoding or "utf-8")]
    cand = [c for i, c in enumerate(cand) if c and c not in cand[:i]]  # 重複除去

    domain = up.urlparse(url).netloc
    if "mirait-one.com" in domain:
        # このサイトは実際はUTF-8。念のため明示デコード
        html = r.content.decode("utf-8", errors="replace")
    else:
        html = _decode_best(r.content, cand)

    soup = BeautifulSoup(html, "html.parser")

    def _norm_text(s: str) -> str:
        s = re.sub(r"\s+", " ", s or "").strip()
        return unicodedata.normalize("NFKC", s)

    rows = []
    page_seen_hrefs = set()  # 同一ページ内リンクの重複除去

    for a in soup.find_all("a")[:200]:
        t = _norm_text(a.get_text(" ", strip=True))
        href = a.get("href") or ""
        if not t or not href:
            continue
        if len(t) < 6 or href.startswith(("javascript:", "#")):
            continue

        # 絶対URL化
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = up.urljoin(url, href)

        # 正規化（?utm=…等を削って重複抑制）
        href = normalize_url(href)

        # 対象ドメイン外は除外（外部リンク排除）
        base_netloc = up.urlparse(url).netloc
        link_netloc = up.urlparse(href).netloc
        if link_netloc and link_netloc != base_netloc:
            continue

        # 採用/IR/ポリシー/サイトマップ/検索/問い合わせなどのノイズ除外
        if is_blocked(href, t):
            continue

        # 一覧ではなく“記事っぽい”URLだけに絞る
        if not looks_like_article(href, url):
            continue

        # 同一ページ内での重複除去
        if href in page_seen_hrefs:
            continue
        page_seen_hrefs.add(href)

        rows.append(dict(
            id=sha(href),
            source=name,
            title=t,
            url=href,
            published=dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S%z"),
            summary="",
            tags="company,site",
            raw=""
        ))
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
    for feed in (CFG.get("extra_rss") or []):
        name, url, tags = feed.get("name"), feed.get("url"), feed.get("tags",[])
        if not url: 
            continue
        print("[INFO] extra", name, url)
        rows = fetch_rss(name, url, tags)
        total_new += upsert(rows)
        time.sleep(1)

    # 会社サイトの一覧ページからキーワード拾い上げ
    for s in (CFG.get("site_feeds", []) or []):
        print("[INFO] site", s.get("name"), s.get("url"))
        rows = fetch_site_list(s)
        total_new += upsert(rows)

    tidy_and_export()
    print("new items:", total_new)


if __name__ == "__main__":
    main()
