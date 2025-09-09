
# AI Curator (Free Stack)

GitHub Actions + Python だけで、複数サイトの RSS を毎日収集・重複除去・アーカイブし、
- JSON/CSV スナップショット
- 自前 RSS (`data/feed.xml`)
- 読みやすい Markdown 一覧（`index.md`）

を生成します。**完全無料**（GitHub Public リポジトリ前提）。

> X（Twitter）は公式 API が有料のため、無料でやる場合は **Nitter の RSS**（安定性は低め）や **RSSHub 自前ホスト**をご検討ください。本テンプレは Nitter の RSS URL を任意で追加できるようにしています（`config.yml`）。

---

## 使い方（3分）

1. このフォルダを **GitHub の新規 Public リポジトリ**としてアップロード
2. GitHub で **Actions** を有効化
3. （任意）`config.yml` にソースを足す／キーワードを調整
4. 毎朝 JST 7:15 に自動実行。生成物は `data/` とルートの `index.md`・`data/feed.xml` に出力されます。  
   → **GitHub Pages** を有効化すれば、`index.md` がサイトとして閲覧できます。

---

## フォルダ構成

```
ai_curator_free/
├─ main.py               # 収集→正規化→保存→出力
├─ config.yml            # 収集元やキーワード設定
├─ requirements.txt
├─ index.md              # 直近の一覧（自動更新されます）
├─ .github/workflows/daily.yml
└─ data/                 # SQLiteとスナップショット/RSS（自動生成）
```

---

## X（Twitter）を無料で入れる方法（任意）

- **Nitter の RSS**: 例 `https://nitter.net/openai/rss` を `config.yml` の `extra_rss` に追加。  
  ※ 公開 Nitter は不安定です。使えなくなったら別ホストに差し替えてください。
- **RSSHub 自前ホスト**: `docker` で無料構築 → `https://<自分のRSSHub>/twitter/user/openai` などを `extra_rss` に追加。

---

## Notion/Slack 連携（任意）
- Notion: 別ワークフローで DB へ書き込み可能（無料プランでOK）
- Slack: Webhook に最新件のみ通知など（無料プランでも可）

---

## ライセンス
MIT
