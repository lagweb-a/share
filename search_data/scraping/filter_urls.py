# -*- coding: utf-8 -*-
"""
URLフィルタ（学割スポット/アクティビティの公式/販売/施設ページだけを残す）
- 入力:  title,url[,snippet] のCSV
- 出力:  入力と同形式のCSV（不適なURLは出力に含めない＝削除）
- モード:
  * use_llm=True  : Ollama (qwen2.5 など) で高精度フィルタ
  * use_llm=False : ルールベースの軽量フィルタ

CLI 例:
  python filter_urls.py ./csv/tickets.csv --out ./csv/tickets_filtered.csv --llm --model qwen2.5:7b-instruct-q5_1
"""

import csv
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

# ---------------- ユーティリティ ----------------

def ensure_parent(path_str: str) -> str:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)
    return path_str

# ---------------- ルールベース用 辞書 ----------------

# ドメインで弾く（SNS/ニュース/画像/まとめ等）
DOMAIN_BLOCKLIST = {
    "twitter.com", "x.com", "t.co", "facebook.com", "instagram.com", "pinterest.com",
    "note.com", "hatena.ne.jp", "hatenablog.com", "togetter.com",
    "news.yahoo.co.jp", "line.me", "lineblog.me",
    "tripadvisor.com", "ja.tripadvisor.com",
    "navitime.co.jp", "prtimes.jp", "atpress.ne.jp",
    "youtube.com", "youtu.be", "nicovideo.jp",
    "amazon.co.jp", "rakuten.co.jp", "yahoo.co.jp", "zozo.jp",
}

# ファイル拡張子で弾く（非HTML）
EXT_BLOCKLIST = (
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
    ".zip", ".rar", ".7z", ".gz", ".mp4", ".mov", ".wmv", ".avi",
    ".ppt", ".pptx", ".doc", ".docx", ".xls", ".xlsx", ".csv"
)

# 公式/販売/施設っぽい語（弱いホワイト）
POSITIVE_HINTS = [
    "公式", "公式サイト", "施設", "営業時間", "アクセス", "料金", "価格", "チケット", "入場",
    "ご利用案内", "利用案内", "予約", "購入", "券", "Price", "Prices", "Ticket", "Admission"
]

# まとめ/ニュースっぽい語（弱いブラック）
NEGATIVE_HINTS = [
    "まとめ", "一覧", "記事", "ニュース", "口コミ", "レビュー", "PR", "リリース", "配信"
]

# 学割キーワード（スコア補助）
DISCOUNT_HINTS = ["学割", "学生", "学生証", "U25", "U24", "Student", "student"]

def _looks_like_html(url: str) -> bool:
    u = (url or "").lower().split("?", 1)[0]
    return not u.endswith(EXT_BLOCKLIST)

def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

# ---------------- ルールベース判定 ----------------

def rule_based_keep(title: str, url: str, snippet: str) -> bool:
    """“公式/販売/施設の個別ページ”っぽいものだけ True。その他は False（削除）。"""
    if not url or not url.startswith(("http://", "https://")):
        return False
    if not _looks_like_html(url):
        return False

    netloc = _netloc(url)
    if any(netloc == d or netloc.endswith("." + d) for d in DOMAIN_BLOCKLIST):
        return False

    text = f"{title or ''} {snippet or ''}"

    # 学割や価格/チケット語が全然出ないなら弱く減点
    pos_hits = sum(1 for k in POSITIVE_HINTS if k.lower() in text.lower())
    disc_hits = sum(1 for k in DISCOUNT_HINTS if k.lower() in text.lower())
    neg_hits = sum(1 for k in NEGATIVE_HINTS if k.lower() in text.lower())

    # ニュース/まとめ色が強いなら落とす
    if neg_hits >= 2:
        return False

    # 施設らしさ or 学割らしさ がゼロなら落とす
    if pos_hits == 0 and disc_hits == 0:
        return False

    return True

# ---------------- LLM (Ollama) 判定 ----------------

LLM_ENDPOINT = "http://localhost:11434/api/generate"
#LLM_MODEL_DEFAULT = "qwen2.5:7b-instruct-q5_1"
LLM_MODEL_DEFAULT = "qwen2.5:14b-instruct-q4_K_M"

LLM_PROMPT_TMPL = """あなたは日本語の分類アシスタントです。以下のアイテムごとに「学割スポット/アクティビティの
公式・販売・施設情報ページか」を判定してください。返すのは厳密なJSONのみです。

現在学割スポットやチケットの情報をまとめたサイトを作るために情報収集しています。

返却フォーマット:
{{
  "results": [{{"idx": 0, "keep": "YES"|"NO"}} , ...]
}}

判定基準（YESにする基準）:
- 施設の公式サイトや、チケット/料金/入場案内/利用案内/アクセス/予約/購入ページ
- 旅行会社や販売サイトの「商品詳細/チケット詳細」ページ
NOにする基準:
- 「〜まとめ」「〜⚪︎選」「〜特集」などの施設・サービスの公式サイトではない第三者によるまとめ記事
- ニュース・SNS・画像投稿・口コミ・通販比較・会社プレス・無関係な一般記事
- 明らかに学割と関係ないサイトやまとめサイトを除外し、怪しいと思ったものは除外せず残してください。
- 同一施設・サービスが重複しないよう、URLが多少違くても最終的に重複しているものは一つに絞ってください

アイテム:
{items}
"""

def llm_filter_batch(items, model=LLM_MODEL_DEFAULT, endpoint=LLM_ENDPOINT,
                     temperature=0.0, timeout=60, debug=False):
    """
    items: [{"idx": i, "title":..., "url":..., "snippet":...}, ...]
    return: dict idx -> True/False
    """
    import json
    prompt = LLM_PROMPT_TMPL.format(items=json.dumps(items, ensure_ascii=False, indent=2))
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature}
    }
    try:
        r = requests.post(endpoint, json=payload, timeout=timeout)
        r.raise_for_status()
        raw = (r.json() or {}).get("response", "").strip()
        if debug:
            print("[LLM raw]", raw[:300].replace("\n", " ") + ("..." if len(raw) > 300 else ""))
        m = re.search(r"\{.*\}", raw, flags=re.S)
        s = m.group(0) if m else raw
        obj = json.loads(s)
        out = {}
        for res in obj.get("results", []):
            out[int(res.get("idx", -1))] = str(res.get("keep", "NO")).upper() == "YES"
        return out
    except Exception as e:
        if debug:
            print("[LLM error]", e)
        # LLM失敗時は通過させすぎないよう“ルールベース”にフォールバックで True/False 決め直し
        return {it["idx"]: rule_based_keep(it.get("title",""), it.get("url",""), it.get("snippet","")) for it in items}

# ---------------- メイン関数 ----------------

def filter_urls(input_csv: str, output_csv: str,
                use_llm: bool = False,
                model: str = LLM_MODEL_DEFAULT,
                batch_size: int = 12,
                timeout: int = 60,
                debug: bool = False) -> str:
    """
    input_csv  : Step1のCSV(title,url[,snippet])
    output_csv : フィルタ後のCSV（不適URLは“削除済み”）
    """
    ensure_parent(output_csv)

    rows = []
    with open(input_csv, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        fns = r.fieldnames or []
        has_snippet = "snippet" in set(fns)
        seen = set()
        for row in r:
            title = (row.get("title") or "").strip()
            url   = (row.get("url") or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            snippet = (row.get("snippet") or "").strip() if has_snippet else ""
            rows.append({"title": title, "url": url, "snippet": snippet})

    keep = [False] * len(rows)
    if use_llm:
        # LLMバッチ
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            items = [{"idx": i+j, **batch[j]} for j in range(len(batch))]
            result = llm_filter_batch(items, model=model, timeout=timeout, debug=debug)
            for j in range(len(batch)):
                keep[i+j] = bool(result.get(i+j, False))
            time.sleep(0.2)
    else:
        # ルールベース
        for i, r in enumerate(rows):
            keep[i] = rule_based_keep(r["title"], r["url"], r["snippet"])

    # 出力（残す＝書く／落とす＝書かない）
    fieldnames = ["title", "url"] + (["snippet"] if any(r.get("snippet") for r in rows) else [])
    with open(output_csv, "w", newline="", encoding="utf-8") as fo:
        w = csv.DictWriter(fo, fieldnames=fieldnames)
        w.writeheader()
        kept = 0
        for r, k in zip(rows, keep):
            if k:
                o = {"title": r["title"], "url": r["url"]}
                if "snippet" in fieldnames:
                    o["snippet"] = r["snippet"]
                w.writerow(o)
                kept += 1

    print(f"✅ Filtered: {kept} / {len(rows)} → {output_csv} (mode={'LLM' if use_llm else 'rule'})")
    return output_csv

# ---------------- CLI ----------------

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Filter URLs for student-discount spots (keep only official/sales/venue pages).")
    p.add_argument("input_csv")
    p.add_argument("--out", default="./csv/filtered.csv")
    p.add_argument("--llm", action="store_true", help="use Ollama LLM for filtering")
    p.add_argument("--model", default=LLM_MODEL_DEFAULT)
    p.add_argument("--batch", type=int, default=12)
    p.add_argument("--timeout", type=int, default=150)
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    filter_urls(args.input_csv, args.out, use_llm=args.llm, model=args.model,
                batch_size=args.batch, timeout=args.timeout, debug=args.debug)
